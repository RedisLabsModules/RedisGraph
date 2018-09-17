/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Apache License, Version 2.0,
 * modified with the Commons Clause restriction.
 */

#include "index.h"
#include "index_type.h"

RedisModuleKey* Index_LookupKey(RedisModuleCtx *ctx, const char *graph, size_t index_id, bool write_access) {
  char *strKey;
  int keylen = asprintf(&strKey, "%s_%s_%lu", INDEX_PREFIX, graph, index_id);

  RedisModuleString *rmIndexId = RedisModule_CreateString(ctx, strKey, keylen);
  free(strKey);

  int mode = write_access ? REDISMODULE_WRITE : REDISMODULE_READ;
  RedisModuleKey *key = RedisModule_OpenKey(ctx, rmIndexId, mode);
  RedisModule_FreeString(ctx, rmIndexId);

  return key;
}

/* Memory management and comparator functions that get attached to
 * string and numeric skiplists as function pointers. */
int compareNodes(NodeID a, NodeID b) {
  return a - b;
}

int compareStrings(SIValue *a, SIValue *b) {
  return strcmp(a->stringval, b->stringval);
}

int compareNumerics(SIValue *a, SIValue *b) {
  double diff = a->doubleval - b->doubleval;
  return COMPARE_RETVAL(diff);
}

/* The index must maintain its own copy of the indexed SIValue
 * so that it becomes outdated but not broken by updates to the property. */
SIValue* cloneKey(SIValue *property) {
  SIValue *clone = malloc(sizeof(SIValue));
  *clone = SI_Clone(*property);
  return clone;
}

void freeKey(SIValue *key) {
  SIValue_Free(key);
  free(key);
}

/* Construct key and retrieve index from Redis keyspace */
Index* Index_Get(RedisModuleCtx *ctx, const char *graph, const char *label, char *property) {
  Index *idx = NULL;

  LabelStore *store = LabelStore_Get(ctx, STORE_NODE, graph, label);
  if (!store) return NULL;

  size_t *index_id = TrieMap_Find(store->properties, property, strlen(property));

  // index_id will be NULL if the label-property pair is not indexed
  if (!index_id || index_id == TRIEMAP_NOTFOUND) return NULL;

  // Open key with read-only access
  RedisModuleKey *key = Index_LookupKey(ctx, graph, *index_id, false);

  if (RedisModule_ModuleTypeGetType(key) == IndexRedisModuleType) {
    idx = RedisModule_ModuleTypeGetValue(key);
  }

  RedisModule_CloseKey(key);

  return idx;
}

int Index_Delete(RedisModuleCtx *ctx, const char *graphName, Graph *g, const char *label, char *prop) {
  LabelStore *store = LabelStore_Get(ctx, STORE_NODE, graphName, label);
  assert(store);

  size_t *index_id = TrieMap_Find(store->properties, prop, strlen(prop));

  if (index_id == TRIEMAP_NOTFOUND) {
    assert(0);
  } else if (index_id == NULL) {
    return INDEX_FAIL;
  }

  size_t dest_id = *index_id;

  // Open key with write access
  RedisModuleKey *key = Index_LookupKey(ctx, graphName, *index_id, true);

  if (RedisModule_ModuleTypeGetType(key) != IndexRedisModuleType) {
    // Reply with error if this key does not exist or does not correspond to an index object
    RedisModule_CloseKey(key);
    return INDEX_FAIL;
  }

  // Decrement the index counter
  g->index_ctr --;

  // NULL out the dropped index in its label schema
  LabelStore_AssignValue(store, prop, NULL);

  size_t src_id = g->index_ctr;
  // No need to migrate if we're replacing the last ID
  if (dest_id == src_id) {
    RedisModule_DeleteKey(key);
    RedisModule_CloseKey(key);
    return INDEX_OK;
  }

  // retrieve last index
  RedisModuleKey *src_key = Index_LookupKey(ctx, graphName, src_id, true);
  Index *src = RedisModule_ModuleTypeGetValue(src_key);
  src->id = dest_id;
  // Update the label schema
  // TODO multi-label will make this inadequate
  LabelStore *src_store = LabelStore_Get(ctx, STORE_NODE, graphName, src->label);
  // Update the ID of the retained index in its label schema
  size_t *updated_id = malloc(sizeof(size_t));
  *updated_id = dest_id;
  LabelStore_AssignValue(src_store, src->property, updated_id);

  // Don't allow the migrated index to be freed on keyspace update
  src->keepalive ++;
  // Update the Redis keyspace
  RedisModule_ModuleTypeSetValue(key, IndexRedisModuleType, src);
  RedisModule_DeleteKey(src_key);
  RedisModule_CloseKey(src_key);

  RedisModule_CloseKey(key);

  return INDEX_OK;
}

void initializeSkiplists(Index *index) {
  index->string_sl = skiplistCreate(compareStrings, compareNodes, cloneKey, freeKey);
  index->numeric_sl = skiplistCreate(compareNumerics, compareNodes, cloneKey, freeKey);
}

/* buildIndex allocates an Index object and populates it with a label-property pair
 * by traversing a label matrix with a TuplesIter. */
Index* buildIndex(Graph *g, const GrB_Matrix label_matrix, const char *label, const char *prop_str) {
  Index *index = malloc(sizeof(Index));

  index->label = strdup(label);
  index->property = strdup(prop_str);
  index->keepalive = 0;

  initializeSkiplists(index);

  TuplesIter *it = TuplesIter_new(label_matrix);

  Node *node;
  EntityProperty *prop;

  skiplist *sl;
  NodeID node_id;
  int found;
  int prop_index = 0;
  while(TuplesIter_next(it, NULL, &node_id) != TuplesIter_DEPLETED) {
    node = Graph_GetNode(g, node_id);
    // If the sought property is at a different offset than it occupied in the previous node,
    // then seek and update
    if (strcmp(prop_str, node->properties[prop_index].name)) {
      found = 0;
      for (int i = 0; i < node->prop_count; i ++) {
        prop = node->properties + i;
        if (!strcmp(prop_str, prop->name)) {
          prop_index = i;
          found = 1;
          break;
        }
      }
    } else {
      found = 1;
    }
    // The targeted property does not exist on this node
    if (!found) continue;

    prop = node->properties + prop_index;
    // This value will be cloned within the skiplistInsert routine if necessary
    SIValue *key = &prop->value;

    assert(key->type == T_STRING || key->type & SI_NUMERIC);
    sl = (key->type & SI_NUMERIC) ? index->numeric_sl: index->string_sl;
    skiplistInsert(sl, key, node_id);
  }

  TuplesIter_free(it);

  return index;
}

// Create and populate index for specified property
// (This function will create separate string and numeric indices if property has mixed types)
int Index_Create(RedisModuleCtx *ctx, const char *graphName, Graph *g, const char *label, char *prop_str) {
  LabelStore *store = LabelStore_Get(ctx, STORE_NODE, graphName, label);
  assert(store);

  size_t *index_id = TrieMap_Find(store->properties, prop_str, strlen(prop_str));
  if (index_id) return INDEX_FAIL; // This property is already indexed.

  // Create a unique index ID and attach it as a value to the property in the label schema.
  // TODO This will only update the passed label; multi-label support will require
  // iterating over labels and updating all schemas.
  index_id = malloc(sizeof(size_t));
  *index_id = Graph_AddIndexID(g);
  LabelStore_AssignValue(store, prop_str, index_id);

  // Open key with write access
  RedisModuleKey *key = Index_LookupKey(ctx, graphName, *index_id, true);
  // Do nothing if this key already exists
  if (RedisModule_ModuleTypeGetType(key) != REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_CloseKey(key);
    return INDEX_FAIL;
  }

  const GrB_Matrix label_matrix = Graph_GetLabel(g, store->id);

  Index *idx = buildIndex(g, label_matrix, label, prop_str);
  idx->id = *index_id;

  RedisModule_ModuleTypeSetValue(key, IndexRedisModuleType, idx);
  RedisModule_CloseKey(key);

  return INDEX_OK;
}

/* Output text for EXPLAIN calls */
const char* Index_OpPrint(AST_IndexNode *indexNode) {
  switch(indexNode->operation) {
    case CREATE_INDEX:
      return "Create Index";
    default:
      return "Drop Index";
  }
}

void Index_Free(Index *idx) {
  if (idx->keepalive-- > 0) return;
  skiplistFree(idx->string_sl);
  skiplistFree(idx->numeric_sl);
  free(idx->label);
  free(idx->property);
  free(idx);
}

