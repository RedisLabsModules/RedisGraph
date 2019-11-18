/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "annotate_entities.h"
#include "../../util/rmalloc.h"


//------------------------------------------------------------------------------
//  Annotation context - graph entity naming
//------------------------------------------------------------------------------

// Compute the number of digits in a non-negative integer.
static inline int _digit_count(int n) {
	int count = 0;
	do {
		n /= 10;
		count++;
	} while(n);
	return count;
}

static inline char *_create_anon_alias(int anon_count) {
	// We need space for "anon_" (5), all digits, and a NULL terminator (1)
	int alias_len = _digit_count(anon_count) + 6;
	char *alias = rm_malloc(alias_len * sizeof(char));
	snprintf(alias, alias_len, "anon_%d", anon_count);
	return alias;
}

// TODO this function is exposed since ORDER BY expressions are named later - possibly can be refactored.
void AST_AttachName(AST *ast, const cypher_astnode_t *node, const char *name) {
	// Annotate AST entity with identifier string.
	AnnotationCtx *name_ctx = AST_AnnotationCtxCollection_GetNameCtx(ast->anot_ctx_collection);
	cypher_astnode_attach_annotation(name_ctx, node, (void *)name, NULL);
}

static void _annotate_entity_names(AST *ast, const cypher_astnode_t *node, uint *anon_count) {
	cypher_astnode_type_t t = cypher_astnode_type(node);
	const cypher_astnode_t *ast_identifier = NULL;
	// TODO write a toString function for CYPHER_AST_SORT_ITEM elements to populate them here
	// (currently added in ExecutionPlan construction)
	if(t == CYPHER_AST_NODE_PATTERN) {
		ast_identifier = cypher_ast_node_pattern_get_identifier(node);
	} else if(t == CYPHER_AST_REL_PATTERN) {
		ast_identifier = cypher_ast_rel_pattern_get_identifier(node);
	} else {
		// Did not encounter a graph entity, recursively visit children.
		uint child_count = cypher_astnode_nchildren(node);
		for(uint i = 0; i < child_count; i++) {
			const cypher_astnode_t *child = cypher_astnode_get_child(node, i);
			_annotate_entity_names(ast, child, anon_count);
		}
		return; // Return to avoid annotating other AST nodes.
	}

	// The AST node is a graph entity.
	char *alias;
	if(ast_identifier) {
		// Graph entity has a user-defined alias, clone it for the annotation.
		alias = rm_strdup(cypher_ast_identifier_get_name(ast_identifier));
	} else {
		// Graph entity is an unaliased, create an anonymous identifier.
		alias = _create_anon_alias((*anon_count)++);
	}

	// Add AST annotation.
	AST_AttachName(ast, node, alias);
}

// AST annotation callback routine for freeing generated entity names.
static void _FreeNameAnnotationCallback(void *unused, const cypher_astnode_t *node,
										void *annotation) {
	rm_free(annotation);
}

// Construct a new annotation context for holding AST entity names (aliases or anonymous identifiers).
static AnnotationCtx *_AST_NewNameContext(void) {
	AnnotationCtx *name_ctx = cypher_ast_annotation_context();
	cypher_ast_annotation_context_release_handler_t handler = &_FreeNameAnnotationCallback;
	cypher_ast_annotation_context_set_release_handler(name_ctx, handler, NULL);
	return name_ctx;
}

void AST_AnnotateEntities(AST *ast) {
	// Instantiate an annotation context for accessing AST entity names.
	AST_AnnotationCtxCollection_SetNameCtx(ast->anot_ctx_collection, _AST_NewNameContext());
	uint anon_count = 0;
	// Generate all name annotations.
	_annotate_entity_names(ast, ast->root, &anon_count);
}
