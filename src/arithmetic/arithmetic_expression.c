/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "./arithmetic_expression.h"

#include "../RG.h"
#include "funcs.h"
#include "rax.h"
#include "../errors.h"
#include "./aggregate.h"
#include "../util/arr.h"
#include "./repository.h"
#include "../query_ctx.h"
#include "../util/strcmp.h"
#include "../graph/graph.h"
#include "../util/rmalloc.h"
#include "../graph/graphcontext.h"
#include "../datatypes/temporal_value.h"
#include "../datatypes/array.h"
#include "../ast/ast_shared.h"

#include <ctype.h>
#include <assert.h>

// Forward declaration
static AR_EXP_Result _AR_EXP_Evaluate(AR_ExpNode *root, const Record r, SIValue *result);
// Clear an op node internals, without free the node allocation itself.
static void _AR_EXP_FreeOpInternals(AR_ExpNode *op_node);

static AR_ExpNode *_AR_EXP_CloneOperand(AR_ExpNode *exp) {
	AR_ExpNode *clone = rm_calloc(1, sizeof(AR_ExpNode));
	clone->type = AR_EXP_OPERAND;
	switch(exp->operand.type) {
	case AR_EXP_CONSTANT:
		clone->operand.type = AR_EXP_CONSTANT;
		clone->operand.constant = SI_CloneValue(exp->operand.constant);
		break;
	case AR_EXP_VARIADIC:
		clone->operand.type = exp->operand.type;
		clone->operand.variadic.entity_alias = exp->operand.variadic.entity_alias;
		clone->operand.variadic.entity_alias_idx = exp->operand.variadic.entity_alias_idx;
		break;
	case AR_EXP_PARAM:
		clone->operand.type = AR_EXP_PARAM;
		clone->operand.param_name = exp->operand.param_name;
		break;
	case AR_EXP_BORROW_RECORD:
		clone->operand.type = AR_EXP_BORROW_RECORD;
		break;
	default:
		assert(false);
		break;
	}
	return clone;
}

static AR_ExpNode *_AR_EXP_NewOpNode(const char *func_name, uint child_count) {
	ASSERT(func_name != NULL);

	AR_ExpNode *node = rm_calloc(1, sizeof(AR_ExpNode));
	node->type = AR_EXP_OP;
	node->op.func_name = func_name;
	node->op.child_count = child_count;
	node->op.children = rm_malloc(child_count * sizeof(AR_ExpNode *));
	return node;
}

static AR_ExpNode *_AR_EXP_CloneOp(AR_ExpNode *exp) {
	AR_ExpNode *clone = _AR_EXP_NewOpNode(exp->op.func_name, exp->op.child_count);
	if(exp->op.type == AR_OP_FUNC) {
		clone->op.type = AR_OP_FUNC;
		/* If the function has private data, the function descriptor
		 * itself should be cloned. Otherwise, we can perform a direct assignment. */
		if(exp->op.f->privdata) clone->op.f = AR_CloneFuncDesc(exp->op.f);
		else clone->op.f = exp->op.f;
	} else {
		clone->op.agg_func = Agg_CloneCtx(exp->op.agg_func);
		clone->op.type = AR_OP_AGGREGATE;
	}
	for(uint i = 0; i < exp->op.child_count; i++) {
		AR_ExpNode *child = AR_EXP_Clone(exp->op.children[i]);
		clone->op.children[i] = child;
	}
	return clone;
}

AR_ExpNode *AR_EXP_NewOpNode(const char *func_name, uint child_count) {

	AR_ExpNode *node = _AR_EXP_NewOpNode(func_name, child_count);

	/* Determine function type. */
	AR_FuncDesc *func = AR_GetFunc(func_name);
	if(func != NULL) {
		node->op.f = func;
		node->op.type = AR_OP_FUNC;
	} else {
		/* Either this is an aggregation function
		 * or the requested function does not exists. */
		AggCtx *agg_func;
		Agg_GetFunc(func_name, false, &agg_func);

		/* TODO: handle Unknown function. */
		ASSERT(agg_func != NULL);
		node->op.agg_func = agg_func;
		node->op.type = AR_OP_AGGREGATE;
	}

	return node;
}

AR_ExpNode *AR_EXP_NewDistinctOpNode(const char *func_name, uint child_count) {
	AR_ExpNode *node = _AR_EXP_NewOpNode(func_name, child_count);

	AggCtx *agg_func;
	Agg_GetFunc(func_name, true, &agg_func);

	/* TODO: handle Unknown function. */
	ASSERT(agg_func != NULL);
	node->op.agg_func = agg_func;
	node->op.type = AR_OP_AGGREGATE;

	return node;
}

bool AR_EXP_PerformDistinct(AR_ExpNode *op) {
	return op->type == AR_EXP_OP && op->op.type == AR_OP_AGGREGATE && op->op.agg_func->isDistinct;
}

static inline AR_ExpNode *_AR_EXP_InitializeOperand(AR_OperandNodeType type) {
	AR_ExpNode *node = rm_calloc(1, sizeof(AR_ExpNode));
	node->type = AR_EXP_OPERAND;
	node->operand.type = type;
	return node;
}

AR_ExpNode *AR_EXP_NewVariableOperandNode(const char *alias) {
	AR_ExpNode *node = _AR_EXP_InitializeOperand(AR_EXP_VARIADIC);
	node->operand.variadic.entity_alias = alias;
	node->operand.variadic.entity_alias_idx = IDENTIFIER_NOT_FOUND;

	return node;
}

AR_ExpNode *AR_EXP_NewAttributeAccessNode(AR_ExpNode *entity,
										  const char *attr) {

	ASSERT(attr != NULL);
	ASSERT(entity != NULL);

	// use property index when possible, prop_idx is set to ATTRIBUTE_NOTFOUND
	// if the graph is not aware of it in which case we'll try to resolve
	// the property using its string representation

	GraphContext *gc = QueryCtx_GetGraphCtx();
	SIValue prop_idx = SI_LongVal(ATTRIBUTE_NOTFOUND);
	SIValue prop_name = SI_ConstStringVal((char *)attr);
	Attribute_ID idx = GraphContext_GetAttributeID(gc, attr);

	if(idx != ATTRIBUTE_NOTFOUND) prop_idx = SI_LongVal(idx);

	// entity is an expression which should be evaluated to a graph entity
	// attr is the name of the attribute we want to extract from entity
	AR_ExpNode *root = AR_EXP_NewOpNode("property", 3);
	root->op.children[0] = entity;
	root->op.children[1] = AR_EXP_NewConstOperandNode(prop_name);
	root->op.children[2] = AR_EXP_NewConstOperandNode(prop_idx);

	return root;
}

AR_ExpNode *AR_EXP_NewConstOperandNode(SIValue constant) {
	AR_ExpNode *node = _AR_EXP_InitializeOperand(AR_EXP_CONSTANT);
	node->operand.constant = constant;
	return node;
}

AR_ExpNode *AR_EXP_NewParameterOperandNode(const char *param_name) {
	AR_ExpNode *node = _AR_EXP_InitializeOperand(AR_EXP_PARAM);
	node->operand.param_name = param_name;
	return node;
}

AR_ExpNode *AR_EXP_NewRecordNode() {
	return _AR_EXP_InitializeOperand(AR_EXP_BORROW_RECORD);
}

/* Compact tree by evaluating constant expressions
 * e.g. MINUS(X) where X is a constant number will be reduced to
 * a single node with the value -X
 * PLUS(MINUS(A), B) will be reduced to a single constant: B-A. */
bool AR_EXP_ReduceToScalar(AR_ExpNode *root, bool reduce_params, SIValue *val) {
	if(val != NULL) *val = SI_NullVal();
	if(root->type == AR_EXP_OPERAND) {
		// In runtime, parameters are set so they can be evaluated
		if(reduce_params && AR_EXP_IsParameter(root)) {
			SIValue v = AR_EXP_Evaluate(root, NULL);
			if(val != NULL) *val = v;
			return true;
		}
		if(AR_EXP_IsConstant(root)) {
			// Root is already a constant
			if(val != NULL) *val = root->operand.constant;
			return true;
		}
		// Root is variadic, no way to reduce.
		return false;
	} else {
		// root represents an operation.
		ASSERT(root->type == AR_EXP_OP);

		if(root->op.type == AR_OP_FUNC) {
			/* See if we're able to reduce each child of root
			 * if so we'll be able to reduce root. */
			bool reduce_children = true;
			for(int i = 0; i < root->op.child_count; i++) {
				if(!AR_EXP_ReduceToScalar(root->op.children[i], reduce_params, NULL)) {
					// Root reduce is not possible, but continue to reduce every reducable child.
					reduce_children = false;
				}
			}
			// Can't reduce root as one of its children is not a constant.
			if(!reduce_children) return false;

			// All child nodes are constants, make sure function is marked as reducible.
			AR_FuncDesc *func_desc = AR_GetFunc(root->op.func_name);
			ASSERT(func_desc != NULL);
			if(!func_desc->reducible) return false;

			// Evaluate function.
			SIValue v = AR_EXP_Evaluate(root, NULL);
			if(val != NULL) *val = v;
			if(SIValue_IsNull(v)) return false;

			// Reduce.
			// Clear children and function context.
			_AR_EXP_FreeOpInternals(root);
			// In-place update, set as constant.
			root->type = AR_EXP_OPERAND;
			root->operand.type = AR_EXP_CONSTANT;
			root->operand.constant = v;
			return true;
		}
		// Root is an aggregation function, can't reduce.
		return false;
	}
}

static bool _AR_EXP_ValidateInvocation(AR_FuncDesc *fdesc, SIValue *argv, uint argc) {
	SIType actual_type;
	SIType expected_type = T_NULL;

	// Make sure number of arguments is as expected.
	if(fdesc->min_argc > argc) {
		// Set the query-level error.
		ErrorCtx_SetError("Received %d arguments to function '%s', expected at least %d", argc, fdesc->name,
						  fdesc->min_argc);
		return false;
	}

	if(fdesc->max_argc < argc) {
		// Set the query-level error.
		ErrorCtx_SetError("Received %d arguments to function '%s', expected at most %d", argc, fdesc->name,
						  fdesc->max_argc);
		return false;
	}

	uint expected_types_count = array_len(fdesc->types);
	for(int i = 0; i < argc; i++) {
		actual_type = SI_TYPE(argv[i]);
		/* For a function that accepts a variable number of arguments.
		* the last specified type in fdesc->types is repeatable. */
		if(i < expected_types_count) {
			expected_type = fdesc->types[i];
		}
		if(!(actual_type & expected_type)) {
			/* TODO extend string-building logic to better express multiple acceptable types, like:
			 * RETURN 'a' * 2
			 * "Type mismatch: expected Float, Integer or Duration but was String" */
			Error_SITypeMismatch(argv[i], expected_type);
			return false;
		}
	}


	return true;
}

/* Evaluating an expression tree constructs an array of SIValues.
 * Free all of these values, in case an intermediate node on the tree caused a heap allocation.
 * For example, in the expression:
 * a.first_name + toUpper(a.last_name)
 * the result of toUpper() is allocated within this tree, and will leak if not freed here. */
static inline void _AR_EXP_FreeResultsArray(SIValue *results, int count) {
	for(int i = 0; i < count; i ++) {
		SIValue_Free(results[i]);
	}
}

static AR_EXP_Result _AR_EXP_EvaluateFunctionCall(AR_ExpNode *node, const Record r,
												  SIValue *result) {
	AR_EXP_Result res = EVAL_OK;
	// Handle aggregate function.
	if(node->op.type == AR_OP_AGGREGATE) {
		// The AggCtx will ultimately free its result.
		*result = SI_ShareValue(node->op.agg_func->result);
		return EVAL_OK;
	}

	int child_count = node->op.child_count;
	// Functions with private data will have it appended as an additional child.
	bool include_privdata = (node->op.f->privdata != NULL);
	if(include_privdata) child_count ++;
	/* Evaluate each child before evaluating current node. */
	SIValue sub_trees[child_count];

	for(int child_idx = 0; child_idx < node->op.child_count; child_idx++) {
		SIValue v;
		AR_EXP_Result eval_result = _AR_EXP_Evaluate(node->op.children[child_idx], r, &v);
		if(eval_result == EVAL_ERR) {
			/* Encountered an error while evaluating a subtree.
			 * Free all values generated up to this point. */
			_AR_EXP_FreeResultsArray(sub_trees, child_idx);
			// Propagate the error upwards.
			return eval_result;
		}
		if(eval_result == EVAL_FOUND_PARAM) res = EVAL_FOUND_PARAM;
		sub_trees[child_idx] = v;
	}

	// Add the function's private data, if any.
	if(include_privdata) sub_trees[child_count - 1] = SI_PtrVal(node->op.f->privdata);

	/* Validate before evaluation. */
	if(!_AR_EXP_ValidateInvocation(node->op.f, sub_trees, child_count)) {
		// The expression tree failed its validations and set an error message.
		res = EVAL_ERR;
		goto cleanup;
	}

	/* Evaluate self. */
	*result = node->op.f->func(sub_trees, child_count);

	if(SIValue_IsNull(*result) && ErrorCtx_EncounteredError()) {
		/* An error was encountered while evaluating this function, and has already been set in
		 * the QueryCtx. Exit with an error. */
		res = EVAL_ERR;
	}

cleanup:
	_AR_EXP_FreeResultsArray(sub_trees, node->op.child_count);
	return res;
}

static bool _AR_EXP_UpdateEntityIdx(AR_OperandNode *node, const Record r) {
	if(!r) {
// Set the query-level error.
		ErrorCtx_SetError("_AR_EXP_UpdateEntityIdx: No record was given to locate a value with alias %s",
						  node->variadic.entity_alias);
		return false;
	}
	int entry_alias_idx = Record_GetEntryIdx(r, node->variadic.entity_alias);
	if(entry_alias_idx == INVALID_INDEX) {
		// Set the query-level error.
		ErrorCtx_SetError("_AR_EXP_UpdateEntityIdx: Unable to locate a value with alias %s within the record",
						  node->variadic.entity_alias);
		return false;
	} else {
		node->variadic.entity_alias_idx = entry_alias_idx;
		return true;
	}
}

static AR_EXP_Result _AR_EXP_EvaluateVariadic(AR_ExpNode *node, const Record r, SIValue *result) {
	// Make sure entity record index is known.
	if(node->operand.variadic.entity_alias_idx == IDENTIFIER_NOT_FOUND) {
		if(!_AR_EXP_UpdateEntityIdx(&node->operand, r)) return EVAL_ERR;
	}

	int aliasIdx = node->operand.variadic.entity_alias_idx;

	// the value was not created here; share with the caller
	*result = SI_ShareValue(Record_Get(r, aliasIdx));

	return EVAL_OK;
}

static AR_EXP_Result _AR_EXP_EvaluateParam(AR_ExpNode *node, SIValue *result) {
	rax *params = QueryCtx_GetParams();
	AR_ExpNode *param_node = raxFind(params, (unsigned char *)node->operand.param_name,
									 strlen(node->operand.param_name));
	if(param_node == raxNotFound) {
		// Set the query-level error.
		ErrorCtx_SetError("Missing parameters");
		return EVAL_ERR;
	}
	// In place replacement;
	node->operand.type = AR_EXP_CONSTANT;
	node->operand.constant = SI_ShareValue(param_node->operand.constant);
	*result = node->operand.constant;
	return EVAL_FOUND_PARAM;
}

static inline AR_EXP_Result _AR_EXP_EvaluateBorrowRecord(AR_ExpNode *node, const Record r,
														 SIValue *result) {
	// Wrap the current Record in an SI pointer.
	*result = SI_PtrVal(r);
	return EVAL_OK;
}

/* Evaluate an expression tree, placing the calculated value in 'result' and returning
 * whether an error occurred during evaluation. */
static AR_EXP_Result _AR_EXP_Evaluate(AR_ExpNode *root, const Record r, SIValue *result) {
	AR_EXP_Result res = EVAL_OK;
	switch(root->type) {
	case AR_EXP_OP:
		return _AR_EXP_EvaluateFunctionCall(root, r, result);
	case AR_EXP_OPERAND:
		switch(root->operand.type) {
		case AR_EXP_CONSTANT:
			// The value is constant or has been computed elsewhere, and is shared with the caller.
			*result = SI_ShareValue(root->operand.constant);
			return res;
		case AR_EXP_VARIADIC:
			return _AR_EXP_EvaluateVariadic(root, r, result);
		case AR_EXP_PARAM:
			return _AR_EXP_EvaluateParam(root, result);
		case AR_EXP_BORROW_RECORD:
			return _AR_EXP_EvaluateBorrowRecord(root, r, result);
		default:
			assert(false && "Invalid expression type");
		}
	default:
		assert(false && "Unknown expression type");
	}

	return res;
}

SIValue AR_EXP_Evaluate(AR_ExpNode *root, const Record r) {
	SIValue result;
	AR_EXP_Result res = _AR_EXP_Evaluate(root, r, &result);
	if(res == EVAL_ERR) {
		ErrorCtx_RaiseRuntimeException();  // Raise an exception if we're in a run-time context.
		return SI_NullVal(); // Otherwise return NULL; the query-level error will be emitted after cleanup.
	}
	// At least one param node was encountered during evaluation, tree should be param node free.
	// Try reducing the tree.
	if(res == EVAL_FOUND_PARAM) AR_EXP_ReduceToScalar(root, true, NULL);
	return result;
}

void AR_EXP_Aggregate(const AR_ExpNode *root, const Record r) {
	if(root->type == AR_EXP_OP) {
		if(root->op.type == AR_OP_AGGREGATE) {
			/* Process child nodes before aggregating. */
			SIValue sub_trees[root->op.child_count];
			int i = 0;
			for(; i < root->op.child_count; i++) {
				AR_ExpNode *child = root->op.children[i];
				sub_trees[i] = AR_EXP_Evaluate(child, r);
			}

			/* Aggregate. */
			AggCtx *agg = root->op.agg_func;
			agg->Step(agg, sub_trees, root->op.child_count);
			_AR_EXP_FreeResultsArray(sub_trees, root->op.child_count);
		} else {
			/* Keep searching for aggregation nodes. */
			for(int i = 0; i < root->op.child_count; i++) {
				AR_ExpNode *child = root->op.children[i];
				AR_EXP_Aggregate(child, r);
			}
		}
	}
}

void AR_EXP_Reduce(const AR_ExpNode *root) {
	if(root->type == AR_EXP_OP) {
		if(root->op.type == AR_OP_AGGREGATE) {
			/* Reduce. */
			AggCtx *agg = root->op.agg_func;
			Agg_Finalize(agg);
		} else {
			/* Keep searching for aggregation nodes. */
			for(int i = 0; i < root->op.child_count; i++) {
				AR_ExpNode *child = root->op.children[i];
				AR_EXP_Reduce(child);
			}
		}
	}
}

void AR_EXP_CollectEntities(AR_ExpNode *root, rax *aliases) {
	if(root->type == AR_EXP_OP) {
		for(int i = 0; i < root->op.child_count; i ++) {
			AR_EXP_CollectEntities(root->op.children[i], aliases);
		}
	} else { // type == AR_EXP_OPERAND
		if(root->operand.type == AR_EXP_VARIADIC) {
			const char *entity = root->operand.variadic.entity_alias;
			raxInsert(aliases, (unsigned char *)entity, strlen(entity), NULL, NULL);
		}
	}
}

void AR_EXP_CollectAttributes(AR_ExpNode *root, rax *attributes) {
	if(root->type == AR_EXP_OP) {
		if(RG_STRCMP(root->op.func_name, "property") == 0) {
			AR_ExpNode *arg = root->op.children[1];
			ASSERT(AR_EXP_IsConstant(arg));
			ASSERT(SI_TYPE(arg->operand.constant) == T_STRING);

			const char *attr = arg->operand.constant.stringval;
			raxInsert(attributes, (unsigned char *)attr, strlen(attr), NULL, NULL);
		}

		// continue scanning expression
		for(int i = 0; i < root->op.child_count; i ++) {
			AR_EXP_CollectAttributes(root->op.children[i], attributes);
		}
	}
}

bool AR_EXP_ContainsAggregation(AR_ExpNode *root) {
	if(root->type == AR_EXP_OP && root->op.type == AR_OP_AGGREGATE) return true;

	if(root->type == AR_EXP_OP) {
		for(int i = 0; i < root->op.child_count; i++) {
			AR_ExpNode *child = root->op.children[i];
			if(AR_EXP_ContainsAggregation(child)) return true;
		}
	}

	return false;
}

bool AR_EXP_ContainsFunc(const AR_ExpNode *root, const char *func) {
	if(root == NULL) return false;
	if(root->type == AR_EXP_OP) {
		if(strcasecmp(root->op.func_name, func) == 0) return true;
		for(int i = 0; i < root->op.child_count; i++) {
			if(AR_EXP_ContainsFunc(root->op.children[i], func)) return true;
		}
	}
	return false;
}

bool inline AR_EXP_IsConstant(const AR_ExpNode *exp) {
	return exp->type == AR_EXP_OPERAND && exp->operand.type == AR_EXP_CONSTANT;
}

bool inline AR_EXP_IsParameter(const AR_ExpNode *exp) {
	return exp->type == AR_EXP_OPERAND && exp->operand.type == AR_EXP_PARAM;
}

bool AR_EXP_IsAttribute(const AR_ExpNode *exp, char **attr) {
	ASSERT(exp != NULL);

	// an arithmetic expression performs attribute extraction
	// if it applys the "property" function, in which case the left-handside
	// child represents the graph entity from which we access the attribute
	// while the right-handside represents the attribute name

	if(exp->type != AR_EXP_OP) return false;
	if(RG_STRCMP(exp->op.func_name, "property") != 0) return false;

	if(attr != NULL) {
		AR_ExpNode *r = exp->op.children[1];
		ASSERT(AR_EXP_IsConstant(r));
		SIValue v = r->operand.constant;
		ASSERT(SI_TYPE(v) == T_STRING);
		*attr = v.stringval;
	}

	return true;
}

bool AR_EXP_ReturnsBoolean(const AR_ExpNode *exp) {
	ASSERT(exp != NULL && exp->type != AR_EXP_UNKNOWN);

	// If the node does not represent a constant, assume it returns a boolean.
	// TODO We can add greater introspection in the future if required.
	if(exp->type == AR_EXP_OP) return true;

	// Operand node, return true if it is a boolean or NULL constant.
	if(exp->operand.type == AR_EXP_CONSTANT) {
		return (SI_TYPE(exp->operand.constant) & (T_BOOL | T_NULL));
	}

	// Node is a variable or parameter, whether it evaluates to boolean cannot be determined now.
	return true;
}

void _AR_EXP_ToString(const AR_ExpNode *root, char **str, size_t *str_size,
					  size_t *bytes_written) {
	/* Make sure there are at least 64 bytes in str. */
	if(*str == NULL) {
		*bytes_written = 0;
		*str_size = 128;
		*str = rm_calloc(*str_size, sizeof(char));
	}

	if((*str_size - strlen(*str)) < 64) {
		*str_size += 128;
		*str = rm_realloc(*str, sizeof(char) * *str_size);
	}
	/* Concat Op. */
	if(root->type == AR_EXP_OP) {
		/* Binary operation? */
		char binary_op = 0;

		if(strcmp(root->op.func_name, "ADD") == 0) binary_op = '+';
		else if(strcmp(root->op.func_name, "SUB") == 0) binary_op = '-';
		else if(strcmp(root->op.func_name, "MUL") == 0) binary_op = '*';
		else if(strcmp(root->op.func_name, "DIV") == 0)  binary_op = '/';

		if(binary_op) {
			_AR_EXP_ToString(root->op.children[0], str, str_size, bytes_written);

			/* Make sure there are at least 64 bytes in str. */
			if((*str_size - strlen(*str)) < 64) {
				*str_size += 128;
				*str = rm_realloc(*str, sizeof(char) * *str_size);
			}

			*bytes_written += sprintf((*str + *bytes_written), " %c ", binary_op);

			_AR_EXP_ToString(root->op.children[1], str, str_size, bytes_written);
		} else {
			/* Operation isn't necessarily a binary operation, use function call representation. */
			*bytes_written += sprintf((*str + *bytes_written), "%s(", root->op.func_name);

			for(int i = 0; i < root->op.child_count ; i++) {
				_AR_EXP_ToString(root->op.children[i], str, str_size, bytes_written);

				/* Make sure there are at least 64 bytes in str. */
				if((*str_size - strlen(*str)) < 64) {
					*str_size += 128;
					*str = rm_realloc(*str, sizeof(char) * *str_size);
				}
				if(i < (root->op.child_count - 1)) {
					*bytes_written += sprintf((*str + *bytes_written), ",");
				}
			}
			*bytes_written += sprintf((*str + *bytes_written), ")");
		}
	} else {
		// Concat Operand node.
		if(root->operand.type == AR_EXP_CONSTANT) {
			SIValue_ToString(root->operand.constant, str, str_size, bytes_written);
		} else {
			*bytes_written += sprintf((*str + *bytes_written), "%s", root->operand.variadic.entity_alias);
		}
	}
}

void AR_EXP_ToString(const AR_ExpNode *root, char **str) {
	size_t str_size = 0;
	size_t bytes_written = 0;
	*str = NULL;
	_AR_EXP_ToString(root, str, &str_size, &bytes_written);
}

// Generate a heap-allocated name for an arithmetic expression.
// This routine is only used to name ORDER BY expressions.
char *AR_EXP_BuildResolvedName(AR_ExpNode *root) {
	char *name = NULL;
	AR_EXP_ToString(root, &name);
	return name;
}

AR_ExpNode *AR_EXP_Clone(AR_ExpNode *exp) {
	if(exp == NULL) return NULL;
	AR_ExpNode *clone;
	switch(exp->type) {
	case AR_EXP_OPERAND:
		clone = _AR_EXP_CloneOperand(exp);
		break;
	case AR_EXP_OP:
		clone = _AR_EXP_CloneOp(exp);
		break;
	default:
		assert(false);
		break;
	}
	clone->resolved_name = exp->resolved_name;
	return clone;
}

static inline void _AR_EXP_FreeOpInternals(AR_ExpNode *op_node) {
	if(op_node->op.type == AR_OP_FUNC && op_node->op.f->bfree) {
		op_node->op.f->bfree(op_node->op.f->privdata); // Free the function's private data.
		rm_free(op_node->op.f); // The function descriptor itself is an allocation in this case.
	}
	for(int child_idx = 0; child_idx < op_node->op.child_count; child_idx++) {
		AR_EXP_Free(op_node->op.children[child_idx]);
	}
	rm_free(op_node->op.children);
	if(op_node->op.type == AR_OP_AGGREGATE) {
		AggCtx_Free(op_node->op.agg_func);
	}
}

inline void AR_EXP_Free(AR_ExpNode *root) {
	if(root->type == AR_EXP_OP) {
		_AR_EXP_FreeOpInternals(root);
	} else if(root->operand.type == AR_EXP_CONSTANT) {
		SIValue_Free(root->operand.constant);
	}
	rm_free(root);
}

