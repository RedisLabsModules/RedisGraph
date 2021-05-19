/*
 * Copyright 2018-2020 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/types.h>

// forward declaration of edge
struct QGEdge;

typedef struct {
	int *labelsID;             // labels ID array
	const char *alias;         // user-provided alias associated with this node
	const char **labels;       // labels string array
	bool highly_connected;     // node degree > 2
	struct QGEdge **outgoing_edges;   // array of incoming edges (ME)<-(SRC)
	struct QGEdge **incoming_edges;   // array of outgoing edges (ME)->(DEST)
} QGNode;

// creates a new node
QGNode *QGNode_New(const char *alias);

// returns true if the node is labeled
bool QGNode_Labeled(const QGNode *n);

// returns number of labels attached to node
uint QGNode_LabelCount(const QGNode *n);

// returns the 'idx' label ID of 'n'
int QGNode_GetLabelID(const QGNode *n, uint idx);

// returns the 'idx' label of 'n'
const char *QGNode_GetLabel(const QGNode *n, uint idx);

// returns true if 'n' has label 'l'
bool QGNode_HasLabel(const QGNode *n, const char *l);

// label 'n' as 'l'
void QGNode_AddLabel(QGNode *n, const char *l, int l_id);

// returns true if node is highly connected, false otherwise
bool QGNode_HighlyConnected(const QGNode *n);

// returns the number of both incoming and outgoing edges
int QGNode_Degree(const QGNode *n);

// returns number of edges pointing into node
int QGNode_IncomeDegree(const QGNode *n);

// returns number of edges pointing out of node
int QGNode_OutgoingDegree(const QGNode *n);

// returns to total number of edges (incoming & outgoing)
int QGNode_EdgeCount(const QGNode *n);

// connects source node to destination node by edge
void QGNode_ConnectNode(QGNode *src, QGNode *dest, struct QGEdge *e);

// removes given incoming edge from node
void QGNode_RemoveIncomingEdge(QGNode *n, struct QGEdge *e);

// removes given outgoing edge from node
void QGNode_RemoveOutgoingEdge(QGNode *n, struct QGEdge *e);

// clones given node
QGNode *QGNode_Clone(const QGNode *n);

// gets a string representation of given node
int QGNode_ToString(const QGNode *n, char *buff, int buff_len);

// frees allocated space by given node
void QGNode_Free(QGNode *node);

