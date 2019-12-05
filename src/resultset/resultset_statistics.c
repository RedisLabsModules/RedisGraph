/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "resultset_statistics.h"

bool ResultSetStat_IndicateModification(ResultSetStatistics stats) {
	return (stats.labels_added > 0
			|| stats.nodes_created > 0
			|| stats.properties_set > 0
			|| stats.relationships_created > 0
			|| stats.nodes_deleted > 0
			|| stats.relationships_deleted > 0
			|| stats.indicies_created != NOT_SET
			|| stats.indicies_deleted != NOT_SET);
}
