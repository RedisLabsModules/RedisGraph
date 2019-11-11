import os
import sys
from redisgraph import Graph, Node

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from base import FlowTestsBase
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../../')
from demo import QueryInfo

GRAPH_ID = "G"
redis_graph = None

class testParams(FlowTestsBase):
    def __init__(self):
        super(testParams, self).__init__()
        global redis_graph
        redis_con = self.env.getConnection()
        redis_graph = Graph(GRAPH_ID, redis_con)

    def setUp(self):
        self.env.flush() 
    
    def test_simple_params(self):
        params = [1, 2.3, "str", True, False, None, [0, 1, 2]]

        for param in params:
            query = "RETURN $param"
            expected_results = [[param]]
            
            query_info = QueryInfo(query = query, description="Tests simple params", expected_result = expected_results)
            self._assert_resultset_equals_expected(redis_graph.query(query, {'param': param}), query_info)

    def test_expression_on_param(self):
        query = "RETURN $param + 1"
        expected_results = [[2]]
            
        query_info = QueryInfo(query = query, description="Tests expression on param", expected_result = expected_results)
        self._assert_resultset_equals_expected(redis_graph.query(query, {'param': 1}), query_info)

    def test_node_retrival(self):
        p0 = Node(node_id=0, label="Person", properties={'name': 'a'})
        p1 = Node(node_id=1, label="Person", properties={'name': 'b'})
        p2 = Node(node_id=2, label="NoPerson", properties={'name': 'a'})
        redis_graph.add_node(p0)
        redis_graph.add_node(p1)
        redis_graph.add_node(p2)
        redis_graph.flush()

        query = "MATCH (n :Person {name:$name}) RETURN n"
        expected_results = [[p0]]
            
        query_info = QueryInfo(query = query, description="Tests expression on param", expected_result = expected_results)
        self._assert_resultset_equals_expected(redis_graph.query(query, {'name': 'a'}), query_info)
