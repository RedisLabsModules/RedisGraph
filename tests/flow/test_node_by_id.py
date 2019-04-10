import os
import sys
import unittest
from redisgraph import Graph, Node, Edge

import redis
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from disposableredis import DisposableRedis

from base import FlowTestsBase

GRAPH_ID = "g"
redis_graph = None

def disposable_redis():
    return DisposableRedis(loadmodule=os.path.dirname(os.path.abspath(__file__)) + '/../../src/redisgraph.so')

class NodeByIDFlowTest(FlowTestsBase):
    @classmethod
    def setUpClass(cls):
        print "NodeByIDFlowTest"
        global redis_graph
        cls.r = disposable_redis()
        cls.r.start()
        cls.populate_graph()

    @classmethod
    def tearDownClass(cls):
        cls.r.stop()
        # pass

    @classmethod
    def populate_graph(cls):
        global redis_graph
        redis_con = cls.r.client()
        redis_graph = Graph(GRAPH_ID, redis_con)
        # Create entities
        for i in range(10):            
            node = Node(label="person", properties={"id": i})
            redis_graph.add_node(node)
        redis_graph.commit()

        # Make sure node id attribute matches node's internal ID.
        query = """MATCH (n) SET n.id = ID(n)"""
        redis_graph.query(query)

    # Expect an error when trying to use a function which does not exists.
    def test_get_nodes(self):
        # All nodes, not including first node.
        query = """MATCH (n) WHERE ID(n) > 0 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id > 0 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # All nodes.
        query = """MATCH (n) WHERE ID(n) >= 0 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id >= 0 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # A single node.
        query = """MATCH (n) WHERE ID(n) = 0 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id = 0 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # 4 nodes (6,7,8,9)
        query = """MATCH (n) WHERE ID(n) > 5 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id > 5 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # 5 nodes (5, 6,7,8,9)
        query = """MATCH (n) WHERE ID(n) >= 5 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id >= 5 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # 5 nodes (0,1,2,3,4)
        query = """MATCH (n) WHERE ID(n) < 5 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id < 5 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # 6 nodes (0,1,2,3,4,5)
        query = """MATCH (n) WHERE ID(n) <= 5 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id <= 5 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # All nodes except last one.
        query = """MATCH (n) WHERE ID(n) < 9 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id < 9 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # All nodes.
        query = """MATCH (n) WHERE ID(n) <= 9 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id <= 9 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)

        # All nodes.
        query = """MATCH (n) WHERE ID(n) < 100 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id < 100 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)
        
        # All nodes.
        query = """MATCH (n) WHERE ID(n) <= 100 RETURN n ORDER BY n.id"""
        resultsetA = redis_graph.query(query).result_set
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (n) WHERE n.id <= 100 RETURN n ORDER BY n.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        self.assertEqual(resultsetA, resultsetB)

        # cartesian product, tests reset works as expected.
        query = """MATCH (a), (b) WHERE ID(a) > 5 AND ID(b) <= 5 RETURN a,b ORDER BY a.id, b.id"""
        resultsetA = redis_graph.query(query).result_set
        print "resultsetA: %s\n" % resultsetA
        self.assertIn("NodeByIdSeek", redis_graph.execution_plan(query))
        query = """MATCH (a), (b) WHERE a.id > 5 AND b.id <= 5 RETURN a,b ORDER BY a.id, b.id"""
        self.assertNotIn("NodeByIdSeek", redis_graph.execution_plan(query))
        resultsetB = redis_graph.query(query).result_set
        print "resultsetB: %s\n" % resultsetB
        self.assertEqual(resultsetA, resultsetB)

if __name__ == '__main__':
    unittest.main()
