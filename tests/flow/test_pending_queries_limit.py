from RLTest import Env
from redisgraph import Graph
import utils
from utils import *

# 1.test getting and setting config
# 2. test overflowing the server when there's a limit
#    expect to get error!
# 3. test overflowing the server when there's no limit
#    expect not to get any exceptions

error_encountered = False

GRAPH_NAME = "max_pending_queries"
# note: if test_03_overflow_with_limit is flaky 
# on machines with many cores on some runs the rate at which the queries been executed by the workers 
# might be greater than the insertion to the work queues. 
# This ends up were there is at most one job in the queue which failed
# test_03_overflow_with_limit. Increasing the execution time of the query should resolved this.
SLOW_QUERY = "UNWIND range (0, 100000) AS x WITH x WHERE (x / 2) = 50  RETURN x"

def issue_query(q):
    try:
        utils.con.execute_command("GRAPH.QUERY", GRAPH_NAME, q)
        return False
    except Exception as e:
        assert "Max pending queries exceeded" in str(e)
        return True

class testPendingQueryLimit():
    def __init__(self):
        self.env = Env(decodeResponses=True)
        self.conn = self.env.getConnection()

    def test_01_query_limit_config(self):
        # read max queued queries config
        result = self.conn.execute_command("GRAPH.CONFIG", "GET", "MAX_QUEUED_QUERIES")
        max_queued_queries = result[1]
        self.env.assertEquals(max_queued_queries, 4294967295)

        # update configuration, set max queued queries
        self.conn.execute_command("GRAPH.CONFIG", "SET", "MAX_QUEUED_QUERIES", 10)

        # re-read configuration
        result = self.conn.execute_command("GRAPH.CONFIG", "GET", "MAX_QUEUED_QUERIES")
        max_queued_queries = result[1]
        self.env.assertEquals(max_queued_queries, 10)

    def stress_server(self):
        global error_encountered

        threads = []
        threadpool_size = self.conn.execute_command("GRAPH.CONFIG", "GET", "THREAD_COUNT")[1]
        thread_count = threadpool_size * 5

        results = Multiproc.run_multiproc(self.env, thread_count, issue_query, (SLOW_QUERY,))
        error_encountered = any(results)


    def test_02_overflow_no_limit(self):
        global error_encountered
        error_encountered = False

        # no limit on number of pending queries
        limit = 4294967295
        self.conn.execute_command("GRAPH.CONFIG", "SET", "MAX_QUEUED_QUERIES", limit)

        self.stress_server()

        self.env.assertFalse(error_encountered)

    def test_03_overflow_with_limit(self):
        global error_encountered
        error_encountered = False

        # limit number of pending queries
        limit = 1
        self.conn.execute_command("GRAPH.CONFIG", "SET", "MAX_QUEUED_QUERIES", limit)

        self.stress_server()

        self.env.assertTrue(error_encountered)
