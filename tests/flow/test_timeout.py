import sys
from RLTest import Env
from base import FlowTestsBase
from redis import ResponseError

redis_con = None

class testQueryTimeout(FlowTestsBase):
    def __init__(self):
        # skip test if we're running under Valgrind
        if Env().envRunner.debugger is not None:
            Env().skip() # queries will be much slower under Valgrind

        self.env = Env(decodeResponses=True)
        global redis_con
        redis_con = self.env.getConnection()

    def test_read_query_timeout(self):
        query = "UNWIND range(0,100000) AS x WITH x AS x WHERE x = 10000 RETURN x"
        try:
            # The query is expected to time out
            redis_con.execute_command("GRAPH.QUERY", "g", query, "timeout", 1)
            assert(False)
        except ResponseError as error:
            self.env.assertTrue(isinstance(error, ResponseError))
            self.env.assertContains("Query timed out", str(error))

        try:
            # The query is expected to succeed
            redis_con.execute_command("GRAPH.QUERY", "g", query, "timeout", 100)
        except:
            assert(False)

    def test_write_query_timeout(self):
        query = "create ()"
        try:
            redis_con.execute_command("GRAPH.QUERY", "g", query, "timeout", 1)
            assert(False)
        except:
            # Expecting an error.
            pass
