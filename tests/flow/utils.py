from RLTest import Env
import multiprocessing as mp

con = None

def init_process_local_connection(env):
    global con
    con = env.getConnection()
    return 1

class Multiproc:

    # create and init n_procs workers and their connections which will execute fn(args)
    def __init__(self, env, n_procs):
        # on macOS the spawn start method is now the default one since python 3.8.
        # the spawn start methon fails when it stumbles upon objects that contain locks/fds
        # For this reason need to forcefully set the context to fork.
        # For more details see: https://bugs.python.org/issue33725
        # or the python docs: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
        self.ctx = mp.get_context('fork')

        # creating connetion on pool init for increasing the fn calls rate, also this way the env is not being pickled
        # we are passing the env and not the connection so that a new connection will be created for each process.
        self.pool = self.ctx.Pool(n_procs, initializer=init_process_local_connection, initargs=(env,))
    
    def __del__(self):
        self.pool.terminate()

    def __enter__(self):
        self.pool._check_running()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.terminate()

    # get a fn to run async and it args and returns AsyncResult
    def run_async(self, fn, args=tuple()):
        return [self.pool.apply_async(fn, args=args)]

    # gets a tuple of funcs and it's corresponding arguments and execute them using the workers.
    # wait for all work to end and return the results
    def add_work_wait(self, fns=tuple(), args=tuple(tuple())):
        results = []
        multiple_results = []
        assert len(fns) == len(args) and len(fns) != 0
        for fn,args_i in zip(fns, args):
            multiple_results.extend(self.run_async(fn, args_i))
        results = [res.get() for res in multiple_results]
        return results

    # create and init n_procs workers and their connections which will execute fn(args)
    @staticmethod
    def run_multiproc(env, n_procs, fn, args=tuple()):
        # on macOS the spawn start method is now the default one since python 3.8.
        # the spawn start methon fails when it stumbles upon objects that contain locks/fds
        # For this reason need to forcefully set the context to fork.
        # For more details see: https://bugs.python.org/issue33725
        # or the python docs: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
        ctx = mp.get_context('fork')

        # creating connetion on pool init for increasing the fn calls rate, also this way the env is not being pickled
        # we are passing the env and not the connection so that a new connection will be created for each process.
        with ctx.Pool(n_procs, initializer=init_process_local_connection, initargs=(env,)) as p:
            multiple_results = [p.apply_async(fn, args=args) for i in range(n_procs)]
            results = [res.get() for res in multiple_results]
            return results