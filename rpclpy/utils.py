import functools
import time
def timeit_callback(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        class_name = self.__class__.__name__
        start_time = time.perf_counter()
        # Optionally, record the start time in Redis
        self.knowledge.write(f"{class_name}:start_execution", time.time())
        result = func(self, *args, **kwargs)
        exec_time = time.perf_counter() - start_time
        # Record the execution time in Redis
        self.knowledge.write(f"{class_name}:execution_time", exec_time)
        return result
    return wrapper
