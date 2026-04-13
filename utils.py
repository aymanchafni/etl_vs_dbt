import os
import gc
import psutil
from time import time
import resource

class Benchmark:
    def __init__(self, name, start_time, end_time, memory_peak, cpu_time, row_count, result):
        self.name = name
        self.start_time = start_time
        self.end_time = end_time
        self.memory_peak = memory_peak
        self.cpu_time = cpu_time
        self.row_count = row_count
        self.result = result
        self.execution_time = end_time - start_time
        self.throughput = round(row_count / self.execution_time, 2) if self.execution_time > 0 and row_count else None

    def to_dict(self):
        return {
            "analysis_name"  : self.name,
            "start_time"     : self.start_time,
            "end_time"       : self.end_time,
            "execution_time" : self.execution_time,
            "cpu_time"       : self.cpu_time,
            "memory_peak"    : self.memory_peak,
            "row_count"      : self.row_count,
            "throughput"     : self.throughput,
        }
    
def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024) # En MB

def benchmark_decorator(analysis_name, func, row_count_func=None):
    """
    row_count_func : callable optionnel appelé sur result pour obtenir le nombre de lignes.
                     Ex: lambda r: len(r)  pour un DataFrame
                         lambda r: r       si la fonction retourne directement un int
    """
    def wrapper(*args, **kwargs):
        gc.collect()

        ru_before  = resource.getrusage(resource.RUSAGE_SELF)
        start_time = time()

        result = func(*args, **kwargs)

        end_time  = time()
        ru_after  = resource.getrusage(resource.RUSAGE_SELF)

        memory_peak = ru_after.ru_maxrss / (1024 * 1024)
        cpu_time    = round(
            (ru_after.ru_utime - ru_before.ru_utime) +
            (ru_after.ru_stime - ru_before.ru_stime), 4
        )

        row_count = None
        if row_count_func is not None:
            try:
                row_count = row_count_func(result)
            except Exception:
                row_count = None

        print(f"  Memory peak    : {memory_peak:.2f} MB")
        print(f"  CPU time       : {cpu_time:.4f} s")
        print(f"  Execution time : {end_time - start_time:.4f} s")
        if row_count is not None:
            throughput = round(row_count / (end_time - start_time), 2) if (end_time - start_time) > 0 else None
            print(f"  Rows produced  : {row_count:,}")
            print(f"  Throughput     : {throughput:,.0f} rows/s" if throughput else "")

        return Benchmark(
            name=analysis_name,
            start_time=start_time,
            end_time=end_time,
            memory_peak=memory_peak,
            cpu_time=cpu_time,
            row_count=row_count,
            result=result,
        )
    return wrapper