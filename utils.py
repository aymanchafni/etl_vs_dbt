"""Benchmark helpers.

Memory is measured by a background thread that samples the RSS of the *whole process
tree* (this process + subprocesses such as `dbt`, which runs DuckDB). Measuring only the
current process (as `resource.getrusage(RUSAGE_SELF)` does) misses all the work done
inside the dbt subprocess and makes the ELT pipeline look artificially lean.
All memory values are in MB on every OS.
"""

import gc
import os
import threading
from time import perf_counter, time

import psutil


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
        self.throughput = (
            round(row_count / self.execution_time, 2) if self.execution_time > 0 and row_count else None
        )

    def to_dict(self):
        return {
            "analysis_name": self.name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "execution_time": self.execution_time,
            "cpu_time": self.cpu_time,
            "memory_peak": self.memory_peak,
            "row_count": self.row_count,
            "throughput": self.throughput,
        }


def _tree_rss_mb(proc: psutil.Process) -> float:
    total = 0
    for p in [proc, *proc.children(recursive=True)]:
        try:
            total += p.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total / (1024 * 1024)


class PeakMemorySampler:
    """Context manager recording the peak RSS (MB) of this process and its children."""

    def __init__(self, interval: float = 0.02):
        self.interval = interval
        self.peak_mb = 0.0
        self._proc = psutil.Process(os.getpid())
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            self.peak_mb = max(self.peak_mb, _tree_rss_mb(self._proc))
            self._stop.wait(self.interval)

    def __enter__(self):
        self.peak_mb = _tree_rss_mb(self._proc)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()
        self.peak_mb = max(self.peak_mb, _tree_rss_mb(self._proc))


def _cpu_seconds(proc: psutil.Process) -> float:
    """CPU time of this process plus its *terminated* children (Linux/macOS)."""
    t = proc.cpu_times()
    return t.user + t.system + getattr(t, "children_user", 0.0) + getattr(t, "children_system", 0.0)


def get_memory_usage():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)


def benchmark_decorator(analysis_name, func, row_count_func=None):
    """
    row_count_func : optional callable applied to the result to get a row count.
                     e.g. lambda r: len(r) for a DataFrame, lambda r: r if func returns an int
    """

    def wrapper(*args, **kwargs):
        gc.collect()
        proc = psutil.Process(os.getpid())
        cpu_before = _cpu_seconds(proc)

        with PeakMemorySampler() as mem:
            start_time = time()
            t0 = perf_counter()
            result = func(*args, **kwargs)
            elapsed = perf_counter() - t0

        cpu_time = round(_cpu_seconds(proc) - cpu_before, 4)
        end_time = start_time + elapsed

        row_count = None
        if row_count_func is not None:
            try:
                row_count = row_count_func(result)
            except Exception:
                row_count = None

        print(f"  Memory peak    : {mem.peak_mb:.1f} MB")
        print(f"  CPU time       : {cpu_time:.4f} s")
        print(f"  Execution time : {elapsed:.4f} s")
        if row_count:
            print(f"  Rows produced  : {row_count:,}  ({row_count / elapsed:,.0f} rows/s)")

        return Benchmark(
            name=analysis_name,
            start_time=start_time,
            end_time=end_time,
            memory_peak=round(mem.peak_mb, 1),
            cpu_time=cpu_time,
            row_count=row_count,
            result=result,
        )

    return wrapper
