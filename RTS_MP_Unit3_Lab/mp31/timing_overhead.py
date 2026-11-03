"""Mini Project 3.1, step 9: dispatch-latency and synchronisation-overhead measurement.

Part 1 times, over 200 iterations, the interval between starting a thread and that
thread signalling a threading.Event back to the main thread. This is the Python
analogue of context-switch / interrupt-response latency (Sections 3.2 and 3.5): the
mean is the "average case" and the maximum is the observed worst case.

Part 2 measures how the cost of holding a mutex scales with concurrency, which is the
synchronisation-overhead half of the objective. The critical section holds the lock for
a short fixed interval (the analogue of time spent inside an RTOS critical region) so
that serialisation with task count is measurable, and the unlocked variant is timed
alongside it to show the correctness cost of removing the mutex.
"""

import argparse
import csv
import statistics
import sys
import threading
import time
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

SHARED = 0
LOCK = threading.Lock()


def high_resolution_timer(enable=True):
    """Ask Windows for 1 ms timer granularity so ms-scale delays are meaningful."""
    if sys.platform != "win32":
        return False
    try:
        winmm = __import__("ctypes").windll.winmm
        if enable:
            winmm.timeBeginPeriod(1)
        else:
            winmm.timeEndPeriod(1)
        return True
    except Exception:
        return False


def set_event(event):
    event.set()


def dispatch_latency(iterations):
    intervals = []
    for _ in range(iterations):
        event = threading.Event()
        start = time.perf_counter()
        worker = threading.Thread(target=set_event, args=(event,))
        worker.start()
        event.wait()
        intervals.append(time.perf_counter() - start)
        worker.join()
    ordered = sorted(intervals)
    p95 = ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]
    return {
        "iterations": iterations,
        "min_ms": round(min(intervals) * 1000, 4),
        "max_ms": round(max(intervals) * 1000, 4),
        "mean_ms": round(statistics.fmean(intervals) * 1000, 4),
        "median_ms": round(statistics.median(intervals) * 1000, 4),
        "p95_ms": round(p95 * 1000, 4),
        "stdev_ms": round(statistics.pstdev(intervals) * 1000, 4),
    }


def increment_threads(thread_count, iterations, use_lock, critical_sleep):
    """Run `iterations` read-modify-write updates per task, with or without the mutex."""
    global SHARED
    SHARED = 0

    def body():
        global SHARED
        for _ in range(iterations):
            if use_lock:
                with LOCK:
                    current = SHARED
                    time.sleep(critical_sleep)
                    SHARED = current + 1
            else:
                current = SHARED
                time.sleep(critical_sleep)
                SHARED = current + 1

    threads = [threading.Thread(target=body) for _ in range(thread_count)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    expected = thread_count * iterations
    return {
        "threads": thread_count,
        "elapsed_ms": round(elapsed * 1000, 1),
        "correct": SHARED == expected,
        "final_value": SHARED,
        "expected": expected,
    }


def print_dispatch(stats):
    print(f"Dispatch latency over {stats['iterations']} thread-dispatch events:")
    print(f"  min    = {stats['min_ms']:.4f} ms")
    print(f"  mean   = {stats['mean_ms']:.4f} ms")
    print(f"  median = {stats['median_ms']:.4f} ms")
    print(f"  p95    = {stats['p95_ms']:.4f} ms")
    print(f"  max    = {stats['max_ms']:.4f} ms  <- worst-case observed dispatch latency")
    print(f"  stdev  = {stats['stdev_ms']:.4f} ms")
    print(f"  worst/mean ratio = {stats['max_ms'] / stats['mean_ms']:.1f}x")


def overhead_scaling(thread_counts, iterations, critical_ms):
    rows = []
    critical_sleep = critical_ms / 1000.0
    print(f"\nSynchronisation overhead vs concurrency "
          f"({iterations} updates/task, {critical_ms:.2f} ms held per update):")
    print(f"{'threads':>8} {'no-lock ms':>11} {'mutex ms':>10} {'mutex/no-lock':>14} "
          f"{'no-lock correct':>16} {'no-lock final':>14}")
    for count, free, locked in thread_count_rows(thread_counts, iterations,
                                                 critical_sleep):
        ratio = locked["elapsed_ms"] / free["elapsed_ms"]
        rows.append({
            "threads": count,
            "updates_per_thread": iterations,
            "critical_hold_ms": critical_ms,
            "no_lock_ms": free["elapsed_ms"],
            "mutex_ms": locked["elapsed_ms"],
            "serialisation_x": round(ratio, 2),
            "no_lock_correct": free["correct"],
            "no_lock_final": free["final_value"],
            "expected": free["expected"],
            "lost_updates": free["expected"] - free["final_value"],
            "mutex_final": locked["final_value"],
        })
        print(f"{count:>8} {free['elapsed_ms']:>11.1f} {locked['elapsed_ms']:>10.1f} "
              f"{ratio:>13.2f}x {str(free['correct']):>16} "
              f"{free['final_value']:>7}/{free['expected']:<6}")
    return rows


def thread_count_rows(thread_counts, iterations, critical_sleep):
    """Yield the unlocked and locked timing for each requested concurrency level."""
    for count in thread_counts:
        free = increment_threads(count, iterations, use_lock=False,
                                 critical_sleep=critical_sleep)
        locked = increment_threads(count, iterations, use_lock=True,
                                   critical_sleep=critical_sleep)
        yield count, free, locked


def write_csv(rows):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / "mp31_timing_scaling.csv"
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--threads", type=str, default="1,2,5,10")
    parser.add_argument("--updates", type=int, default=300,
                        help="shared updates per task for the overhead scaling part")
    parser.add_argument("--critical-ms", type=float, default=0.25,
                        help="time the mutex is held per update, in milliseconds")
    parser.add_argument("--no-timer", action="store_true",
                        help="skip the Windows high-resolution timer request")
    args = parser.parse_args()

    if not args.no_timer:
        high_resolution_timer(True)
    try:
        print_dispatch(dispatch_latency(args.iterations))
        rows = overhead_scaling([int(c) for c in args.threads.split(",")],
                                args.updates, args.critical_ms)
        write_csv(rows)
    finally:
        if not args.no_timer:
            high_resolution_timer(False)
