"""Mini Project 3.1, steps 5-6: bounded queue standing in for an RTOS message queue.

One producer task puts timestamped items into a bounded queue.Queue; one consumer task
gets them and records the send-to-receipt latency. Blocking puts (the producer being
rate-limited by a full queue) are counted separately, mirroring an RTOS queue that
pends a producer task when it is full.
"""

import argparse
import csv
import queue
import statistics
import sys
import threading
import time
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def high_resolution_timer(enable=True):
    """Ask Windows for 1 ms timer granularity so ms-scale latencies are meaningful."""
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


def producer_task(q, items, cadence, block_stats):
    """Put `items` timestamped payloads; record how long each put() blocks."""
    rng_state = 12345
    for i in range(items):
        rng_state = (1103515245 * rng_state + 12345) % (1 << 31)
        payload = (i, time.perf_counter(), rng_state % 100)
        t0 = time.perf_counter()
        q.put(payload)                      # blocks automatically when the queue is full
        blocked = time.perf_counter() - t0
        if blocked > 0.0005:
            block_stats.append((i, blocked))
        if cadence:                         # +/-25% arrival jitter
            time.sleep(cadence * (0.75 + 0.5 * (rng_state % 100) / 100.0))


def consumer_task(q, items, service_time, latencies, done_event):
    """Get every item, record latency, and optionally spend time 'servicing' it."""
    for _ in range(items):
        i, sent, _payload = q.get()
        latencies.append((i, time.perf_counter() - sent))
        if service_time:
            time.sleep(service_time)
        q.task_done()
    done_event.set()


def measure(maxsize, items, service_time, cadence=0.0):
    q = queue.Queue(maxsize=maxsize)
    latencies, block_stats = [], []
    done = threading.Event()

    prod = threading.Thread(target=producer_task,
                            args=(q, items, cadence, block_stats))
    cons = threading.Thread(target=consumer_task,
                            args=(q, items, service_time, latencies, done))
    t0 = time.perf_counter()
    prod.start()
    cons.start()
    done.wait()
    prod.join()
    cons.join()
    elapsed = time.perf_counter() - t0

    values = [lat for _i, lat in latencies]
    return {
        "maxsize": maxsize,
        "items": items,
        "service_time_ms": round(service_time * 1000, 3),
        "min_latency_ms": round(min(values) * 1000, 3),
        "max_latency_ms": round(max(values) * 1000, 3),
        "mean_latency_ms": round(statistics.fmean(values) * 1000, 3),
        "p95_latency_ms": round(quantile(values, 0.95) * 1000, 3),
        "blocking_puts": len(block_stats),
        "blocked_time_ms": round(sum(b for _i, b in block_stats) * 1000, 3),
        "elapsed_s": round(elapsed, 3),
    }


def quantile(values, q):
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[idx]


def print_run(result):
    print(f"maxsize={result['maxsize']}  items={result['items']}  "
          f"consumer_service={result['service_time_ms']}ms")
    print(f"  latency min/mean/max = {result['min_latency_ms']:.3f} / "
          f"{result['mean_latency_ms']:.3f} / {result['max_latency_ms']:.3f} ms "
          f"(p95 {result['p95_latency_ms']:.3f} ms)")
    print(f"  blocking puts={result['blocking_puts']}  "
          f"total producer blocked={result['blocked_time_ms']:.3f} ms")
    print(f"  run time={result['elapsed_s']:.3f}s")


def sweep(capacities, items, service_time, cadence=0.0):
    rows = []
    print(f"Queue capacity sweep (lab task 4), consumer service "
          f"{service_time * 1000:.1f} ms, producer cadence {cadence * 1000:.1f} ms:")
    print(f"{'maxsize':>8} {'mean ms':>9} {'p95 ms':>9} {'max ms':>9} "
          f"{'blocking puts':>14} {'blocked ms':>11}")
    for cap in capacities:
        res = measure(cap, items, service_time, cadence)
        rows.append(res)
        print(f"{cap:>8} {res['mean_latency_ms']:>9.3f} {res['p95_latency_ms']:>9.3f} "
              f"{res['max_latency_ms']:>9.3f} {res['blocking_puts']:>14} "
              f"{res['blocked_time_ms']:>11.3f}")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / "mp31_queue_sweep.csv"
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maxsize", type=int, default=5)
    parser.add_argument("--items", type=int, default=20)
    parser.add_argument("--service", type=float, default=0.0,
                        help="consumer service time per item, seconds")
    parser.add_argument("--cadence", type=float, default=0.0,
                        help="mean producer inter-item delay, seconds")
    parser.add_argument("--sweep", type=str, default=None,
                        help="comma separated list of maxsize values to tabulate")
    args = parser.parse_args()

    high_resolution_timer(True)
    try:
        if args.sweep:
            capacities = [int(c) for c in args.sweep.split(",")]
            sweep(capacities, args.items, args.service, args.cadence)
        else:
            print_run(measure(args.maxsize, args.items, args.service, args.cadence))
    finally:
        high_resolution_timer(False)
