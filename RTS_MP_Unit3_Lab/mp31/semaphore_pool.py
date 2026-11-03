"""Mini Project 3.1, steps 7-8: counting semaphore as a pool of resource blocks.

Eight task threads contend for a pool of `count` identical resource blocks represented
by threading.Semaphore. Each thread records how long it blocked in acquire(), how long
it held the block, and the run tracks the maximum number of simultaneous holders, which
must never exceed the semaphore count.
"""

import argparse
import csv
import random
import statistics
import threading
import time
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def pool_task(task_id, semaphore, hold_range, records, state, gate):
    gate.wait()                             # all tasks contend from the same instant
    t0 = time.perf_counter()
    semaphore.acquire()                     # pends like a blocked RTOS task
    waited = time.perf_counter() - t0

    with state["lock"]:
        state["holders"] += 1
        state["max_holders"] = max(state["max_holders"], state["holders"])

    hold = random.uniform(*hold_range)
    time.sleep(hold)

    with state["lock"]:
        state["holders"] -= 1
    semaphore.release()
    records.append((task_id, waited, hold))


def measure(count, task_count, hold_range, seed=20261103):
    random.seed(seed)
    semaphore = threading.Semaphore(count)
    gate = threading.Event()
    records = []
    state = {"lock": threading.Lock(), "holders": 0, "max_holders": 0}

    threads = [
        threading.Thread(target=pool_task,
                         args=(i, semaphore, hold_range, records, state, gate))
        for i in range(task_count)
    ]
    start = time.perf_counter()
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join()
    makespan = time.perf_counter() - start

    records.sort()
    waits = [w for _i, w, _h in records]
    immediate = sum(1 for w in waits if w < 0.0005)
    return {
        "count": count,
        "tasks": task_count,
        "max_holders": state["max_holders"],
        "immediate_tasks": immediate,
        "blocked_tasks": task_count - immediate,
        "mean_wait_ms": round(statistics.fmean(waits) * 1000, 3),
        "max_wait_ms": round(max(waits) * 1000, 3),
        "total_hold_ms": round(sum(h for _i, _w, h in records) * 1000, 1),
        "makespan_ms": round(makespan * 1000, 1),
    }, records


def report(result, records):
    print(f"semaphore count={result['count']}  tasks={result['tasks']}")
    print(f"  max simultaneous holders = {result['max_holders']} "
          f"(must be <= {result['count']})")
    print(f"  tasks that proceeded immediately = {result['immediate_tasks']}, "
          f"tasks that blocked = {result['blocked_tasks']}")
    print(f"  wait time mean={result['mean_wait_ms']:.3f} ms  "
          f"max={result['max_wait_ms']:.3f} ms")
    print("  per-task wait / hold (ms):")
    for task_id, waited, hold in records:
        print(f"    task {task_id}: waited {waited * 1000:7.3f}  held {hold * 1000:6.1f}")
    print(f"  makespan={result['makespan_ms']:.1f} ms")


def sweep(counts, task_count, hold_ms):
    rows = []
    hold_range = (hold_ms / 1000.0) * 0.5, (hold_ms / 1000.0) * 1.5
    print("Semaphore-count sweep (lab task 2):")
    print(f"{'count':>6} {'max holders':>12} {'immediate':>10} {'blocked':>8} "
          f"{'mean wait ms':>13} {'max wait ms':>12} {'makespan ms':>12}")
    for count in counts:
        res, _records = measure(count, task_count, hold_range)
        rows.append(res)
        print(f"{count:>6} {res['max_holders']:>12} {res['immediate_tasks']:>10} "
              f"{res['blocked_tasks']:>8} {res['mean_wait_ms']:>13.3f} "
              f"{res['max_wait_ms']:>12.3f} {res['makespan_ms']:>12.1f}")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / "mp31_semaphore_sweep.csv"
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--tasks", type=int, default=8)
    parser.add_argument("--hold-ms", type=float, default=40.0)
    parser.add_argument("--sweep", type=str, default=None,
                        help="comma separated semaphore counts to tabulate")
    args = parser.parse_args()

    if args.sweep:
        sweep([int(c) for c in args.sweep.split(",")], args.tasks, args.hold_ms)
    else:
        hold_range = (args.hold_ms / 1000.0) * 0.5, (args.hold_ms / 1000.0) * 1.5
        result, records = measure(args.count, args.tasks, hold_range)
        report(result, records)
