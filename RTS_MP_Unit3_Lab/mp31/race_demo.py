"""Mini Project 3.1, steps 1-3: reproduce the lost-update race condition.

Each thread executes a non-atomic read -> (widen window) -> write on the module-level
shared_counter, so two tasks can read the same value and one update is lost.
"""

import sys
import threading
import time

shared_counter = 0

ITERATIONS = 1000
INTERLEAVE_SLEEP = 0.0001


def increment_unsafe(iterations):
    global shared_counter
    for _ in range(iterations):
        current = shared_counter            # read
        time.sleep(INTERLEAVE_SLEEP)        # widen the interleaving window
        shared_counter = current + 1        # write


def run(thread_count, iterations):
    global shared_counter
    shared_counter = 0
    threads = [
        threading.Thread(target=increment_unsafe, args=(iterations,))
        for _ in range(thread_count)
    ]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    expected = thread_count * iterations
    lost = expected - shared_counter
    print(f"threads={thread_count}  iterations/thread={iterations}")
    print(f"expected={expected}  actual={shared_counter}  "
          f"lost_updates={lost} ({100.0 * lost / expected:.2f}%)")
    print(f"elapsed={elapsed:.3f}s")
    return shared_counter, expected


if __name__ == "__main__":
    counts = [int(a) for a in sys.argv[1:]] or [5]
    if len(counts) == 1:
        run(counts[0], ITERATIONS)
    else:
        run(counts[0], counts[1])
