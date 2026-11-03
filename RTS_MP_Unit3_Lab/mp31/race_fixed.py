"""Mini Project 3.1, step 4: repair the race with a mutex.

Identical to race_demo.py except that the critical section (read -> write) is wrapped
in `with counter_lock`, so the read-modify-write becomes atomic with respect to the
other tasks and the final value is exactly threads * iterations.
"""

import sys
import threading
import time

shared_counter = 0
counter_lock = threading.Lock()

ITERATIONS = 1000
CRITICAL_SLEEP = 0.0001


def increment_safe(iterations):
    global shared_counter
    for _ in range(iterations):
        with counter_lock:                  # mutual exclusion (mutex semantics)
            current = shared_counter        # read
            time.sleep(CRITICAL_SLEEP)      # sleep inside the CS keeps the analogy honest
            shared_counter = current + 1    # write


def run(thread_count, iterations):
    threads = [
        threading.Thread(target=increment_safe, args=(iterations,))
        for _ in range(thread_count)
    ]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    expected = thread_count * iterations
    print(f"threads={thread_count}  iterations/thread={iterations}")
    print(f"expected={expected}  actual={shared_counter}  "
          f"lost_updates={expected - shared_counter}")
    print(f"elapsed={elapsed:.3f}s")
    return shared_counter, expected


if __name__ == "__main__":
    args = [int(a) for a in sys.argv[1:]]
    if len(args) >= 1:
        run(args[0], args[1] if len(args) > 1 else ITERATIONS)
    else:
        run(5, ITERATIONS)
