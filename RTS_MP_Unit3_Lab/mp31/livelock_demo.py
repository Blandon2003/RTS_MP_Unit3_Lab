"""Mini Project 3.1, lab task 3: livelock reproduced with a retry counter.

Two tasks each need both shared locks but acquire them in opposite order. They start in
lock-step (threading.Barrier), each grabs one lock, and then both poll for the other
lock with non-blocking acquire() and an identical back-off. Neither releases the lock it
already holds, so both stay fully active - burning CPU on retries - while the
completed-work counter never advances. That is livelock as defined in Section 3.8, as
opposed to deadlock, where the tasks would be blocked inside the kernel instead.

The run is ended externally by a deadline (the tasks themselves never time out, which
is the point). `--mode ordered` repeats the same experiment with one agreed acquisition
order for both tasks, and progress is restored immediately.
"""

import argparse
import threading
import time

lock_a = threading.Lock()
lock_b = threading.Lock()
stats_lock = threading.Lock()
START_BARRIER = threading.Barrier(2)
REQUEST_BARRIER = threading.Barrier(2)


def worker(task_id, first, second, ordered, state, stop, retry_ms):
    START_BARRIER.wait()                    # both tasks enter at the same instant
    first.acquire()                         # hold one resource
    if not ordered:
        REQUEST_BARRIER.wait()              # both now hold one and want the other

    while not stop.is_set():
        if second.acquire(blocking=False):  # busy retry - never blocks
            with stats_lock:
                state["completed"] += 1
                state["rounds_seen"] += 1
            second.release()
            if ordered:
                first.release()
                time.sleep(retry_ms / 1000.0)
                first.acquire()
                continue
            break
        with stats_lock:
            state["retries"][task_id] += 1
        time.sleep(retry_ms / 1000.0)       # identical deterministic back-off
    first.release()


def monitor(state, stop, sample_every_s, samples):
    while not stop.wait(sample_every_s):
        with stats_lock:
            samples.append((round(time.perf_counter() - state["t0"], 2),
                            state["completed"],
                            sum(state["retries"])))


def run(mode, seconds, retry_ms):
    ordered = mode == "ordered"
    state = {"retries": [0, 0], "completed": 0, "rounds_seen": 0,
             "t0": time.perf_counter()}
    stop = threading.Event()
    samples = []

    workers = [
        threading.Thread(target=worker,
                         args=(0, lock_a, lock_b, ordered, state, stop, retry_ms)),
        threading.Thread(target=worker,
                         args=(1, lock_b if not ordered else lock_a,
                               lock_a if not ordered else lock_b,
                               ordered, state, stop, retry_ms)),
    ]
    watcher = threading.Thread(target=monitor, args=(state, stop, seconds / 4, samples))

    for w in workers:
        w.start()
    watcher.start()
    time.sleep(seconds)
    stop.set()
    for w in workers:
        w.join()
    watcher.join()

    retries = sum(state["retries"])
    print(f"mode={mode}  duration={seconds:.1f}s  back-off={retry_ms:.2f} ms")
    print(f"  completed critical sections = {state['completed']}")
    print(f"  retries (back-off events)   = {retries} "
          f"(task0={state['retries'][0]}, task1={state['retries'][1]})")
    print("  sampled (elapsed s, completed, total retries):")
    for t, completed, retry_total in samples:
        print(f"    t={t:5.2f}s  completed={completed:<4}  retries={retry_total}")
    if not ordered:
        print("  => livelock: both tasks stayed runnable the whole window "
              f"({retries} retries) and produced {state['completed']} completed "
              "critical sections (Section 3.8)")
    else:
        print(f"  => consistent lock ordering: {state['completed']} completions in the "
              "same window with the same polling loop")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["livelock", "ordered"], default="livelock")
    parser.add_argument("--seconds", type=float, default=3.0)
    parser.add_argument("--retry-ms", type=float, default=0.5)
    args = parser.parse_args()
    run(args.mode, args.seconds, args.retry_ms)
