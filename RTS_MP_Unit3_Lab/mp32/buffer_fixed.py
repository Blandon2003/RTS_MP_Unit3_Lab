"""Mini Project 3.2 (corrected version): lock-ordered bounded buffer.

The only structural change from buffer_flawed.py is the lock discipline:

  * EVERY task acquires buffer_lock before stats_lock, never the reverse, so circular
    wait (Coffman condition 4) cannot form.
  * The blocking conditions ("wait for space", "wait for an item") are evaluated on a
    threading.Condition bound to buffer_lock, so a waiting task RELEASES buffer_lock
    instead of spinning while holding it - it no longer holds-and-waits.

The watchdog is kept in place and must stay silent. Per-item producer and consumer
blocking times and overall throughput are recorded for the capacity experiment.

Usage:
    python buffer_fixed.py --capacity 5 --producers 3 --consumers 3 --items 240
"""

import argparse
import json
import statistics
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def high_resolution_timer():
    if sys.platform == "win32":
        try:
            __import__("ctypes").windll.winmm.timeBeginPeriod(1)
        except Exception:
            pass


@contextmanager
def both_locks(buffer_lock, stats_lock):
    """The agreed global order: buffer_lock first, then stats_lock, always."""
    buffer_lock.acquire()
    try:
        stats_lock.acquire()
        try:
            yield
        finally:
            stats_lock.release()
    finally:
        buffer_lock.release()


def producer_body(cond, stats_lock, buffer, capacity, item_ids, stats, waits,
                  burst, burst_pause_s):
    for index, item in enumerate(item_ids):
        t0 = time.perf_counter()
        with cond:                                  # holds buffer_lock
            while len(buffer) >= capacity:          # wait for space, lock released
                cond.wait()
            buffer.append(item)
            waited = time.perf_counter() - t0
            waits.append(waited)
            with stats_lock:                        # same order as every other task
                stats["produced"] += 1
                stats["producer_blocked"] += 1 if waited > 0.0005 else 0
            cond.notify_all()                       # wake consumers after publishing
        # Bursty arrival: emit `burst` items back to back, then go quiet for a while.
        # This is the traffic pattern in which buffer capacity actually buys something.
        if burst and (index + 1) % burst == 0:
            time.sleep(burst_pause_s)


def consumer_body(cond, stats_lock, buffer, count, stats, waits, service_s):
    for _ in range(count):
        t0 = time.perf_counter()
        with cond:
            while not buffer:                       # wait for an item, lock released
                cond.wait()
            buffer.pop(0)
            waited = time.perf_counter() - t0
            waits.append(waited)
            with stats_lock:
                stats["consumed"] += 1
                stats["consumer_blocked"] += 1 if waited > 0.0005 else 0
            cond.notify_all()                       # wake producers after freeing space
        if service_s:
            time.sleep(service_s)


def watchdog_body(stop_event, stats, buffer, report, stall_s, max_s, total_items):
    start = time.perf_counter()
    last = None
    last_change = time.perf_counter()
    while not stop_event.wait(0.2):
        now = time.perf_counter()
        progress = stats["produced"] + stats["consumed"]
        if progress != last:
            last = progress
            last_change = now
        elif now - last_change >= stall_s:
            report["deadlock"] = True
            report["stalled_s"] = round(now - last_change, 3)
            return
        if now - start >= max_s:
            report["timeout_without_deadlock"] = True
            return


def run_once(capacity=5, producers=3, consumers=3, total_items=240, service_s=0.001,
             cadence_s=0.0005, stall_s=3.0, max_s=60.0, burst=0, burst_pause_s=0.008):
    buffer = []
    buffer_lock = threading.Lock()
    stats_lock = threading.Lock()
    cond = threading.Condition(buffer_lock)
    stats = {"produced": 0, "consumed": 0, "producer_blocked": 0, "consumer_blocked": 0}
    producer_waits, consumer_waits = [], []
    report = {"deadlock": False}
    stop_event = threading.Event()

    per_producer = total_items // producers
    per_consumer = total_items // consumers

    prod_threads = [
        threading.Thread(target=producer_body,
                         args=(cond, stats_lock, buffer, capacity,
                               list(range(p * per_producer, (p + 1) * per_producer)),
                               stats, producer_waits, burst, burst_pause_s),
                         name=f"P{p}")
        for p in range(producers)
    ]
    cons_threads = [
        threading.Thread(target=consumer_body,
                         args=(cond, stats_lock, buffer, per_consumer, stats,
                               consumer_waits, service_s),
                         name=f"C{c}")
        for c in range(consumers)
    ]
    watch = threading.Thread(target=watchdog_body,
                             args=(stop_event, stats, buffer, report, stall_s, max_s,
                                   total_items),
                             name="watchdog")

    start = time.perf_counter()
    for t in prod_threads + cons_threads:
        t.start()
    watch.start()
    for t in prod_threads + cons_threads:
        t.join()
    elapsed = time.perf_counter() - start
    stop_event.set()
    watch.join()

    def summarise(values):
        if not values:
            return {"mean_ms": 0.0, "max_ms": 0.0, "count_blocked": 0}
        return {
            "mean_ms": round(statistics.fmean(values) * 1000, 3),
            "max_ms": round(max(values) * 1000, 3),
            "p95_ms": round(sorted(values)[min(len(values) - 1,
                                               int(0.95 * (len(values) - 1)))] * 1000, 3),
        }

    result = {
        "capacity": capacity,
        "producers": producers,
        "consumers": consumers,
        "total_items": producers * per_producer,
        "produced": stats["produced"],
        "consumed": stats["consumed"],
        "balanced": stats["produced"] == stats["consumed"],
        "elapsed_s": round(elapsed, 3),
        "throughput_items_per_s": round(stats["consumed"] / elapsed, 1),
        "producer_wait": summarise(producer_waits),
        "consumer_wait": summarise(consumer_waits),
        "producer_blocked": stats["producer_blocked"],
        "consumer_blocked": stats["consumer_blocked"],
        "deadlock": report["deadlock"],
        "timeout_without_deadlock": report.get("timeout_without_deadlock", False),
    }
    return result


def print_result(result):
    print(f"capacity={result['capacity']} producers={result['producers']} "
          f"consumers={result['consumers']} items={result['total_items']}")
    if result["deadlock"]:
        print("  WATCHDOG: DEADLOCK DETECTED (fix failed)")
    else:
        print("  watchdog stayed silent - no stall, fix verified")
    print(f"  produced={result['produced']} consumed={result['consumed']} "
          f"balanced={result['balanced']}")
    print(f"  elapsed={result['elapsed_s']}s  throughput={result['throughput_items_per_s']} items/s")
    pw, cw = result["producer_wait"], result["consumer_wait"]
    print(f"  producer wait for space: mean={pw['mean_ms']:.3f}ms "
          f"p95={pw.get('p95_ms', 0):.3f}ms max={pw['max_ms']:.3f}ms "
          f"(blocked {result['producer_blocked']} times)")
    print(f"  consumer wait for items: mean={cw['mean_ms']:.3f}ms "
          f"p95={cw.get('p95_ms', 0):.3f}ms max={cw['max_ms']:.3f}ms "
          f"(blocked {result['consumer_blocked']} times)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capacity", type=int, default=5)
    parser.add_argument("--producers", type=int, default=3)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--items", type=int, default=240)
    parser.add_argument("--service-ms", type=float, default=1.0)
    parser.add_argument("--burst", type=int, default=0,
                        help="items produced back-to-back before pausing (0 = steady flow)")
    parser.add_argument("--burst-pause-ms", type=float, default=8.0)
    parser.add_argument("--json", action="store_true", help="dump machine-readable result")
    args = parser.parse_args()

    high_resolution_timer()
    result = run_once(capacity=args.capacity, producers=args.producers,
                      consumers=args.consumers, total_items=args.items,
                      service_s=args.service_ms / 1000.0, burst=args.burst,
                      burst_pause_s=args.burst_pause_ms / 1000.0)
    print_result(result)
    if args.json:
        print(json.dumps(result, indent=2))
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        (RESULTS_DIR / "mp32_fixed_run.json").write_text(
            json.dumps(result, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
