"""Mini Project 3.2 (flawed version): bounded buffer that deadlocks.

Two locks guard two shared structures:

  buffer_lock  -> the bounded list standing in for the shared buffer
  stats_lock   -> the counter dictionary both roles must update

Producers take buffer_lock first and then stats_lock. Consumers take them in the
opposite order, and BOTH roles evaluate their blocking condition (buffer full / buffer
empty) while already holding a lock, so a task that is waiting also holds - which is
precisely Coffman's hold-and-wait plus no-preemption. A watchdog thread reports the
stall, so the deadlock is confirmed without a manual kill.

Usage:
    python buffer_flawed.py --capacity 4 --producers 3 --consumers 3 --items 240
    python buffer_flawed.py --attempts 5          # repeat until the deadlock reproduces
"""

import argparse
import json
import sys
import threading
import time
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def high_resolution_timer():
    if sys.platform == "win32":
        try:
            __import__("ctypes").windll.winmm.timeBeginPeriod(1)
        except Exception:
            pass


class Registry:
    """Per-task lock bookkeeping so the watchdog can print a lock-wait snapshot."""

    def __init__(self):
        self._lock = threading.Lock()
        self.tasks = {}

    def set(self, name, **fields):
        with self._lock:
            entry = self.tasks.setdefault(name, {"name": name})
            entry.update(fields)
            entry["updated"] = time.perf_counter()

    def snapshot(self):
        with self._lock:
            return [dict(v) for v in self.tasks.values()]


def producer_body(name, buffer, buffer_lock, stats_lock, capacity, item_ids,
                  stats, registry, cadence_s, stop):
    for item in item_ids:
        if stop.is_set():
            return
        # FLAWED ORDER: buffer_lock first ...
        buffer_lock.acquire()
        registry.set(name, role="producer", holding=["buffer_lock"], waiting_for=None)

        # ... and the "wait for free space" condition is evaluated WHILE holding
        # buffer_lock, so the holder cannot be divested of the lock (no preemption).
        while len(buffer) >= capacity:
            if stop.is_set():
                buffer_lock.release()
                registry.set(name, holding=[], waiting_for=None)
                return
            registry.set(name, holding=["buffer_lock"],
                         waiting_for="buffer space (needs a consumer to pop)")
            time.sleep(0.001)

        buffer.append(item)

        # ... then stats_lock second (hold and wait: buffer_lock is still held).
        registry.set(name, holding=["buffer_lock"], waiting_for="stats_lock")
        stats_lock.acquire()
        registry.set(name, holding=["buffer_lock", "stats_lock"], waiting_for=None)
        stats["produced"] += 1
        stats_lock.release()
        registry.set(name, holding=["buffer_lock"], waiting_for=None)
        buffer_lock.release()
        registry.set(name, holding=[], waiting_for=None)

        if cadence_s:
            time.sleep(cadence_s)
    registry.set(name, role="producer", holding=[], waiting_for="finished")


def consumer_body(name, buffer, buffer_lock, stats_lock, count, stats, registry,
                  service_s, stop):
    for _ in range(count):
        if stop.is_set():
            return
        # FLAWED ORDER: stats_lock first ...
        stats_lock.acquire()
        registry.set(name, role="consumer", holding=["stats_lock"], waiting_for=None)

        # ... then buffer_lock - the inverted order that closes the circular wait.
        registry.set(name, holding=["stats_lock"], waiting_for="buffer_lock")
        while not buffer_lock.acquire(timeout=0.25):
            if stop.is_set():
                registry.set(name, holding=["stats_lock"], waiting_for="abandoned")
                stats_lock.release()
                registry.set(name, holding=[], waiting_for=None)
                return
            registry.set(name, holding=["stats_lock"], waiting_for="buffer_lock")

        # ... and "wait for an item" is evaluated while holding BOTH locks.
        while not buffer:
            if stop.is_set():
                buffer_lock.release()
                stats_lock.release()
                registry.set(name, holding=[], waiting_for=None)
                return
            registry.set(name, holding=["stats_lock", "buffer_lock"],
                         waiting_for="buffer item (needs a producer to push)")
            time.sleep(0.001)

        buffer.pop(0)
        stats["consumed"] += 1
        buffer_lock.release()
        registry.set(name, holding=["stats_lock"], waiting_for=None)
        stats_lock.release()
        registry.set(name, holding=[], waiting_for=None)

        if service_s:
            time.sleep(service_s)
    registry.set(name, role="consumer", holding=[], waiting_for="finished")


def watchdog_body(stop_event, registry, stats, buffer, capacity, report, stall_s, max_s):
    """Flag stalled progress; no manual kill needed."""
    start = time.perf_counter()
    last_progress = None
    last_change = time.perf_counter()
    while not stop_event.wait(0.2):
        now = time.perf_counter()
        progress = stats["produced"] + stats["consumed"]
        if progress != last_progress:
            last_progress = progress
            last_change = now
            continue
        stalled = now - last_change
        if stalled >= stall_s:
            report["deadlock"] = True
            report["stalled_s"] = round(stalled, 3)
            report["progress"] = {"produced": stats["produced"],
                                  "consumed": stats["consumed"],
                                  "buffer_occupancy": len(buffer)}
            report["snapshot"] = registry.snapshot()
            report["live_threads"] = sorted(
                t.name for t in threading.enumerate()
                if t is not threading.current_thread() and t.is_alive()
            )
            return
        if now - start >= max_s:
            report["timeout_without_deadlock"] = True
            report["progress"] = {"produced": stats["produced"],
                                  "consumed": stats["consumed"],
                                  "buffer_occupancy": len(buffer)}
            return


def run_once(capacity, producers, consumers, items_per_producer, stall_s, max_s):
    buffer = []
    buffer_lock = threading.Lock()
    stats_lock = threading.Lock()
    stats = {"produced": 0, "consumed": 0}
    registry = Registry()
    stop_event = threading.Event()
    report = {"deadlock": False, "capacity": capacity, "producers": producers,
              "consumers": consumers, "items_per_producer": items_per_producer}

    prod_threads = [
        threading.Thread(
            target=producer_body,
            args=(f"P{p}", buffer, buffer_lock, stats_lock, capacity,
                  list(range(p * items_per_producer, (p + 1) * items_per_producer)),
                  stats, registry, 0.0005, stop_event),
            name=f"P{p}", daemon=True,
        )
        for p in range(producers)
    ]
    total_items = producers * items_per_producer
    per_consumer = total_items // consumers
    cons_threads = [
        threading.Thread(
            target=consumer_body,
            args=(f"C{c}", buffer, buffer_lock, stats_lock, per_consumer, stats,
                  registry, 0.001, stop_event),
            name=f"C{c}", daemon=True,
        )
        for c in range(consumers)
    ]

    watch = threading.Thread(target=watchdog_body,
                             args=(stop_event, registry, stats, buffer, capacity, report,
                                   stall_s, max_s),
                             name="watchdog")
    start = time.perf_counter()
    for t in prod_threads + cons_threads:
        t.start()
    watch.start()
    watch.join()
    stop_event.set()

    if report["deadlock"]:
        report["elapsed_s"] = round(time.perf_counter() - start, 3)
        return report

    for t in prod_threads + cons_threads:
        t.join(timeout=10)
    report["completed"] = stats["consumed"] >= total_items
    report["elapsed_s"] = round(time.perf_counter() - start, 3)
    return report


def print_deadlock(report):
    print("=" * 78)
    print(f"WATCHDOG: DEADLOCK DETECTED - no progress for {report['stalled_s']:.2f}s "
          f"(run age {report['elapsed_s']:.2f}s)")
    prog = report["progress"]
    print(f"  items produced = {prog['produced']}, items consumed = {prog['consumed']}, "
          f"buffer occupancy = {prog['buffer_occupancy']} / capacity {report['capacity']}")
    print("  LOCK-WAIT SNAPSHOT (task -> holds -> waits for):")
    for entry in report["snapshot"]:
        holds = "+".join(entry.get("holding", [])) or "-"
        print(f"    {entry['name']:<3} role={entry.get('role', '?'):<9} "
              f"holds=[{holds:<26}] waiting_for={entry.get('waiting_for')}")
    print(f"  live task threads: {', '.join(report['live_threads'])}")
    print("  Coffman conditions in force: mutual exclusion (two exclusive locks),")
    print("  hold-and-wait (each role holds one lock while requesting the other),")
    print("  no preemption (a spinning holder cannot be divested of its lock),")
    print("  circular wait (P holds buffer_lock and needs stats_lock;")
    print("  C holds stats_lock and needs buffer_lock).")
    print("=" * 78)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capacity", type=int, default=4)
    parser.add_argument("--producers", type=int, default=3)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--items", type=int, default=240,
                        help="total items to produce across all producers")
    parser.add_argument("--stall-seconds", type=float, default=2.0)
    parser.add_argument("--max-seconds", type=float, default=25.0)
    parser.add_argument("--attempts", type=int, default=5)
    args = parser.parse_args()

    high_resolution_timer()
    items_per_producer = max(1, args.items // args.producers)

    summary = {"version": "flawed", "capacity": args.capacity,
               "producers": args.producers, "consumers": args.consumers,
               "items_per_producer": items_per_producer, "attempts": []}
    deadlocked = False
    for attempt in range(1, args.attempts + 1):
        print(f"\n--- attempt {attempt}: capacity={args.capacity} "
              f"producers={args.producers} consumers={args.consumers} "
              f"items/producer={items_per_producer} ---")
        report = run_once(args.capacity, args.producers, args.consumers,
                          items_per_producer, args.stall_seconds, args.max_seconds)
        if report["deadlock"]:
            print_deadlock(report)
        elif report.get("timeout_without_deadlock"):
            print("  watchdog: progress continued for the whole window - no stall")
        else:
            print(f"  run finished without deadlock in {report['elapsed_s']}s "
                  f"(consumed={report['progress']['consumed']})")

        summary["attempts"].append({
            "attempt": attempt,
            "deadlock": report["deadlock"],
            "stalled_s": report.get("stalled_s"),
            "elapsed_s": report.get("elapsed_s"),
            "progress": report.get("progress"),
            "snapshot": [
                {"task": s["name"], "role": s.get("role"), "holding": s.get("holding"),
                 "waiting_for": s.get("waiting_for")}
                for s in report.get("snapshot", [])
            ],
            "live_threads": report.get("live_threads"),
        })
        if report["deadlock"]:
            deadlocked = True
            break

    stuck = summary["attempts"][-1] if deadlocked else None
    summary["reproduced"] = deadlocked
    summary["reproduced_on_attempt"] = len(summary["attempts"]) if deadlocked else None
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / "mp32_deadlock_evidence.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nreproduced={deadlocked} "
          f"attempt={summary['reproduced_on_attempt']}  evidence -> {out}")
    if stuck:
        print("stuck tasks: " + ", ".join(
            f"{s['task']}({s['role']}) holds {s['holding']} wants {s['waiting_for']}"
            for s in stuck["snapshot"] if s["holding"]))
    return 2 if deadlocked else 1


if __name__ == "__main__":
    sys.exit(main())
