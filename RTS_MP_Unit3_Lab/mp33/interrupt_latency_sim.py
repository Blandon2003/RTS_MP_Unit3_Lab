"""Mini Project 3.3: priority-based interrupt latency simulator.

Models a single simulated CPU, several periodic interrupt sources of different
priority, and the top-half / bottom-half split of Section 3.5:

  * each source thread waits for its period, stamps the assertion time, then takes the
    CPU lock and runs its top half. Top halves always get the CPU first, which is where
    interrupt latency (assertion -> first top-half instruction) is measured;
  * every top half enqueues a bottom-half job tagged with priority and assertion time
    into a heapq ready queue;
  * one worker thread executes the highest-priority ready bottom half, in slices. It
    releases the CPU between slices, so a newly arriving top half can take it and a
    higher-priority bottom half can displace the running one - the remainder of the
    displaced job is requeued (preemption, with its time counted).

Two distinct latencies are logged per occurrence, matching Section 3.5:
  interrupt latency = assertion -> top half starts on the CPU
  response latency  = assertion -> bottom half finishes

Usage:
    python interrupt_latency_sim.py --config baseline --seconds 20
    python interrupt_latency_sim.py --configs baseline,long_top,burst
    python interrupt_latency_sim.py --sweep 15,25,40,60,100,150 --seconds 10
"""

import argparse
import csv
import heapq
import json
import statistics
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS_DIR = HERE.parent / "results"
FIGURES_DIR = HERE.parent / "figures"

SLICE_MS = 1.0                     # bottom-half scheduling quantum (context-switch analogue)

# name, period_ms, priority (1 = highest), top_half_ms, bottom_half_ms
BASELINE = [
    {"name": "IRQ_HIGH", "period_ms": 40.0, "priority": 1, "top_ms": 1.0, "bottom_ms": 6.0},
    {"name": "IRQ_MED", "period_ms": 100.0, "priority": 2, "top_ms": 1.0, "bottom_ms": 10.0},
    {"name": "IRQ_LOW", "period_ms": 250.0, "priority": 3, "top_ms": 1.0, "bottom_ms": 18.0},
]

CONFIGS = {
    # well-behaved: every top half is short (Section 3.5 guidance)
    "baseline": [dict(s) for s in BASELINE],
    # deliberate violation: the highest-priority source runs a 12 ms top half
    "long_top": [dict(BASELINE[0], top_ms=12.0), dict(BASELINE[1]), dict(BASELINE[2])],
    # arrival-rate stress: the highest-priority source fires 2.7x more often
    "burst": [dict(BASELINE[0], period_ms=15.0), dict(BASELINE[1]), dict(BASELINE[2])],
}


def high_resolution_timer(enable=True):
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
        return True


class Simulation:
    def __init__(self, sources, seconds, slice_ms=SLICE_MS, timeline_window_ms=None,
                 timeline_start_s=None):
        self.sources = sources
        self.seconds = seconds
        self.slice_s = slice_ms / 1000.0
        self.cpu_lock = threading.Lock()          # the single simulated CPU
        self.heap_lock = threading.Lock()
        self.ready = []                            # (priority, seq, job)
        self.seq = 0
        self.worker_wake = threading.Condition(threading.Lock())
        self.log = []
        self.log_lock = threading.Lock()
        self.timeline = []
        self.timeline_lock = threading.Lock()
        self.timeline_window_s = (timeline_window_ms or 0) / 1000.0
        self.timeline_start_s = timeline_start_s
        self.start_time = None
        self.stop = threading.Event()

    # ---------------------------------------------------------------- helpers
    def record(self, row):
        with self.log_lock:
            self.log.append(row)

    def record_timeline(self, kind, source, t0, t1, extra=""):
        if self.timeline_window_s <= 0:
            return
        rel0 = t0 - self.start_time
        rel1 = t1 - self.start_time
        if self.timeline_start_s is not None and rel1 < self.timeline_start_s:
            return
        if rel0 > self.timeline_start_s + self.timeline_window_s:
            return
        with self.timeline_lock:
            self.timeline.append({"kind": kind, "source": source,
                                  "start_ms": round((max(0.0, rel0 - self.timeline_start_s)) * 1000, 3),
                                  "end_ms": round((min(self.timeline_start_s + self.timeline_window_s, rel1)
                                                   - self.timeline_start_s) * 1000, 3),
                                  "detail": extra})

    def enqueue(self, job):
        with self.heap_lock:
            self.seq += 1
            heapq.heappush(self.ready, (job["priority"], self.seq, job))
        with self.worker_wake:
            self.worker_wake.notify()

    def pop_ready(self):
        with self.heap_lock:
            if self.ready:
                return heapq.heappop(self.ready)[2]
        return None

    def peek_priority(self):
        with self.heap_lock:
            return self.ready[0][0] if self.ready else None

    # ---------------------------------------------------------------- top half
    def source_thread(self, source):
        period_s = source["period_ms"] / 1000.0
        top_s = source["top_ms"] / 1000.0
        next_fire = self.start_time + period_s
        while not self.stop.is_set():
            delay = next_fire - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            next_fire += period_s
            assert_time = time.perf_counter()

            # interrupt latency: waiting to actually get the CPU / run the first
            # top-half instruction
            with self.cpu_lock:
                top_start = time.perf_counter()
                time.sleep(top_s)
                top_end = time.perf_counter()
            self.record_timeline("top", source["name"], top_start, top_end,
                                 f"{source['top_ms']:.1f} ms")
            self.enqueue({"priority": source["priority"], "source": source["name"],
                          "assert_time": assert_time, "top_start": top_start,
                          "top_end": top_end,
                          "remaining": source["bottom_ms"] / 1000.0,
                          "preemptions": 0, "cpu_waits": 0})

    # ------------------------------------------------------------ bottom half
    def worker_thread(self):
        while True:
            job = self.pop_ready()
            if job is None:
                if self.stop.is_set():
                    return                      # queue drained, all occurrences logged
                with self.worker_wake:
                    self.worker_wake.wait(0.002)
                continue

            while job["remaining"] > 0.0:
                # strict priority among bottom halves: yield to higher-priority work
                higher = self.peek_priority()
                if higher is not None and higher < job["priority"]:
                    job["preemptions"] += 1
                    self.enqueue(job)
                    break

                wait0 = time.perf_counter()
                acquired = self.cpu_lock.acquire(timeout=0.05)
                cpu_wait = time.perf_counter() - wait0
                if not acquired:
                    job["preemptions"] += 1
                    self.enqueue(job)
                    break
                job["cpu_waits"] += 1 if cpu_wait > 0.0008 else 0
                slice_s = min(self.slice_s, job["remaining"])
                t0 = time.perf_counter()
                time.sleep(slice_s)
                actual = time.perf_counter() - t0
                self.cpu_lock.release()
                self.record_timeline("bottom", job["source"], t0, t0 + actual,
                                     f"slice {actual * 1000:.2f} ms")
                job["remaining"] -= actual

            if job["remaining"] <= 0.0:
                finish = time.perf_counter()
                self.record({
                    "source": job["source"],
                    "priority": job["priority"],
                    "assert_rel_ms": round((job["assert_time"] - self.start_time) * 1000, 3),
                    "interrupt_latency_ms": round((job["top_start"] - job["assert_time"]) * 1000, 3),
                    "top_half_ms": round((job["top_end"] - job["top_start"]) * 1000, 3),
                    "response_latency_ms": round((finish - job["assert_time"]) * 1000, 3),
                    "preemptions": job["preemptions"],
                    "cpu_waits": job["cpu_waits"],
                })

    # ------------------------------------------------------------------- drive
    def run(self):
        self.start_time = time.perf_counter() + 0.05
        threads = [threading.Thread(target=self.source_thread, args=(s,), name=s["name"])
                   for s in self.sources]
        worker = threading.Thread(target=self.worker_thread, name="cpu-worker")
        for t in threads:
            t.start()
        worker.start()
        time.sleep(self.seconds)
        self.stop.set()
        for t in threads:
            t.join()
        with self.worker_wake:
            self.worker_wake.notify_all()
        worker.join(timeout=10.0)              # drains the ready queue before exiting


def p95(values):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]


def summarise(rows):
    per_source = {}
    for row in rows:
        per_source.setdefault(row["source"], []).append(row)
    summary = {}
    for source, entries in per_source.items():
        response = [e["response_latency_ms"] for e in entries]
        interrupt = [e["interrupt_latency_ms"] for e in entries]
        summary[source] = {
            "occurrences": len(entries),
            "priority": entries[0]["priority"],
            "interrupt_latency_min_ms": round(min(interrupt), 3),
            "interrupt_latency_mean_ms": round(statistics.fmean(interrupt), 3),
            "interrupt_latency_max_ms": round(max(interrupt), 3),
            "response_min_ms": round(min(response), 3),
            "response_mean_ms": round(statistics.fmean(response), 3),
            "response_p95_ms": round(p95(response), 3),
            "response_max_ms": round(max(response), 3),
            "preempted_jobs": sum(1 for e in entries if e["preemptions"] > 0),
            "total_preemptions": sum(e["preemptions"] for e in entries),
        }
    return summary


def print_summary(config_name, summary, seconds):
    print(f"\n=== configuration '{config_name}' ({seconds:.0f}s run) ===")
    header = (f"{'source':<10}{'pri':>4}{'occ':>6}{'int min':>9}{'int mean':>10}"
              f"{'int max':>9}{'resp min':>10}{'resp mean':>11}{'resp p95':>10}"
              f"{'resp max':>10}{'preempt':>9}")
    print(header)
    for source in sorted(summary, key=lambda s: summary[s]["priority"]):
        st = summary[source]
        print(f"{source:<10}{st['priority']:>4}{st['occurrences']:>6}"
              f"{st['interrupt_latency_min_ms']:>9.2f}"
              f"{st['interrupt_latency_mean_ms']:>10.2f}"
              f"{st['interrupt_latency_max_ms']:>9.2f}"
              f"{st['response_min_ms']:>10.2f}{st['response_mean_ms']:>11.2f}"
              f"{st['response_p95_ms']:>10.2f}{st['response_max_ms']:>10.2f}"
              f"{st['total_preemptions']:>9}")


def write_outputs(payload):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / "mp33_results.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    fields = ["source", "priority", "assert_rel_ms", "interrupt_latency_ms",
              "top_half_ms", "response_latency_ms", "preemptions", "cpu_waits"]
    log_path = RESULTS_DIR / "mp33_occurrence_log.csv"
    rows = list(payload.get("occurrence_log", []))
    for block in payload.get("configs", {}).values():
        rows.extend(block.get("occurrence_log", []))
    payload.pop("occurrence_log", None)
    if rows:
        with log_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    print(f"\nwrote {out}\nwrote {log_path} ({len(rows)} occurrences)")


def run_configs(names, seconds, slice_ms, timeline_names=()):
    payload = {}
    for name in names:
        sim = Simulation([dict(s) for s in CONFIGS[name]], seconds, slice_ms=slice_ms,
                         timeline_window_ms=600.0 if name in timeline_names else 0.0,
                         timeline_start_s=2.0 if name in timeline_names else None)
        sim.run()
        summary = summarise(sim.log)
        print_summary(name, summary, seconds)
        payload[name] = {
            "sources": CONFIGS[name],
            "summary": summary,
            "occurrence_log": sim.log,
            "timeline": sim.timeline,
        }
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=str, default="baseline",
                        choices=sorted(CONFIGS))
    parser.add_argument("--configs", type=str, default=None,
                        help="comma separated list; runs each and compares")
    parser.add_argument("--sweep", type=str, default=None,
                        help="comma separated periods for the highest-priority source")
    parser.add_argument("--full", action="store_true",
                        help="run all three configurations and the arrival-rate sweep")
    parser.add_argument("--seconds", type=float, default=20.0)
    parser.add_argument("--sweep-seconds", type=float, default=10.0)
    parser.add_argument("--slice-ms", type=float, default=SLICE_MS)
    parser.add_argument("--timeline", action="store_true",
                        help="capture a representative window for the timeline plot")
    args = parser.parse_args()

    high_resolution_timer(True)
    payload = {"slice_ms": args.slice_ms, "configs": {}, "sweep": None}
    try:
        timeline_names = ()
        if args.full:
            names = sorted(CONFIGS)
            if args.timeline:
                timeline_names = ("baseline", "long_top")
        elif args.sweep:
            names = []
            payload["sweep"] = run_period_sweep(
                [float(p) for p in args.sweep.split(",")], args.sweep_seconds,
                args.slice_ms)
            print_period_sweep(payload["sweep"])
        else:
            names = ([args.config] if not args.configs
                     else [c.strip() for c in args.configs.split(",")])

        if names:
            payload["configs"] = run_configs(names, args.seconds, args.slice_ms,
                                             timeline_names)
        if args.full:
            payload["sweep"] = run_period_sweep(
                [15.0, 25.0, 40.0, 60.0, 100.0, 150.0], args.sweep_seconds,
                args.slice_ms)
            print_period_sweep(payload["sweep"])

        write_outputs(payload)
        timeline_payload = {n: b for n, b in payload["configs"].items() if b["timeline"]}
        if timeline_payload:
            write_timeline_figure(timeline_payload)
    finally:
        high_resolution_timer(False)


def run_period_sweep(periods, seconds, slice_ms):
    """Sweep the highest-priority source's arrival rate; watch the lowest one suffer."""
    rows = []
    for period in periods:
        sources = [dict(CONFIGS["baseline"][0], period_ms=period),
                   dict(CONFIGS["baseline"][1]), dict(CONFIGS["baseline"][2])]
        sim = Simulation(sources, seconds, slice_ms=slice_ms)
        sim.run()
        summary = summarise(sim.log)
        low = summary.get("IRQ_LOW", {})
        high = summary.get("IRQ_HIGH", {})
        med = summary.get("IRQ_MED", {})
        rows.append({
            "high_period_ms": period,
            "high_occurrences": high.get("occurrences", 0),
            "high_response_max_ms": high.get("response_max_ms"),
            "med_response_p95_ms": med.get("response_p95_ms"),
            "low_response_min_ms": low.get("response_min_ms"),
            "low_response_mean_ms": low.get("response_mean_ms"),
            "low_response_p95_ms": low.get("response_p95_ms"),
            "low_response_max_ms": low.get("response_max_ms"),
            "low_interrupt_max_ms": low.get("interrupt_latency_max_ms"),
            "low_preemptions": low.get("total_preemptions", 0),
            "low_occurrences": low.get("occurrences", 0),
        })
    return rows


def print_period_sweep(rows):
    print("\n=== arrival-rate sweep of IRQ_HIGH (lowest-priority victim: IRQ_LOW) ===")
    print(f"{'high period ms':>15}{'high occ':>10}{'low mean ms':>13}"
          f"{'low p95 ms':>12}{'low max ms':>12}{'low preempt':>12}")
    for r in rows:
        print(f"{r['high_period_ms']:>15.1f}{r['high_occurrences']:>10}"
              f"{r['low_response_mean_ms']:>13.2f}{r['low_response_p95_ms']:>12.2f}"
              f"{r['low_response_max_ms']:>12.2f}{r['low_preemptions']:>12}")


def write_timeline_figure(config_payload):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(len(config_payload), 1, figsize=(9.4, 2.6 * len(config_payload)),
                             sharex=True)
    if len(config_payload) == 1:
        axes = [axes]
    order = {"IRQ_HIGH": 2, "IRQ_MED": 1, "IRQ_LOW": 0, "top": 3}
    colors = {"IRQ_HIGH": "#c0504d", "IRQ_MED": "#4f8a3b", "IRQ_LOW": "#1f4e79"}
    for ax, (name, block) in zip(axes, config_payload.items()):
        events = block.get("timeline", [])
        if not events:
            ax.set_ylabel(name)
            ax.text(0.02, 0.5, "no timeline captured", transform=ax.transAxes)
            continue
        for event in events:
            lane = order.get(event["source"], 1) if event["kind"] == "bottom" else 3
            ax.barh(lane, event["end_ms"] - event["start_ms"], left=event["start_ms"],
                    height=0.6 if lane < 3 else 0.35,
                    color=colors.get(event["source"], "#7f7f7f"),
                    alpha=0.85 if event["kind"] == "bottom" else 1.0,
                    edgecolor="black", linewidth=0.3)
        ax.set_yticks([0, 1, 2, 3])
        ax.set_yticklabels(["IRQ_LOW", "IRQ_MED", "IRQ_HIGH", "top half"], fontsize=8)
        ax.set_ylabel(name)
        ax.grid(axis="x", alpha=0.3)
        ax.set_title(f"'{name}' - representative 600 ms window "
                     "(sliced bars = preemption points)", fontsize=10)
    axes[-1].set_xlabel("time in window (ms)")
    fig.tight_layout()
    png = FIGURES_DIR / "fig333_timeline.png"
    fig.savefig(png, dpi=170)
    plt.close(fig)
    print(f"wrote {png}")


if __name__ == "__main__":
    main()
