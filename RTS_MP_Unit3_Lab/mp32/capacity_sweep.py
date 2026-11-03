"""Mini Project 3.2, steps 6-7: throughput and blocking time versus buffer capacity.

Runs the corrected (lock-ordered) simulation over several buffer capacities with the
task counts and item totals held fixed, tabulates throughput and average blocking time,
identifies the point of diminishing returns, and plots the result.
"""

import csv
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import buffer_fixed  # noqa: E402
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS_DIR = HERE.parent / "results"
FIGURES_DIR = HERE.parent / "figures"

CAPACITIES = [1, 2, 5, 10, 25, 50]
PRODUCERS = 3
CONSUMERS = 3
TOTAL_ITEMS = 900
SERVICE_MS = 0.5
REPEATS = 5
BURST = 40                 # items produced back-to-back before the producer goes quiet
BURST_PAUSE_MS = 8.0       # arrival gap that lets a small buffer drain


def main():
    buffer_fixed.high_resolution_timer()
    rows = []
    for capacity in CAPACITIES:
        runs = [
            buffer_fixed.run_once(capacity=capacity, producers=PRODUCERS,
                                 consumers=CONSUMERS, total_items=TOTAL_ITEMS,
                                 service_s=SERVICE_MS / 1000.0, burst=BURST,
                                 burst_pause_s=BURST_PAUSE_MS / 1000.0)
            for _ in range(REPEATS)
        ]
        deadlocks = sum(r["deadlock"] for r in runs)
        row = {
            "capacity": capacity,
            "throughput_items_per_s": round(
                statistics.median(r["throughput_items_per_s"] for r in runs), 1),
            "elapsed_s": round(statistics.fmean(r["elapsed_s"] for r in runs), 3),
            "producer_wait_mean_ms": round(
                statistics.fmean(r["producer_wait"]["mean_ms"] for r in runs), 3),
            "consumer_wait_mean_ms": round(
                statistics.fmean(r["consumer_wait"]["mean_ms"] for r in runs), 3),
            "total_blocking_mean_ms": round(
                statistics.fmean(r["producer_wait"]["mean_ms"] +
                                 r["consumer_wait"]["mean_ms"] for r in runs), 3),
            "producer_blocked_events": int(
                statistics.fmean(r["producer_blocked"] for r in runs)),
            "consumer_blocked_events": int(
                statistics.fmean(r["consumer_blocked"] for r in runs)),
            "max_producer_wait_ms": round(
                max(r["producer_wait"]["max_ms"] for r in runs), 3),
            "max_consumer_wait_ms": round(
                max(r["consumer_wait"]["max_ms"] for r in runs), 3),
            "deadlocks": deadlocks,
            "balanced": all(r["balanced"] for r in runs),
        }
        rows.append(row)
        print(f"capacity={capacity:>3}  throughput={row['throughput_items_per_s']:>8.1f} "
              f"items/s  producer wait={row['producer_wait_mean_ms']:>7.3f} ms  "
              f"consumer wait={row['consumer_wait_mean_ms']:>7.3f} ms  "
              f"blocked(P/C)={row['producer_blocked_events']}/"
              f"{row['consumer_blocked_events']}  deadlocks={deadlocks}")

    # marginal throughput gain per added slot, to locate diminishing returns
    print("\nmarginal gain per extra buffer slot:")
    for prev, cur in zip(rows, rows[1:]):
        step = cur["capacity"] - prev["capacity"]
        gain = cur["throughput_items_per_s"] - prev["throughput_items_per_s"]
        print(f"  {prev['capacity']:>2} -> {cur['capacity']:>2}: "
              f"{gain:+8.1f} items/s over {step} slots "
              f"= {gain / step:+7.2f} items/s per slot")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = RESULTS_DIR / "mp32_capacity_sweep.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    (RESULTS_DIR / "mp32_capacity_sweep.json").write_text(
        json.dumps({"producers": PRODUCERS, "consumers": CONSUMERS,
                    "total_items": TOTAL_ITEMS, "service_ms": SERVICE_MS,
                    "repeats": REPEATS, "burst": BURST,
                    "burst_pause_ms": BURST_PAUSE_MS, "rows": rows}, indent=2),
        encoding="utf-8")

    caps = [r["capacity"] for r in rows]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8.2, 6.2), sharex=True)
    ax1.plot(caps, [r["throughput_items_per_s"] for r in rows], "o-", color="#1f4e79",
             lw=2, label="throughput")
    ax1.set_ylabel("throughput (items/s)")
    ax1.set_xscale("log")
    ax1.grid(alpha=0.3)
    ax1.legend(loc="lower right")
    ax1.set_title("MP3.2 corrected producer-consumer: throughput and blocking\n"
                  f"{PRODUCERS} producers / {CONSUMERS} consumers, "
                  f"{TOTAL_ITEMS} items, {SERVICE_MS:.1f} ms service time")

    ax2.plot(caps, [r["producer_wait_mean_ms"] for r in rows], "s-", color="#c0504d",
             lw=2, label="producer wait for space")
    ax2.plot(caps, [r["consumer_wait_mean_ms"] for r in rows], "^-", color="#4f8a3b",
             lw=2, label="consumer wait for items")
    ax2.set_ylabel("mean blocking time (ms)")
    ax2.set_xlabel("buffer capacity (items)")
    ax2.set_xscale("log")
    ax2.set_xticks(caps)
    ax2.set_xticklabels(caps)
    ax2.grid(alpha=0.3)
    ax2.legend(loc="best")
    fig.tight_layout()
    png = FIGURES_DIR / "fig323_throughput_vs_capacity.png"
    fig.savefig(png, dpi=170)
    plt.close(fig)

    print(f"\nwrote {csv_path}")
    print(f"wrote {png}")


if __name__ == "__main__":
    main()
