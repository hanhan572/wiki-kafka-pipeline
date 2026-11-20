"""Offline evaluation of the edit-rate anomaly detectors on REAL stored data with INJECTED spikes.

Real streams have no ground-truth anomaly labels, so we build them: take the real "edits per 10 s" series
(global + busiest wikis), multiply randomly chosen buckets by (1 + m) for m in {0.5, 1, 2, 4}, and ask each detector
to find them. Metrics: recall per spike size, and false alarms per hour on the untouched buckets.

Detectors compared (same warm-up, same Anscombe transform):
  static  – z-score against mean/std of the first 25 % of the series (no adaptation)
  ewma    – exponentially weighted moving average level
  ar-rls  – online AR(3) fitted by recursive least squares  (the model used in the pipeline)

usage: python scripts/evaluate_anomaly.py   (needs the stack running and a few hours of data in Postgres)
"""
import json
import math
import os
import pathlib
import random
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import psycopg

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from pipeline.anomaly import EwmaModel, RlsArModel, anscombe  # noqa: E402

DSN = os.environ.get("DATABASE_URL", "postgresql://wiki:wiki@localhost:5433/wiki")
BUCKET, WARMUP, MIN_COUNT = 10, 30, 6
MAGNITUDES = [0.5, 1.0, 2.0, 4.0]
THRESHOLDS = [2, 2.5, 3, 3.5, 4, 4.5, 5, 6, 7, 8]
OPERATING_Z = 4.0
OUT = pathlib.Path(__file__).resolve().parents[1]


def load_series(top_wikis: int = 5):
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT wiki, floor(extract(epoch FROM event_time) / %s)::bigint AS b, count(*) FROM edits WHERE title NOT LIKE '[demo]%%' GROUP BY 1, 2", (BUCKET,))
        rows = cur.fetchall()
    buckets = sorted({b for _, b, _ in rows})
    lo, hi = buckets[0] + 1, buckets[-1] - 1             # drop partial first / last bucket
    idx = {b: i for i, b in enumerate(range(lo, hi + 1))}
    per_wiki: dict[str, np.ndarray] = {}
    for w, b, n in rows:
        if b in idx:
            per_wiki.setdefault(w, np.zeros(len(idx)))[idx[b]] += n
    total = sum(per_wiki.values())
    # Collection outages (laptop asleep, broker restart) show up as runs of all-zero buckets: that is a gap in OUR data,
    # not a quiet Wikipedia, so cut those runs out instead of letting the models "learn" them.
    keep = np.ones(len(total), dtype=bool)
    run_start = None
    for i, v in enumerate(list(total) + [1]):
        if v == 0 and run_start is None:
            run_start = i
        elif v != 0 and run_start is not None:
            if i - run_start >= 3:
                keep[run_start:i] = False
            run_start = None
    per_wiki = {w: a[keep] for w, a in per_wiki.items()}
    total = total[keep]
    print(f"removed {int((~keep).sum())} outage buckets ({(~keep).sum() * BUCKET / 60:.0f} min) from the {len(keep)}-bucket window")
    busiest = sorted(per_wiki, key=lambda w: -per_wiki[w].sum())[:top_wikis]
    series = {"ALL": total, **{w: per_wiki[w] for w in busiest}}
    return series, int(keep.sum()) * BUCKET / 3600


def inject(x: np.ndarray, rng: random.Random, n_spikes: int):
    x = x.copy()
    labels = np.zeros(len(x), dtype=int)                # 0 = clean, otherwise the magnitude index + 1
    mags = np.zeros(len(x))
    candidates = list(range(WARMUP + 20, len(x) - 5))
    rng.shuffle(candidates)
    chosen: list[int] = []
    for c in candidates:
        if len(chosen) >= n_spikes:
            break
        if all(abs(c - o) > 25 for o in chosen) and x[c] >= 2:
            chosen.append(c)
    for c in chosen:
        m = rng.choice(MAGNITUDES)
        x[c] = round(x[c] * (1 + m))
        labels[c], mags[c] = 1, m
    return x, labels, mags


def z_static(x: np.ndarray) -> np.ndarray:
    y = np.array([anscombe(v) for v in x])
    train = y[: max(int(len(y) * 0.25), 20)]
    return (y - train.mean()) / max(train.std(), 0.5)


def z_online(model, x: np.ndarray) -> np.ndarray:
    return np.array([model.update(v)[1] for v in x])


def detectors(x):
    return {"static": z_static(x), "ewma": z_online(EwmaModel(), x), "ar-rls": z_online(RlsArModel(), x)}


BUSY = ("ALL", "enwiki")      # high-volume series (tens of edits per 10 s); the others see only a handful


def metrics_at(z, lab, mag, clean_hours, t):
    pos, neg = lab == 1, lab == 0
    flagged = z >= t
    tp, fp, fn = int((flagged & pos).sum()), int((flagged & neg).sum()), int((~flagged & pos).sum())
    prec, rec = tp / max(tp + fp, 1), tp / max(tp + fn, 1)
    by_mag = {str(m): round(float((flagged & (mag == m)).sum() / max((mag == m).sum(), 1)), 3) for m in MAGNITUDES}
    return {"precision": round(prec, 3), "recall": round(rec, 3), "f1": round(2 * prec * rec / max(prec + rec, 1e-9), 3),
            "false_alarms_per_hour": round(fp / max(clean_hours, 1e-9), 2), "recall_by_spike_size": by_mag, "n_spikes": int(pos.sum())}


def drift(x: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """SIMULATED slow change of the level (like the day/night cycle): scale 0.6x -> 2.0x over the window, re-sampled as Poisson."""
    f = np.linspace(0.6, 2.0, len(x))
    return rng.poisson(x * f).astype(float)


def main() -> None:
    series, hours = load_series()
    print(f"loaded {len(series)} series, {hours:.1f} h of (outage-free) data, {{ {', '.join(f'{k}: {int(v.sum())}' for k, v in series.items())} }}")
    names = ("static", "ewma", "ar-rls")
    results = {"hours_of_data": round(hours, 2), "series": list(series), "operating_z": OPERATING_Z, "min_count": MIN_COUNT,
               "scenarios": {}, "threshold_sweep": {}}
    example = None
    for scenario in ("real", "drift"):
        rng, nprng = random.Random(7), np.random.default_rng(11)
        pooled = {g: {d: {"z": [], "label": [], "mag": []} for d in names} for g in ("busy", "small")}
        clean_hours = {"busy": 0.0, "small": 0.0}
        for name, x0 in series.items():
            group = "busy" if name in BUSY else "small"
            x = drift(x0, nprng) if scenario == "drift" else x0
            for rep in range(8):
                xi, labels, mags = inject(x, rng, n_spikes=max(len(x) // 100, 5))
                zs = detectors(xi)
                valid = np.arange(len(xi)) > WARMUP
                keep = valid & (xi >= MIN_COUNT)
                clean_hours[group] += valid.sum() * BUCKET / 3600
                for d, z in zs.items():
                    pooled[group][d]["z"].append(z[keep]); pooled[group][d]["label"].append(labels[keep]); pooled[group][d]["mag"].append(mags[keep])
                if name == "ALL" and rep == 0 and scenario == "real":
                    example = (xi, labels, zs["ar-rls"])
        results["scenarios"][scenario], results["threshold_sweep"][scenario] = {}, {}
        for g in pooled:
            results["scenarios"][scenario][g], results["threshold_sweep"][scenario][g] = {}, {}
            for d in names:
                z, lab, mag = (np.concatenate(pooled[g][d][k]) for k in ("z", "label", "mag"))
                results["scenarios"][scenario][g][d] = metrics_at(z, lab, mag, clean_hours[g], OPERATING_Z)
                results["threshold_sweep"][scenario][g][d] = [{"z": t, **{k: v for k, v in metrics_at(z, lab, mag, clean_hours[g], t).items()
                                                                        if k in ("recall", "precision", "false_alarms_per_hour")}} for t in THRESHOLDS]
    (OUT / "docs").mkdir(exist_ok=True)
    (OUT / "docs" / "anomaly_results.json").write_text(json.dumps(results, indent=2))

    # ---- figure
    fig, ax = plt.subplots(1, 4, figsize=(23, 4.8))
    colors = {"static": "#8a8a8a", "ewma": "#d9822b", "ar-rls": "#3b6ea8"}
    for d, c in results["threshold_sweep"]["real"]["busy"].items():
        ax[0].plot([p["false_alarms_per_hour"] for p in c], [p["recall"] for p in c], marker="o", label=d, color=colors[d])
    ax[0].set_xscale("symlog", linthresh=0.5); ax[0].set_xlabel("false alarms per hour (symlog)"); ax[0].set_ylabel("recall (injected spikes found)")
    ax[0].set_title("Real series, high-volume: recall vs false alarms\n(threshold sweep z = 2 … 8)"); ax[0].legend(); ax[0].grid(alpha=.3)
    w = 0.26
    for i, d in enumerate(names):
        ax[1].bar(np.arange(len(MAGNITUDES)) + (i - 1) * w, [results["scenarios"]["real"]["busy"][d]["recall_by_spike_size"][str(m)] for m in MAGNITUDES], w, label=d, color=colors[d])
    ax[1].set_xticks(range(len(MAGNITUDES))); ax[1].set_xticklabels([f"+{int(m * 100)}%" for m in MAGNITUDES])
    ax[1].set_title(f"Real series, high-volume: recall by spike size (z ≥ {OPERATING_Z})"); ax[1].set_xlabel("injected increase over the normal level"); ax[1].set_ylim(0, 1.05); ax[1].legend()
    settings = [("real", "busy", "real\nhigh-volume"), ("real", "small", "real\nsmall wikis"), ("drift", "busy", "drifting level\nhigh-volume"), ("drift", "small", "drifting level\nsmall wikis")]
    for i, d in enumerate(names):
        ax[2].bar(np.arange(len(settings)) + (i - 1) * w, [results["scenarios"][sc][g][d]["f1"] for sc, g, _ in settings], w, label=d, color=colors[d])
    ax[2].set_xticks(range(len(settings))); ax[2].set_xticklabels([l for _, _, l in settings], fontsize=8)
    ax[2].set_title(f"F1 score at z ≥ {OPERATING_Z}"); ax[2].set_ylim(0, 1.05); ax[2].legend()
    xi, labels, z = example
    sl = slice(0, min(len(xi), 330))
    ax[3].plot(xi[sl], color="#3b6ea8", lw=1, label="edits / 10 s (with injected spikes)")
    hit = np.where((z[sl] >= OPERATING_Z) & (xi[sl] >= MIN_COUNT))[0]
    ax[3].scatter(hit, xi[sl][hit], color="#d64545", marker="x", s=60, label="flagged by AR-RLS", zorder=3)
    inj = np.where(labels[sl] == 1)[0]
    ax[3].scatter(inj, xi[sl][inj], facecolors="none", edgecolors="#4f9d69", s=130, label="injected spike", zorder=2)
    ax[3].set_title("Example: global series"); ax[3].set_xlabel("10-second bucket"); ax[3].legend(loc="upper left", fontsize=8)
    plt.tight_layout(); (OUT / "images").mkdir(exist_ok=True); plt.savefig(OUT / "images" / "anomaly_evaluation.png", dpi=120); plt.close()

    for sc in results["scenarios"]:
        for g in results["scenarios"][sc]:
            print(f"\n== {sc} / {g} series (z >= {OPERATING_Z}) ==")
            for d, m in results["scenarios"][sc][g].items():
                print(f"{d:7s} P={m['precision']:.2f} R={m['recall']:.2f} F1={m['f1']:.2f} FA/h={m['false_alarms_per_hour']:.2f} n={m['n_spikes']} by size={m['recall_by_spike_size']}")


if __name__ == "__main__":
    main()
