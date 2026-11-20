"""Draws docs/architecture.png (run: python scripts/make_diagram.py)."""
import pathlib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

fig, ax = plt.subplots(figsize=(17, 8.2))
ax.set_xlim(0, 17); ax.set_ylim(0, 8.2); ax.axis("off")
C = {"src": "#e8eef7", "app": "#dfeede", "kafka": "#fbe3c8", "db": "#e6dff3", "ui": "#f9dede"}


def box(x, y, w, h, title, sub="", kind="app", bold=True):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12", fc=C[kind], ec="#555", lw=1.2))
    ax.text(x + w / 2, y + h * (0.64 if sub else 0.5), title, ha="center", va="center", fontsize=10.5, fontweight="bold" if bold else None)
    if sub:
        ax.text(x + w / 2, y + h * 0.28, sub, ha="center", va="center", fontsize=8.2, color="#333")


def arrow(x1, y1, x2, y2, label="", rad=0.0, color="#444"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="-|>", lw=1.6, color=color, connectionstyle=f"arc3,rad={rad}"))
    if label:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.17, label, ha="center", fontsize=8, color="#444")


# column 1-2: source + producer
box(0.2, 3.5, 2.2, 1.2, "Wikimedia\nEventStreams", "live SSE feed\n(real edits, no API key)", "src")
box(2.85, 3.5, 2.45, 1.2, "Producer", "python -m pipeline.producer\nfilter · flatten · key=wiki", "app")
# column 3: raw topic
box(5.7, 3.35, 2.3, 1.5, "Kafka topic\nwikipedia.edits", "3 partitions · key = wiki\nJSON events", "kafka")
# consumers
box(8.8, 6.3, 2.5, 1.2, "db-sink", "group edits-to-postgres\ncopy every event", "app")
box(8.8, 4.1, 2.5, 1.2, "stats-processor", "group edit-stats-processor\n1-min windows + alert rules", "app")
box(8.8, 1.2, 2.5, 1.2, "anomaly-detector", "group rate-anomaly-detector\nonline AR(3) forecaster  (bonus)", "app")
# derived topics
box(8.8, 2.65, 2.5, 1.0, "wikipedia.edit-alerts", "derived topic", "kafka", bold=True)
box(11.95, 0.2, 2.5, 1.0, "wikipedia.rate-scores", "derived topic", "kafka")
box(12.0, 2.55, 2.1, 1.2, "derived-sink", "group derived-to-postgres", "app")
# database
box(14.7, 3.35, 2.1, 2.9, "PostgreSQL", "edits\nedit_stats_minute\nalerts\nrate_scores", "db")
box(14.7, 0.5, 2.1, 1.3, "Streamlit\ndashboard", "auto-refresh 5 s\n7 numbers · 6 charts", "ui")

arrow(2.4, 4.1, 2.85, 4.1)
arrow(5.3, 4.1, 5.7, 4.1, "events")
arrow(8.0, 4.6, 8.8, 6.7, rad=-0.1); arrow(8.0, 4.1, 8.8, 4.7); arrow(8.0, 3.7, 8.8, 1.8, rad=0.1)
arrow(11.3, 6.9, 14.7, 5.9, "INSERT edits", rad=-0.1)
arrow(11.3, 4.7, 14.7, 4.7, "UPSERT 1-min stats")
arrow(10.05, 4.1, 10.05, 3.65, "alerts")
arrow(11.3, 3.15, 12.0, 3.15)
arrow(11.3, 1.55, 12.0, 0.9, "scores"); 
arrow(13.2, 1.2, 13.2, 2.55)
arrow(14.1, 3.15, 14.7, 3.75, "INSERT")
arrow(15.75, 3.35, 15.75, 1.8, "SELECT (read-only)")
ax.text(0.2, 7.7, "Real-time Wikipedia edit pipeline", fontsize=16, fontweight="bold")
ax.text(0.2, 7.3, "one `docker compose up` starts Kafka (KRaft), Postgres, 5 Python services and the dashboard", fontsize=10, color="#444")
out = pathlib.Path(__file__).resolve().parents[1] / "docs" / "architecture.png"
plt.savefig(out, dpi=140, bbox_inches="tight", facecolor="white"); print("saved", out)
