"""Stream processor #2 (group `rate-anomaly-detector`) — BONUS: sequential modelling on the stream.

Builds the "edits per 10 s" series (globally and for every busy wiki) from event time, scores each closed bucket
with an online AR(3) forecaster (recursive least squares), and publishes one message per scored bucket to topic
`wikipedia.rate-scores`: observed, forecast, z-scores and an `is_anomaly` flag. A downstream sink stores them.
"""
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone

from . import config
from .anomaly import SeriesScorer
from .kafka_utils import ensure_topics, make_consumer, make_producer, safe_commit
from .processing import parse_ts

log = logging.getLogger("anomaly-detector")
BUCKET = 10                       # seconds
Z_THRESHOLD = float(config.env("Z_THRESHOLD", "4.0"))
MAX_GAP_BUCKETS = 3                # > 30 s without any event anywhere = outage, not signal
MIN_COUNT = int(config.env("ANOMALY_MIN_COUNT", "6"))      # ignore "spikes" of a handful of edits
MIN_WIKI_VOLUME = int(config.env("MIN_WIKI_VOLUME", "40"))  # events needed in the last 30 min to score a wiki separately


class Detector:
    def __init__(self):
        self.counts: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))   # series -> bucket -> count
        self.scorers: dict[str, SeriesScorer] = {}
        self.recent: dict[str, list[float]] = defaultdict(list)                          # wiki -> event ts (30 min)
        self.next_bucket: int | None = None                                             # first bucket not scored yet
        self.watermark = 0

    def add(self, ev: dict) -> None:
        ts = parse_ts(ev["event_time"])
        b = int(ts // BUCKET)
        if self.next_bucket is not None and b - self.watermark > MAX_GAP_BUCKETS:
            # The whole stream went silent (producer / network outage, not a quiet Wikipedia): do not score the empty
            # buckets as "0 edits"; restart bucketing after the gap and keep the learned models.
            self.counts.clear()
            self.next_bucket = b + 1
            self.watermark = b
        self.counts["ALL"][b] += 1
        self.counts[ev["wiki"]][b] += 1
        self.recent[ev["wiki"]].append(ts)
        self.watermark = max(self.watermark, b)
        if self.next_bucket is None:
            self.next_bucket = b + 1                    # skip the (partial) first bucket

    def closed_results(self) -> list[dict]:
        """Score every bucket that ended at least one bucket ago."""
        out = []
        if self.next_bucket is None:
            return out
        while self.next_bucket < self.watermark:        # bucket `watermark` may still receive late events
            b = self.next_bucket
            for series in list(self.counts):
                if series != "ALL" and len([t for t in self.recent[series] if t > b * BUCKET - 1800]) < MIN_WIKI_VOLUME:
                    self.counts[series].pop(b, None)
                    continue
                x = self.counts[series].pop(b, 0)
                scorer = self.scorers.setdefault(series, SeriesScorer())
                r = scorer.score(x)
                out.append({
                    "series": series,
                    "bucket_time": datetime.fromtimestamp(b * BUCKET, tz=timezone.utc).isoformat(),
                    "observed": x, "forecast": round(r["forecast"], 3),
                    "z_ar": round(r["z_ar"], 3), "z_ewma": round(r["z_ewma"], 3),
                    "is_anomaly": bool(r["ready"] and r["z_ar"] >= Z_THRESHOLD and x >= MIN_COUNT),
                })
            self.next_bucket += 1
        # housekeeping: forget old timestamps / idle series
        cutoff = (self.watermark * BUCKET) - 1800
        for w in list(self.recent):
            self.recent[w] = [t for t in self.recent[w] if t > cutoff]
            if not self.recent[w] and w != "ALL":
                self.recent.pop(w)
                self.counts.pop(w, None)
                self.scorers.pop(w, None)
        return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ensure_topics([config.TOPIC_EDITS, config.TOPIC_RATE])
    consumer = make_consumer(config.GROUP_ANOMALY, [config.TOPIC_EDITS])
    producer = make_producer()
    det, n_scored, n_anom, last_report = Detector(), 0, 0, time.time()
    try:
        while True:
            for m in consumer.consume(num_messages=500, timeout=1.0):
                if m.error():
                    log.error("kafka error: %s", m.error())
                    continue
                try:
                    det.add(json.loads(m.value()))
                except (json.JSONDecodeError, KeyError, TypeError) as exc:
                    log.warning("skipping bad message: %s", exc)
            results = det.closed_results()
            for r in results:
                producer.produce(config.TOPIC_RATE, key=r["series"], value=json.dumps(r))
                n_scored += 1
                if r["is_anomaly"]:
                    n_anom += 1
                    log.info("ANOMALY %s %s observed=%d forecast=%.1f z=%.1f", r["series"], r["bucket_time"], r["observed"], r["forecast"], r["z_ar"])
            producer.poll(0)
            if results:
                producer.flush(10)
                safe_commit(consumer)
            if time.time() - last_report > 30:
                log.info("scored %d buckets, %d anomalies, %d series tracked", n_scored, n_anom, len(det.scorers))
                last_report = time.time()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
