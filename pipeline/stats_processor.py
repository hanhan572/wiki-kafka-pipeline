"""Stream processor #1 (group `edit-stats-processor`) — reads the raw topic and WORKS SOMETHING OUT:

 * tumbling 1-minute windows per wiki (edits, bots vs humans, new pages, bytes added/removed, unique users)
   -> UPSERTed into table `edit_stats_minute` every few seconds while the window is still filling
 * rule-based alerts (mass deletion, edit burst, high-rate user) -> produced to topic `wikipedia.edit-alerts`

Offsets are committed only after the window rows were stored and the alerts were flushed to Kafka.
"""
import json
import logging
import time

from . import config, db
from .kafka_utils import ensure_topics, make_consumer, make_producer, safe_commit
from .processing import AlertRules, WindowAggregator

log = logging.getLogger("stats-processor")
FLUSH_SECONDS = 5

UPSERT = """
INSERT INTO edit_stats_minute (wiki, minute_start, edits, bot_edits, human_edits, new_pages, bytes_added, bytes_removed, unique_users)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (wiki, minute_start) DO UPDATE SET
    edits = GREATEST(edit_stats_minute.edits, EXCLUDED.edits),
    bot_edits = GREATEST(edit_stats_minute.bot_edits, EXCLUDED.bot_edits),
    human_edits = GREATEST(edit_stats_minute.human_edits, EXCLUDED.human_edits),
    new_pages = GREATEST(edit_stats_minute.new_pages, EXCLUDED.new_pages),
    bytes_added = GREATEST(edit_stats_minute.bytes_added, EXCLUDED.bytes_added),
    bytes_removed = GREATEST(edit_stats_minute.bytes_removed, EXCLUDED.bytes_removed),
    unique_users = GREATEST(edit_stats_minute.unique_users, EXCLUDED.unique_users),
    updated_at = now()
"""   # GREATEST: counters only grow inside a window, so a restart that re-aggregates part of a window never lowers it


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ensure_topics([config.TOPIC_EDITS, config.TOPIC_ALERTS])
    conn = db.connect()
    db.init_schema(conn)
    consumer = make_consumer(config.GROUP_STATS, [config.TOPIC_EDITS])
    producer = make_producer()
    agg, rules = WindowAggregator(), AlertRules()
    last_flush, n_events, n_alerts = time.time(), 0, 0
    try:
        while True:
            for m in consumer.consume(num_messages=500, timeout=1.0):
                if m.error():
                    log.error("kafka error: %s", m.error())
                    continue
                try:
                    ev = json.loads(m.value())
                    agg.add(ev)
                    for alert in rules.process(ev):
                        producer.produce(config.TOPIC_ALERTS, key=alert["wiki"], value=json.dumps(alert))
                        n_alerts += 1
                    n_events += 1
                except (json.JSONDecodeError, KeyError, TypeError) as exc:
                    log.warning("skipping bad message: %s", exc)
            producer.poll(0)
            if time.time() - last_flush >= FLUSH_SECONDS:
                rows = agg.pop_dirty_rows()
                if rows:
                    with conn.cursor() as cur:
                        cur.executemany(UPSERT, rows)
                    conn.commit()
                producer.flush(10)
                safe_commit(consumer)
                agg.prune()
                last_flush = time.time()
                log.info("processed %d events, %d alerts, %d open windows", n_events, n_alerts, len(agg.windows))
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
