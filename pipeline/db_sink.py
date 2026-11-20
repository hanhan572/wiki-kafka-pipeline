"""Consumer #1 (group `edits-to-postgres`): copy every event from Kafka into the `edits` table.

At-least-once delivery made effectively-once: we commit the Kafka offsets only AFTER the Postgres
transaction committed, and the insert is `ON CONFLICT DO NOTHING` on the unique event id, so a replay
after a crash cannot create duplicates.
"""
import json
import logging
import time

from . import config, db
from .kafka_utils import ensure_topics, make_consumer, safe_commit

log = logging.getLogger("db-sink")

COLUMNS = ["event_id", "rc_id", "event_time", "wiki", "domain", "event_type", "namespace", "title",
           "user_name", "is_bot", "is_minor", "length_old", "length_new", "byte_delta", "comment"]
INSERT = (f"INSERT INTO edits ({', '.join(COLUMNS)}) VALUES ({', '.join(['%s'] * len(COLUMNS))}) "
          "ON CONFLICT (event_id) DO NOTHING")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ensure_topics([config.TOPIC_EDITS])
    conn = db.connect()
    db.init_schema(conn)
    consumer = make_consumer(config.GROUP_DB_SINK, [config.TOPIC_EDITS])
    stored, last_prune, last_report = 0, 0.0, time.time()
    try:
        while True:
            msgs = consumer.consume(num_messages=500, timeout=1.0)
            rows = []
            for m in msgs:
                if m.error():
                    log.error("kafka error: %s", m.error())
                    continue
                try:
                    ev = json.loads(m.value())
                    rows.append(tuple(ev.get(c) for c in COLUMNS))
                except (json.JSONDecodeError, TypeError):
                    log.warning("skipping malformed message at offset %s", m.offset())
            if rows:
                with conn.cursor() as cur:
                    cur.executemany(INSERT, rows)
                conn.commit()
                safe_commit(consumer)       # only after the DB commit
                stored += len(rows)
            now = time.time()
            if now - last_prune > 60:                    # retention keeps the free-tier database small
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM edits WHERE event_time < now() - %s * interval '1 hour'", (config.RETENTION_HOURS,))
                    cur.execute("DELETE FROM edit_stats_minute WHERE minute_start < now() - interval '7 days'")
                    cur.execute("DELETE FROM rate_scores WHERE bucket_time < now() - interval '2 days'")
                conn.commit()
                last_prune = now
            if now - last_report > 10:
                log.info("stored %d events so far", stored)
                last_report = now
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
