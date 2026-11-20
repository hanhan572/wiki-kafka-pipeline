"""Consumer #3 (group `derived-to-postgres`): store the processors' OUTPUT topics in tables.

  wikipedia.edit-alerts  -> table `alerts`
  wikipedia.rate-scores  -> table `rate_scores`
"""
import json
import logging
import time

from . import config, db
from .kafka_utils import ensure_topics, make_consumer, safe_commit

log = logging.getLogger("derived-sink")

INSERT_ALERT = """INSERT INTO alerts (alert_id, alert_time, rule, severity, wiki, title, user_name, value, detail)
VALUES (%(alert_id)s, %(alert_time)s, %(rule)s, %(severity)s, %(wiki)s, %(title)s, %(user_name)s, %(value)s, %(detail)s)
ON CONFLICT (alert_id) DO NOTHING"""
INSERT_RATE = """INSERT INTO rate_scores (series, bucket_time, observed, forecast, z_ar, z_ewma, is_anomaly)
VALUES (%(series)s, %(bucket_time)s, %(observed)s, %(forecast)s, %(z_ar)s, %(z_ewma)s, %(is_anomaly)s)
ON CONFLICT (series, bucket_time) DO UPDATE SET observed = EXCLUDED.observed, forecast = EXCLUDED.forecast,
    z_ar = EXCLUDED.z_ar, z_ewma = EXCLUDED.z_ewma, is_anomaly = EXCLUDED.is_anomaly"""


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    topics = [config.TOPIC_ALERTS, config.TOPIC_RATE]
    ensure_topics(topics)
    conn = db.connect()
    db.init_schema(conn)
    consumer = make_consumer(config.GROUP_DERIVED_SINK, topics)
    n_alerts = n_rate = 0
    last_report = time.time()
    try:
        while True:
            alerts, rates = [], []
            for m in consumer.consume(num_messages=500, timeout=1.0):
                if m.error():
                    log.error("kafka error: %s", m.error())
                    continue
                try:
                    payload = json.loads(m.value())
                except json.JSONDecodeError:
                    continue
                (alerts if m.topic() == config.TOPIC_ALERTS else rates).append(payload)
            if alerts or rates:
                with conn.cursor() as cur:
                    if alerts:
                        cur.executemany(INSERT_ALERT, alerts)
                    if rates:
                        cur.executemany(INSERT_RATE, rates)
                conn.commit()
                safe_commit(consumer)
                n_alerts += len(alerts)
                n_rate += len(rates)
            if time.time() - last_report > 30:
                log.info("stored %d alerts, %d rate scores", n_alerts, n_rate)
                last_report = time.time()
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
