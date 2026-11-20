"""Source + producer: read the live Wikipedia edit stream (Server-Sent Events) and write each edit to Kafka.

Start with:  python -m pipeline.producer      (docker-compose does this for you)

Message key = wiki code (e.g. "enwiki"): all edits of one wiki go to the same partition, so they stay in
order and the stateful processors can keep per-wiki state without coordination.
"""
import json
import logging
import time

import requests

from . import config
from .kafka_utils import ensure_topics, make_producer

log = logging.getLogger("producer")


def sse_events():
    """Yield decoded JSON events forever, reconnecting with back-off when the connection drops."""
    backoff = 1
    headers = {"User-Agent": config.WIKI_USER_AGENT, "Accept": "text/event-stream"}
    while True:
        try:
            with requests.get(config.WIKI_STREAM_URL, headers=headers, stream=True, timeout=(10, 90)) as resp:
                resp.raise_for_status()
                log.info("connected to %s", config.WIKI_STREAM_URL)
                backoff = 1
                for line in resp.iter_lines(decode_unicode=True):
                    if line and line.startswith("data:"):
                        try:
                            yield json.loads(line[5:])
                        except json.JSONDecodeError:
                            continue
        except Exception as exc:
            log.warning("stream error: %s -- reconnecting in %ss", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def to_event(raw: dict) -> dict | None:
    """Flatten a Wikimedia 'recentchange' record into our event schema (None = not interesting)."""
    if raw.get("type") not in ("edit", "new"):
        return None
    domain = (raw.get("meta") or {}).get("domain") or ""
    if config.WIKI_DOMAIN_SUFFIX and not domain.endswith(config.WIKI_DOMAIN_SUFFIX):
        return None
    length = raw.get("length") or {}
    new_len = length.get("new")
    if new_len is None:
        return None
    old_len = length.get("old") or 0          # brand-new pages have no old length
    return {
        "event_id": raw["meta"]["id"],
        "rc_id": raw.get("id"),
        "event_time": raw["meta"]["dt"],
        "wiki": raw.get("wiki"),
        "domain": domain,
        "event_type": raw["type"],
        "namespace": raw.get("namespace"),
        "title": raw.get("title"),
        "user_name": raw.get("user"),
        "is_bot": bool(raw.get("bot")),
        "is_minor": bool(raw.get("minor")),
        "length_old": old_len,
        "length_new": new_len,
        "byte_delta": new_len - old_len,
        "comment": (raw.get("comment") or "")[:500],
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ensure_topics([config.TOPIC_EDITS, config.TOPIC_ALERTS, config.TOPIC_RATE])
    producer = make_producer()
    sent = failed = 0
    last_report, report_sent = time.time(), 0

    def on_delivery(err, _msg):
        nonlocal failed
        if err is not None:
            failed += 1
            log.error("delivery failed: %s", err)

    for raw in sse_events():
        event = to_event(raw)
        if event is None:
            continue
        producer.produce(config.TOPIC_EDITS, key=event["wiki"], value=json.dumps(event), on_delivery=on_delivery)
        producer.poll(0)
        sent += 1
        if time.time() - last_report >= 10:
            rate = (sent - report_sent) / (time.time() - last_report)
            log.info("produced %d events total (%.1f/s), failures=%d", sent, rate, failed)
            last_report, report_sent = time.time(), sent


if __name__ == "__main__":
    main()
