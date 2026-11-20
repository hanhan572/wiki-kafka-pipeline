"""DEMO TOOL: inject a short, clearly-labelled SYNTHETIC burst of edits into Kafka to show the anomaly detector
and the alert rules reacting live. Injected events use user names starting with "demo-" and titles starting with
"[demo]" so they are easy to spot (and to delete: DELETE FROM edits WHERE title LIKE '[demo]%').

usage: python scripts/inject_burst.py [--seconds 25] [--per-second 12] [--bootstrap localhost:29092]
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=int, default=25)
ap.add_argument("--per-second", type=int, default=12, help="extra edits per second on top of the real traffic")
ap.add_argument("--bootstrap", default="localhost:29092")
ap.add_argument("--topic", default="wikipedia.edits")
args = ap.parse_args()

producer = Producer({"bootstrap.servers": args.bootstrap})
rnd = random.Random()
users = [f"demo-user-{i}" for i in range(8)]
print(f"injecting ~{args.seconds * args.per_second} synthetic edits over {args.seconds}s ...")
sent = 0
for sec in range(args.seconds):
    t0 = time.time()
    for i in range(args.per_second):
        now = datetime.now(timezone.utc)
        delta = rnd.randint(-300, 600)
        title = "[demo] Breaking news" if rnd.random() < 0.5 else f"[demo] Page {rnd.randint(1, 30)}"
        if sec == 3 and i == 0:                                    # one huge human deletion -> MASS_DELETION alert
            delta, title = -45_000, "[demo] Blanked article"
        ev = {"event_id": str(uuid.uuid4()), "rc_id": None, "event_time": now.isoformat().replace("+00:00", "Z"),
              "wiki": "enwiki", "domain": "en.wikipedia.org", "event_type": "edit", "namespace": 0, "title": title,
              "user_name": rnd.choice(users), "is_bot": False, "is_minor": False, "length_old": 5000,
              "length_new": 5000 + delta, "byte_delta": delta, "comment": "synthetic burst injected by scripts/inject_burst.py"}
        producer.produce(args.topic, key="enwiki", value=json.dumps(ev))
        sent += 1
    producer.poll(0)
    time.sleep(max(0, 1 - (time.time() - t0)))
producer.flush(10)
print(f"done: {sent} events sent")
