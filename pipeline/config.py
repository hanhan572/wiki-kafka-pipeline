"""All configuration comes from environment variables (see .env.example)."""
import os


def env(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key, default)


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:29092")
DATABASE_URL = env("DATABASE_URL", "postgresql://wiki:wiki@localhost:5433/wiki")

# Topics (named for the domain)
TOPIC_EDITS = env("TOPIC_EDITS", "wikipedia.edits")                 # raw events, key = wiki
TOPIC_ALERTS = env("TOPIC_ALERTS", "wikipedia.edit-alerts")         # derived: rule-based alerts
TOPIC_RATE = env("TOPIC_RATE", "wikipedia.rate-scores")             # derived: forecast + anomaly score per 10 s bucket
TOPIC_PARTITIONS = int(env("TOPIC_PARTITIONS", "3"))

# Consumer groups (one per independent job, so each job gets its own copy of the stream)
GROUP_DB_SINK = env("GROUP_DB_SINK", "edits-to-postgres")
GROUP_STATS = env("GROUP_STATS", "edit-stats-processor")
GROUP_DERIVED_SINK = env("GROUP_DERIVED_SINK", "derived-to-postgres")
GROUP_ANOMALY = env("GROUP_ANOMALY", "rate-anomaly-detector")

# Source
WIKI_STREAM_URL = env("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
WIKI_USER_AGENT = env("WIKI_USER_AGENT", "wiki-kafka-pipeline/1.0 (student project)")
WIKI_DOMAIN_SUFFIX = env("WIKI_DOMAIN_SUFFIX", "wikipedia.org")

RETENTION_HOURS = float(env("RETENTION_HOURS", "6"))
