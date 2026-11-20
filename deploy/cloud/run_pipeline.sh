#!/usr/bin/env bash
# All-in-one pipeline for Streamlit Community Cloud (started once by streamlit_app.py):
#   Kafka (KRaft, localhost) + producer + consumers/processors, writing to the external Postgres in $DATABASE_URL.
# Streamlit itself serves the dashboard, so this script only runs the background part.
set -u
cd "$(dirname "$0")/../.."
ROOT="$(pwd)"
PY="${PIPELINE_PYTHON:-python}"
KAFKA_VERSION=3.8.1
KAFKA_DIR="/tmp/kafka_2.13-${KAFKA_VERSION}"
export KAFKA_BOOTSTRAP=127.0.0.1:9092
export KAFKA_HEAP_OPTS="-Xmx384m -Xms128m"
export PYTHONPATH="$ROOT"
export RETENTION_HOURS="${RETENTION_HOURS:-6}"
log() { echo "[pipeline $(date +%H:%M:%S)] $*"; }

command -v java >/dev/null || { log "Java not found (add default-jre-headless to packages.txt)"; exit 1; }
if [ ! -d "$KAFKA_DIR" ]; then
  log "downloading Kafka ${KAFKA_VERSION}"
  curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" | tar xz -C /tmp || { log "Kafka download failed"; exit 1; }
fi

log "starting Kafka"
rm -rf /tmp/kafka-data
"$KAFKA_DIR/bin/kafka-storage.sh" format -t "$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)" -c "$ROOT/deploy/cloud/server.properties" >/dev/null
"$KAFKA_DIR/bin/kafka-server-start.sh" "$ROOT/deploy/cloud/server.properties" >/tmp/kafka.log 2>&1 &
KAFKA_PID=$!
for i in $(seq 1 60); do
  "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1 && break
  kill -0 "$KAFKA_PID" 2>/dev/null || { log "Kafka exited:"; tail -5 /tmp/kafka.log; exit 1; }
  sleep 2
done
log "Kafka is up"

# every service restarts itself if it ever crashes
run() { while true; do "$PY" -m "pipeline.$1"; log "$1 exited, restarting in 5s"; sleep 5; done; }
for svc in producer db_sink stats_processor derived_sink anomaly_detector; do run "$svc" & done

# if Kafka itself dies, stop so streamlit_app.py can start a fresh pipeline on the next visit
while kill -0 "$KAFKA_PID" 2>/dev/null; do sleep 10; done
log "Kafka died - stopping"
kill 0
