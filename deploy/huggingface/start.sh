#!/usr/bin/env bash
# All-in-one entrypoint: Kafka (KRaft) + producer + consumers/processors + Streamlit dashboard in ONE container.
# Postgres is external (Supabase): set the DATABASE_URL secret.
set -u
: "${DATABASE_URL:?Set the DATABASE_URL secret (Supabase connection string)}"
export KAFKA_BOOTSTRAP=127.0.0.1:9092
export KAFKA_HEAP_OPTS="-Xmx512m -Xms256m"
export RETENTION_HOURS="${RETENTION_HOURS:-6}"

echo ">> formatting + starting Kafka"
rm -rf /tmp/kafka-data
/opt/kafka/bin/kafka-storage.sh format -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c /app/server.properties >/dev/null
/opt/kafka/bin/kafka-server-start.sh /app/server.properties >/tmp/kafka.log 2>&1 &
KAFKA_PID=$!
for i in $(seq 1 60); do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1 && break
  sleep 2
done
echo ">> Kafka is up"

# each service restarts itself if it ever crashes
run() { while true; do python -m "pipeline.$1"; echo "!! $1 exited, restarting in 5s"; sleep 5; done; }
for svc in producer db_sink stats_processor derived_sink anomaly_detector; do run "$svc" & done

# if Kafka itself dies, exit so the platform restarts the whole container
( while kill -0 "$KAFKA_PID" 2>/dev/null; do sleep 10; done; echo "!! Kafka died"; kill -TERM $$ ) &

echo ">> starting dashboard on :7860"
exec streamlit run dashboard/app.py --server.port 7860 --server.address 0.0.0.0 --server.headless true \
     --server.enableCORS false --server.enableXsrfProtection false --browser.gatherUsageStats false
