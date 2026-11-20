# All-in-one image for a Hugging Face "Docker" Space: Kafka + pipeline services + Streamlit dashboard.
FROM apache/kafka:3.8.1 AS kafka

FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless bash && rm -rf /var/lib/apt/lists/*
COPY --from=kafka /opt/kafka /opt/kafka
RUN useradd -m -u 1000 user
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline ./pipeline
COPY dashboard ./dashboard
COPY server.properties start.sh ./
RUN chmod +x start.sh && chown -R user:user /app /opt/kafka
USER user
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
EXPOSE 7860
CMD ["/app/start.sh"]
