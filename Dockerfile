FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline ./pipeline
COPY dashboard ./dashboard
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "pipeline.producer"]
