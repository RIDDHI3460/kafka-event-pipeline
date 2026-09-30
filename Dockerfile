FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/ ./producer/
COPY consumer/ ./consumer/

# Default command is overridden per-service in docker-compose.yml
CMD ["python", "producer/producer.py"]
