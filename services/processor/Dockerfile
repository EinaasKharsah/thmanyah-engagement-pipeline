FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY faust_app.py redis_utils.py clickhouse_utils.py .

CMD ["faust", "-A", "faust_app:app", "worker", "-l", "info"]


