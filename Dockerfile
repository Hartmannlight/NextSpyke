FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY schema.sql /app/schema.sql
COPY src /app/src

ENV PYTHONPATH=/app/src

CMD ["python", "-m", "nextspyke.app"]
