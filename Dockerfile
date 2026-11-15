# ATLAS API image. Also used by the scheduled ingest job, so the whole
# package ships rather than just the API.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# psycopg[binary] bundles its own libpq, so no apt packages are needed.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render injects $PORT. The default keeps `docker run -p 8000:8000` working
# locally without extra flags.
CMD ["sh", "-c", "uvicorn atlas.api.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
