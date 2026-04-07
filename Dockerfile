# ── Base image ──────────────────────────────────────────────────────────────
FROM python:3.11-slim

# ── System dependencies ──────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# ── Working directory ────────────────────────────────────────────────────────
WORKDIR /app

# ── Python dependencies ──────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application source ───────────────────────────────────────────────────────
COPY src/ ./src/

# ── Create runtime directories ───────────────────────────────────────────────
RUN mkdir -p data/raw data/silver docs/plots

# ── Environment defaults (overridden by docker-compose env) ─────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

# ── Default command: run the full ingestion + silver pipeline ────────────────
CMD ["bash", "-c", \
     "python src/01_ingest_raw.py && \
      python src/02_silver_processing.py && \
      python src/03_load_silver_pg.py"]
