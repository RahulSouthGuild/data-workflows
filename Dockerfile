# Pidilite DataWiz - ETL Application Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt pyproject.toml ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Copy application code
COPY config/ ./config/
COPY core/ ./core/
COPY scheduler/ ./scheduler/
COPY notifications/ ./notifications/
COPY db/ ./db/
COPY observability/ ./observability/
COPY utils/ ./utils/
COPY schemas/ ./schemas/

# Create necessary directories
RUN mkdir -p /app/data/data_historical/raw \
             /app/data/data_historical/raw_parquets \
             /app/data/data_historical/cleaned_parquets \
             /app/data/data_incremental/raw_parquets \
             /app/data/data_incremental/cleaned_parquets \
             /app/data/data_incremental/incremental \
             /app/data/data_incremental/checkpoint \
             /app/logs/scheduler \
             /app/logs/etl \
             /app/logs/notifications

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command (can be overridden in docker-compose)
CMD ["python", "scheduler/orchestrator.py"]
