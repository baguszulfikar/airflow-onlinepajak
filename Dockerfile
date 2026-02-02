# Student Data Pipeline - Dockerfile
# Lightweight container for running the data pipeline

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-downloaded wheels (workaround for Docker DNS issues)
COPY wheels/ /tmp/wheels/

# Install Python dependencies from local wheels
RUN pip install --no-cache-dir --no-index --find-links=/tmp/wheels/ pandas duckdb python-dateutil pytest \
    && rm -rf /tmp/wheels/

# Copy source code
COPY src/ src/
COPY dags/ dags/

# Create data directories
RUN mkdir -p data/raw data/bronze data/silver data/gold

# Default command: run the pipeline
CMD ["python", "src/main.py", "--data-dir", "/app/data/raw"]
