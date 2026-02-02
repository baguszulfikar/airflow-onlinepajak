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
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies (without Airflow for lightweight image)
RUN pip install --no-cache-dir pandas duckdb python-dateutil pytest

# Copy source code
COPY src/ src/
COPY dags/ dags/

# Create data directories
RUN mkdir -p data/raw data/bronze data/silver data/gold

# Default command: run the pipeline
CMD ["python", "src/main.py", "--data-dir", "/app/data/raw"]
