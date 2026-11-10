FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Ensure data directories exist
RUN mkdir -p /app/data/raw /app/data/processed /app/data/text_samples

# Set python path
ENV PYTHONPATH=/app

# Default command: run scheduler
CMD ["python", "-m", "data_pipeline.scheduler"]
