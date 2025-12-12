FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt-duckdb
RUN pip install --no-cache-dir dbt-duckdb

# Copy project files
COPY . .

# Create data directory structure
RUN mkdir -p data/movies data/movie_details data/seeds

# Copy dbt profiles
RUN mkdir -p /root/.dbt
COPY profiles.yml.example /root/.dbt/profiles.yml

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/root/.dbt

# Default command - opens a bash shell
CMD ["/bin/bash"]