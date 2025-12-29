FROM python:3.11-slim

WORKDIR /app

# Install minimal OS deps (bash for interactive dev shells; curl optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    git \
  && rm -rf /var/lib/apt/lists/*

# Create non-root user early
RUN useradd -m appuser

# Install Python deps (cache-friendly)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create writable dirs + set ownership for non-root runtime
RUN mkdir -p data/movies data/movie_details data/seeds \
  && chown -R appuser:appuser /app

# Put dbt profiles under the non-root user's home (dev default)
RUN mkdir -p /home/appuser/.dbt \
  && cp profiles.yml.example /home/appuser/.dbt/profiles.yml \
  && chown -R appuser:appuser /home/appuser/.dbt

ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/home/appuser/.dbt

USER appuser

# Dev default (override in compose / Cloud Run Jobs)
CMD ["bash"]
