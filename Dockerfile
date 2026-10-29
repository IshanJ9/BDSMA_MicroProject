# Reproducible Linux environment for the AirWatch pipeline (Java + PySpark).
# Works the same on Windows, macOS and Linux, and avoids Spark's winutils requirement on Windows.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code is mounted at runtime by docker-compose, so data/ and reports/ persist on the host.
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "pipeline.clean"]
