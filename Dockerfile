# Reproducible benchmark environment (Linux, pinned dependencies).
#   docker build -t etl-vs-dbt .
#   docker run --rm -v "$PWD/results:/out" etl-vs-dbt
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Run the full study inside the container filesystem (bind mounts would skew I/O timings),
# then copy the results out.
CMD python main.py all --sf 1 \
    && mkdir -p /out \
    && cp -r docs *_results.json /out/
