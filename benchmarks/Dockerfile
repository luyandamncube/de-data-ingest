FROM nedbank-de-challenge/base:1.0

WORKDIR /app

# Benchmark image is intentionally separate from the submission image so the
# benchmark loop can evolve without changing the scored runtime contract.
RUN pip install --no-cache-dir "numpy<2" pandas==2.1.0 polars==1.35.2 datafusion deltalake
RUN curl https://clickhouse.com/ | sh \
    && mv ./clickhouse /usr/local/bin/clickhouse \
    && chmod +x /usr/local/bin/clickhouse

COPY infrastructure/prewarm_delta_jars.py infrastructure/prewarm_delta_jars.py
RUN python infrastructure/prewarm_delta_jars.py

COPY benchmarks/ benchmarks/
COPY pipeline/ pipeline/
COPY config/ config/

CMD ["python", "-m", "benchmarks.container_entrypoint", "--help"]
