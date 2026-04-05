FROM nedbank-de-challenge/base:1.0

WORKDIR /app

# Benchmark image is intentionally separate from the submission image so the
# benchmark loop can evolve without changing the scored runtime contract.
RUN pip install --no-cache-dir polars==1.35.2

COPY benchmarks/ benchmarks/
COPY pipeline/ pipeline/
COPY config/ config/

CMD ["python", "-m", "benchmarks.container_entrypoint", "--help"]
