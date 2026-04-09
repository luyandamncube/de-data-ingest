FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bake the Delta Lake JVM artifacts into Spark's jars directory during build so
# runtime sessions work under --network=none without Maven/Ivy resolution.
COPY infrastructure/prewarm_delta_jars.py infrastructure/prewarm_delta_jars.py
RUN python infrastructure/prewarm_delta_jars.py

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/

# Create a real non-root user with UID/GID 1000 so Spark/Hadoop can resolve a
# concrete username under the local harness, while still matching the writable
# output mount ownership.
RUN groupadd -g 1000 pipeline && \
    useradd -m -u 1000 -g 1000 -s /bin/bash pipeline

# The local scorer harness mounts /data/output as a user-owned temp directory
# while dropping all container capabilities. Running as UID/GID 1000 keeps the
# submission image able to write into that mount under the harness contract.
USER pipeline:pipeline

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "-m", "pipeline.run_all"]
