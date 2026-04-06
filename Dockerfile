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

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "-m", "pipeline.run_all"]
