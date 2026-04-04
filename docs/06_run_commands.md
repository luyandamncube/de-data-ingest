## 12. Run commands

### Copy the starter kit

```bash
cp -r stage1/starter_kit ./submission
cd submission
```

### Build the image

```bash
docker build -t my-submission:test .
```

### Run locally

```bash
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  my-submission:test
```

### Check outputs

```bash
ls /tmp/test-data/output/bronze
ls /tmp/test-data/output/silver
ls /tmp/test-data/output/gold
```

### Run the supplied harness

```bash
bash run_tests.sh --stage 1 --data-dir /tmp/test-data --image my-submission:test
```

### Run validation SQL locally with DuckDB

```bash
duckdb
INSTALL delta;
LOAD delta;
SET VARIABLE gold_path = '/tmp/test-data/output/gold';
.read docs/validation_queries.sql
```