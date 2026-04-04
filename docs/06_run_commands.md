## 12. Run commands

> Note: `nedbank-de-challenge/base:1.0` is a **local base image** built from `infrastructure/Dockerfile.base`. 

### Copy the starter kit and required support files

```bash
cp -r stage1/starter_kit ./submission
mkdir -p submission/infrastructure submission/docs
cp stage1/infrastructure/Dockerfile.base submission/infrastructure/
cp stage1/infrastructure/run_tests.sh submission/
cp stage1/docs/validation_queries.sql submission/docs/
cd submission
```

### Build the local base image

```bash
docker build -t nedbank-de-challenge/base:1.0 -f infrastructure/Dockerfile.base .
```

### Build the submission image

```bash
docker build -t my-submission:test .
```
### Copy data
```bash
mkdir -p /tmp/test-data/input /tmp/test-data/config /tmp/test-data/output

cp data/accounts.csv /tmp/test-data/input/
cp data/customers.csv /tmp/test-data/input/
cp data/transactions.jsonl /tmp/test-data/input/
cp config/pipeline_config.yaml /tmp/test-data/config/
```

### Run locally

> Ensure your mounted data directory contains `/input`, `/config`, and `/output` subdirectories.


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

### Run test harness

```bash
bash infrastructure/run_tests.sh --stage 1 --data-dir /tmp/test-data --image my-submission:test 
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
