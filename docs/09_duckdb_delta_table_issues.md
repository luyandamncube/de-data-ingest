```bash
# verify if host duckdb is the issue
duckdb -c "SELECT version();"
duckdb -c "INSTALL delta; LOAD delta; SELECT 1;"

┌─────────────┐
│ "version"() │
│   varchar   │
├─────────────┤
│ v1.1.3      │
└─────────────┘
┌───────┐
│   1   │
│ int32 │
├───────┤
│     1 │
└───────┘

# temp dir
mkdir -p /tmp/de-debug-output

# preserve the output and test one table directly.
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --pids-limit=512 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  --cap-drop=ALL \
  --security-opt no-new-privileges \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v /tmp/test-data/input:/data/input:ro \
  -v /tmp/test-data/config:/data/config:ro \
  -v /tmp/de-debug-output:/data/output:rw \
  my-submission:test

# inspect
find /tmp/de-debug-output/gold -maxdepth 3 | sort
find /tmp/de-debug-output/gold/dim_customers -maxdepth 2 -type f | sort

# test one table manually
duckdb -c "INSTALL delta; LOAD delta; SELECT COUNT(*) FROM delta_scan('file:///tmp/de-debug-output/gold/dim_customers');"


```

The error I experienced:

```bash
IO Error: Hit DeltaKernel FFI error (from: get_engine_interface_builder for path file:///tmp/de-debug-output/gold/dim_customers/): Hit error: 27 (InvalidTableLocation) with message (Invalid table location: Path does not exist: "/tmp/de-debug-output/gold/dim_customers/".)
```

Further testing

```bash
duckdb -csv -c "INSTALL delta; LOAD delta; SELECT COUNT(*) AS cnt FROM delta_scan('/tmp/de-debug-output/gold/dim_customers');"
duckdb -csv -c "INSTALL delta; LOAD delta; SELECT COUNT(*) AS cnt FROM delta_scan('/tmp/de-debug-output/gold/dim_accounts');"
duckdb -csv -c "INSTALL delta; LOAD delta; SELECT COUNT(*) AS cnt FROM delta_scan('/tmp/de-debug-output/gold/fact_transactions');"

# validation queries
cat <<'SQL' | duckdb -csv
INSTALL delta;
LOAD delta;

CREATE VIEW fact_transactions AS SELECT * FROM delta_scan('/tmp/de-debug-output/gold/fact_transactions');
CREATE VIEW dim_accounts      AS SELECT * FROM delta_scan('/tmp/de-debug-output/gold/dim_accounts');
CREATE VIEW dim_customers     AS SELECT * FROM delta_scan('/tmp/de-debug-output/gold/dim_customers');

SELECT 'Q1' AS query, COUNT(*) AS result_rows FROM (
    SELECT transaction_type, COUNT(*) AS cnt, SUM(amount) AS total_amount
    FROM fact_transactions
    GROUP BY transaction_type
    ORDER BY transaction_type
);

SELECT 'Q2' AS query, COUNT(*) AS unlinked_accounts
FROM dim_accounts a
LEFT JOIN dim_customers c ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT 'Q3' AS query, COUNT(*) AS result_rows FROM (
    SELECT c.province, COUNT(DISTINCT a.account_id) AS account_count
    FROM dim_accounts a
    JOIN dim_customers c ON a.customer_id = c.customer_id
    GROUP BY c.province
    ORDER BY c.province
);

```

Issue with my local setup it seems:

```bash
OUTPUT_DIR="$(mktemp -d /tmp/de_test_output.XXXXXX)"
```