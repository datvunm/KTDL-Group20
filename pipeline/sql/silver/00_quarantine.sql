-- Quarantine table initialization and batch reset
CREATE TABLE IF NOT EXISTS lake.silver.quarantine (
  source_table string,
  reason string,
  payload string,
  _batch_id string,
  _ingest_ts timestamp
) USING iceberg;

-- Idempotency: delete quarantine rows from prior runs of this batch_id
DELETE FROM lake.silver.quarantine WHERE _batch_id = '${run_id}';
