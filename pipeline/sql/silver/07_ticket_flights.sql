-- Silver Ticket Flights with Quarantine Handling
CREATE TABLE IF NOT EXISTS lake.silver.quarantine (
  source_table string,
  reason string,
  payload string,
  _batch_id string,
  _ingest_ts timestamp
) USING iceberg;

-- Quarantine bad ticket_flights
INSERT INTO lake.silver.quarantine
SELECT
  'ticket_flights' AS source_table,
  CASE
    WHEN tf.amount < 0 THEN 'amount < 0'
    WHEN f.flight_id IS NULL THEN 'flight_id not in silver flights'
    WHEN t.ticket_no IS NULL THEN 'ticket_no not in silver tickets'
    ELSE 'unknown'
  END AS reason,
  to_json(struct(
    tf.ticket_no, tf.flight_id, tf.fare_conditions, tf.amount
  )) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM (
  SELECT *,
    row_number() OVER (PARTITION BY ticket_no, flight_id ORDER BY _ingest_ts DESC) AS rn
  FROM lake.bronze.ticket_flights
  WHERE _batch_id = '${run_id}'
) tf
LEFT JOIN lake.silver.flights_enriched f ON tf.flight_id = f.flight_id
LEFT JOIN lake.silver.tickets t ON tf.ticket_no = t.ticket_no
WHERE tf.rn = 1
  AND (
    tf.amount < 0
    OR f.flight_id IS NULL
    OR t.ticket_no IS NULL
  );

CREATE TABLE IF NOT EXISTS lake.silver.ticket_flights (
  ticket_no string,
  flight_id int,
  fare_conditions string,
  amount double
) USING iceberg
PARTITIONED BY (bucket(8, flight_id));

MERGE INTO lake.silver.ticket_flights AS target
USING (
  SELECT
    tf.ticket_no,
    tf.flight_id,
    tf.fare_conditions,
    cast(tf.amount as double) AS amount
  FROM (
    SELECT *,
      row_number() OVER (PARTITION BY ticket_no, flight_id ORDER BY _ingest_ts DESC) AS rn
    FROM lake.bronze.ticket_flights
    WHERE _batch_id = '${run_id}'
  ) tf
  JOIN lake.silver.flights_enriched f ON tf.flight_id = f.flight_id
  JOIN lake.silver.tickets t ON tf.ticket_no = t.ticket_no
  WHERE tf.rn = 1
    AND tf.amount >= 0
) AS source
ON target.ticket_no = source.ticket_no AND target.flight_id = source.flight_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
