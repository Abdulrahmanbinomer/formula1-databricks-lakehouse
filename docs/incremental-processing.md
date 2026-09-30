# Incremental processing: verified implementation

The orchestration notebooks are exported from the Databricks workspace and retained in `notebooks/05_incremental/`.

| Notebook | Verified responsibility |
|---|---|
| `Create Control Table.py` | Creates the `control` schema and `batch_control` table if absent. |
| `identify new batch.py` | Finds the earliest landing batch not marked `in_progress` or `completed`, then sets job task values. |
| `Create new batch.py` | Reads widget `p_batch_id` and appends an `in_progress` Delta row. |
| `complete batch.py` | Reads widget `p_batch_id` and merges the matching `in_progress` row to `completed`. |

## `batch_control` schema

```sql
batch_id STRING,
status STRING,
created_timestamp TIMESTAMP,
updated_timestamp TIMESTAMP
```

## Selection logic

The notebook calls `dbutils.fs.ls("/Volumes/formula1-incr/landing/files/")`, keeps directory names, reads tracked batches with:

```sql
SELECT DISTINCT batch_id
FROM `formula1-incr`.control.batch_control
WHERE status IN ('in_progress', 'completed')
```

It then subtracts and sorts the Python lists. If a batch is available, it sets `p_batch_id` to the first result and `has_batch` to `true`; otherwise it sets an empty batch ID and `has_batch` to `false`.

## Observed successful execution

The verified notebook output showed `Landing batches: ['2025-01']`, `Tracked batches: []`, and `Next batch to process: 2025-01`.

## Operational note

The exported notebooks show the individual tasks and job task-value contract. The exact visual job DAG/run history was available in the Databricks workspace but is not included in the ZIP export, so this documentation does not assert unverified task dependencies.
