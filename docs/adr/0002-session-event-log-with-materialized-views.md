# Use Append-Only Session Events With Materialized Views

Status: accepted

Session history will be recorded as immutable, monotonically sequenced Session Events with idempotency keys; current Session state and Summary are materialized views. This preserves ordering, auditability, cross-node synchronization, and migration metadata without requiring every read to replay a complete event stream.
