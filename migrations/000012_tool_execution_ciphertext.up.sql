-- Add encrypted replay results without rewriting the historical 000011
-- migration. This is safe for databases that already ran 000011.
ALTER TABLE tool_executions ADD COLUMN IF NOT EXISTS result_ciphertext BYTEA;
