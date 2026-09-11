-- Runtime discovery and maintenance add no business facts and never collect
-- existing receipts, parts, attempts, observations or capacity. C collation makes
-- public keyset cursors agree with the ASCII identity order in the Go runners.
CREATE INDEX gateway_delivery_runtime_accounts
 ON gateway_delivery_intents(provider,account_id COLLATE "C",deadline,intent_id);
CREATE INDEX gateway_delivery_runtime_deadlines
 ON gateway_delivery_intents(deadline,intent_id);
CREATE INDEX gateway_delivery_runtime_pending
 ON gateway_delivery_parts(intent_id,part_index)
 INCLUDE(part_id,next_attempt_at,attempt_number,preparation_attempts)
 WHERE state IN ('PENDING','NOT_SENT');
CREATE INDEX gateway_delivery_runtime_unknown
 ON gateway_delivery_parts(current_attempt_id COLLATE "C",part_id)
 WHERE state='UNKNOWN';
CREATE INDEX gateway_delivery_runtime_attempts
 ON gateway_delivery_attempts(attempt_id COLLATE "C",part_id);
