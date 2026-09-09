-- Queue generations fence old Redis deliveries after a durable deferral.
ALTER TABLE agent_run ADD COLUMN schedule_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE agent_run ADD COLUMN next_attempt_at TIMESTAMPTZ;
ALTER TABLE agent_run ADD COLUMN deferred_count INTEGER NOT NULL DEFAULT 0;
CREATE INDEX run_conversation_pending ON agent_run(conversation_id,turn_seq)
    WHERE status IN ('queued','running','failed','waiting');
CREATE INDEX run_dependency_wait ON agent_run(tenant_id,app_id,revision_id,next_attempt_at)
    WHERE status='waiting' AND error_type='model_unavailable';
ALTER TABLE queue_outbox ADD COLUMN lane TEXT NOT NULL DEFAULT 'recent';
CREATE INDEX queue_outbox_lane_due ON queue_outbox(lane,next_attempt_at,created_at)
    WHERE status IN ('pending','publishing');

-- One waiting notice and one final response per request, using the existing sender.
ALTER TABLE outbound_message ADD COLUMN message_kind TEXT NOT NULL DEFAULT 'result';
ALTER TABLE outbound_message DROP CONSTRAINT outbound_message_request_id_key;
ALTER TABLE outbound_message ADD UNIQUE(request_id,message_kind);

-- Workers must not gain UPDATE privileges on delivery state (e.g. forge sent).
-- A narrowly scoped trigger withdraws only unsent waiting notices on completion.
CREATE FUNCTION platform_cancel_waiting_notice() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
 IF NEW.status IN ('completed','dead','expired','cancelled') THEN
  UPDATE outbound_message SET status='cancelled'
  WHERE tenant_id=NEW.tenant_id AND request_id=NEW.request_id
    AND message_kind='waiting' AND status='pending';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION platform_cancel_waiting_notice() FROM PUBLIC;
DO $$ BEGIN
 EXECUTE format('ALTER FUNCTION %I.platform_cancel_waiting_notice() SET search_path = %I, pg_temp',current_schema(),current_schema());
END $$;
CREATE TRIGGER cancel_waiting_notice AFTER UPDATE OF status ON agent_run
FOR EACH ROW EXECUTE FUNCTION platform_cancel_waiting_notice();

-- Existing gaps remain historical audit facts, never silently replayed by upgrade.
ALTER TABLE channel_poll_gap ADD COLUMN status TEXT NOT NULL DEFAULT 'skipped';
ALTER TABLE channel_poll_gap ADD COLUMN config_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE channel_poll_gap ADD COLUMN cursor_at TIMESTAMPTZ;
ALTER TABLE channel_poll_gap ADD COLUMN last_error TEXT NOT NULL DEFAULT '';
CREATE INDEX channel_gap_pending ON channel_poll_gap(tenant_id,channel_binding_id,chat_hash,previous_through)
    WHERE status='pending';

CREATE OR REPLACE VIEW platform_backlog WITH (security_barrier=true) AS
WITH counts AS (
 SELECT r.tenant_id,'queue'::text AS stage,q.status AS state,count(*) AS items,min(q.created_at) AS oldest
 FROM queue_outbox q JOIN agent_run r ON r.request_id=q.payload->>'request_id'
 WHERE q.status IN ('pending','publishing') GROUP BY r.tenant_id,q.status
 UNION ALL
 SELECT tenant_id,'run',status,count(*),min(created_at) FROM agent_run
 WHERE status IN ('queued','running','failed','dead','waiting') GROUP BY tenant_id,status
 UNION ALL
 SELECT tenant_id,'outbound',status,count(*),min(created_at) FROM outbound_message
 WHERE status IN ('pending','sending','dead') GROUP BY tenant_id,status
 UNION ALL
 SELECT tenant_id,'background',status,count(*),min(created_at) FROM background_job
 WHERE status IN ('pending','running','dead') GROUP BY tenant_id,status
 UNION ALL
 SELECT tenant_id,'delivery',status,count(*),min(updated_at) FROM channel_delivery_attempt
 WHERE status IN ('attempting','unknown') GROUP BY tenant_id,status
 UNION ALL
 SELECT c.tenant_id,'checkpoint','active',count(*),min(c.through_at)
 FROM channel_poll_checkpoint c JOIN channel_binding b ON b.tenant_id=c.tenant_id AND b.channel_binding_id=c.channel_binding_id
 WHERE b.status='active' GROUP BY c.tenant_id
 UNION ALL
 SELECT tenant_id,'backfill',status,count(*),min(cursor_at) FROM channel_poll_gap
 WHERE status IN ('pending','blocked') GROUP BY tenant_id,status
), categories(stage,state) AS (VALUES
 ('queue','pending'),('queue','publishing'),('run','queued'),('run','running'),('run','failed'),('run','dead'),('run','waiting'),
 ('outbound','pending'),('outbound','sending'),('outbound','dead'),('background','pending'),('background','running'),('background','dead'),
 ('delivery','attempting'),('delivery','unknown'),('checkpoint','active'),('backfill','pending'),('backfill','blocked')
)
SELECT t.tenant_id,k.stage,k.state,COALESCE(c.items,0)::bigint AS items,
 GREATEST(0,COALESCE(EXTRACT(EPOCH FROM (clock_timestamp()-c.oldest)),0))::double precision AS oldest_age_seconds
FROM tenant t CROSS JOIN categories k
LEFT JOIN counts c ON c.tenant_id=t.tenant_id AND c.stage=k.stage AND c.state=k.state
WHERE t.status='active';
