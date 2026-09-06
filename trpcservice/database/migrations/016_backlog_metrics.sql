-- Aggregate-only observability surface. Runtime monitor roles do not need
-- SELECT access to arbitrary chat payloads just to measure pending work.
CREATE VIEW platform_backlog WITH (security_barrier=true) AS
WITH counts AS (
  SELECT r.tenant_id, 'queue'::text AS stage, q.status AS state, count(*) AS items, min(q.created_at) AS oldest
    FROM queue_outbox q JOIN agent_run r ON r.request_id=q.payload->>'request_id'
    WHERE q.status IN ('pending','publishing') GROUP BY r.tenant_id,q.status
  UNION ALL
  SELECT tenant_id,'run',status,count(*),min(created_at) FROM agent_run
    WHERE status IN ('queued','running','failed','dead') GROUP BY tenant_id,status
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
), categories(stage,state) AS (VALUES
 ('queue','pending'),('queue','publishing'),('run','queued'),('run','running'),('run','failed'),('run','dead'),
 ('outbound','pending'),('outbound','sending'),('outbound','dead'),('background','pending'),('background','running'),('background','dead'),
 ('delivery','attempting'),('delivery','unknown'),('checkpoint','active')
)
SELECT t.tenant_id,k.stage,k.state,COALESCE(c.items,0)::bigint AS items,
       GREATEST(0,COALESCE(EXTRACT(EPOCH FROM (clock_timestamp()-c.oldest)),0))::double precision AS oldest_age_seconds
FROM tenant t CROSS JOIN categories k
LEFT JOIN counts c ON c.tenant_id=t.tenant_id AND c.stage=k.stage AND c.state=k.state
WHERE t.status='active';
