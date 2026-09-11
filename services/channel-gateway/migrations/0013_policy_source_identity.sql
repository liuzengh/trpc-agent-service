-- Source identity is durable across Gateway restarts, independent of readiness.
CREATE TABLE gateway_policy_sources (
 scope_id text PRIMARY KEY,
 source_epoch text NOT NULL,
 stream_name text NOT NULL CHECK(stream_name='CHANNEL_ACCESS_POLICIES_V1'),
 stream_created text NOT NULL,
 blocked_reason text NOT NULL DEFAULT '' CHECK(blocked_reason IN ('','SOURCE_CHANGED','UNBOUND_HISTORY'))
);
CREATE FUNCTION gateway_policy_source_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'policy source deletion requires explicit recovery' USING ERRCODE='23514';
 END IF;
 IF ROW(NEW.scope_id,NEW.source_epoch,NEW.stream_name,NEW.stream_created)
    IS DISTINCT FROM ROW(OLD.scope_id,OLD.source_epoch,OLD.stream_name,OLD.stream_created)
    OR (OLD.blocked_reason<>'' AND NEW.blocked_reason<>OLD.blocked_reason) THEN
  RAISE EXCEPTION 'policy source identity fence violation' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER gateway_policy_source_fence BEFORE UPDATE OR DELETE ON gateway_policy_sources
 FOR EACH ROW EXECUTE FUNCTION gateway_policy_source_fence();
