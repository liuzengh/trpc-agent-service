ALTER TABLE audit_log ADD COLUMN event_hash TEXT;

-- Append-only, idempotent retry without granting runtime roles SELECT/UPDATE
-- on the audit table. Hash is computed by PostgreSQL, not supplied by caller.
CREATE FUNCTION platform_audit_append(p JSONB) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE fingerprint TEXT; written INTEGER;
BEGIN
 IF p->>'audit_id' IS NULL OR p->>'audit_id' !~ '^audit-[A-Za-z0-9_-]{1,55}$' THEN
  RAISE EXCEPTION 'invalid audit identity';
 END IF;
 fingerprint := encode(sha256(convert_to(p::text,'UTF8')),'hex');
 INSERT INTO audit_log(audit_id,occurred_at,tenant_id,channel,channel_binding_id,user_id,session_id,message_id,request_id,trace_id,agent_name,revision_id,tool_name,decision,latency_ms,error_type,cost,details,event_hash)
 VALUES(p->>'audit_id',(p->>'occurred_at')::timestamptz,p->>'tenant_id',NULLIF(p->>'channel',''),NULLIF(p->>'channel_binding_id',''),NULLIF(p->>'user_id',''),NULLIF(p->>'session_id',''),NULLIF(p->>'message_id',''),NULLIF(p->>'request_id',''),NULLIF(p->>'trace_id',''),NULLIF(p->>'agent_name',''),NULLIF(p->>'revision_id',''),NULLIF(p->>'tool_name',''),p->>'decision',COALESCE((p->>'latency')::bigint,0)/1000000,NULLIF(p->>'error_type',''),COALESCE((p->>'cost')::numeric,0),COALESCE(p->'details','{}'::jsonb),fingerprint)
 ON CONFLICT(audit_id) DO NOTHING;
 GET DIAGNOSTICS written = ROW_COUNT;
 IF written=1 THEN RETURN TRUE; END IF;
 RETURN EXISTS(SELECT 1 FROM audit_log WHERE audit_id=p->>'audit_id' AND tenant_id=p->>'tenant_id' AND event_hash=fingerprint);
END $$;

-- The caller cannot select its own cutoff or bypass a zero/absent retention.
-- Receipt and deletion commit together. Jobs has EXECUTE, not table DELETE.
CREATE FUNCTION platform_audit_prune(p_tenant TEXT,p_limit INTEGER) RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE days INTEGER; configured TEXT; removed INTEGER;
BEGIN
 SELECT audit_policy->>'retention_days' INTO configured FROM tenant WHERE tenant_id=p_tenant;
 IF configured IS NULL OR configured !~ '^[0-9]{1,5}$' THEN RETURN 0; END IF;
 days:=configured::integer;
 IF days<1 OR days>36500 THEN RETURN 0; END IF;
 WITH candidates AS (
  SELECT audit_id FROM audit_log WHERE tenant_id=p_tenant AND occurred_at<now()-make_interval(days=>days)
  ORDER BY occurred_at LIMIT GREATEST(1,LEAST(p_limit,1000)) FOR UPDATE SKIP LOCKED
 ) DELETE FROM audit_log a USING candidates c WHERE a.audit_id=c.audit_id;
 GET DIAGNOSTICS removed = ROW_COUNT;
 IF removed>0 THEN
  INSERT INTO audit_log(audit_id,tenant_id,decision,details,cost,latency_ms)
  VALUES('audit-retention-'||md5(p_tenant||clock_timestamp()::text||random()::text),p_tenant,'audit_retention_pruned',jsonb_build_object('deleted_records',removed,'retention_days',days),0,0);
 END IF;
 RETURN removed;
END $$;

REVOKE ALL ON FUNCTION platform_audit_append(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION platform_audit_prune(TEXT,INTEGER) FROM PUBLIC;
-- Include pg_temp explicitly last so caller temp tables cannot shadow tables
-- used by SECURITY DEFINER functions, including in isolated test schemas.
DO $$ BEGIN
 EXECUTE format('ALTER FUNCTION %I.platform_audit_append(JSONB) SET search_path = %I, pg_temp',current_schema(),current_schema());
 EXECUTE format('ALTER FUNCTION %I.platform_audit_prune(TEXT,INTEGER) SET search_path = %I, pg_temp',current_schema(),current_schema());
END $$;
