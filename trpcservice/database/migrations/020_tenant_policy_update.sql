-- Policy and mandatory security receipt commit atomically. Admin receives
-- EXECUTE only, never UPDATE on tenant or DELETE on audit_log.
CREATE FUNCTION platform_tenant_policy_update(p_tenant TEXT,p_version BIGINT,p_quota JSONB,p_audit JSONB,p_actor TEXT,p_trace TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE n INTEGER;
BEGIN
 IF jsonb_typeof(p_quota) IS DISTINCT FROM 'object' OR jsonb_typeof(p_audit) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'policies must be objects'; END IF;
 UPDATE tenant SET quota_config=p_quota,audit_policy=p_audit,version=version+1,updated_at=now()
 WHERE tenant_id=p_tenant AND version=p_version;
 GET DIAGNOSTICS n=ROW_COUNT;
 IF n<>1 THEN RETURN FALSE; END IF;
 INSERT INTO audit_log(audit_id,tenant_id,user_id,trace_id,decision,details,cost,latency_ms)
 VALUES('audit-policy-'||md5(p_tenant||clock_timestamp()::text||random()::text),p_tenant,p_actor,NULLIF(p_trace,''),'admin_tenant_policy_updated',jsonb_build_object('version',p_version+1),0,0);
 RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION platform_tenant_policy_update(TEXT,BIGINT,JSONB,JSONB,TEXT,TEXT) FROM PUBLIC;
DO $$ BEGIN
 EXECUTE format('ALTER FUNCTION %I.platform_tenant_policy_update(TEXT,BIGINT,JSONB,JSONB,TEXT,TEXT) SET search_path = %I, pg_temp',current_schema(),current_schema());
END $$;
