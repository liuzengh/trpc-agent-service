CREATE FUNCTION platform_reconcile_outbound_part(t TEXT,o TEXT,i INTEGER,expected_owner TEXT,outcome TEXT,provider TEXT,evidence_hash TEXT,actor TEXT,trace TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE p outbound_part%ROWTYPE; parent outbound_message%ROWTYPE; n INTEGER;
BEGIN
 IF outcome NOT IN ('sent','not_sent') OR evidence_hash !~ '^[a-f0-9]{64}$' THEN RETURN FALSE; END IF;
 SELECT * INTO parent FROM outbound_message WHERE tenant_id=t AND outbound_id=o FOR UPDATE;
 IF NOT FOUND OR parent.status='sent' OR (parent.locked_until IS NOT NULL AND parent.locked_until>now()) THEN RETURN FALSE; END IF;
 SELECT * INTO p FROM outbound_part WHERE tenant_id=t AND outbound_id=o AND part_index=i FOR UPDATE;
 IF NOT FOUND OR p.owner<>expected_owner OR p.status NOT IN ('attempting','unknown') THEN RETURN FALSE; END IF;
 UPDATE outbound_part SET status=CASE WHEN outcome='sent' THEN 'sent' ELSE 'pending' END,
 provider_message_id=CASE WHEN outcome='sent' THEN provider ELSE '' END,updated_at=now()
 WHERE tenant_id=t AND outbound_id=o AND part_index=i;
 SELECT count(*) INTO n FROM outbound_part WHERE tenant_id=t AND outbound_id=o AND status='sent';
 UPDATE outbound_message SET status=CASE WHEN n=p.total_parts THEN 'sent' ELSE 'pending' END,
 next_attempt_at=now(),locked_by=NULL,locked_until=NULL,
 sent_at=CASE WHEN n=p.total_parts THEN now() ELSE sent_at END
 WHERE tenant_id=t AND outbound_id=o;
 INSERT INTO audit_log(audit_id,tenant_id,user_id,trace_id,decision,details,cost,latency_ms)
 VALUES('audit-part-'||md5(t||clock_timestamp()::text||random()::text),t,actor,NULLIF(trace,''),'admin_outbound_part_reconciled',jsonb_build_object('outbound_id',o,'part_index',i,'outcome',outcome,'evidence_hash',evidence_hash),0,0);
 RETURN TRUE;
END $$;
REVOKE ALL ON FUNCTION platform_reconcile_outbound_part(TEXT,TEXT,INTEGER,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;
DO $$ BEGIN
 EXECUTE format('ALTER FUNCTION %I.platform_reconcile_outbound_part(TEXT,TEXT,INTEGER,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT) SET search_path = %I, pg_temp',current_schema(),current_schema());
END $$;
