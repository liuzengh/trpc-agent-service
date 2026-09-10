INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-a','Probe A'),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-b','Probe B')
ON CONFLICT DO NOTHING;

INSERT INTO model_profile(tenant_id,model_profile_id,profile_key,display_name,status,current_version)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','model','probe-model','Probe Model','active',NULL)
ON CONFLICT DO NOTHING;
INSERT INTO model_profile_revision(tenant_id,model_profile_id,profile_version,schema_version,provider,model_name,content_digest)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','model',1,1,'probe','probe',repeat('d',64))
ON CONFLICT DO NOTHING;
UPDATE model_profile SET current_version=1
WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND model_profile_id='model' AND current_version IS NULL;

INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name,status,current_revision,next_revision) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-a','Probe A','disabled',NULL,2),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','app_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-b','Probe B','draft',NULL,2)
ON CONFLICT DO NOTHING;

INSERT INTO agent_app_revision(tenant_id,agent_app_id,revision,state,draft_version,agent_kind,schema_version,instruction,model_profile_id,model_profile_version,content_digest,published_at)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,'published',1,'llm',1,'probe','model',1,repeat('a',64),now())
ON CONFLICT DO NOTHING;

UPDATE agent_app SET status='active',current_revision=1,version=version+1
WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV'
  AND agent_app_id='app_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND current_revision IS NULL;

INSERT INTO config_snapshot(tenant_id,config_version,schema_version,payload,content_digest,state,actor_id,reason_code,correlation_id,trace_id,published_at)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,1,'{"policy_version":1}'::jsonb,repeat('b',64),'published','probe','probe','probe','probe',now())
ON CONFLICT DO NOTHING;

INSERT INTO session_head(tenant_id,agent_app_id,session_id) VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-session')
ON CONFLICT DO NOTHING;

INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,agent_app_id,session_id,input_seq,state,payload_ref,payload_digest,key_version)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','fake','probe','probe-message','probe-request','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-session',1,'dispatch_ready','payload://probe',repeat('c',64),1)
ON CONFLICT DO NOTHING;

INSERT INTO execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-request',1,'app_01ARZ3NDEKTSV4RRFFQ69G5FAV',2,1,repeat('a',64),1,1,'probe-session','probe-user','fake',1,'payload://probe')
ON CONFLICT DO NOTHING;

DO $$
DECLARE
  v_outbox_count integer;
  v_outbox_payload text;
BEGIN
  BEGIN
    INSERT INTO session_event(tenant_id,agent_app_id,session_id,session_seq,request_id,input_seq,event_seq,event_id,event_type,payload_ref)
    VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW','app_01ARZ3NDEKTSV4RRFFQ69G5FAW','probe-session',1,'cross-tenant',1,1,'event','test','{"ref":"payload://probe","payload":{}}');
    RAISE EXCEPTION 'cross-tenant session FK was accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL;
  END;
  INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
  VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-outbox-1','dispatch','probe',1,'probe-duplicate','payload://probe')
  ON CONFLICT DO NOTHING;
  INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
  VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-outbox-2','dispatch','probe',1,'probe-duplicate','payload://other');
  SELECT count(*),min(payload_ref) INTO v_outbox_count,v_outbox_payload
    FROM outbox
    WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV'
      AND kind='dispatch' AND idempotency_key='probe-duplicate';
  IF v_outbox_count <> 1 OR v_outbox_payload <> 'payload://probe' THEN
    RAISE EXCEPTION 'semantic outbox duplicate did not converge';
  END IF;
  BEGIN
    PERFORM * FROM commit_turn(
      't_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-session',
      'probe-request','probe-commit',repeat('a',64),'terminal',1,1,99,'succeeded',
      '[]'::jsonb,'{}'::jsonb,NULL,NULL,NULL,'[]'::jsonb);
    RAISE EXCEPTION 'stale session version was accepted';
  EXCEPTION WHEN serialization_failure THEN NULL;
  END;
  -- A governance decision writes its audit fact before the terminal commit.
  -- CommitTurn must treat that matching durable outbox fact as an idempotent
  -- replay rather than fail the whole execution with a unique violation.
  INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
  VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-governance-audit','audit','probe-request',1,
    'governance:probe-decision','governance://probe')
  ON CONFLICT DO NOTHING;
  PERFORM * FROM commit_turn(
    't_01ARZ3NDEKTSV4RRFFQ69G5FAV','app_01ARZ3NDEKTSV4RRFFQ69G5FAV','probe-session',
    'probe-request','probe-governance-commit',repeat('e',64),'terminal',1,1,0,'denied',
    '[]'::jsonb,'{}'::jsonb,NULL,NULL,NULL,
    '[{"kind":"audit","idempotency_key":"governance:probe-decision","payload_ref":"governance://probe","event_seq":1}]'::jsonb);
  SELECT count(*) INTO v_outbox_count FROM outbox
    WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV'
      AND kind='audit' AND idempotency_key='governance:probe-decision';
  IF v_outbox_count <> 1 THEN
    RAISE EXCEPTION 'governance outbox duplicate did not converge';
  END IF;
END;
$$;

-- Knowledge migration mutation intent is durable, idempotent, immutable, and
-- must block authority verification while repair remains outstanding.
INSERT INTO config_snapshot(tenant_id,config_version,schema_version,payload,content_digest,state,
  actor_id,reason_code,correlation_id,trace_id,published_at)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV',2,1,'{"policy_version":2}'::jsonb,repeat('e',64),
  'published','probe','probe','probe-knowledge','probe-knowledge',now())
ON CONFLICT DO NOTHING;

INSERT INTO backend_profile(tenant_id,backend_profile_id,display_name,status,profile_key)
VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-source','Knowledge Source','active','knowledge-source'),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-target','Knowledge Target','active','knowledge-target')
ON CONFLICT DO NOTHING;
INSERT INTO backend_profile_revision(tenant_id,backend_profile_id,profile_version,schema_version,provider,
  configuration,capabilities,content_digest)
VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-source',1,1,'vector-source','{}',ARRAY['tenant_filter'],repeat('1',64)),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-target',1,1,'vector-target','{}',ARRAY['tenant_filter'],repeat('2',64))
ON CONFLICT DO NOTHING;
UPDATE backend_profile SET current_version=1
WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND backend_profile_id IN ('knowledge-source','knowledge-target')
  AND current_version IS NULL;
INSERT INTO backend_binding(tenant_id,config_version,domain,backend_profile_id,backend_version,required)
VALUES
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV',1,'knowledge','knowledge-source',1,ARRAY['tenant_filter']),
  ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV',2,'knowledge','knowledge-target',1,ARRAY['tenant_filter'])
ON CONFLICT DO NOTHING;
INSERT INTO backend_migration(tenant_id,migration_id,domain,epoch,
  source_config_version,source_backend_profile_id,source_backend_version,
  target_config_version,target_backend_profile_id,target_backend_version,state,
  snapshot_watermark,dual_write_ref,backfill_checkpoint,next_batch_seq,backfill_complete,
  created_at,updated_at)
VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-probe','knowledge',1,
  1,'knowledge-source',1,2,'knowledge-target',1,'backfill',
  'knowledge-chunk-v1:snapshot','knowledge-mutation://knowledge-probe','eof',2,true,
  now(),now());

SELECT record_knowledge_migration_mutation('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-probe','mutation-1',1,
  'kb-a',1,'chunk-a','upsert',1,repeat('3',64),1,now());
SELECT record_knowledge_migration_mutation('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-probe','mutation-1',1,
  'kb-a',1,'chunk-a','upsert',1,repeat('3',64),1,now());

DO $$
BEGIN
  BEGIN
    PERFORM record_knowledge_migration_mutation('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-probe','mutation-1',1,
      'kb-a',1,'chunk-a','upsert',2,repeat('3',64),1,now());
    RAISE EXCEPTION 'knowledge mutation collision was accepted';
  EXCEPTION WHEN unique_violation THEN NULL;
  END;
  BEGIN
    PERFORM record_knowledge_migration_mutation('t_01ARZ3NDEKTSV4RRFFQ69G5FAV','knowledge-probe','premature-target-write',1,
      'kb-a',1,'chunk-target','upsert',1,repeat('4',64),2,now());
    RAISE EXCEPTION 'pre-cutover reverse write was accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    UPDATE knowledge_migration_mutation SET chunk_id='forged',version=version+1,updated_at=now()
      WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND migration_id='knowledge-probe';
    RAISE EXCEPTION 'knowledge mutation identity update was accepted';
  EXCEPTION WHEN integrity_constraint_violation THEN NULL;
  END;
  BEGIN
    UPDATE backend_migration SET state='verify',version=version+1,updated_at=now()
      WHERE tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND migration_id='knowledge-probe';
    RAISE EXCEPTION 'knowledge verification with repair backlog was accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
END;
$$;
