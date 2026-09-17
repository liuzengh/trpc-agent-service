BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'audit_retention_purger') THEN
    CREATE ROLE audit_retention_purger NOLOGIN;
  END IF;
END
$$;
--
-- PostgreSQL database dump
--


-- Dumped from database version 16.15 (Debian 16.15-1.pgdg13+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg13+2)

SET LOCAL check_function_bodies = false;

--
-- Name: begin_knowledge_backend_observation(text, text, bigint, bigint, timestamp with time zone, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.begin_knowledge_backend_observation(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_at timestamp with time zone, p_observe_until timestamp with time zone) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND OR v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'knowledge' OR v_migration.state<>'cutover' OR v_tenant.active_config_version<>v_migration.target_config_version OR
     p_at IS NULL OR p_observe_until IS NULL OR p_observe_until<=p_at THEN
    RAISE EXCEPTION 'knowledge observation authority conflict' USING ERRCODE='40001';
  END IF;
  UPDATE public.backend_migration SET state='observe',observe_until=p_observe_until,updated_at=p_at,version=version+1
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;


--
-- Name: begin_knowledge_indexing(text, text, bigint, bigint, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.begin_knowledge_indexing(p_tenant_id text, p_knowledge_id text, p_version bigint, p_chunk_total bigint, p_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE; v_count bigint;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR
     p_chunk_total<1 OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge indexing input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state <> 'staging' THEN
    RAISE EXCEPTION 'knowledge manifest is not staging' USING ERRCODE='23514';
  END IF;
  SELECT count(*) INTO v_count FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version;
  IF v_count <> p_chunk_total THEN
    RAISE EXCEPTION 'knowledge chunk total does not match staged chunks' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_manifest SET state='indexing',chunk_total=p_chunk_total,updated_at=p_at,record_version=record_version+1
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
END;
$$;


--
-- Name: begin_knowledge_manifest(text, text, bigint, text, text, text, text, bigint, text, jsonb, text, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.begin_knowledge_manifest(p_tenant_id text, p_knowledge_id text, p_version bigint, p_source_uri text, p_source_digest text, p_chunking_pipeline_version text, p_embedder_profile_id text, p_embedder_version bigint, p_vector_collection_generation text, p_metadata_schema jsonb, p_content_watermark text, p_created_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
DECLARE v_existing public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR
     length(btrim(p_source_uri))=0 OR p_source_digest!~'^[0-9a-f]{64}$' OR
     length(btrim(p_chunking_pipeline_version))=0 OR length(btrim(p_embedder_profile_id))=0 OR p_embedder_version<1 OR
     length(btrim(p_vector_collection_generation))=0 OR p_metadata_schema IS NULL OR jsonb_typeof(p_metadata_schema)<>'array' OR
     EXISTS (SELECT 1 FROM jsonb_array_elements(p_metadata_schema) AS schema_item(value) WHERE jsonb_typeof(schema_item.value)<>'string' OR length(btrim(schema_item.value #>> '{}'))=0) OR
     (SELECT count(*) FROM jsonb_array_elements_text(p_metadata_schema)) <>
       (SELECT count(DISTINCT value) FROM jsonb_array_elements_text(p_metadata_schema) AS schema_value(value)) OR
     p_content_watermark IS NULL OR p_created_at IS NULL THEN
    RAISE EXCEPTION 'knowledge manifest input is invalid' USING ERRCODE='22023';
  END IF;
  INSERT INTO public.knowledge_manifest(tenant_id,knowledge_id,version,source_uri,source_digest,
    chunking_pipeline_version,embedder_profile_id,embedder_version,vector_collection_generation,
    metadata_schema,content_watermark,state,created_at,updated_at)
  VALUES(p_tenant_id,p_knowledge_id,p_version,p_source_uri,p_source_digest,
    p_chunking_pipeline_version,p_embedder_profile_id,p_embedder_version,p_vector_collection_generation,
    p_metadata_schema,p_content_watermark,'staging',p_created_at,p_created_at)
  ON CONFLICT (tenant_id,knowledge_id,version) DO NOTHING;
  SELECT * INTO v_existing FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
  IF (v_existing.source_uri,v_existing.source_digest,v_existing.chunking_pipeline_version,
      v_existing.embedder_profile_id,v_existing.embedder_version,v_existing.vector_collection_generation,
      v_existing.metadata_schema,v_existing.content_watermark,v_existing.created_at)
     IS DISTINCT FROM
     (p_source_uri,p_source_digest,p_chunking_pipeline_version,
      p_embedder_profile_id,p_embedder_version,p_vector_collection_generation,
      p_metadata_schema,p_content_watermark,p_created_at) THEN
    RAISE EXCEPTION 'knowledge manifest id collision' USING ERRCODE='23505';
  END IF;
END;
$_$;


--
-- Name: begin_knowledge_verifying(text, text, bigint, text, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.begin_knowledge_verifying(p_tenant_id text, p_knowledge_id text, p_version bigint, p_verification_digest text, p_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE; v_indexed bigint; v_computed_digest text;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR
     p_verification_digest!~'^[0-9a-f]{64}$' OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge verifying input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state <> 'indexing' THEN
    RAISE EXCEPTION 'knowledge manifest is not indexing' USING ERRCODE='23514';
  END IF;
  SELECT count(*) INTO v_indexed FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version AND indexed_at IS NOT NULL;
  IF v_indexed <> v_manifest.chunk_total THEN
    RAISE EXCEPTION 'knowledge indexing is incomplete' USING ERRCODE='23514';
  END IF;
  SELECT encode(sha256(convert_to(string_agg(length(image_digest)::text || ':' || image_digest,'' ORDER BY image_digest),'UTF8')),'hex')
    INTO v_computed_digest FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version;
  IF v_computed_digest IS NULL OR v_computed_digest<>p_verification_digest THEN
    RAISE EXCEPTION 'knowledge verification digest does not match chunk set' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_manifest SET state='verifying',verification_digest=p_verification_digest,
    updated_at=p_at,record_version=record_version+1
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
END;
$_$;


--
-- Name: begin_session_backend_observation(text, text, bigint, bigint, timestamp with time zone, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.begin_session_backend_observation(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_at timestamp with time zone, p_observe_until timestamp with time zone) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'session' OR v_migration.state<>'cutover' OR v_tenant.active_config_version<>v_migration.target_config_version THEN
    RAISE EXCEPTION 'observation authority conflict' USING ERRCODE='40001';
  END IF;
  IF p_at IS NULL OR p_observe_until IS NULL OR p_observe_until<=p_at THEN
    RAISE EXCEPTION 'observation window is invalid' USING ERRCODE='22023';
  END IF;
  UPDATE public.backend_migration SET state='observe',observe_until=p_observe_until,updated_at=p_at,version=version+1
   WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;


--
-- Name: business_audit_watermark(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.business_audit_watermark(p_tenant text) RETURNS timestamp with time zone
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog'
    AS $$
  SELECT min(created_at) FROM public.outbox
  WHERE tenant_id = p_tenant AND kind = 'audit' AND state <> 'published';
$$;


--
-- Name: capture_session_migration_mutation(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.capture_session_migration_mutation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE
  v_migration public.backend_migration%ROWTYPE;
  v_config_version bigint;
  v_direction text;
BEGIN
  SELECT config_version INTO v_config_version
  FROM public.execution_record
  WHERE tenant_id=NEW.tenant_id AND request_id=NEW.request_id;
  SELECT * INTO v_migration
  FROM public.backend_migration
  WHERE tenant_id=NEW.tenant_id AND domain='session'
    AND state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe')
    AND v_config_version IN (source_config_version,target_config_version)
  ORDER BY epoch DESC LIMIT 1 FOR SHARE;
  IF NOT FOUND THEN RETURN NEW; END IF;
  v_direction := CASE WHEN v_config_version=v_migration.target_config_version THEN 'reverse' ELSE 'forward' END;
  INSERT INTO public.session_migration_mutation(
    tenant_id,migration_id,mutation_id,epoch,direction,agent_app_id,session_id,
    source_version,mutation_digest,not_before,created_at,updated_at)
  VALUES(NEW.tenant_id,v_migration.migration_id,NEW.commit_id,v_migration.epoch,v_direction,
    NEW.agent_app_id,NEW.session_id,NEW.session_version,NEW.request_digest,
    NEW.created_at,NEW.created_at,NEW.created_at)
  ON CONFLICT (tenant_id,migration_id,agent_app_id,session_id,mutation_id) DO NOTHING;
  RETURN NEW;
END;
$$;


SET LOCAL default_tablespace = '';

SET LOCAL default_table_access_method = heap;

--
-- Name: inbox; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.inbox (
    tenant_id text NOT NULL,
    channel text NOT NULL,
    external_account_id text NOT NULL,
    external_message_id text NOT NULL,
    request_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text,
    input_seq bigint,
    state text NOT NULL,
    payload_ref text NOT NULL,
    payload_digest text NOT NULL,
    terminal_reason text,
    result_ref text,
    key_version bigint NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    external_chat_id text DEFAULT ''::text NOT NULL,
    external_user_id text DEFAULT ''::text NOT NULL,
    CONSTRAINT inbox_input_seq_check CHECK (((input_seq IS NULL) OR (input_seq >= 1))),
    CONSTRAINT inbox_key_version_check CHECK ((key_version >= 1)),
    CONSTRAINT inbox_payload_digest_check CHECK ((payload_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT inbox_state_check CHECK ((state = ANY (ARRAY['preprocess_pending'::text, 'dispatch_pending'::text, 'dispatch_ready'::text, 'terminal'::text]))),
    CONSTRAINT inbox_version_check CHECK ((version >= 0))
);


--
-- Name: claim_channel_inbox(text, text, text, text, text, text, text, text, text, text, text, bigint, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.claim_channel_inbox(p_tenant_id text, p_channel text, p_external_account_id text, p_external_message_id text, p_request_id text, p_agent_app_id text, p_session_id text, p_external_chat_id text, p_external_user_id text, p_payload_ref text, p_payload_digest text, p_key_version bigint, p_initial_state text) RETURNS SETOF public.inbox
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE v_row public.inbox%ROWTYPE;
BEGIN
  IF p_initial_state NOT IN ('preprocess_pending', 'dispatch_pending')
     OR p_payload_digest !~ '^[0-9a-f]{64}$'
     OR length(btrim(p_external_chat_id)) > 512
     OR length(btrim(p_external_user_id)) > 512 THEN
    RAISE EXCEPTION 'invalid channel inbox claim' USING ERRCODE = '22023';
  END IF;
  INSERT INTO public.inbox(
    tenant_id, channel, external_account_id, external_message_id, request_id,
    agent_app_id, session_id, external_chat_id, external_user_id, state,
    payload_ref, payload_digest, key_version
  ) VALUES (
    p_tenant_id, p_channel, p_external_account_id, p_external_message_id,
    p_request_id, p_agent_app_id, p_session_id, p_external_chat_id,
    p_external_user_id, p_initial_state, p_payload_ref, p_payload_digest,
    p_key_version
  ) ON CONFLICT (tenant_id, channel, external_account_id, external_message_id) DO NOTHING;
  SELECT * INTO v_row FROM public.inbox
    WHERE tenant_id = p_tenant_id AND channel = p_channel
      AND external_account_id = p_external_account_id
      AND external_message_id = p_external_message_id FOR UPDATE;
  IF v_row.payload_digest <> p_payload_digest OR v_row.payload_ref <> p_payload_ref
     OR v_row.agent_app_id <> p_agent_app_id
     OR v_row.session_id IS DISTINCT FROM p_session_id
     OR v_row.external_chat_id <> p_external_chat_id
     OR v_row.external_user_id <> p_external_user_id THEN
    RAISE EXCEPTION 'channel inbox idempotency collision' USING ERRCODE = '23505';
  END IF;
  RETURN NEXT v_row;
END;
$_$;


--
-- Name: claim_inbox(text, text, text, text, text, text, text, text, text, bigint, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.claim_inbox(p_tenant_id text, p_channel text, p_external_account_id text, p_external_message_id text, p_request_id text, p_agent_app_id text, p_session_id text, p_payload_ref text, p_payload_digest text, p_key_version bigint, p_initial_state text) RETURNS SETOF public.inbox
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE v_row public.inbox%ROWTYPE;
BEGIN
  IF p_initial_state NOT IN ('preprocess_pending', 'dispatch_pending')
     OR p_payload_digest !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid inbox claim' USING ERRCODE = '22023';
  END IF;
  INSERT INTO public.inbox(
    tenant_id, channel, external_account_id, external_message_id, request_id,
    agent_app_id, session_id, state, payload_ref, payload_digest, key_version
  ) VALUES (
    p_tenant_id, p_channel, p_external_account_id, p_external_message_id,
    p_request_id, p_agent_app_id, p_session_id, p_initial_state,
    p_payload_ref, p_payload_digest, p_key_version
  ) ON CONFLICT (tenant_id, channel, external_account_id, external_message_id) DO NOTHING;
  SELECT * INTO v_row FROM public.inbox
    WHERE tenant_id = p_tenant_id AND channel = p_channel
      AND external_account_id = p_external_account_id
      AND external_message_id = p_external_message_id FOR UPDATE;
  IF v_row.payload_digest <> p_payload_digest OR v_row.payload_ref <> p_payload_ref
     OR v_row.agent_app_id <> p_agent_app_id
     OR v_row.session_id IS DISTINCT FROM p_session_id THEN
    RAISE EXCEPTION 'inbox idempotency collision' USING ERRCODE = '23505';
  END IF;
  RETURN NEXT v_row;
END;
$_$;


--
-- Name: cleanup_knowledge_backend_migration(text, text, bigint, bigint, text, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cleanup_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_rolled_back boolean; v_old_config bigint;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  SELECT EXISTS(SELECT 1 FROM public.backend_migration_config_switch WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND direction='rollback') INTO v_rolled_back;
  IF NOT FOUND OR v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'knowledge' OR v_migration.state<>'observe' OR p_at IS NULL OR p_at<v_migration.observe_until OR
     p_rollback_sync_watermark<>v_migration.verify_target_watermark OR
     (v_rolled_back AND v_tenant.active_config_version<>v_migration.source_config_version) OR
     (NOT v_rolled_back AND v_tenant.active_config_version<>v_migration.target_config_version) THEN
    RAISE EXCEPTION 'knowledge cleanup authority conflict' USING ERRCODE='40001';
  END IF;
  v_old_config := CASE WHEN v_rolled_back THEN v_migration.target_config_version ELSE v_migration.source_config_version END;
  IF EXISTS (SELECT 1 FROM public.knowledge_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') OR
     EXISTS (SELECT 1 FROM public.execution_record WHERE tenant_id=p_tenant_id AND config_version=v_old_config
       AND outcome IN ('queued','running','pending','blocked','waiting_confirmation')) THEN
    RAISE EXCEPTION 'knowledge cleanup drain is incomplete' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cleanup',rollback_sync_watermark=p_rollback_sync_watermark,updated_at=p_at,version=version+1
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;


--
-- Name: cleanup_session_backend_migration(text, text, bigint, bigint, text, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cleanup_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_rolled_back boolean; v_old_config bigint;
BEGIN
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  SELECT EXISTS(SELECT 1 FROM public.backend_migration_config_switch WHERE tenant_id=p_tenant_id
    AND migration_id=p_migration_id AND direction='rollback') INTO v_rolled_back;
  IF v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'session' OR v_migration.state<>'observe' OR p_at IS NULL OR p_at<v_migration.observe_until OR
     p_rollback_sync_watermark<>v_migration.verify_target_watermark THEN
    RAISE EXCEPTION 'cleanup authority conflict' USING ERRCODE='40001';
  END IF;
  IF (v_rolled_back AND v_tenant.active_config_version<>v_migration.source_config_version) OR
     (NOT v_rolled_back AND v_tenant.active_config_version<>v_migration.target_config_version) THEN
    RAISE EXCEPTION 'active config does not match migration direction' USING ERRCODE='23514';
  END IF;
  v_old_config := CASE WHEN v_rolled_back THEN v_migration.target_config_version ELSE v_migration.source_config_version END;
  IF EXISTS (SELECT 1 FROM public.session_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') OR
     EXISTS (SELECT 1 FROM public.execution_record WHERE tenant_id=p_tenant_id AND config_version=v_old_config
       AND outcome IN ('queued','running','pending','blocked','waiting_confirmation')) THEN
    RAISE EXCEPTION 'migration cleanup drain is incomplete' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cleanup',rollback_sync_watermark=p_rollback_sync_watermark,
    updated_at=p_at,version=version+1 WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  RETURN QUERY SELECT v_tenant.version,v_tenant.active_config_version;
END;
$$;


--
-- Name: commit_turn(text, text, text, text, text, text, text, bigint, bigint, bigint, text, jsonb, jsonb, jsonb, text, text, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.commit_turn(p_tenant_id text, p_agent_app_id text, p_session_id text, p_request_id text, p_commit_id text, p_request_digest text, p_stage text, p_input_seq bigint, p_fence bigint, p_expected_version bigint, p_outcome text, p_events jsonb, p_state_delta jsonb, p_summary jsonb, p_result_ref text, p_reply_cursor text, p_outbox jsonb) RETURNS TABLE(commit_id text, outcome text, input_seq bigint, session_version bigint, result_ref text, reply_cursor text, already_terminal boolean)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_head public.session_head%ROWTYPE; v_existing public.session_commit%ROWTYPE;
  v_terminal public.session_commit%ROWTYPE; v_event jsonb; v_out jsonb;
  v_ordinal bigint; v_new_version bigint; v_new_last_seq bigint;
  v_execution public.execution_record%ROWTYPE;
BEGIN
  SELECT * INTO v_head FROM public.session_head WHERE tenant_id = p_tenant_id
    AND agent_app_id = p_agent_app_id AND session_id = p_session_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'session not found' USING ERRCODE = 'P0002'; END IF;
  SELECT * INTO v_execution FROM public.execution_record WHERE tenant_id = p_tenant_id
    AND request_id = p_request_id FOR UPDATE;
  IF NOT FOUND OR v_execution.agent_app_id <> p_agent_app_id
     OR v_execution.session_id <> p_session_id OR v_execution.input_seq <> p_input_seq THEN
    RAISE EXCEPTION 'execution scope mismatch' USING ERRCODE = '42501';
  END IF;
  SELECT * INTO v_existing FROM public.session_commit WHERE tenant_id = p_tenant_id
    AND agent_app_id = p_agent_app_id AND session_id = p_session_id AND session_commit.commit_id = p_commit_id;
  IF FOUND THEN
    IF v_existing.request_digest <> p_request_digest THEN
      RAISE EXCEPTION 'commit id collision' USING ERRCODE = '23505';
    END IF;
    RETURN QUERY SELECT v_existing.commit_id, v_existing.outcome, v_existing.input_seq,
      v_existing.session_version, v_existing.result_ref, v_existing.reply_cursor, false;
    RETURN;
  END IF;
  IF p_input_seq < v_head.next_input_seq THEN
    SELECT * INTO v_terminal FROM public.session_commit WHERE tenant_id = p_tenant_id
      AND agent_app_id = p_agent_app_id AND session_id = p_session_id AND session_commit.input_seq = p_input_seq
      AND session_commit.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout');
    IF NOT FOUND THEN RAISE EXCEPTION 'terminal invariant missing' USING ERRCODE = 'XX001'; END IF;
    RETURN QUERY SELECT v_terminal.commit_id, v_terminal.outcome, v_terminal.input_seq,
      v_terminal.session_version, v_terminal.result_ref, v_terminal.reply_cursor, true;
    RETURN;
  END IF;
  IF p_input_seq > v_head.next_input_seq THEN RAISE EXCEPTION 'input not ready' USING ERRCODE = '55000'; END IF;
  IF p_expected_version <> v_head.version THEN RAISE EXCEPTION 'session version conflict' USING ERRCODE = '40001'; END IF;
  IF p_fence < v_head.last_fence THEN RAISE EXCEPTION 'stale fence' USING ERRCODE = '40001'; END IF;
  v_new_version := v_head.version + 1;
  v_new_last_seq := v_head.last_session_seq + jsonb_array_length(COALESCE(p_events, '[]'::jsonb));
  FOR v_event, v_ordinal IN SELECT value, ordinality FROM jsonb_array_elements(COALESCE(p_events, '[]'::jsonb)) WITH ORDINALITY LOOP
    INSERT INTO public.session_event(tenant_id, agent_app_id, session_id, session_seq,
      request_id, input_seq, event_seq, event_id, event_type, payload_ref)
    VALUES (p_tenant_id, p_agent_app_id, p_session_id, v_head.last_session_seq + v_ordinal,
      p_request_id, p_input_seq, (v_event->>'event_seq')::bigint, v_event->>'event_id', v_event->>'event_type', v_event->>'payload_ref');
  END LOOP;
  UPDATE public.session_head SET version = v_new_version,
    last_fence = GREATEST(last_fence, p_fence), last_session_seq = v_new_last_seq,
    next_input_seq = next_input_seq + CASE WHEN p_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN 1 ELSE 0 END,
    state_json = state_json || COALESCE(p_state_delta, '{}'::jsonb), updated_at = now()
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id AND session_id = p_session_id;
  IF p_summary IS NOT NULL AND p_summary <> 'null'::jsonb THEN
    INSERT INTO public.session_summary(tenant_id, agent_app_id, session_id, summary_id,
      base_session_seq, last_event_id, cutoff_at, content_ref)
    VALUES (p_tenant_id, p_agent_app_id, p_session_id, p_summary->>'summary_id',
      (p_summary->>'base_session_seq')::bigint, p_summary->>'last_event_id',
      (p_summary->>'cutoff_at')::timestamptz, p_summary->>'content_ref')
    ON CONFLICT (tenant_id, agent_app_id, session_id, base_session_seq) DO NOTHING;
    UPDATE public.session_head SET summary_id = p_summary->>'summary_id'
      WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id AND session_id = p_session_id
        AND (summary_id IS NULL OR (p_summary->>'base_session_seq')::bigint > COALESCE((SELECT base_session_seq FROM public.session_summary s WHERE s.tenant_id = p_tenant_id AND s.agent_app_id = p_agent_app_id AND s.session_id = p_session_id AND s.summary_id = session_head.summary_id), 0));
  END IF;
  INSERT INTO public.session_commit(tenant_id, agent_app_id, session_id, commit_id,
    request_id, request_digest, input_seq, stage, outcome, fence, session_version, reply_cursor, result_ref)
  VALUES (p_tenant_id, p_agent_app_id, p_session_id, p_commit_id, p_request_id,
    p_request_digest, p_input_seq, p_stage, p_outcome, p_fence, v_new_version, p_reply_cursor, p_result_ref);
  UPDATE public.execution_record SET outcome = p_outcome, result_ref = p_result_ref,
    version = version + 1
    WHERE tenant_id = p_tenant_id AND request_id = p_request_id;
  FOR v_out IN SELECT value FROM jsonb_array_elements(COALESCE(p_outbox, '[]'::jsonb)) LOOP
    INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq,
      idempotency_key, payload_ref, traceparent)
    VALUES (p_tenant_id, format('%s:%s', v_out->>'kind', v_out->>'idempotency_key'),
      v_out->>'kind', p_request_id, (v_out->>'event_seq')::bigint,
      v_out->>'idempotency_key', v_out->>'payload_ref', v_out->>'traceparent')
    -- Governance decisions may durably emit their audit fact before the
    -- terminal session commit references that same fact. The outbox key is
    -- the business idempotency boundary, so a replay must converge instead
    -- of leaving the execution pending on a unique-constraint error.
    ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
  END LOOP;
  RETURN QUERY SELECT p_commit_id, p_outcome, p_input_seq, v_new_version, p_result_ref, p_reply_cursor, false;
END;
$$;


--
-- Name: cutover_knowledge_backend_migration(text, text, bigint, bigint, bigint, bigint, text, text, text, text, text, timestamp with time zone, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cutover_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_source_count bigint, p_target_count bigint, p_source_digest text, p_target_digest text, p_source_watermark text, p_target_watermark text, p_sample_digest text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_existing public.backend_migration_config_switch%ROWTYPE;
BEGIN
  IF p_at IS NULL OR COALESCE(length(btrim(p_switch_id)),0)=0 OR length(p_switch_id)>128 OR
     COALESCE(length(btrim(p_actor_id)),0)=0 OR COALESCE(length(btrim(p_reason_code)),0)=0 OR
     COALESCE(length(btrim(p_correlation_id)),0)=0 OR COALESCE(length(btrim(p_trace_id)),0)=0 OR
     p_source_count IS NULL OR p_target_count IS NULL OR p_source_count<0 OR p_source_count<>p_target_count OR
     p_source_digest!~'^[0-9a-f]{64}$' OR p_source_digest<>p_target_digest OR
     COALESCE(length(p_source_watermark),0)=0 OR p_source_watermark<>p_target_watermark OR p_sample_digest!~'^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'knowledge cutover input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_existing FROM public.backend_migration_config_switch
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND switch_id=p_switch_id;
  IF FOUND THEN
    SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
    IF v_existing.direction<>'cutover' OR v_existing.previous_tenant_version<>p_expected_tenant_version OR
       v_existing.migration_result_version<>p_expected_migration_version+1 OR v_existing.actor_id<>p_actor_id OR
       v_existing.reason_code<>p_reason_code OR v_existing.correlation_id<>p_correlation_id OR v_existing.trace_id<>p_trace_id OR
       v_existing.occurred_at<>p_at OR v_migration.verify_source_count<>p_source_count OR v_migration.verify_target_count<>p_target_count OR
       v_migration.verify_source_digest<>p_source_digest OR v_migration.verify_target_digest<>p_target_digest OR
       v_migration.verify_source_watermark<>p_source_watermark OR v_migration.verify_target_watermark<>p_target_watermark OR
       v_migration.verify_sample_digest<>p_sample_digest THEN
      RAISE EXCEPTION 'switch id collision' USING ERRCODE='23505';
    END IF;
    RETURN QUERY SELECT v_existing.tenant_result_version,v_existing.active_config_version; RETURN;
  END IF;
  IF v_tenant.status='disabled' OR v_tenant.version<>p_expected_tenant_version THEN
    RAISE EXCEPTION 'knowledge tenant authority conflict' USING ERRCODE='40001';
  END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'knowledge' OR v_migration.state<>'verify' OR v_migration.version<>p_expected_migration_version OR
     v_tenant.active_config_version<>v_migration.source_config_version OR EXISTS (
       SELECT 1 FROM public.knowledge_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') THEN
    RAISE EXCEPTION 'knowledge cutover authority conflict' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cutover',verify_source_count=p_source_count,verify_target_count=p_target_count,
    verify_source_digest=p_source_digest,verify_target_digest=p_target_digest,verify_source_watermark=p_source_watermark,
    verify_target_watermark=p_target_watermark,verify_sample_digest=p_sample_digest,cutover_config_version=target_config_version,
    cutover_at=p_at,updated_at=p_at,version=version+1 WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  UPDATE public.tenant SET active_config_version=v_migration.target_config_version,version=version+1 WHERE tenant_id=p_tenant_id;
  INSERT INTO public.backend_migration_config_switch(tenant_id,migration_id,switch_id,direction,previous_config_version,
    active_config_version,migration_result_version,previous_tenant_version,tenant_result_version,actor_id,reason_code,
    correlation_id,trace_id,occurred_at)
  VALUES(p_tenant_id,p_migration_id,p_switch_id,'cutover',v_migration.source_config_version,v_migration.target_config_version,
    v_migration.version+1,v_tenant.version,v_tenant.version+1,p_actor_id,p_reason_code,p_correlation_id,p_trace_id,p_at);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent) VALUES
    (p_tenant_id,format('knowledge-migration-cutover-audit:%s:%s',p_migration_id,p_switch_id),'audit',p_migration_id,v_tenant.version+1,
      format('knowledge-migration:%s:%s:cutover-audit',p_migration_id,p_switch_id),format('backend-migration-switch://%s/%s/%s',p_tenant_id,p_migration_id,p_switch_id),p_traceparent),
    (p_tenant_id,format('knowledge-migration-cutover-invalidation:%s:%s',p_migration_id,p_switch_id),'config-invalidation',p_tenant_id,v_tenant.version+1,
      format('knowledge-migration:%s:%s:cutover-invalidate',p_migration_id,p_switch_id),format('config://%s/%s',p_tenant_id,v_migration.target_config_version),p_traceparent);
  RETURN QUERY SELECT v_tenant.version+1,v_migration.target_config_version;
END;
$_$;


--
-- Name: cutover_session_backend_migration(text, text, bigint, bigint, bigint, bigint, text, text, text, text, text, timestamp with time zone, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.cutover_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_source_count bigint, p_target_count bigint, p_source_digest text, p_target_digest text, p_source_watermark text, p_target_watermark text, p_sample_digest text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_existing public.backend_migration_config_switch%ROWTYPE;
BEGIN
  IF p_at IS NULL OR COALESCE(length(btrim(p_switch_id)),0)=0 OR length(p_switch_id)>128 OR
     COALESCE(length(btrim(p_actor_id)),0)=0 OR COALESCE(length(btrim(p_reason_code)),0)=0 OR
     COALESCE(length(btrim(p_correlation_id)),0)=0 OR COALESCE(length(btrim(p_trace_id)),0)=0 THEN
    RAISE EXCEPTION 'cutover metadata is incomplete' USING ERRCODE='22023';
  END IF;
  IF p_source_count IS NULL OR p_target_count IS NULL OR p_source_count<0 OR p_source_count<>p_target_count OR
     p_source_digest!~'^[0-9a-f]{64}$' OR p_source_digest<>p_target_digest OR
     COALESCE(length(p_source_watermark),0)=0 OR p_source_watermark<>p_target_watermark OR
     p_sample_digest!~'^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'verification evidence is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_existing FROM public.backend_migration_config_switch
   WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND switch_id=p_switch_id;
  IF FOUND THEN
    SELECT * INTO v_migration FROM public.backend_migration
      WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
    IF v_existing.direction<>'cutover' OR v_existing.previous_tenant_version<>p_expected_tenant_version OR
       v_existing.migration_result_version<>p_expected_migration_version+1 OR v_existing.actor_id<>p_actor_id OR
       v_existing.reason_code<>p_reason_code OR v_existing.correlation_id<>p_correlation_id OR
       v_existing.trace_id<>p_trace_id OR v_existing.occurred_at<>p_at OR
       v_migration.verify_source_count<>p_source_count OR v_migration.verify_target_count<>p_target_count OR
       v_migration.verify_source_digest<>p_source_digest OR v_migration.verify_target_digest<>p_target_digest OR
       v_migration.verify_source_watermark<>p_source_watermark OR v_migration.verify_target_watermark<>p_target_watermark OR
       v_migration.verify_sample_digest<>p_sample_digest THEN
      RAISE EXCEPTION 'switch id collision' USING ERRCODE='23505';
    END IF;
    RETURN QUERY SELECT v_existing.tenant_result_version,v_existing.active_config_version; RETURN;
  END IF;
  IF v_tenant.status='disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE='55000'; END IF;
  IF v_tenant.version<>p_expected_tenant_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE='40001'; END IF;
  SELECT * INTO v_migration FROM public.backend_migration
   WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'session' OR v_migration.state<>'verify' OR
     v_migration.version<>p_expected_migration_version OR
     v_tenant.active_config_version<>v_migration.source_config_version THEN
    RAISE EXCEPTION 'cutover authority conflict' USING ERRCODE='40001';
  END IF;
  IF EXISTS (SELECT 1 FROM public.session_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND state<>'applied') THEN
    RAISE EXCEPTION 'session migration repair backlog is not drained' USING ERRCODE='23514';
  END IF;
  UPDATE public.backend_migration SET state='cutover',verify_source_count=p_source_count,
    verify_target_count=p_target_count,verify_source_digest=p_source_digest,verify_target_digest=p_target_digest,
    verify_source_watermark=p_source_watermark,verify_target_watermark=p_target_watermark,
    verify_sample_digest=p_sample_digest,cutover_config_version=target_config_version,cutover_at=p_at,
    updated_at=p_at,version=version+1
  WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id;
  UPDATE public.tenant SET active_config_version=v_migration.target_config_version,version=version+1
   WHERE tenant_id=p_tenant_id;
  INSERT INTO public.backend_migration_config_switch(tenant_id,migration_id,switch_id,direction,
    previous_config_version,active_config_version,migration_result_version,previous_tenant_version,tenant_result_version,
    actor_id,reason_code,correlation_id,trace_id,occurred_at)
  VALUES(p_tenant_id,p_migration_id,p_switch_id,'cutover',v_migration.source_config_version,v_migration.target_config_version,
    v_migration.version+1,v_tenant.version,v_tenant.version+1,p_actor_id,p_reason_code,p_correlation_id,p_trace_id,p_at);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent) VALUES
    (p_tenant_id,format('migration-cutover-audit:%s:%s',p_migration_id,p_switch_id),'audit',p_migration_id,v_tenant.version+1,
      format('migration:%s:%s:cutover-audit',p_migration_id,p_switch_id),format('backend-migration-switch://%s/%s/%s',p_tenant_id,p_migration_id,p_switch_id),p_traceparent),
    (p_tenant_id,format('migration-cutover-invalidation:%s:%s',p_migration_id,p_switch_id),'config-invalidation',p_tenant_id,v_tenant.version+1,
      format('migration:%s:%s:cutover-invalidate',p_migration_id,p_switch_id),format('config://%s/%s',p_tenant_id,v_migration.target_config_version),p_traceparent);
  RETURN QUERY SELECT v_tenant.version+1,v_migration.target_config_version;
END;
$_$;


--
-- Name: decide_confirmation(text, text, text, boolean, bigint, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.decide_confirmation(p_tenant_id text, p_confirmation_id text, p_subject_id text, p_approve boolean, p_expected_version bigint, p_decided_at timestamp with time zone) RETURNS TABLE(confirmation_id text, state text, version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE c public.confirmation%ROWTYPE; v_grant_id text; v_cancel_requested boolean; v_outcome text;
BEGIN
  SELECT x.* INTO c FROM public.confirmation x WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'confirmation not found' USING ERRCODE='P0002'; END IF;
  IF c.subject_id<>p_subject_id THEN RAISE EXCEPTION 'confirmation subject mismatch' USING ERRCODE='42501'; END IF;
  IF c.state<>'pending' THEN RETURN QUERY SELECT c.confirmation_id,c.state,c.version; RETURN; END IF;
  IF c.version<>p_expected_version THEN RAISE EXCEPTION 'confirmation version conflict' USING ERRCODE='40001'; END IF;
  SELECT e.cancel_requested_at IS NOT NULL,e.outcome INTO v_cancel_requested,v_outcome FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=c.request_id FOR UPDATE;
  IF v_cancel_requested OR v_outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
    UPDATE public.confirmation x SET state='denied',decision_at=p_decided_at,version=x.version+1,updated_at=now()
      WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
  ELSIF c.expires_at<=p_decided_at THEN
    UPDATE public.confirmation x SET state='expired',decision_at=p_decided_at,version=x.version+1,updated_at=now()
      WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
  ELSIF p_approve THEN
    UPDATE public.confirmation x SET state='approved',decision_at=p_decided_at,version=x.version+1,updated_at=now()
      WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
    v_grant_id := 'grant_'||substr(p_confirmation_id,6);
    INSERT INTO public.confirmation_grant(tenant_id,grant_id,confirmation_id,request_id,subject_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,state)
      VALUES(p_tenant_id,v_grant_id,p_confirmation_id,c.request_id,c.subject_id,c.tool_id,c.tool_version,c.tool_call_id,c.args_digest,c.policy_version,'available');
  ELSE
    UPDATE public.confirmation x SET state='denied',decision_at=p_decided_at,version=x.version+1,updated_at=now()
      WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
  END IF;
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
    VALUES(p_tenant_id,'confirmation-dispatch:'||p_confirmation_id,'dispatch',c.request_id,2,
      'confirmation:'||p_confirmation_id||':dispatch','execution://'||p_tenant_id||'/'||c.request_id)
    ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
  RETURN QUERY SELECT x.confirmation_id,x.state,x.version FROM public.confirmation x
    WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
END;
$$;


--
-- Name: enqueue_next_parked_wakeup(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enqueue_next_parked_wakeup() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_execution public.execution_record%ROWTYPE;
BEGIN
  IF NEW.next_input_seq <= OLD.next_input_seq THEN RETURN NEW; END IF;
  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id=NEW.tenant_id AND e.agent_app_id=NEW.agent_app_id
      AND e.session_id=NEW.session_id AND e.input_seq=NEW.next_input_seq
      AND e.outcome IN ('pending','blocked')
    FOR UPDATE;
  IF NOT FOUND THEN RETURN NEW; END IF;

  IF v_execution.outcome='blocked'
     OR (v_execution.outcome='pending' AND v_execution.park_deadline IS NOT NULL
         AND v_execution.park_deadline <= clock_timestamp()) THEN
    UPDATE public.execution_record SET outcome='pending',park_attempt=0,
      not_before=clock_timestamp(),park_deadline=NULL,
      blocked_at=NULL,blocked_reason=NULL,version=version+1
      WHERE tenant_id=v_execution.tenant_id AND request_id=v_execution.request_id
      RETURNING * INTO v_execution;
  END IF;

  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,
    idempotency_key,payload_ref,traceparent,next_attempt_at)
  VALUES(v_execution.tenant_id,format('wakeup:%s:recovered:%s',v_execution.request_id,v_execution.version),
    'wakeup',v_execution.request_id,GREATEST(1,v_execution.version),
    format('wakeup:%s:recovered:%s',v_execution.request_id,v_execution.version),
    format('execution://%s/%s',v_execution.tenant_id,v_execution.request_id),
    v_execution.traceparent,GREATEST(now(),COALESCE(v_execution.not_before,now())))
  ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
  RETURN NEW;
END;
$$;


--
-- Name: ensure_config_policy_snapshot(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.ensure_config_policy_snapshot() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_policy_version bigint;
BEGIN
  v_policy_version := (NEW.payload->>'policy_version')::bigint;
  IF v_policy_version IS NULL OR v_policy_version < 1 THEN
    RAISE EXCEPTION 'config policy version is invalid' USING ERRCODE='22023';
  END IF;
  INSERT INTO public.policy_snapshot(tenant_id,policy_version,schema_version,payload,content_digest,pricing_version,state)
  VALUES(NEW.tenant_id,v_policy_version,1,
    '{"schema_version":1,"default_action":"deny","input_dlp":"disabled","output_dlp":"disabled","budget":{"max_input_tokens":0,"max_output_tokens":0,"max_cost_micros_per_run":0}}'::jsonb,
    '9e0969fee4ce512275943bd0a66d147d1fba1bc6498f67abf88d6b7d1742ef20',NULL,'published')
  ON CONFLICT (tenant_id,policy_version) DO NOTHING;
  RETURN NEW;
END;
$$;


--
-- Name: execute_business_audit_purge(text, text, text, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.execute_business_audit_purge(p_tenant text, p_batch text, p_owner text, p_chunk bigint) RETURNS text
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  b public.business_audit_purge_batch%ROWTYPE;
  v_watermark timestamptz;
  v_safe timestamptz;
  v_events bigint;
  v_outbox bigint;
  v_digest text;
  v_from timestamptz;
  v_to timestamptz;
  v_deleted bigint;
BEGIN
  IF NOT pg_has_role(session_user, 'audit_retention_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to execute business audit purge' USING ERRCODE = '42501';
  END IF;
  IF p_tenant IS NULL OR btrim(p_tenant) = '' OR p_batch IS NULL OR btrim(p_batch) = '' OR
     p_owner IS NULL OR btrim(p_owner) = '' OR p_chunk IS NULL OR p_chunk < 1 OR p_chunk > 1000000 THEN
    RAISE EXCEPTION 'business audit purge execution input is invalid' USING ERRCODE = '22023';
  END IF;

  SELECT * INTO b FROM public.business_audit_purge_batch
    WHERE tenant_id = p_tenant AND batch_id = p_batch FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'business audit purge batch not found' USING ERRCODE = '02000';
  END IF;
  IF b.state = 'completed' THEN
    RETURN 'completed';
  END IF;
  IF b.state NOT IN ('planned','executing','failed') THEN
    RAISE EXCEPTION 'business audit purge batch is not executable' USING ERRCODE = '23514';
  END IF;
  IF b.not_before > clock_timestamp() THEN
    RETURN 'not_before';
  END IF;
  IF b.state = 'executing'
     AND b.claim_owner IS DISTINCT FROM p_owner
     AND (b.claim_until IS NULL OR b.claim_until >= clock_timestamp()) THEN
    RETURN 'claimed_by_another';
  END IF;

  UPDATE public.business_audit_purge_batch SET
    state = 'executing', claim_owner = p_owner,
    claim_until = clock_timestamp() + interval '5 minutes',
    version = version + 1, updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch;

  -- Recompute the watermark and candidate set; any divergence from the plan
  -- fails closed and never deletes.
  v_watermark := public.business_audit_watermark(p_tenant);
  v_safe := b.cutoff_at;
  IF v_watermark IS NOT NULL AND v_watermark < b.cutoff_at THEN
    v_safe := v_watermark;
  END IF;
  IF v_safe <> b.safe_cutoff_at THEN
    UPDATE public.business_audit_purge_batch SET state = 'failed', last_error_class = 'watermark_drift',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'watermark_drift';
  END IF;

  SELECT count(*) INTO v_events FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;
  SELECT count(*) INTO v_outbox FROM public.outbox o
    WHERE o.tenant_id = p_tenant AND o.kind = 'audit' AND o.state = 'published' AND o.created_at < v_safe;
  SELECT COALESCE(encode(sha256(convert_to(string_agg(e.audit_id, chr(10) ORDER BY e.occurred_at, e.audit_id), 'UTF8')), 'hex'),
                  encode(sha256(convert_to('', 'UTF8')), 'hex'))
    INTO v_digest
    FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;
  IF v_events <> b.planned_events OR v_outbox <> b.planned_outbox OR v_digest <> b.planned_digest THEN
    UPDATE public.business_audit_purge_batch SET state = 'failed', last_error_class = 'divergence',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'divergence';
  END IF;

  -- Guarded deletion via the transaction-local marker. Un-exported outbox rows
  -- are excluded by the state predicate and never deleted.
  PERFORM set_config('audit.purge_authorized', 'on', true);

  SELECT COALESCE(min(occurred_at), v_safe), COALESCE(max(occurred_at), v_safe)
    INTO v_from, v_to FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;

  v_deleted := 0;
  LOOP
    WITH doomed AS (
      SELECT e.audit_id FROM public.audit_event e
      WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe
      ORDER BY e.occurred_at, e.audit_id LIMIT p_chunk
    )
    DELETE FROM public.audit_event e USING doomed d WHERE e.tenant_id = p_tenant AND e.audit_id = d.audit_id;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    EXIT WHEN v_deleted = 0;
  END LOOP;

  DELETE FROM public.outbox o
    WHERE o.tenant_id = p_tenant AND o.kind = 'audit' AND o.state = 'published' AND o.created_at < v_safe;

  INSERT INTO public.business_audit_purge_certificate(tenant_id,batch_id,from_occurred_at,to_occurred_at,
    event_count,outbox_count,watermark_at,event_digest,approved_by,reason)
  VALUES(p_tenant, b.batch_id, v_from, v_to, v_events, v_outbox, b.watermark_at, v_digest, p_owner, 'retention');

  UPDATE public.business_audit_purge_batch SET state = 'completed',
    deleted_events = v_events, deleted_outbox = v_outbox,
    version = version + 1, updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch;

  RETURN 'completed';
END;
$$;


--
-- Name: expire_confirmations(timestamp with time zone, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.expire_confirmations(p_now timestamp with time zone, p_limit integer) RETURNS TABLE(tenant_id text, confirmation_id text)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF p_limit < 1 OR p_limit > 1000 THEN RAISE EXCEPTION 'invalid expiry batch size' USING ERRCODE='22023'; END IF;
  RETURN QUERY
  WITH due AS (
    SELECT c.tenant_id,c.confirmation_id,c.request_id
    FROM public.confirmation c
    WHERE c.state='pending' AND c.expires_at<=p_now
    ORDER BY c.expires_at,c.tenant_id,c.confirmation_id
    FOR UPDATE SKIP LOCKED LIMIT p_limit
  ), expired AS (
    UPDATE public.confirmation c SET state='expired',decision_at=p_now,version=c.version+1,updated_at=now()
    FROM due d WHERE c.tenant_id=d.tenant_id AND c.confirmation_id=d.confirmation_id
    RETURNING c.tenant_id,c.confirmation_id,c.request_id
  ), emitted AS (
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
    SELECT e.tenant_id,'confirmation-dispatch:'||e.confirmation_id,'dispatch',e.request_id,2,
      'confirmation:'||e.confirmation_id||':dispatch','execution://'||e.tenant_id||'/'||e.request_id FROM expired e
    ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING RETURNING 1
  ) SELECT e.tenant_id,e.confirmation_id FROM expired e;
END;
$$;


--
-- Name: fail_knowledge_version(text, text, bigint, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fail_knowledge_version(p_tenant_id text, p_knowledge_id text, p_version bigint, p_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge fail input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state NOT IN ('staging','indexing','verifying') THEN
    RAISE EXCEPTION 'knowledge manifest is not failable' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_manifest SET state='failed',updated_at=p_at,record_version=record_version+1
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
END;
$$;


--
-- Name: guard_agent_app_current_revision(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_agent_app_current_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE v_state text;
BEGIN
  IF NEW.current_revision IS NULL OR NEW.current_revision IS NOT DISTINCT FROM OLD.current_revision THEN
    RETURN NEW;
  END IF;
  SELECT state INTO v_state FROM public.agent_app_revision
    WHERE tenant_id = NEW.tenant_id AND agent_app_id = NEW.agent_app_id
      AND revision = NEW.current_revision;
  IF NOT FOUND OR v_state <> 'published' THEN
    RAISE EXCEPTION 'current revision must be published' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_agent_app_revision_child(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_agent_app_revision_child() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_state text;
  v_digest text;
BEGIN
  SELECT state, content_digest INTO v_state, v_digest
  FROM public.agent_app_revision
  WHERE tenant_id = NEW.tenant_id
    AND agent_app_id = NEW.child_agent_app_id
    AND revision = NEW.child_revision;
  IF NOT FOUND OR v_state <> 'published' OR v_digest <> NEW.child_digest THEN
    RAISE EXCEPTION 'child revision must be published with matching digest'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_agent_app_revision_knowledge_published(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_agent_app_revision_knowledge_published() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  IF OLD.state<>'published' AND NEW.state='published' AND EXISTS (
    SELECT 1 FROM public.agent_app_revision_knowledge AS ref
    LEFT JOIN public.knowledge_manifest AS manifest
      ON manifest.tenant_id=ref.tenant_id AND manifest.knowledge_id=ref.knowledge_id AND manifest.version=ref.knowledge_version
    WHERE ref.tenant_id=NEW.tenant_id AND ref.agent_app_id=NEW.agent_app_id AND ref.revision=NEW.revision
      AND (manifest.state IS NULL OR manifest.state<>'published')
  ) THEN
    RAISE EXCEPTION 'agent app revision references unpublished knowledge' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_agent_model_profile_publish(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_agent_model_profile_publish() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $_$
BEGIN
  IF NEW.state = 'published' AND OLD.state = 'draft' AND NEW.agent_kind = 'llm' THEN
    IF NEW.model_profile_id IS NULL OR NEW.model_profile_version IS NULL THEN
      RAISE EXCEPTION 'published LLM agent requires a fixed model profile' USING ERRCODE = '23503';
    END IF;
    PERFORM 1 FROM public.model_profile p
      JOIN public.model_profile_revision v USING (tenant_id, model_profile_id)
      WHERE p.tenant_id = NEW.tenant_id
        AND p.model_profile_id = NEW.model_profile_id
        AND v.profile_version = NEW.model_profile_version
        AND p.status = 'active'
        AND v.content_digest ~ '^[0-9a-f]{64}$';
    IF NOT FOUND THEN
      RAISE EXCEPTION 'model profile is missing, inactive, or invalid' USING ERRCODE = '23503';
    END IF;
  END IF;
  RETURN NEW;
END;
$_$;


--
-- Name: guard_backend_migration_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_backend_migration_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
DECLARE
  expected_state text;
BEGIN
  IF (NEW.tenant_id,NEW.migration_id,NEW.domain,NEW.epoch,
      NEW.source_config_version,NEW.source_backend_profile_id,NEW.source_backend_version,
      NEW.target_config_version,NEW.target_backend_profile_id,NEW.target_backend_version,NEW.created_at)
     IS DISTINCT FROM
     (OLD.tenant_id,OLD.migration_id,OLD.domain,OLD.epoch,
      OLD.source_config_version,OLD.source_backend_profile_id,OLD.source_backend_version,
      OLD.target_config_version,OLD.target_backend_profile_id,OLD.target_backend_version,OLD.created_at) THEN
    RAISE EXCEPTION 'backend migration identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'backend migration version must advance exactly once' USING ERRCODE='40001';
  END IF;
  expected_state := CASE OLD.state
    WHEN 'planned' THEN 'snapshot' WHEN 'snapshot' THEN 'dual_write'
    WHEN 'dual_write' THEN 'backfill' WHEN 'backfill' THEN 'verify'
    WHEN 'verify' THEN 'cutover' WHEN 'cutover' THEN 'observe'
    WHEN 'observe' THEN 'cleanup' ELSE NULL END;
  IF NEW.state <> OLD.state AND NEW.state IS DISTINCT FROM expected_state THEN
    RAISE EXCEPTION 'illegal backend migration transition' USING ERRCODE='23514';
  END IF;
  IF NEW.state = OLD.state AND (
    OLD.state <> 'backfill' OR OLD.backfill_complete OR
    NEW.next_batch_seq <> OLD.next_batch_seq + 1 OR
    NEW.backfill_count < OLD.backfill_count OR
    NEW.backfill_checkpoint = OLD.backfill_checkpoint OR
    (NEW.snapshot_watermark,NEW.dual_write_ref,
     NEW.verify_source_count,NEW.verify_target_count,
     NEW.verify_source_digest,NEW.verify_target_digest,
     NEW.verify_source_watermark,NEW.verify_target_watermark,NEW.verify_sample_digest,
     NEW.cutover_config_version,NEW.cutover_at,NEW.observe_until,NEW.rollback_sync_watermark)
      IS DISTINCT FROM
    (OLD.snapshot_watermark,OLD.dual_write_ref,
     OLD.verify_source_count,OLD.verify_target_count,
     OLD.verify_source_digest,OLD.verify_target_digest,
     OLD.verify_source_watermark,OLD.verify_target_watermark,OLD.verify_sample_digest,
     OLD.cutover_config_version,OLD.cutover_at,OLD.observe_until,OLD.rollback_sync_watermark)
  ) THEN
    RAISE EXCEPTION 'backfill checkpoint update is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state <> OLD.state AND
     (NEW.backfill_checkpoint,NEW.next_batch_seq,NEW.backfill_count,NEW.backfill_complete)
       IS DISTINCT FROM
     (OLD.backfill_checkpoint,OLD.next_batch_seq,OLD.backfill_count,OLD.backfill_complete) THEN
    RAISE EXCEPTION 'state transition cannot mutate backfill progress' USING ERRCODE='23514';
  END IF;
  IF NEW.state='snapshot' AND length(NEW.snapshot_watermark)=0 THEN
    RAISE EXCEPTION 'snapshot watermark is required' USING ERRCODE='23514';
  END IF;
  IF NEW.state='dual_write' AND length(NEW.dual_write_ref)=0 THEN
    RAISE EXCEPTION 'dual write authority is required' USING ERRCODE='23514';
  END IF;
  IF NEW.state='verify' AND NOT NEW.backfill_complete THEN
    RAISE EXCEPTION 'backfill must complete before verification' USING ERRCODE='23514';
  END IF;
  IF NEW.state IN ('cutover','observe','cleanup') AND (
    NEW.verify_source_count IS NULL OR NEW.verify_target_count IS NULL OR
    NEW.verify_source_count<>NEW.verify_target_count OR
    NEW.verify_source_digest !~ '^[0-9a-f]{64}$' OR NEW.verify_target_digest<>NEW.verify_source_digest OR
    length(NEW.verify_source_watermark)=0 OR NEW.verify_target_watermark<>NEW.verify_source_watermark OR
    NEW.verify_sample_digest !~ '^[0-9a-f]{64}$' OR
    NEW.cutover_config_version IS NULL OR NEW.cutover_config_version<>NEW.target_config_version OR
    NEW.cutover_at IS NULL) THEN
    RAISE EXCEPTION 'verification evidence is incomplete' USING ERRCODE='23514';
  END IF;
  IF NEW.state IN ('observe','cleanup') AND (NEW.observe_until IS NULL OR NEW.observe_until <= NEW.cutover_at) THEN
    RAISE EXCEPTION 'observation window is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='cleanup' AND (NEW.updated_at < NEW.observe_until OR NEW.rollback_sync_watermark <> NEW.verify_target_watermark) THEN
    RAISE EXCEPTION 'rollback sync is incomplete' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$_$;


--
-- Name: guard_business_audit_purge_batch_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_business_audit_purge_batch_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE ok boolean;
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'business audit purge batch is immutable' USING ERRCODE='55000';
  END IF;
  IF (NEW.tenant_id,NEW.batch_id,NEW.cutoff_at,NEW.watermark_at,NEW.safe_cutoff_at,NEW.planned_events,
      NEW.planned_outbox,NEW.planned_digest,NEW.created_at)
    IS DISTINCT FROM
     (OLD.tenant_id,OLD.batch_id,OLD.cutoff_at,OLD.watermark_at,OLD.safe_cutoff_at,OLD.planned_events,
      OLD.planned_outbox,OLD.planned_digest,OLD.created_at) THEN
    RAISE EXCEPTION 'business audit purge batch identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'business audit purge batch version must advance exactly once' USING ERRCODE='40001';
  END IF;
  ok := false;
  IF NEW.state = OLD.state THEN
    ok := NEW.state IN ('planned','executing','failed');
  ELSIF OLD.state='planned' AND NEW.state IN ('executing','failed') THEN
    ok := true;
  ELSIF OLD.state='executing' AND NEW.state IN ('completed','failed','quarantined') THEN
    ok := true;
  ELSIF OLD.state='failed' AND NEW.state IN ('executing','quarantined') THEN
    ok := true;
  END IF;
  IF NOT ok THEN
    RAISE EXCEPTION 'illegal business audit purge batch transition' USING ERRCODE='23514';
  END IF;
  IF NEW.state = 'executing' AND (NEW.claim_owner = '' OR NEW.claim_until IS NULL) THEN
    RAISE EXCEPTION 'executing business audit purge batch requires a claim' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_execution_cancel_intent(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_execution_cancel_intent() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF NEW.outcome = 'cancelled'
     AND OLD.cancel_requested_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM public.tenant t WHERE t.tenant_id=OLD.tenant_id AND t.status='disabled') THEN
    RAISE EXCEPTION 'cancelled outcome requires a durable cancellation intent' USING ERRCODE = 'P0902';
  END IF;
  IF (OLD.cancel_requested_at IS NOT NULL
       OR EXISTS (SELECT 1 FROM public.tenant t WHERE t.tenant_id=OLD.tenant_id AND t.status='disabled'))
     AND NEW.outcome IN ('succeeded','denied','failed','confirmation_denied','confirmation_timeout') THEN
    RAISE EXCEPTION 'execution cancellation requested' USING ERRCODE = 'P0902';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_knowledge_manifest_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_knowledge_manifest_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  IF (NEW.tenant_id,NEW.knowledge_id,NEW.version,NEW.source_uri,NEW.source_digest,
      NEW.chunking_pipeline_version,NEW.embedder_profile_id,NEW.embedder_version,
      NEW.vector_collection_generation,NEW.metadata_schema,NEW.content_watermark,NEW.created_at)
    IS DISTINCT FROM
     (OLD.tenant_id,OLD.knowledge_id,OLD.version,OLD.source_uri,OLD.source_digest,
      OLD.chunking_pipeline_version,OLD.embedder_profile_id,OLD.embedder_version,
      OLD.vector_collection_generation,OLD.metadata_schema,OLD.content_watermark,OLD.created_at) THEN
    RAISE EXCEPTION 'knowledge manifest identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.record_version <> OLD.record_version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'knowledge manifest version must advance exactly once' USING ERRCODE='40001';
  END IF;
  IF NOT ((OLD.state='staging' AND NEW.state IN ('indexing','failed')) OR
          (OLD.state='indexing' AND NEW.state IN ('verifying','failed')) OR
          (OLD.state='verifying' AND NEW.state IN ('published','failed'))) THEN
    RAISE EXCEPTION 'illegal knowledge manifest state transition' USING ERRCODE='23514';
  END IF;
  IF OLD.chunk_total IS NOT NULL AND NEW.chunk_total IS DISTINCT FROM OLD.chunk_total THEN
    RAISE EXCEPTION 'knowledge manifest chunk_total is frozen' USING ERRCODE='23514';
  END IF;
  IF OLD.verification_digest <> '' AND NEW.verification_digest IS DISTINCT FROM OLD.verification_digest THEN
    RAISE EXCEPTION 'knowledge manifest verification_digest is frozen' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_knowledge_migration_authority_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_knowledge_migration_authority_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  IF NEW.domain='knowledge' AND NEW.state IN ('verify','cutover','cleanup') AND EXISTS (
    SELECT 1 FROM public.knowledge_migration_mutation
      WHERE tenant_id=NEW.tenant_id AND migration_id=NEW.migration_id AND state<>'applied'
  ) THEN
    RAISE EXCEPTION 'knowledge migration repair backlog is not drained' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_knowledge_migration_direction_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_knowledge_migration_direction_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF NEW.direction<>OLD.direction THEN
    RAISE EXCEPTION 'knowledge migration mutation direction is immutable' USING ERRCODE='23000';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_knowledge_migration_mutation_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_knowledge_migration_mutation_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $_$
BEGIN
  IF (NEW.tenant_id,NEW.migration_id,NEW.mutation_id,NEW.epoch,NEW.knowledge_id,
      NEW.knowledge_version,NEW.chunk_id,NEW.operation,NEW.source_revision,NEW.mutation_digest,NEW.created_at)
    IS DISTINCT FROM
     (OLD.tenant_id,OLD.migration_id,OLD.mutation_id,OLD.epoch,OLD.knowledge_id,
      OLD.knowledge_version,OLD.chunk_id,OLD.operation,OLD.source_revision,OLD.mutation_digest,OLD.created_at) THEN
    RAISE EXCEPTION 'knowledge migration mutation identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version<>OLD.version+1 OR NEW.updated_at<OLD.updated_at THEN
    RAISE EXCEPTION 'knowledge migration mutation version must advance exactly once' USING ERRCODE='40001';
  END IF;
  IF NOT ((OLD.state='pending' AND NEW.state='applying') OR
          (OLD.state='applying' AND NEW.state='applying' AND OLD.lease_until<=NEW.updated_at) OR
          (OLD.state='applying' AND NEW.state IN ('pending','applied'))) THEN
    RAISE EXCEPTION 'illegal knowledge migration mutation transition' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applying' AND (length(NEW.lease_owner)=0 OR NEW.lease_until IS NULL OR
      NEW.lease_until<=NEW.updated_at OR NEW.attempt<>OLD.attempt+1 OR
      (NEW.not_before,NEW.last_error_class,NEW.target_revision,NEW.target_digest,NEW.applied_at)
        IS DISTINCT FROM (OLD.not_before,OLD.last_error_class,OLD.target_revision,OLD.target_digest,OLD.applied_at)) THEN
    RAISE EXCEPTION 'knowledge migration mutation claim is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='pending' AND (length(NEW.lease_owner)>0 OR NEW.lease_until IS NOT NULL OR
      NEW.not_before<NEW.updated_at OR NEW.attempt<>OLD.attempt OR
      (NEW.target_revision,NEW.target_digest,NEW.applied_at)
        IS DISTINCT FROM (OLD.target_revision,OLD.target_digest,OLD.applied_at)) THEN
    RAISE EXCEPTION 'knowledge migration mutation retry is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applied' AND (NEW.target_revision IS NULL OR NEW.target_digest!~'^[0-9a-f]{64}$' OR
      NEW.applied_at IS NULL OR length(NEW.lease_owner)>0 OR NEW.lease_until IS NOT NULL OR NEW.attempt<>OLD.attempt) THEN
    RAISE EXCEPTION 'knowledge migration mutation result is invalid' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$_$;


--
-- Name: guard_outbox_idempotency(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_outbox_idempotency() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(
    hashtextextended(NEW.tenant_id || chr(31) || NEW.kind || chr(31) || NEW.idempotency_key, 0)
  );

  PERFORM 1
    FROM public.outbox
    WHERE tenant_id = NEW.tenant_id
      AND kind = NEW.kind
      AND idempotency_key = NEW.idempotency_key;
  IF FOUND THEN
    RETURN NULL;
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: FUNCTION guard_outbox_idempotency(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.guard_outbox_idempotency() IS 'Enforces semantic idempotency for public.outbox (tenant_id, kind, idempotency_key).';


--
-- Name: guard_profile_identity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_profile_identity() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.profile_key <> OLD.profile_key
     OR (TG_TABLE_NAME = 'model_profile'
       AND to_jsonb(NEW)->>'model_profile_id' <> to_jsonb(OLD)->>'model_profile_id')
     OR (TG_TABLE_NAME = 'backend_profile'
       AND to_jsonb(NEW)->>'backend_profile_id' <> to_jsonb(OLD)->>'backend_profile_id') THEN
    RAISE EXCEPTION 'profile identity is immutable' USING ERRCODE = '55000';
  END IF;
  IF OLD.status = 'disabled' AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'disabled profile is immutable' USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_profile_revision_immutable(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_profile_revision_immutable() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  RAISE EXCEPTION 'profile revisions are immutable' USING ERRCODE = '55000';
END;
$$;


--
-- Name: guard_revision_child_write(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_revision_child_write() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE v_state text;
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    SELECT state INTO v_state FROM public.agent_app_revision
      WHERE tenant_id = OLD.tenant_id AND agent_app_id = OLD.agent_app_id
        AND revision = OLD.revision FOR UPDATE;
    IF NOT FOUND OR v_state <> 'draft' THEN
      RAISE EXCEPTION 'source revision is not mutable' USING ERRCODE = '55000';
    END IF;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    SELECT state INTO v_state FROM public.agent_app_revision
      WHERE tenant_id = NEW.tenant_id AND agent_app_id = NEW.agent_app_id
        AND revision = NEW.revision FOR UPDATE;
    IF NOT FOUND OR v_state <> 'draft' THEN
      RAISE EXCEPTION 'target revision is not mutable' USING ERRCODE = '55000';
    END IF;
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_session_migration_authority_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_session_migration_authority_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  IF NEW.domain='session' AND NEW.state IN ('verify','cutover','cleanup') AND EXISTS (
    SELECT 1 FROM public.session_migration_mutation
    WHERE tenant_id=NEW.tenant_id AND migration_id=NEW.migration_id AND state<>'applied'
  ) THEN
    RAISE EXCEPTION 'session migration repair backlog is not drained' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_session_migration_direction_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_session_migration_direction_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF NEW.direction<>OLD.direction THEN
    RAISE EXCEPTION 'session migration mutation direction is immutable' USING ERRCODE='23000';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_session_migration_mutation_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_session_migration_mutation_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $_$
BEGIN
  IF (NEW.tenant_id,NEW.migration_id,NEW.mutation_id,NEW.epoch,NEW.agent_app_id,
      NEW.session_id,NEW.source_version,NEW.mutation_digest,NEW.created_at)
    IS DISTINCT FROM
     (OLD.tenant_id,OLD.migration_id,OLD.mutation_id,OLD.epoch,OLD.agent_app_id,
      OLD.session_id,OLD.source_version,OLD.mutation_digest,OLD.created_at) THEN
    RAISE EXCEPTION 'session migration mutation identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'session migration mutation version must advance exactly once' USING ERRCODE='40001';
  END IF;
  IF NOT ((OLD.state='pending' AND NEW.state='applying') OR
          (OLD.state='applying' AND NEW.state='applying' AND OLD.lease_until<=NEW.updated_at) OR
          (OLD.state='applying' AND NEW.state IN ('pending','applied'))) THEN
    RAISE EXCEPTION 'illegal session migration mutation transition' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applying' AND (length(NEW.lease_owner)=0 OR NEW.lease_until IS NULL OR NEW.lease_until<=NEW.updated_at) THEN
    RAISE EXCEPTION 'session migration mutation lease is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applying' AND (NEW.attempt<>OLD.attempt+1 OR
      (NEW.not_before,NEW.last_error_class,NEW.target_version,NEW.target_digest,NEW.applied_at)
        IS DISTINCT FROM
      (OLD.not_before,OLD.last_error_class,OLD.target_version,OLD.target_digest,OLD.applied_at)) THEN
    RAISE EXCEPTION 'session migration mutation claim is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='pending' AND (length(NEW.lease_owner)>0 OR NEW.lease_until IS NOT NULL OR NEW.not_before<NEW.updated_at) THEN
    RAISE EXCEPTION 'session migration mutation retry is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='pending' AND (NEW.attempt<>OLD.attempt OR
      (NEW.target_version,NEW.target_digest,NEW.applied_at)
        IS DISTINCT FROM (OLD.target_version,OLD.target_digest,OLD.applied_at)) THEN
    RAISE EXCEPTION 'session migration mutation retry result is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='applied' AND (NEW.target_version IS NULL OR NEW.target_digest !~ '^[0-9a-f]{64}$' OR
      NEW.applied_at IS NULL OR length(NEW.lease_owner)>0 OR NEW.lease_until IS NOT NULL OR NEW.attempt<>OLD.attempt) THEN
    RAISE EXCEPTION 'session migration mutation result is invalid' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$_$;


--
-- Name: guard_session_summary_content(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_session_summary_content() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE
  old_base bigint;
  new_base bigint;
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.state <> 'delete_claimed' THEN
      RAISE EXCEPTION 'summary content must be claimed before deletion' USING ERRCODE='55000';
    END IF;
    RETURN OLD;
  END IF;
  IF TG_OP = 'INSERT' THEN
    RETURN NEW;
  END IF;
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.agent_app_id <> OLD.agent_app_id OR NEW.session_id <> OLD.session_id OR
     NEW.summary_id <> OLD.summary_id OR NEW.content_ref <> OLD.content_ref OR NEW.content_digest <> OLD.content_digest OR
     NEW.content <> OLD.content OR NEW.created_at <> OLD.created_at THEN
    RAISE EXCEPTION 'summary content identity is immutable' USING ERRCODE='23514';
  END IF;
  IF NEW.record_version <> OLD.record_version + 1 THEN
    RAISE EXCEPTION 'summary content version must advance by one' USING ERRCODE='40001';
  END IF;
  IF OLD.state = 'active' AND NEW.state = 'superseded' THEN
    SELECT base_session_seq INTO old_base FROM public.session_summary
      WHERE tenant_id=OLD.tenant_id AND agent_app_id=OLD.agent_app_id AND session_id=OLD.session_id AND summary_id=OLD.summary_id;
    SELECT base_session_seq INTO new_base FROM public.session_summary
      WHERE tenant_id=NEW.tenant_id AND agent_app_id=NEW.agent_app_id AND session_id=NEW.session_id AND summary_id=NEW.superseded_by_summary_id;
    IF new_base IS NULL OR new_base <= old_base THEN
      RAISE EXCEPTION 'summary replacement must advance watermark' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
  END IF;
  IF (OLD.state = 'superseded' AND NEW.state = 'delete_claimed') OR
     (OLD.state = 'delete_claimed' AND NEW.state IN ('delete_claimed','superseded')) THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'illegal summary content transition' USING ERRCODE='23514';
END;
$$;


--
-- Name: guard_skill_catalog(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.guard_skill_catalog() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'skill catalog is append-only' USING ERRCODE='23514';
  END IF;
  IF TG_OP = 'INSERT' THEN
    RETURN NEW;
  END IF;
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.skill_id <> OLD.skill_id OR NEW.skill_version <> OLD.skill_version OR
     NEW.content_digest <> OLD.content_digest OR NEW.relative_path <> OLD.relative_path OR NEW.created_at <> OLD.created_at THEN
    RAISE EXCEPTION 'skill catalog identity is immutable' USING ERRCODE='23514';
  END IF;
  IF NEW.record_version <> OLD.record_version + 1 THEN
    RAISE EXCEPTION 'skill catalog version must advance by one' USING ERRCODE='40001';
  END IF;
  IF OLD.state='staged' AND NEW.state IN ('published','failed') THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'illegal skill catalog transition' USING ERRCODE='23514';
END;
$$;


--
-- Name: inspect_execution_wakeup(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.inspect_execution_wakeup(p_tenant_id text, p_request_id text) RETURNS TABLE(tenant_id text, tenant_version bigint, agent_app_id text, agent_app_version bigint, agent_app_revision bigint, agent_content_digest text, config_version bigint, policy_version bigint, request_id text, session_id text, user_id text, channel text, input_seq bigint, payload_ref text, traceparent text, outcome text, result_ref text, created_at timestamp with time zone, ready boolean, blocked boolean, execution_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_probe public.execution_record%ROWTYPE; v_execution public.execution_record%ROWTYPE; v_next bigint;
BEGIN
  SELECT * INTO v_probe FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE='P0002'; END IF;
  SELECT h.next_input_seq INTO v_next FROM public.session_head h
    WHERE h.tenant_id=v_probe.tenant_id AND h.agent_app_id=v_probe.agent_app_id
      AND h.session_id=v_probe.session_id FOR UPDATE;
  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id FOR UPDATE;
  IF v_execution.outcome='pending' AND v_execution.park_deadline IS NOT NULL
     AND clock_timestamp() >= v_execution.park_deadline THEN
    UPDATE public.execution_record SET outcome='blocked',blocked_at=clock_timestamp(),
      blocked_reason='park_deadline_exceeded',version=version+1
      WHERE execution_record.tenant_id=p_tenant_id AND execution_record.request_id=p_request_id
      RETURNING * INTO v_execution;
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
      VALUES(p_tenant_id,format('park-blocked:%s',p_request_id),'audit',p_request_id,1,
        format('park-blocked:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id))
      ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
  END IF;
  RETURN QUERY SELECT v_execution.tenant_id,v_execution.tenant_version,v_execution.agent_app_id,
    v_execution.agent_app_version,v_execution.agent_app_revision,v_execution.agent_content_digest,
    v_execution.config_version,v_execution.policy_version,v_execution.request_id,v_execution.session_id,
    v_execution.user_id,v_execution.channel,v_execution.input_seq,v_execution.payload_ref,
    v_execution.traceparent,v_execution.outcome,v_execution.result_ref,v_execution.created_at,
    (v_execution.outcome='pending' AND v_execution.input_seq=v_next
      AND COALESCE(v_execution.not_before,'-infinity'::timestamptz)<=clock_timestamp()),
    (v_execution.outcome='blocked'),v_execution.version;
END;
$$;


--
-- Name: knowledge_backend_migration_drain_status(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.knowledge_backend_migration_drain_status(p_tenant_id text, p_migration_id text) RETURNS TABLE(source_in_flight bigint, target_in_flight bigint, forward_outstanding bigint, reverse_outstanding bigint, active_config_version bigint, rolled_back boolean)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog'
    AS $$
  SELECT
    (SELECT count(*) FROM public.execution_record e WHERE e.tenant_id=m.tenant_id AND e.config_version=m.source_config_version
      AND e.outcome IN ('queued','running','pending','blocked','waiting_confirmation')),
    (SELECT count(*) FROM public.execution_record e WHERE e.tenant_id=m.tenant_id AND e.config_version=m.target_config_version
      AND e.outcome IN ('queued','running','pending','blocked','waiting_confirmation')),
    (SELECT count(*) FROM public.knowledge_migration_mutation x WHERE x.tenant_id=m.tenant_id AND x.migration_id=m.migration_id AND x.direction='forward' AND x.state<>'applied'),
    (SELECT count(*) FROM public.knowledge_migration_mutation x WHERE x.tenant_id=m.tenant_id AND x.migration_id=m.migration_id AND x.direction='reverse' AND x.state<>'applied'),
    t.active_config_version,
    EXISTS(SELECT 1 FROM public.backend_migration_config_switch s WHERE s.tenant_id=m.tenant_id AND s.migration_id=m.migration_id AND s.direction='rollback')
  FROM public.backend_migration m JOIN public.tenant t ON t.tenant_id=m.tenant_id
  WHERE m.tenant_id=p_tenant_id AND m.migration_id=p_migration_id;
$$;


--
-- Name: maintain_tenant_update(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.maintain_tenant_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.tenant_key <> OLD.tenant_key THEN
    RAISE EXCEPTION 'tenant identity is immutable' USING ERRCODE = '23000';
  END IF;
  IF NEW.version <> OLD.version + 1 THEN
    RAISE EXCEPTION 'tenant version must increase by exactly one' USING ERRCODE = '40001';
  END IF;
  IF NEW.status <> OLD.status AND NOT (
    (OLD.status = 'active' AND NEW.status IN ('suspended', 'disabled'))
    OR (OLD.status = 'suspended' AND NEW.status IN ('active', 'disabled'))
  ) THEN
    RAISE EXCEPTION 'illegal tenant status transition' USING ERRCODE = '23514';
  END IF;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END;
$$;


--
-- Name: mark_knowledge_chunk_indexed(text, text, bigint, text, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.mark_knowledge_chunk_indexed(p_tenant_id text, p_knowledge_id text, p_knowledge_version bigint, p_chunk_id text, p_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_knowledge_version<1 OR
     length(btrim(p_chunk_id))=0 OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge chunk index input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_knowledge_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state<>'indexing' THEN
    RAISE EXCEPTION 'knowledge manifest is not indexing' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_chunk SET indexed_at=COALESCE(indexed_at,p_at)
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version AND chunk_id=p_chunk_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge chunk does not exist' USING ERRCODE='P0002'; END IF;
END;
$$;


--
-- Name: mark_knowledge_probe_verified(text, text, bigint, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.mark_knowledge_probe_verified(p_tenant_id text, p_knowledge_id text, p_knowledge_version bigint, p_probe_id text) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_knowledge_version<1 OR
     length(btrim(p_probe_id))=0 THEN
    RAISE EXCEPTION 'knowledge probe verify input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_knowledge_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state<>'verifying' THEN
    RAISE EXCEPTION 'knowledge manifest is not verifying' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_probe SET verified=true
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version AND probe_id=p_probe_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge probe does not exist' USING ERRCODE='P0002'; END IF;
END;
$$;


--
-- Name: park_execution(text, text, bigint, bigint, bigint, bigint, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.park_execution(p_tenant_id text, p_request_id text, p_input_seq bigint, p_base_delay_seconds bigint, p_max_delay_seconds bigint, p_deadline_seconds bigint, p_max_attempts integer) RETURNS TABLE(disposition text, attempt integer, execution_version bigint, not_before timestamp with time zone, deadline timestamp with time zone)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_probe public.execution_record%ROWTYPE;
  v_execution public.execution_record%ROWTYPE;
  v_next bigint;
  v_attempt integer;
  v_now timestamptz := clock_timestamp();
  v_deadline timestamptz;
  v_not_before timestamptz;
BEGIN
  IF p_base_delay_seconds < 1 OR p_max_delay_seconds < p_base_delay_seconds
     OR p_deadline_seconds < p_max_delay_seconds OR p_max_attempts < 1 OR p_max_attempts > 64 THEN
    RAISE EXCEPTION 'invalid park policy' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_probe FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE='P0002'; END IF;
  IF v_probe.input_seq <> p_input_seq THEN
    RAISE EXCEPTION 'execution park scope mismatch' USING ERRCODE='42501';
  END IF;

  SELECT h.next_input_seq INTO v_next FROM public.session_head h
    WHERE h.tenant_id=v_probe.tenant_id AND h.agent_app_id=v_probe.agent_app_id
      AND h.session_id=v_probe.session_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'session not found' USING ERRCODE='P0002'; END IF;

  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id FOR UPDATE;
  IF v_execution.input_seq <> p_input_seq OR v_execution.agent_app_id <> v_probe.agent_app_id
     OR v_execution.session_id <> v_probe.session_id THEN
    RAISE EXCEPTION 'execution changed during park' USING ERRCODE='40001';
  END IF;
  IF v_execution.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')
     OR p_input_seq < v_next THEN
    PERFORM 1 FROM public.session_commit c WHERE c.tenant_id=v_execution.tenant_id
      AND c.agent_app_id=v_execution.agent_app_id AND c.session_id=v_execution.session_id
      AND c.input_seq=v_execution.input_seq
      AND c.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout');
    IF NOT FOUND OR v_execution.outcome NOT IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
      RAISE EXCEPTION 'terminal invariant missing during park' USING ERRCODE='XX001';
    END IF;
    RETURN QUERY SELECT 'terminal'::text,v_execution.park_attempt,v_execution.version,
      v_execution.not_before,v_execution.park_deadline;
    RETURN;
  END IF;
  IF p_input_seq = v_next THEN
    RETURN QUERY SELECT 'ready'::text,v_execution.park_attempt,v_execution.version,
      v_execution.not_before,v_execution.park_deadline;
    RETURN;
  END IF;

  v_deadline := COALESCE(v_execution.park_deadline,
    v_now + make_interval(secs => p_deadline_seconds::double precision));
  IF v_execution.outcome = 'pending' THEN
    IF v_now >= v_deadline THEN
      UPDATE public.execution_record SET outcome='blocked',blocked_at=v_now,
        blocked_reason='park_deadline_exceeded',park_deadline=v_deadline,version=version+1
        WHERE tenant_id=p_tenant_id AND request_id=p_request_id
        RETURNING park_attempt,version INTO v_attempt,execution_version;
      INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
        VALUES(p_tenant_id,format('park-blocked:%s',p_request_id),'audit',p_request_id,1,
          format('park-blocked:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id))
        ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
      RETURN QUERY SELECT 'blocked'::text,v_attempt,execution_version,NULL::timestamptz,v_deadline;
      RETURN;
    END IF;
    RETURN QUERY SELECT 'parked'::text,v_execution.park_attempt,v_execution.version,
      v_execution.not_before,v_deadline;
    RETURN;
  END IF;

  v_attempt := v_execution.park_attempt + 1;
  IF v_attempt > p_max_attempts OR v_now >= v_deadline THEN
    UPDATE public.execution_record SET outcome='blocked',park_attempt=v_attempt,
      park_deadline=v_deadline,blocked_at=v_now,
      blocked_reason=CASE WHEN v_attempt > p_max_attempts THEN 'park_attempts_exhausted' ELSE 'park_deadline_exceeded' END,
      not_before=NULL,version=version+1
      WHERE tenant_id=p_tenant_id AND request_id=p_request_id
      RETURNING version INTO execution_version;
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
      VALUES(p_tenant_id,format('park-blocked:%s',p_request_id),'audit',p_request_id,1,
        format('park-blocked:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id))
      ON CONFLICT ON CONSTRAINT outbox_tenant_id_kind_idempotency_key_key DO NOTHING;
    RETURN QUERY SELECT 'blocked'::text,v_attempt,execution_version,NULL::timestamptz,v_deadline;
    RETURN;
  END IF;

  v_not_before := LEAST(v_deadline, v_now + make_interval(secs => LEAST(p_max_delay_seconds::double precision,
    p_base_delay_seconds::double precision * power(2::double precision,v_attempt-1))));
  UPDATE public.execution_record SET outcome='pending',park_attempt=v_attempt,
    not_before=v_not_before,park_deadline=v_deadline,blocked_at=NULL,blocked_reason=NULL,
    version=version+1 WHERE tenant_id=p_tenant_id AND request_id=p_request_id
    RETURNING version INTO execution_version;
  RETURN QUERY SELECT 'parked'::text,v_attempt,execution_version,v_not_before,v_deadline;
END;
$$;


--
-- Name: plan_business_audit_purge(text, timestamp with time zone, text, text, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.plan_business_audit_purge(p_tenant text, p_cutoff timestamp with time zone, p_actor text, p_reason text, p_now timestamp with time zone) RETURNS text
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_batch text;
  v_watermark timestamptz;
  v_safe timestamptz;
  v_events bigint;
  v_outbox bigint;
  v_digest text;
BEGIN
  IF NOT pg_has_role(session_user, 'audit_retention_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to plan business audit purge' USING ERRCODE = '42501';
  END IF;
  IF p_tenant IS NULL OR length(btrim(p_tenant))=0 OR p_cutoff IS NULL OR p_now IS NULL OR
     length(btrim(p_actor))=0 OR length(btrim(p_reason))=0 THEN
    RAISE EXCEPTION 'business audit purge input is invalid' USING ERRCODE = '22023';
  END IF;

  v_batch := 'bap_' || encode(sha256(convert_to(
    'business-audit-purge-v1' || chr(31) || p_tenant || chr(31) || (EXTRACT(EPOCH FROM p_cutoff))::bigint::text,
    'UTF8')), 'hex');
  IF EXISTS (SELECT 1 FROM public.business_audit_purge_batch WHERE tenant_id = p_tenant AND batch_id = v_batch) THEN
    RETURN v_batch;
  END IF;

  v_watermark := public.business_audit_watermark(p_tenant);
  v_safe := p_cutoff;
  IF v_watermark IS NOT NULL AND v_watermark < p_cutoff THEN
    v_safe := v_watermark;
  END IF;

  SELECT count(*) INTO v_events FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;
  SELECT count(*) INTO v_outbox FROM public.outbox o
    WHERE o.tenant_id = p_tenant AND o.kind = 'audit' AND o.state = 'published' AND o.created_at < v_safe;
  SELECT COALESCE(encode(sha256(convert_to(string_agg(e.audit_id, chr(10) ORDER BY e.occurred_at, e.audit_id), 'UTF8')), 'hex'),
                  encode(sha256(convert_to('', 'UTF8')), 'hex'))
    INTO v_digest
    FROM public.audit_event e
    WHERE e.tenant_id = p_tenant AND e.occurred_at < v_safe;

  INSERT INTO public.business_audit_purge_batch(tenant_id,batch_id,state,cutoff_at,watermark_at,safe_cutoff_at,
    planned_events,planned_outbox,planned_digest,not_before,created_at,updated_at)
  VALUES(p_tenant, v_batch, 'planned', p_cutoff, v_watermark, v_safe,
    v_events, v_outbox, v_digest, p_now, p_now, p_now);
  RETURN v_batch;
END;
$$;


--
-- Name: populate_channel_send_secret(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.populate_channel_send_secret() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_secret jsonb;
  v_ref text;
  v_version bigint;
BEGIN
  NEW.send_secret_ref := NULL;
  NEW.send_secret_version := NULL;
  SELECT entries.item->'send_secret_ref'
    INTO v_secret
  FROM public.config_snapshot snapshot,
       LATERAL jsonb_array_elements(COALESCE(snapshot.payload->'channel_bindings', '[]'::jsonb)) AS entries(item)
  WHERE snapshot.tenant_id = NEW.tenant_id
    AND snapshot.config_version = NEW.config_version
    AND entries.item->>'binding_id' = NEW.binding_id
  LIMIT 1;

  IF v_secret IS NOT NULL AND jsonb_typeof(v_secret) = 'object' THEN
    v_ref := NULLIF(btrim(v_secret->>'ref'), '');
    BEGIN
      v_version := NULLIF(v_secret->>'version', '')::bigint;
    EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
      RAISE EXCEPTION 'invalid channel send secret version' USING ERRCODE = '22023';
    END;
    IF v_ref IS NOT NULL AND v_version >= 1 THEN
      NEW.send_secret_ref := v_ref;
      NEW.send_secret_version := v_version;
    END IF;
  END IF;

  IF NEW.channel IN ('feishu', 'wecom') AND
     (NEW.send_secret_ref IS NULL OR NEW.send_secret_version IS NULL) THEN
    RAISE EXCEPTION 'channel send secret is required' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: prepare_dispatch(text, bigint, text, bigint, bigint, text, bigint, bigint, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.prepare_dispatch(p_tenant_id text, p_expected_tenant_version bigint, p_agent_app_id text, p_expected_app_version bigint, p_agent_app_revision bigint, p_agent_content_digest text, p_config_version bigint, p_policy_version bigint, p_request_id text, p_session_id text, p_user_id text, p_channel text, p_payload_ref text, p_traceparent text) RETURNS TABLE(input_seq bigint, accepted boolean, terminal_reason text)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_tenant_status text; v_tenant_version bigint; v_active_config bigint;
  v_app_status text; v_app_version bigint; v_current_revision bigint;
  v_revision_digest text; v_inbox public.inbox%ROWTYPE; v_input bigint;
BEGIN
  SELECT status,version,active_config_version INTO v_tenant_status,v_tenant_version,v_active_config
    FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant not found' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_inbox FROM public.inbox WHERE tenant_id=p_tenant_id AND request_id=p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'inbox not found' USING ERRCODE='P0002'; END IF;
  IF v_inbox.state='dispatch_ready' THEN
    SELECT e.input_seq INTO v_input FROM public.execution_record e
      WHERE e.tenant_id=p_tenant_id AND e.request_id=p_request_id
        AND e.agent_app_id=p_agent_app_id AND e.session_id=p_session_id
        AND e.payload_ref=p_payload_ref AND e.agent_app_version=p_expected_app_version
        AND e.agent_app_revision=p_agent_app_revision AND e.agent_content_digest=p_agent_content_digest
        AND e.config_version=p_config_version AND e.policy_version=p_policy_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'dispatch idempotency collision' USING ERRCODE='23505'; END IF;
    RETURN QUERY SELECT v_input,true,NULL::text; RETURN;
  END IF;
  IF v_inbox.state='terminal' THEN
    RETURN QUERY SELECT NULL::bigint,false,v_inbox.terminal_reason; RETURN;
  END IF;
  IF v_inbox.state NOT IN ('preprocess_pending','dispatch_pending')
     OR v_inbox.agent_app_id<>p_agent_app_id OR v_inbox.session_id IS DISTINCT FROM p_session_id
     OR v_inbox.channel<>p_channel
     OR (v_inbox.payload_ref<>p_payload_ref AND NOT EXISTS (
       SELECT 1 FROM public.prepared_payload p JOIN public.preprocess_job j
         ON j.tenant_id=p.tenant_id AND j.request_id=p.request_id
       WHERE p.tenant_id=p_tenant_id AND p.request_id=p_request_id
         AND p.payload_ref=p_payload_ref AND p.source_payload_ref=v_inbox.payload_ref
         AND j.state='ready' AND j.prepared_payload_ref=p.payload_ref
     )) THEN
    RAISE EXCEPTION 'inbox scope or state mismatch' USING ERRCODE='42501';
  END IF;
  IF v_tenant_status<>'active' THEN
    UPDATE public.inbox SET state='terminal',terminal_reason=v_tenant_status,version=version+1,updated_at=now()
      WHERE tenant_id=p_tenant_id AND request_id=p_request_id;
    INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
      VALUES (p_tenant_id,format('dispatch-denied-reply:%s',p_request_id),'reply',p_request_id,1,
        format('dispatch-denied:%s:reply',p_request_id),format('inbox://%s/%s',p_tenant_id,p_request_id),p_traceparent),
      (p_tenant_id,format('dispatch-denied-audit:%s',p_request_id),'audit',p_request_id,1,
        format('dispatch-denied:%s:audit',p_request_id),format('inbox://%s/%s',p_tenant_id,p_request_id),p_traceparent)
      ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING;
    RETURN QUERY SELECT NULL::bigint,false,v_tenant_status; RETURN;
  END IF;
  IF v_tenant_version<>p_expected_tenant_version OR v_active_config<>p_config_version THEN
    RAISE EXCEPTION 'tenant or config version conflict' USING ERRCODE='40001';
  END IF;
  SELECT status,version,current_revision INTO v_app_status,v_app_version,v_current_revision
    FROM public.agent_app WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id FOR UPDATE;
  IF NOT FOUND OR v_app_status<>'active' OR v_app_version<>p_expected_app_version
     OR v_current_revision<>p_agent_app_revision THEN
    RAISE EXCEPTION 'agent app binding changed' USING ERRCODE='40001';
  END IF;
  SELECT content_digest INTO v_revision_digest FROM public.agent_app_revision
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND revision=p_agent_app_revision AND state='published';
  IF NOT FOUND OR v_revision_digest<>p_agent_content_digest THEN
    RAISE EXCEPTION 'agent revision binding changed' USING ERRCODE='40001';
  END IF;
  PERFORM 1 FROM public.config_snapshot WHERE tenant_id=p_tenant_id AND config_version=p_config_version
    AND state='published' AND (payload->>'policy_version')::bigint=p_policy_version;
  IF NOT FOUND THEN RAISE EXCEPTION 'policy binding changed' USING ERRCODE='40001'; END IF;
  INSERT INTO public.session_head(tenant_id,agent_app_id,session_id)
    VALUES(p_tenant_id,p_agent_app_id,p_session_id) ON CONFLICT DO NOTHING;
  SELECT last_allocated_input_seq+1 INTO v_input FROM public.session_head
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND session_id=p_session_id FOR UPDATE;
  UPDATE public.session_head SET last_allocated_input_seq=v_input,updated_at=now()
    WHERE tenant_id=p_tenant_id AND agent_app_id=p_agent_app_id AND session_id=p_session_id;
  INSERT INTO public.execution_record(tenant_id,request_id,tenant_version,agent_app_id,agent_app_version,
    agent_app_revision,agent_content_digest,config_version,policy_version,session_id,user_id,channel,input_seq,payload_ref,traceparent)
    VALUES(p_tenant_id,p_request_id,v_tenant_version,p_agent_app_id,v_app_version,p_agent_app_revision,
      p_agent_content_digest,p_config_version,p_policy_version,p_session_id,p_user_id,p_channel,v_input,p_payload_ref,p_traceparent);
  UPDATE public.inbox SET state='dispatch_ready',input_seq=v_input,version=version+1,updated_at=now()
    WHERE tenant_id=p_tenant_id AND request_id=p_request_id;
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
    VALUES(p_tenant_id,format('dispatch:%s',p_request_id),'dispatch',p_request_id,1,
      format('dispatch:%s',p_request_id),format('execution://%s/%s',p_tenant_id,p_request_id),p_traceparent);
  RETURN QUERY SELECT v_input,true,NULL::text;
END;
$$;


--
-- Name: publish_agent_app_revision(text, text, bigint, bigint, bigint, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.publish_agent_app_revision(p_tenant_id text, p_agent_app_id text, p_revision bigint, p_expected_app_version bigint, p_expected_draft_version bigint, p_content_digest text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE
  v_tenant_status text;
  v_app_status text;
  v_app_version bigint;
  v_revision_state text;
  v_draft_version bigint;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'agent app publish metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status INTO v_tenant_status FROM public.tenant
    WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_tenant_status = 'disabled' THEN RAISE EXCEPTION 'tenant is disabled' USING ERRCODE = '55000'; END IF;
  SELECT status, version INTO v_app_status, v_app_version FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'agent app does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_app_status = 'disabled' THEN RAISE EXCEPTION 'agent app is disabled' USING ERRCODE = '55000'; END IF;
  IF v_app_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict' USING ERRCODE = '40001'; END IF;
  SELECT state, draft_version INTO v_revision_state, v_draft_version
    FROM public.agent_app_revision
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id
      AND revision = p_revision FOR UPDATE;
  IF NOT FOUND OR v_revision_state <> 'draft' THEN RAISE EXCEPTION 'revision is not draft' USING ERRCODE = '55000'; END IF;
  IF v_draft_version <> p_expected_draft_version THEN RAISE EXCEPTION 'draft version conflict' USING ERRCODE = '40001'; END IF;
  IF p_content_digest !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'invalid content digest' USING ERRCODE = '22023'; END IF;
  UPDATE public.agent_app_revision SET
    state = 'published', content_digest = p_content_digest,
    published_at = now(), updated_at = now()
  WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id
    AND revision = p_revision;
  v_next_version := v_app_version + 1;
  UPDATE public.agent_app SET
    current_revision = p_revision,
    status = CASE WHEN status = 'draft' THEN 'active' ELSE status END,
    version = v_next_version
  WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id;
  INSERT INTO public.agent_app_change(
    tenant_id, agent_app_id, kind, current_revision,
    previous_status, current_status, previous_version, next_version,
    actor_id, reason_code, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, p_agent_app_id, 'published', p_revision,
    v_app_status, CASE WHEN v_app_status = 'draft' THEN 'active' ELSE v_app_status END,
    v_app_version, v_next_version, p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('app-publish-audit:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'audit', p_agent_app_id, v_next_version, format('app:%s:%s:%s:audit', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app-revision://%s/%s/%s', p_tenant_id, p_agent_app_id, p_revision), p_traceparent),
    (p_tenant_id, format('app-publish-invalidation:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'config-invalidation', p_agent_app_id, v_next_version, format('app:%s:%s:%s:invalidate', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app://%s/%s/%s', p_tenant_id, p_agent_app_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$_$;


--
-- Name: publish_config_snapshot(text, bigint, integer, jsonb, text, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.publish_config_snapshot(p_tenant_id text, p_expected_tenant_version bigint, p_schema_version integer, p_payload jsonb, p_content_digest text, p_default_agent_app_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS TABLE(config_version bigint, tenant_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE
  v_status text;
  v_current_version bigint;
  v_config_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'config publish metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status, version INTO v_status, v_current_version
    FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_status = 'disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE = '55000'; END IF;
  IF v_current_version <> p_expected_tenant_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE = '40001'; END IF;
  IF p_schema_version <> 1 OR jsonb_typeof(p_payload) <> 'object'
     OR p_content_digest !~ '^[0-9a-f]{64}$'
     OR p_payload->>'default_agent_app_id' IS DISTINCT FROM p_default_agent_app_id
     OR COALESCE((p_payload->>'policy_version')::bigint, 0) < 1 THEN
    RAISE EXCEPTION 'invalid config snapshot' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_default_agent_app_id
      AND status = 'active' AND current_revision IS NOT NULL;
  IF NOT FOUND THEN RAISE EXCEPTION 'default app is not active' USING ERRCODE = '23503'; END IF;
  SELECT COALESCE(max(cs.config_version), 0) + 1 INTO v_config_version
    FROM public.config_snapshot cs WHERE cs.tenant_id = p_tenant_id;
  INSERT INTO public.config_snapshot(
    tenant_id, config_version, schema_version, payload, content_digest, state,
    actor_id, reason_code, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, v_config_version, p_schema_version, p_payload, p_content_digest,
    'published', p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.channel_binding(
    tenant_id, config_version, binding_id, channel, external_account_id,
    agent_app_id, secret_ref, secret_version
  )
  SELECT p_tenant_id, v_config_version, item.binding_id, item.channel,
    item.external_account_id, item.agent_app_id,
    item.secret_ref->>'ref', (item.secret_ref->>'version')::bigint
  FROM jsonb_to_recordset(COALESCE(p_payload->'channel_bindings', '[]'::jsonb))
    AS item(binding_id text, channel text, external_account_id text, agent_app_id text, secret_ref jsonb);
  INSERT INTO public.backend_binding(
    tenant_id, config_version, domain, backend_profile_id, backend_version, required
  )
  SELECT p_tenant_id, v_config_version, item.domain, item.backend_profile_id,
    item.backend_version,
    ARRAY(SELECT jsonb_array_elements_text(COALESCE(item.required, '[]'::jsonb)))
  FROM jsonb_to_recordset(COALESCE(p_payload->'backend_bindings', '[]'::jsonb))
    AS item(domain text, backend_profile_id text, backend_version bigint, required jsonb);
  UPDATE public.tenant SET
    default_agent_app_id = p_default_agent_app_id,
    active_config_version = v_config_version,
    version = v_current_version + 1
  WHERE tenant_id = p_tenant_id;
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('config-audit:%s:%s', p_tenant_id, v_config_version), 'audit', p_tenant_id, v_config_version, format('config:%s:%s:audit', p_tenant_id, v_config_version), format('config://%s/%s', p_tenant_id, v_config_version), p_traceparent),
    (p_tenant_id, format('config-invalidation:%s:%s', p_tenant_id, v_config_version), 'config-invalidation', p_tenant_id, v_config_version, format('config:%s:%s:invalidate', p_tenant_id, v_config_version), format('config://%s/%s', p_tenant_id, v_config_version), p_traceparent);
  RETURN QUERY SELECT v_config_version, v_current_version + 1;
END;
$_$;


--
-- Name: publish_knowledge_version(text, text, bigint, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.publish_knowledge_version(p_tenant_id text, p_knowledge_id text, p_version bigint, p_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE; v_unverified bigint; v_probe_count bigint; v_indexed bigint; v_computed_digest text;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_version<1 OR p_at IS NULL THEN
    RAISE EXCEPTION 'knowledge publish input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state <> 'verifying' THEN
    RAISE EXCEPTION 'knowledge manifest is not verifying' USING ERRCODE='23514';
  END IF;
  SELECT count(*) INTO v_indexed FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version AND indexed_at IS NOT NULL;
  IF v_indexed<>v_manifest.chunk_total THEN
    RAISE EXCEPTION 'knowledge indexing is incomplete' USING ERRCODE='23514';
  END IF;
  SELECT count(*),count(*) FILTER (WHERE verified=false) INTO v_probe_count,v_unverified FROM public.knowledge_probe
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version;
  IF v_probe_count=0 OR v_unverified > 0 THEN
    RAISE EXCEPTION 'knowledge sample verification is incomplete' USING ERRCODE='23514';
  END IF;
  SELECT encode(sha256(convert_to(string_agg(length(image_digest)::text || ':' || image_digest,'' ORDER BY image_digest),'UTF8')),'hex')
    INTO v_computed_digest FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_version;
  IF v_computed_digest IS NULL OR v_computed_digest<>v_manifest.verification_digest THEN
    RAISE EXCEPTION 'knowledge verification digest does not match chunk set' USING ERRCODE='23514';
  END IF;
  UPDATE public.knowledge_manifest SET state='published',updated_at=p_at,record_version=record_version+1
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_version;
END;
$$;


--
-- Name: quarantine_business_audit_purge(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.quarantine_business_audit_purge(p_tenant text, p_batch text, p_owner text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE b public.business_audit_purge_batch%ROWTYPE;
BEGIN
  IF NOT pg_has_role(session_user, 'audit_retention_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to quarantine business audit purge' USING ERRCODE = '42501';
  END IF;
  IF p_tenant IS NULL OR btrim(p_tenant) = '' OR p_batch IS NULL OR btrim(p_batch) = '' OR p_owner IS NULL OR btrim(p_owner) = '' THEN
    RAISE EXCEPTION 'business audit purge quarantine input is invalid' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO b FROM public.business_audit_purge_batch WHERE tenant_id=p_tenant AND batch_id=p_batch FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'business audit purge batch not found' USING ERRCODE = '02000';
  END IF;
  IF b.state = 'quarantined' THEN
    RETURN;
  END IF;
  IF b.state NOT IN ('executing','failed') THEN
    RAISE EXCEPTION 'business audit purge batch is not quarantinable' USING ERRCODE = '23514';
  END IF;
  UPDATE public.business_audit_purge_batch SET state='quarantined',claim_owner=p_owner,claim_until=NULL,
    last_error_class=CASE WHEN last_error_class='' THEN 'max_attempts' ELSE last_error_class END,
    version=version+1,updated_at=clock_timestamp()
  WHERE tenant_id=p_tenant AND batch_id=p_batch;
END;
$$;


--
-- Name: record_knowledge_migration_mutation(text, text, text, bigint, text, bigint, text, text, bigint, text, bigint, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.record_knowledge_migration_mutation(p_tenant_id text, p_migration_id text, p_mutation_id text, p_epoch bigint, p_knowledge_id text, p_knowledge_version bigint, p_chunk_id text, p_operation text, p_source_revision bigint, p_mutation_digest text, p_config_version bigint, p_created_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE v_migration public.backend_migration%ROWTYPE; v_existing public.knowledge_migration_mutation%ROWTYPE; v_direction text;
BEGIN
  SELECT * INTO v_migration FROM public.backend_migration
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'knowledge' OR v_migration.epoch<>p_epoch OR
     (v_migration.state IN ('planned','snapshot','dual_write','backfill','verify') AND p_config_version<>v_migration.source_config_version) OR
     (v_migration.state IN ('cutover','observe') AND p_config_version NOT IN (v_migration.source_config_version,v_migration.target_config_version)) OR
     v_migration.state NOT IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe') OR
     p_created_at IS NULL OR p_created_at<v_migration.created_at THEN
    RAISE EXCEPTION 'knowledge migration authority conflict' USING ERRCODE='23514';
  END IF;
  v_direction := CASE WHEN p_config_version=v_migration.target_config_version THEN 'reverse' ELSE 'forward' END;
  INSERT INTO public.knowledge_migration_mutation(tenant_id,migration_id,mutation_id,epoch,direction,
    knowledge_id,knowledge_version,chunk_id,operation,source_revision,mutation_digest,
    not_before,created_at,updated_at)
  VALUES(p_tenant_id,p_migration_id,p_mutation_id,p_epoch,v_direction,p_knowledge_id,p_knowledge_version,
    p_chunk_id,p_operation,p_source_revision,p_mutation_digest,p_created_at,p_created_at,p_created_at)
  ON CONFLICT (tenant_id,migration_id,knowledge_id,knowledge_version,chunk_id,mutation_id) DO NOTHING;
  SELECT * INTO v_existing FROM public.knowledge_migration_mutation
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND mutation_id=p_mutation_id;
  IF (v_existing.epoch,v_existing.direction,v_existing.operation,v_existing.source_revision,
      v_existing.mutation_digest,v_existing.created_at)
     IS DISTINCT FROM (p_epoch,v_direction,p_operation,p_source_revision,p_mutation_digest,p_created_at) THEN
    RAISE EXCEPTION 'knowledge mutation id collision' USING ERRCODE='23505';
  END IF;
END;
$$;


--
-- Name: record_memory_migration_mutation(text, text, text, bigint, text, text, text, bigint, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.record_memory_migration_mutation(p_tenant_id text, p_migration_id text, p_mutation_id text, p_epoch bigint, p_app_name text, p_user_id text, p_source_digest text, p_config_version bigint, p_created_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE v_migration public.backend_migration%ROWTYPE; v_existing public.memory_migration_mutation%ROWTYPE; v_direction text;
BEGIN
  SELECT * INTO v_migration FROM public.backend_migration
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'memory' OR v_migration.epoch<>p_epoch OR
     (v_migration.state IN ('planned','snapshot','dual_write','backfill','verify') AND p_config_version<>v_migration.source_config_version) OR
     (v_migration.state IN ('cutover','observe') AND p_config_version NOT IN (v_migration.source_config_version,v_migration.target_config_version)) OR
     v_migration.state NOT IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe') OR
     p_app_name NOT LIKE p_tenant_id || '/%' OR p_created_at IS NULL OR p_created_at<v_migration.created_at THEN
    RAISE EXCEPTION 'memory migration authority conflict' USING ERRCODE='23514';
  END IF;
  v_direction := CASE WHEN p_config_version=v_migration.target_config_version THEN 'reverse' ELSE 'forward' END;
  INSERT INTO public.memory_migration_mutation(tenant_id,migration_id,mutation_id,epoch,direction,app_name,user_id,source_digest,
    not_before,created_at,updated_at)
  VALUES(p_tenant_id,p_migration_id,p_mutation_id,p_epoch,v_direction,p_app_name,p_user_id,p_source_digest,
    p_created_at,p_created_at,p_created_at)
  ON CONFLICT (tenant_id,migration_id,mutation_id) DO NOTHING;
  SELECT * INTO v_existing FROM public.memory_migration_mutation
    WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND mutation_id=p_mutation_id;
  IF (v_existing.epoch,v_existing.direction,v_existing.app_name,v_existing.user_id,v_existing.source_digest,v_existing.created_at)
     IS DISTINCT FROM (p_epoch,v_direction,p_app_name,p_user_id,p_source_digest,p_created_at) THEN
    RAISE EXCEPTION 'memory mutation id collision' USING ERRCODE='23505';
  END IF;
END;
$$;


--
-- Name: record_knowledge_probe(text, text, bigint, text, text, jsonb, bigint, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.record_knowledge_probe(p_tenant_id text, p_knowledge_id text, p_knowledge_version bigint, p_probe_id text, p_query text, p_expected_chunks jsonb, p_min_recall_ppm bigint, p_created_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE v_existing public.knowledge_probe%ROWTYPE; v_manifest public.knowledge_manifest%ROWTYPE;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_knowledge_version<1 OR
     length(btrim(p_probe_id))=0 OR length(btrim(p_query))=0 OR jsonb_typeof(p_expected_chunks)<>'array' OR
     jsonb_array_length(p_expected_chunks)<1 OR
     EXISTS (SELECT 1 FROM jsonb_array_elements(p_expected_chunks) AS expected_chunk(value) WHERE jsonb_typeof(expected_chunk.value)<>'string' OR length(btrim(expected_chunk.value #>> '{}'))=0) OR
     p_min_recall_ppm<1 OR p_min_recall_ppm>1000000 OR p_created_at IS NULL THEN
    RAISE EXCEPTION 'knowledge probe input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_knowledge_version FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state<>'verifying' THEN
    RAISE EXCEPTION 'knowledge manifest is not verifying' USING ERRCODE='23514';
  END IF;
  IF (SELECT count(*) FROM jsonb_array_elements_text(p_expected_chunks)) <>
     (SELECT count(DISTINCT value) FROM jsonb_array_elements_text(p_expected_chunks) AS expected(value)) OR
     EXISTS (
       SELECT 1 FROM jsonb_array_elements_text(p_expected_chunks) AS expected(chunk_id)
       WHERE NOT EXISTS (
         SELECT 1 FROM public.knowledge_chunk
         WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version
           AND chunk_id=expected.chunk_id
       )
     ) THEN
    RAISE EXCEPTION 'knowledge probe expected chunks are invalid' USING ERRCODE='23514';
  END IF;
  INSERT INTO public.knowledge_probe(tenant_id,knowledge_id,knowledge_version,probe_id,
    query,expected_chunks,min_recall_ppm,created_at)
  VALUES(p_tenant_id,p_knowledge_id,p_knowledge_version,p_probe_id,
    p_query,p_expected_chunks,p_min_recall_ppm,p_created_at)
  ON CONFLICT (tenant_id,knowledge_id,knowledge_version,probe_id) DO NOTHING;
  SELECT * INTO v_existing FROM public.knowledge_probe
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version AND probe_id=p_probe_id;
  IF (v_existing.query,v_existing.expected_chunks,v_existing.min_recall_ppm,v_existing.created_at)
     IS DISTINCT FROM (p_query,p_expected_chunks,p_min_recall_ppm,p_created_at) THEN
    RAISE EXCEPTION 'knowledge probe id collision' USING ERRCODE='23505';
  END IF;
END;
$$;


--
-- Name: reject_agent_app_identity_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_agent_app_identity_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
     OR NEW.agent_app_id IS DISTINCT FROM OLD.agent_app_id
     OR NEW.agent_app_key IS DISTINCT FROM OLD.agent_app_key THEN
    RAISE EXCEPTION 'agent app identity is immutable' USING ERRCODE = '23000';
  END IF;
  IF NEW.version <> OLD.version + 1 THEN
    RAISE EXCEPTION 'agent app version must increase by exactly one' USING ERRCODE = '40001';
  END IF;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END;
$$;


--
-- Name: reject_audit_event_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_audit_event_change() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'audit event is immutable' USING ERRCODE = '55000';
  END IF;
  IF current_setting('audit.purge_authorized', true) IS DISTINCT FROM 'on'
     OR NOT pg_has_role(session_user, 'audit_retention_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'audit event is immutable' USING ERRCODE = '55000';
  END IF;
  RETURN OLD;
END;
$$;


--
-- Name: reject_backend_migration_batch_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_backend_migration_batch_change() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
  RAISE EXCEPTION 'backend migration batches are immutable' USING ERRCODE='23000';
END;
$$;


--
-- Name: reject_backend_migration_config_switch_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_backend_migration_config_switch_change() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  RAISE EXCEPTION 'backend migration config switches are immutable' USING ERRCODE='23000';
END;
$$;


--
-- Name: reject_business_audit_purge_certificate_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_business_audit_purge_certificate_change() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  RAISE EXCEPTION 'business audit purge certificate is immutable' USING ERRCODE='55000';
END;
$$;


--
-- Name: reject_governance_snapshot_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_governance_snapshot_change() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  RAISE EXCEPTION 'published governance snapshot is immutable' USING ERRCODE='55000';
END;
$$;


--
-- Name: reject_published_config_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_published_config_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  RAISE EXCEPTION 'published config snapshot is immutable' USING ERRCODE = '55000';
END;
$$;


--
-- Name: reject_published_revision_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_published_revision_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF OLD.state = 'published' THEN
    RAISE EXCEPTION 'published agent app revision is immutable' USING ERRCODE = '55000';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;


--
-- Name: reject_unpreprocessed_execution(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_unpreprocessed_execution() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_state text;
BEGIN
  SELECT state INTO v_state FROM public.inbox
    WHERE tenant_id = NEW.tenant_id AND request_id = NEW.request_id;
  IF v_state = 'preprocess_pending' THEN
    RAISE EXCEPTION 'execution requires completed preprocess' USING ERRCODE = 'P0904';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: request_cancel_execution(text, text, bigint, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.request_cancel_execution(p_tenant_id text, p_request_id text, p_expected_version bigint, p_actor_id text, p_reason_code text, p_traceparent text) RETURNS TABLE(accepted boolean, execution_version bigint, cancel_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_execution public.execution_record%ROWTYPE;
BEGIN
  IF p_expected_version < 0 OR p_actor_id IS NULL OR p_actor_id='' OR p_reason_code IS NULL OR p_reason_code='' THEN
    RAISE EXCEPTION 'invalid cancel request' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.tenant t WHERE t.tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant not found' USING ERRCODE = 'P0002'; END IF;
  SELECT * INTO v_execution FROM public.execution_record e
    WHERE e.tenant_id = p_tenant_id AND e.request_id = p_request_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'execution not found' USING ERRCODE = 'P0002'; END IF;
  IF v_execution.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout') THEN
    RETURN QUERY SELECT false, v_execution.version, v_execution.cancel_version;
    RETURN;
  END IF;
  IF v_execution.cancel_requested_at IS NOT NULL THEN
    RETURN QUERY SELECT true, v_execution.version, v_execution.cancel_version;
    RETURN;
  END IF;
  IF v_execution.version <> p_expected_version THEN
    RAISE EXCEPTION 'execution version conflict' USING ERRCODE = '40001';
  END IF;
  UPDATE public.execution_record e SET cancel_requested_at = clock_timestamp(),
    cancel_version = e.cancel_version + 1, version = e.version + 1
    WHERE e.tenant_id = p_tenant_id AND e.request_id = p_request_id
    RETURNING e.version,e.cancel_version INTO execution_version,cancel_version;
  INSERT INTO public.execution_cancel_intent(tenant_id,request_id,cancel_version,actor_id,reason_code,traceparent)
    VALUES(p_tenant_id,p_request_id,cancel_version,p_actor_id,p_reason_code,p_traceparent);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent)
    VALUES
      (p_tenant_id,format('cancel-intent-audit:%s:%s',p_request_id,cancel_version),'audit',p_request_id,
       cancel_version,format('cancel-intent:%s:%s:audit',p_request_id,cancel_version),
       format('cancel-intent://%s/%s/%s',p_tenant_id,p_request_id,cancel_version),p_traceparent),
      (p_tenant_id,format('cancel-intent-control:%s:%s',p_request_id,cancel_version),'execution-control',p_request_id,
       cancel_version,format('cancel-intent:%s:%s:control',p_request_id,cancel_version),
       format('cancel-intent://%s/%s/%s',p_tenant_id,p_request_id,cancel_version),p_traceparent);
  RETURN QUERY SELECT true,execution_version,cancel_version;
END;
$$;


--
-- Name: rollback_agent_app_revision(text, text, bigint, bigint, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.rollback_agent_app_revision(p_tenant_id text, p_agent_app_id text, p_target_revision bigint, p_expected_app_version bigint, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_app_version bigint;
  v_app_status text;
  v_previous_revision bigint;
  v_target_state text;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'agent app rollback metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.tenant WHERE tenant_id = p_tenant_id AND status <> 'disabled' FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant unavailable' USING ERRCODE = '55000'; END IF;
  SELECT version, status, current_revision INTO v_app_version, v_app_status, v_previous_revision FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id FOR UPDATE;
  IF NOT FOUND OR v_app_status = 'disabled' THEN RAISE EXCEPTION 'agent app unavailable' USING ERRCODE = '55000'; END IF;
  IF v_app_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict' USING ERRCODE = '40001'; END IF;
  SELECT state INTO v_target_state FROM public.agent_app_revision
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id
      AND revision = p_target_revision;
  IF NOT FOUND OR v_target_state <> 'published' THEN RAISE EXCEPTION 'target revision is not published' USING ERRCODE = '55000'; END IF;
  v_next_version := v_app_version + 1;
  UPDATE public.agent_app SET current_revision = p_target_revision, version = v_next_version
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id;
  INSERT INTO public.agent_app_change(
    tenant_id, agent_app_id, kind, previous_revision, current_revision,
    previous_status, current_status, previous_version, next_version,
    actor_id, reason_code, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, p_agent_app_id, 'rolled_back', v_previous_revision, p_target_revision,
    v_app_status, v_app_status, v_app_version, v_next_version,
    p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('app-rollback-audit:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'audit', p_agent_app_id, v_next_version, format('app:%s:%s:%s:rollback-audit', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app-revision://%s/%s/%s', p_tenant_id, p_agent_app_id, p_target_revision), p_traceparent),
    (p_tenant_id, format('app-rollback-invalidation:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'config-invalidation', p_agent_app_id, v_next_version, format('app:%s:%s:%s:rollback-invalidate', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app://%s/%s/%s', p_tenant_id, p_agent_app_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$$;


--
-- Name: rollback_knowledge_backend_migration(text, text, bigint, bigint, text, timestamp with time zone, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.rollback_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_existing public.backend_migration_config_switch%ROWTYPE;
BEGIN
  IF p_at IS NULL OR COALESCE(length(btrim(p_switch_id)),0)=0 OR COALESCE(length(btrim(p_actor_id)),0)=0 OR
     COALESCE(length(btrim(p_reason_code)),0)=0 OR COALESCE(length(btrim(p_correlation_id)),0)=0 OR
     COALESCE(length(btrim(p_trace_id)),0)=0 OR COALESCE(length(p_rollback_sync_watermark),0)=0 THEN
    RAISE EXCEPTION 'knowledge rollback input is invalid' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  SELECT * INTO v_existing FROM public.backend_migration_config_switch WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND switch_id=p_switch_id;
  IF FOUND THEN
    IF v_existing.direction<>'rollback' OR v_existing.previous_tenant_version<>p_expected_tenant_version OR
       v_existing.migration_result_version<>p_expected_migration_version OR v_existing.rollback_sync_watermark<>p_rollback_sync_watermark OR
       v_existing.actor_id<>p_actor_id OR v_existing.reason_code<>p_reason_code OR v_existing.correlation_id<>p_correlation_id OR
       v_existing.trace_id<>p_trace_id OR v_existing.occurred_at<>p_at THEN RAISE EXCEPTION 'switch id collision' USING ERRCODE='23505'; END IF;
    RETURN QUERY SELECT v_existing.tenant_result_version,v_existing.active_config_version; RETURN;
  END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND OR v_tenant.version<>p_expected_tenant_version OR v_migration.version<>p_expected_migration_version OR
     v_migration.domain<>'knowledge' OR v_migration.state<>'observe' OR p_at<v_migration.updated_at OR
     v_tenant.active_config_version<>v_migration.target_config_version OR p_rollback_sync_watermark<>v_migration.verify_target_watermark OR
     EXISTS (SELECT 1 FROM public.knowledge_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND direction='reverse' AND state<>'applied') THEN
    RAISE EXCEPTION 'knowledge rollback authority conflict' USING ERRCODE='23514';
  END IF;
  UPDATE public.tenant SET active_config_version=v_migration.source_config_version,version=version+1 WHERE tenant_id=p_tenant_id;
  INSERT INTO public.backend_migration_config_switch(tenant_id,migration_id,switch_id,direction,previous_config_version,
    active_config_version,migration_result_version,previous_tenant_version,tenant_result_version,rollback_sync_watermark,
    actor_id,reason_code,correlation_id,trace_id,occurred_at)
  VALUES(p_tenant_id,p_migration_id,p_switch_id,'rollback',v_migration.target_config_version,v_migration.source_config_version,
    v_migration.version,v_tenant.version,v_tenant.version+1,p_rollback_sync_watermark,p_actor_id,p_reason_code,p_correlation_id,p_trace_id,p_at);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent) VALUES
    (p_tenant_id,format('knowledge-migration-rollback-audit:%s:%s',p_migration_id,p_switch_id),'audit',p_migration_id,v_tenant.version+1,
      format('knowledge-migration:%s:%s:rollback-audit',p_migration_id,p_switch_id),format('backend-migration-switch://%s/%s/%s',p_tenant_id,p_migration_id,p_switch_id),p_traceparent),
    (p_tenant_id,format('knowledge-migration-rollback-invalidation:%s:%s',p_migration_id,p_switch_id),'config-invalidation',p_tenant_id,v_tenant.version+1,
      format('knowledge-migration:%s:%s:rollback-invalidate',p_migration_id,p_switch_id),format('config://%s/%s',p_tenant_id,v_migration.source_config_version),p_traceparent);
  RETURN QUERY SELECT v_tenant.version+1,v_migration.source_config_version;
END;
$$;


--
-- Name: rollback_session_backend_migration(text, text, bigint, bigint, text, timestamp with time zone, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.rollback_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS TABLE(tenant_version bigint, active_config_version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_tenant public.tenant%ROWTYPE; v_migration public.backend_migration%ROWTYPE; v_existing public.backend_migration_config_switch%ROWTYPE;
BEGIN
  IF p_at IS NULL OR COALESCE(length(btrim(p_switch_id)),0)=0 OR length(p_switch_id)>128 OR
     COALESCE(length(btrim(p_actor_id)),0)=0 OR COALESCE(length(btrim(p_reason_code)),0)=0 OR
     COALESCE(length(btrim(p_correlation_id)),0)=0 OR COALESCE(length(btrim(p_trace_id)),0)=0 OR
     COALESCE(length(p_rollback_sync_watermark),0)=0 THEN
    RAISE EXCEPTION 'rollback metadata is incomplete' USING ERRCODE='22023';
  END IF;
  SELECT * INTO v_tenant FROM public.tenant WHERE tenant_id=p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE='P0002'; END IF;
  SELECT * INTO v_existing FROM public.backend_migration_config_switch
   WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND switch_id=p_switch_id;
  IF FOUND THEN
    IF v_existing.direction<>'rollback' OR v_existing.previous_tenant_version<>p_expected_tenant_version OR
       v_existing.migration_result_version<>p_expected_migration_version OR
       v_existing.rollback_sync_watermark<>p_rollback_sync_watermark OR v_existing.actor_id<>p_actor_id OR
       v_existing.reason_code<>p_reason_code OR v_existing.correlation_id<>p_correlation_id OR
       v_existing.trace_id<>p_trace_id OR v_existing.occurred_at<>p_at THEN
      RAISE EXCEPTION 'switch id collision' USING ERRCODE='23505';
    END IF;
    RETURN QUERY SELECT v_existing.tenant_result_version,v_existing.active_config_version; RETURN;
  END IF;
  IF v_tenant.status='disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE='55000'; END IF;
  IF v_tenant.version<>p_expected_tenant_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE='40001'; END IF;
  SELECT * INTO v_migration FROM public.backend_migration WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'migration does not exist' USING ERRCODE='P0002'; END IF;
  IF v_migration.domain<>'session' OR v_migration.state<>'observe' OR v_migration.version<>p_expected_migration_version OR
     p_at<v_migration.updated_at OR
     v_tenant.active_config_version<>v_migration.target_config_version THEN
    RAISE EXCEPTION 'rollback authority conflict' USING ERRCODE='40001';
  END IF;
  IF p_rollback_sync_watermark<>v_migration.verify_target_watermark OR EXISTS (
    SELECT 1 FROM public.session_migration_mutation WHERE tenant_id=p_tenant_id AND migration_id=p_migration_id AND direction='reverse' AND state<>'applied') THEN
    RAISE EXCEPTION 'reverse synchronization is incomplete' USING ERRCODE='23514';
  END IF;
  UPDATE public.tenant SET active_config_version=v_migration.source_config_version,version=version+1 WHERE tenant_id=p_tenant_id;
  INSERT INTO public.backend_migration_config_switch(tenant_id,migration_id,switch_id,direction,
    previous_config_version,active_config_version,migration_result_version,previous_tenant_version,tenant_result_version,
    rollback_sync_watermark,actor_id,reason_code,correlation_id,trace_id,occurred_at)
  VALUES(p_tenant_id,p_migration_id,p_switch_id,'rollback',v_migration.target_config_version,v_migration.source_config_version,
    v_migration.version,v_tenant.version,v_tenant.version+1,p_rollback_sync_watermark,p_actor_id,p_reason_code,p_correlation_id,p_trace_id,p_at);
  INSERT INTO public.outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,traceparent) VALUES
    (p_tenant_id,format('migration-rollback-audit:%s:%s',p_migration_id,p_switch_id),'audit',p_migration_id,v_tenant.version+1,
      format('migration:%s:%s:rollback-audit',p_migration_id,p_switch_id),format('backend-migration-switch://%s/%s/%s',p_tenant_id,p_migration_id,p_switch_id),p_traceparent),
    (p_tenant_id,format('migration-rollback-invalidation:%s:%s',p_migration_id,p_switch_id),'config-invalidation',p_tenant_id,v_tenant.version+1,
      format('migration:%s:%s:rollback-invalidate',p_migration_id,p_switch_id),format('config://%s/%s',p_tenant_id,v_migration.source_config_version),p_traceparent);
  RETURN QUERY SELECT v_tenant.version+1,v_migration.source_config_version;
END;
$$;


--
-- Name: session_backend_migration_drain_status(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.session_backend_migration_drain_status(p_tenant_id text, p_migration_id text) RETURNS TABLE(source_in_flight bigint, target_in_flight bigint, forward_outstanding bigint, reverse_outstanding bigint, active_config_version bigint, rolled_back boolean)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog'
    AS $$
  SELECT
    (SELECT count(*) FROM public.execution_record e WHERE e.tenant_id=m.tenant_id AND e.config_version=m.source_config_version
      AND e.outcome IN ('queued','running','pending','blocked','waiting_confirmation')),
    (SELECT count(*) FROM public.execution_record e WHERE e.tenant_id=m.tenant_id AND e.config_version=m.target_config_version
      AND e.outcome IN ('queued','running','pending','blocked','waiting_confirmation')),
    (SELECT count(*) FROM public.session_migration_mutation x WHERE x.tenant_id=m.tenant_id AND x.migration_id=m.migration_id
      AND x.direction='forward' AND x.state<>'applied'),
    (SELECT count(*) FROM public.session_migration_mutation x WHERE x.tenant_id=m.tenant_id AND x.migration_id=m.migration_id
      AND x.direction='reverse' AND x.state<>'applied'),
    t.active_config_version,
    EXISTS(SELECT 1 FROM public.backend_migration_config_switch s WHERE s.tenant_id=m.tenant_id
      AND s.migration_id=m.migration_id AND s.direction='rollback')
  FROM public.backend_migration m JOIN public.tenant t ON t.tenant_id=m.tenant_id
  WHERE m.tenant_id=p_tenant_id AND m.migration_id=p_migration_id;
$$;


--
-- Name: stage_knowledge_chunk(text, text, bigint, text, text, text, text, text, bigint, text, text, jsonb, jsonb, text, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.stage_knowledge_chunk(p_tenant_id text, p_knowledge_id text, p_knowledge_version bigint, p_chunk_id text, p_source_digest text, p_content_digest text, p_metadata_digest text, p_embedding_profile_id text, p_embedding_version bigint, p_vector_generation text, p_content text, p_metadata jsonb, p_vector jsonb, p_mutation_digest text, p_created_at timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
DECLARE v_manifest public.knowledge_manifest%ROWTYPE; v_chunk public.knowledge_chunk%ROWTYPE;
  v_migration_id text; v_epoch bigint; v_source_config bigint; v_mutation_id text;
BEGIN
  IF p_tenant_id IS NULL OR length(btrim(p_tenant_id))=0 OR length(btrim(p_knowledge_id))=0 OR p_knowledge_version<1 OR
     length(btrim(p_chunk_id))=0 OR p_source_digest!~'^[0-9a-f]{64}$' OR p_content_digest!~'^[0-9a-f]{64}$' OR
     p_metadata_digest!~'^[0-9a-f]{64}$' OR length(btrim(p_embedding_profile_id))=0 OR p_embedding_version<1 OR
     length(btrim(p_vector_generation))=0 OR p_content IS NULL OR length(p_content)=0 OR
     p_metadata IS NULL OR p_vector IS NULL OR jsonb_typeof(p_metadata)<>'object' OR jsonb_typeof(p_vector)<>'array' OR jsonb_array_length(p_vector)<1 OR
     p_mutation_digest!~'^[0-9a-f]{64}$' OR
     p_created_at IS NULL THEN
    RAISE EXCEPTION 'knowledge chunk input is invalid' USING ERRCODE='22023';
  END IF;

  SELECT * INTO v_manifest FROM public.knowledge_manifest
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND version=p_knowledge_version FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'knowledge manifest does not exist' USING ERRCODE='P0002'; END IF;
  IF v_manifest.state <> 'staging' THEN
    RAISE EXCEPTION 'knowledge manifest is not staging' USING ERRCODE='23514';
  END IF;
  IF p_source_digest<>v_manifest.source_digest OR p_embedding_profile_id<>v_manifest.embedder_profile_id OR
     p_embedding_version<>v_manifest.embedder_version OR p_vector_generation<>v_manifest.vector_collection_generation OR
     EXISTS (
       SELECT 1 FROM jsonb_object_keys(p_metadata) AS metadata_key(key)
       WHERE NOT EXISTS (
         SELECT 1 FROM jsonb_array_elements_text(v_manifest.metadata_schema) AS allowed_key(value)
         WHERE allowed_key.value=metadata_key.key
       )
     ) THEN
    RAISE EXCEPTION 'knowledge chunk does not match manifest' USING ERRCODE='23514';
  END IF;

  INSERT INTO public.knowledge_chunk(tenant_id,knowledge_id,knowledge_version,chunk_id,
    source_digest,content_digest,metadata_digest,embedding_profile_id,embedding_version,vector_generation,
    image_digest,content,metadata,vector,created_at)
  VALUES(p_tenant_id,p_knowledge_id,p_knowledge_version,p_chunk_id,
    p_source_digest,p_content_digest,p_metadata_digest,p_embedding_profile_id,p_embedding_version,p_vector_generation,
    p_mutation_digest,p_content,p_metadata,p_vector,p_created_at)
  ON CONFLICT (tenant_id,knowledge_id,knowledge_version,chunk_id) DO NOTHING;
  SELECT * INTO v_chunk FROM public.knowledge_chunk
    WHERE tenant_id=p_tenant_id AND knowledge_id=p_knowledge_id AND knowledge_version=p_knowledge_version AND chunk_id=p_chunk_id;
  IF (v_chunk.source_digest,v_chunk.content_digest,v_chunk.metadata_digest,v_chunk.embedding_profile_id,
      v_chunk.embedding_version,v_chunk.vector_generation,v_chunk.image_digest,v_chunk.content,v_chunk.metadata,v_chunk.vector,v_chunk.created_at)
     IS DISTINCT FROM
     (p_source_digest,p_content_digest,p_metadata_digest,p_embedding_profile_id,
      p_embedding_version,p_vector_generation,p_mutation_digest,p_content,p_metadata,p_vector,p_created_at) THEN
    RAISE EXCEPTION 'knowledge chunk id collision' USING ERRCODE='23505';
  END IF;

  SELECT migration_id,epoch,source_config_version INTO v_migration_id,v_epoch,v_source_config
    FROM public.backend_migration
    WHERE tenant_id=p_tenant_id AND domain='knowledge'
      AND state IN ('planned','snapshot','dual_write','backfill','verify','cutover','observe')
    ORDER BY created_at DESC LIMIT 1;
  IF FOUND THEN
    v_mutation_id := 'ingest_' || encode(sha256(convert_to(
      v_migration_id || chr(31) || p_knowledge_id || chr(31) || p_knowledge_version::text || chr(31) || p_chunk_id,
      'UTF8')), 'hex');
    PERFORM public.record_knowledge_migration_mutation(
      p_tenant_id,v_migration_id,v_mutation_id,v_epoch,
      p_knowledge_id,p_knowledge_version,p_chunk_id,'upsert',
      1,p_mutation_digest,v_source_config,p_created_at);
  END IF;
END;
$_$;


--
-- Name: suspend_turn(text, text, text, text, text, text, bigint, bigint, bigint, jsonb, jsonb, text, text, text, text, bigint, text, text, bigint, text, bigint, bigint, bigint, timestamp with time zone, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.suspend_turn(p_tenant_id text, p_agent_app_id text, p_session_id text, p_request_id text, p_commit_id text, p_request_digest text, p_input_seq bigint, p_fence bigint, p_expected_version bigint, p_events jsonb, p_state_delta jsonb, p_confirmation_id text, p_subject_id text, p_channel_binding_id text, p_tool_id text, p_tool_version bigint, p_tool_call_id text, p_args_digest text, p_policy_version bigint, p_checkpoint_ref text, p_input_tokens bigint, p_output_tokens bigint, p_cached_input_tokens bigint, p_expires_at timestamp with time zone, p_outbox jsonb) RETURNS TABLE(confirmation_id text, state text, version bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_commit record; c public.confirmation%ROWTYPE;
BEGIN
  IF p_expires_at <= now() THEN RAISE EXCEPTION 'confirmation already expired' USING ERRCODE='22023'; END IF;
  SELECT * INTO v_commit FROM public.commit_turn(p_tenant_id,p_agent_app_id,p_session_id,
    p_request_id,p_commit_id,p_request_digest,'waiting',p_input_seq,p_fence,p_expected_version,
    'waiting_confirmation',p_events,p_state_delta,'null'::jsonb,p_checkpoint_ref,NULL,p_outbox);
  INSERT INTO public.confirmation(tenant_id,confirmation_id,request_id,request_digest,agent_app_id,session_id,input_seq,
    subject_id,channel_binding_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,checkpoint_ref,
    input_tokens,output_tokens,cached_input_tokens,state,expires_at)
  VALUES(p_tenant_id,p_confirmation_id,p_request_id,p_request_digest,p_agent_app_id,p_session_id,p_input_seq,p_subject_id,
    p_channel_binding_id,p_tool_id,p_tool_version,p_tool_call_id,p_args_digest,p_policy_version,p_checkpoint_ref,
    p_input_tokens,p_output_tokens,p_cached_input_tokens,'pending',p_expires_at)
  ON CONFLICT ON CONSTRAINT confirmation_pkey DO NOTHING;
  SELECT x.* INTO c FROM public.confirmation x WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
  IF c.request_id<>p_request_id OR c.request_digest<>p_request_digest OR c.agent_app_id<>p_agent_app_id
     OR c.session_id<>p_session_id OR c.input_seq<>p_input_seq OR c.subject_id<>p_subject_id
     OR c.channel_binding_id<>p_channel_binding_id OR c.tool_id<>p_tool_id OR c.tool_version<>p_tool_version
     OR c.tool_call_id<>p_tool_call_id OR c.args_digest<>p_args_digest OR c.policy_version<>p_policy_version
     OR c.checkpoint_ref<>p_checkpoint_ref OR c.input_tokens<>p_input_tokens OR c.output_tokens<>p_output_tokens
     OR c.cached_input_tokens<>p_cached_input_tokens OR c.expires_at<>p_expires_at THEN
    RAISE EXCEPTION 'confirmation id collision' USING ERRCODE='23505';
  END IF;
  RETURN QUERY SELECT x.confirmation_id,x.state,x.version FROM public.confirmation x
    WHERE x.tenant_id=p_tenant_id AND x.confirmation_id=p_confirmation_id;
END;
$$;


--
-- Name: transition_agent_app_status(text, text, bigint, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.transition_agent_app_status(p_tenant_id text, p_agent_app_id text, p_expected_app_version bigint, p_next_status text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_status text;
  v_version bigint;
  v_current_revision bigint;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'agent app status metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  PERFORM 1 FROM public.tenant WHERE tenant_id = p_tenant_id AND status <> 'disabled' FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant unavailable' USING ERRCODE = '55000'; END IF;
  SELECT status, version, current_revision INTO v_status, v_version, v_current_revision
    FROM public.agent_app WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'agent app not found' USING ERRCODE = 'P0002'; END IF;
  IF v_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict' USING ERRCODE = '40001'; END IF;
  IF NOT (
    (v_status = 'active' AND p_next_status IN ('suspended', 'disabled'))
    OR (v_status = 'suspended' AND p_next_status IN ('active', 'disabled'))
    OR (v_status = 'draft' AND p_next_status = 'disabled')
  ) THEN RAISE EXCEPTION 'illegal agent app status transition' USING ERRCODE = '23514'; END IF;
  v_next_version := v_version + 1;
  UPDATE public.agent_app SET status = p_next_status, version = v_next_version
    WHERE tenant_id = p_tenant_id AND agent_app_id = p_agent_app_id;
  INSERT INTO public.agent_app_change(
    tenant_id, agent_app_id, kind, current_revision, previous_status,
    current_status, previous_version, next_version, actor_id, reason_code,
    correlation_id, trace_id
  ) VALUES (
    p_tenant_id, p_agent_app_id, 'status_changed', v_current_revision,
    v_status, p_next_status, v_version, v_next_version,
    p_actor_id, p_reason_code, p_correlation_id, p_trace_id
  );
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('app-status-audit:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'audit', p_agent_app_id, v_next_version, format('app:%s:%s:%s:status-audit', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app-change://%s/%s/%s', p_tenant_id, p_agent_app_id, v_next_version), p_traceparent),
    (p_tenant_id, format('app-status-invalidation:%s:%s:%s', p_tenant_id, p_agent_app_id, v_next_version), 'config-invalidation', p_agent_app_id, v_next_version, format('app:%s:%s:%s:status-invalidate', p_tenant_id, p_agent_app_id, v_next_version), format('agent-app://%s/%s/%s', p_tenant_id, p_agent_app_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$$;


--
-- Name: transition_tenant_status(text, bigint, text, text, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.transition_tenant_status(p_tenant_id text, p_expected_version bigint, p_next_status text, p_actor_type text, p_actor_id text, p_reason_code text, p_reason_text_ref text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_previous_status text;
  v_current_version bigint;
  v_next_version bigint;
  v_payload_ref text;
BEGIN
  IF p_actor_type IS NULL OR p_actor_id IS NULL OR p_reason_code IS NULL
     OR p_correlation_id IS NULL OR p_trace_id IS NULL THEN
    RAISE EXCEPTION 'tenant status metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status, version INTO v_previous_status, v_current_version
    FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_current_version <> p_expected_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE = '40001'; END IF;
  v_next_version := v_current_version + 1;
  UPDATE public.tenant SET status = p_next_status, version = v_next_version WHERE tenant_id = p_tenant_id;
  INSERT INTO public.tenant_status_change(
    tenant_id, previous_status, next_status, previous_version, next_version,
    actor_type, actor_id, reason_code, reason_text_ref, correlation_id, trace_id
  ) VALUES (
    p_tenant_id, v_previous_status, p_next_status, v_current_version, v_next_version,
    p_actor_type, p_actor_id, p_reason_code, p_reason_text_ref, p_correlation_id, p_trace_id
  );
  v_payload_ref := format('tenant-status-change://%s/%s', p_tenant_id, v_next_version);
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('tenant-status-audit:%s:%s', p_tenant_id, v_next_version), 'audit', p_tenant_id, v_next_version, format('tenant-status:%s:%s:audit', p_tenant_id, v_next_version), v_payload_ref, p_traceparent),
    (p_tenant_id, format('tenant-status-control:%s:%s', p_tenant_id, v_next_version), 'tenant-control', p_tenant_id, v_next_version, format('tenant-status:%s:%s:control', p_tenant_id, v_next_version), v_payload_ref, p_traceparent);
  RETURN v_next_version;
END;
$$;


--
-- Name: unpack_session_event_payload(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.unpack_session_event_payload() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE v_wrapper jsonb;
BEGIN
  BEGIN
    v_wrapper := NEW.payload_ref::jsonb;
  EXCEPTION WHEN others THEN
    RAISE EXCEPTION 'session event payload wrapper missing' USING ERRCODE = '23514';
  END;
  IF jsonb_typeof(v_wrapper) <> 'object'
     OR COALESCE(v_wrapper->>'ref', '') = ''
     OR v_wrapper->'payload' IS NULL
     OR jsonb_typeof(v_wrapper->'payload') <> 'object' THEN
    RAISE EXCEPTION 'invalid session event payload wrapper' USING ERRCODE = '23514';
  END IF;
  NEW.payload_ref := v_wrapper->>'ref';
  NEW.event_payload := v_wrapper->'payload';
  RETURN NEW;
END;
$$;


--
-- Name: update_tenant_configuration(text, bigint, text, bigint, integer, bigint, bigint, character, integer, text, text, numeric, text, text, bigint, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.update_tenant_configuration(p_tenant_id text, p_expected_version bigint, p_display_name text, p_request_limit_per_minute bigint, p_max_concurrent_executions integer, p_monthly_token_budget bigint, p_monthly_cost_budget_micros bigint, p_billing_currency character, p_audit_retention_days integer, p_audit_payload_mode text, p_log_masking_level text, p_trace_sampling_rate numeric, p_default_agent_app_id text, p_default_backend_profile_id text, p_active_config_version bigint, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE
  v_status text;
  v_version bigint;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'tenant configuration metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  SELECT status, version INTO v_status, v_version FROM public.tenant
    WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_status = 'disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE = '55000'; END IF;
  IF v_version <> p_expected_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE = '40001'; END IF;
  v_next_version := v_version + 1;
  UPDATE public.tenant SET
    display_name = p_display_name,
    request_limit_per_minute = p_request_limit_per_minute,
    max_concurrent_executions = p_max_concurrent_executions,
    monthly_token_budget = p_monthly_token_budget,
    monthly_cost_budget_micros = p_monthly_cost_budget_micros,
    billing_currency = p_billing_currency,
    audit_retention_days = p_audit_retention_days,
    audit_payload_mode = p_audit_payload_mode,
    log_masking_level = p_log_masking_level,
    trace_sampling_rate = p_trace_sampling_rate,
    default_agent_app_id = p_default_agent_app_id,
    default_backend_profile_id = p_default_backend_profile_id,
    active_config_version = p_active_config_version,
    version = v_next_version
  WHERE tenant_id = p_tenant_id;
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('tenant-config-audit:%s:%s', p_tenant_id, v_next_version), 'audit', p_tenant_id, v_next_version, format('tenant-config:%s:%s:audit', p_tenant_id, v_next_version), format('tenant-config://%s/%s', p_tenant_id, v_next_version), p_traceparent),
    (p_tenant_id, format('tenant-config-control:%s:%s', p_tenant_id, v_next_version), 'tenant-control', p_tenant_id, v_next_version, format('tenant-config:%s:%s:control', p_tenant_id, v_next_version), format('tenant-config://%s/%s', p_tenant_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$$;


--
-- Name: agent_app; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_app (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    agent_app_key text NOT NULL,
    display_name text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    current_revision bigint,
    next_revision bigint DEFAULT 1 NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_app_agent_app_id_check CHECK ((agent_app_id ~ '^app_[0-7][0-9A-HJKMNP-TV-Z]{25}$'::text)),
    CONSTRAINT agent_app_agent_app_key_check CHECK ((agent_app_key ~ '^[a-z][a-z0-9-]{1,63}$'::text)),
    CONSTRAINT agent_app_check CHECK ((((status = 'draft'::text) AND (current_revision IS NULL)) OR ((status = ANY (ARRAY['active'::text, 'suspended'::text])) AND (current_revision IS NOT NULL)) OR (status = 'disabled'::text))),
    CONSTRAINT agent_app_description_check CHECK ((length(description) <= 2000)),
    CONSTRAINT agent_app_display_name_check CHECK (((length(btrim(display_name)) >= 1) AND (length(btrim(display_name)) <= 200))),
    CONSTRAINT agent_app_next_revision_check CHECK ((next_revision >= 1)),
    CONSTRAINT agent_app_status_check CHECK ((status = ANY (ARRAY['draft'::text, 'active'::text, 'suspended'::text, 'disabled'::text]))),
    CONSTRAINT agent_app_version_check CHECK ((version >= 1))
);


--
-- Name: agent_app_change; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_app_change (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    event_id bigint NOT NULL,
    kind text NOT NULL,
    previous_revision bigint,
    current_revision bigint,
    previous_status text,
    current_status text,
    previous_version bigint NOT NULL,
    next_version bigint NOT NULL,
    actor_id text NOT NULL,
    reason_code text NOT NULL,
    correlation_id text NOT NULL,
    trace_id text NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_app_change_actor_id_check CHECK ((length(btrim(actor_id)) > 0)),
    CONSTRAINT agent_app_change_check CHECK ((next_version = (previous_version + 1))),
    CONSTRAINT agent_app_change_correlation_id_check CHECK ((length(btrim(correlation_id)) > 0)),
    CONSTRAINT agent_app_change_kind_check CHECK ((kind = ANY (ARRAY['published'::text, 'rolled_back'::text, 'status_changed'::text]))),
    CONSTRAINT agent_app_change_previous_version_check CHECK ((previous_version >= 1)),
    CONSTRAINT agent_app_change_reason_code_check CHECK ((length(btrim(reason_code)) > 0)),
    CONSTRAINT agent_app_change_trace_id_check CHECK ((length(btrim(trace_id)) > 0))
);


--
-- Name: agent_app_change_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.agent_app_change ALTER COLUMN event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.agent_app_change_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: agent_app_revision; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_app_revision (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    revision bigint NOT NULL,
    state text DEFAULT 'draft'::text NOT NULL,
    draft_version bigint DEFAULT 1 NOT NULL,
    agent_kind text NOT NULL,
    schema_version integer DEFAULT 1 NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    instruction text DEFAULT ''::text NOT NULL,
    global_instruction text DEFAULT ''::text NOT NULL,
    model_profile_id text,
    model_profile_version bigint,
    generation_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    runtime_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    content_digest text,
    published_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    agent_spec jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT agent_app_revision_agent_kind_v1_check CHECK ((agent_kind = ANY (ARRAY['llm'::text, 'graph'::text, 'chain'::text, 'parallel'::text, 'cycle'::text]))),
    CONSTRAINT agent_app_revision_agent_spec_v1_check CHECK (((schema_version = 1) AND (jsonb_typeof(agent_spec) = 'object'::text))),
    CONSTRAINT agent_app_revision_check CHECK ((((state = 'draft'::text) AND (content_digest IS NULL) AND (published_at IS NULL)) OR ((state = 'published'::text) AND (content_digest ~ '^[0-9a-f]{64}$'::text) AND (published_at IS NOT NULL)))),
    CONSTRAINT agent_app_revision_description_check CHECK ((length(description) <= 2000)),
    CONSTRAINT agent_app_revision_draft_version_check CHECK ((draft_version >= 1)),
    CONSTRAINT agent_app_revision_generation_config_check CHECK ((jsonb_typeof(generation_config) = 'object'::text)),
    CONSTRAINT agent_app_revision_global_instruction_check CHECK ((length(global_instruction) <= 65536)),
    CONSTRAINT agent_app_revision_instruction_v1_check CHECK ((length(instruction) <= 65536)),
    CONSTRAINT agent_app_revision_model_v1_check CHECK ((((agent_kind = 'llm'::text) AND (length(btrim(instruction)) >= 1) AND (model_profile_id IS NOT NULL) AND (length(btrim(model_profile_id)) > 0) AND (model_profile_version IS NOT NULL) AND (model_profile_version >= 1)) OR ((agent_kind <> 'llm'::text) AND (model_profile_id IS NULL) AND (model_profile_version IS NULL)))),
    CONSTRAINT agent_app_revision_revision_check CHECK ((revision >= 1)),
    CONSTRAINT agent_app_revision_runtime_policy_check CHECK ((jsonb_typeof(runtime_policy) = 'object'::text)),
    CONSTRAINT agent_app_revision_schema_version_check CHECK ((schema_version = 1)),
    CONSTRAINT agent_app_revision_state_check CHECK ((state = ANY (ARRAY['draft'::text, 'published'::text])))
);


--
-- Name: agent_app_revision_child; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_app_revision_child (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    revision bigint NOT NULL,
    node_key text NOT NULL,
    ordinal integer NOT NULL,
    child_agent_app_id text NOT NULL,
    child_revision bigint NOT NULL,
    child_digest text NOT NULL,
    CONSTRAINT agent_app_revision_child_check CHECK (((child_agent_app_id <> agent_app_id) OR (child_revision <> revision))),
    CONSTRAINT agent_app_revision_child_child_digest_check CHECK ((child_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT agent_app_revision_child_child_revision_check CHECK ((child_revision >= 1)),
    CONSTRAINT agent_app_revision_child_node_key_check CHECK ((node_key ~ '^[a-z][a-z0-9_-]{0,63}$'::text)),
    CONSTRAINT agent_app_revision_child_ordinal_check CHECK ((ordinal >= 0))
);


--
-- Name: agent_app_revision_knowledge; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_app_revision_knowledge (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    revision bigint NOT NULL,
    knowledge_id text NOT NULL,
    knowledge_version bigint NOT NULL,
    CONSTRAINT agent_app_revision_knowledge_knowledge_version_check CHECK ((knowledge_version >= 1))
);


--
-- Name: agent_app_revision_skill; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_app_revision_skill (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    revision bigint NOT NULL,
    skill_id text NOT NULL,
    skill_version bigint NOT NULL,
    content_digest text NOT NULL,
    CONSTRAINT agent_app_revision_skill_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT agent_app_revision_skill_skill_version_check CHECK ((skill_version >= 1))
);


--
-- Name: agent_app_revision_tool; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agent_app_revision_tool (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    revision bigint NOT NULL,
    tool_id text NOT NULL,
    tool_version bigint NOT NULL,
    content_digest character(64),
    required boolean DEFAULT false NOT NULL,
    CONSTRAINT agent_app_revision_tool_tool_version_check CHECK ((tool_version >= 1)),
    CONSTRAINT agent_app_revision_tool_content_digest_check CHECK (((content_digest IS NULL) OR (content_digest ~ '^[0-9a-f]{64}$'::text)))
);


--
-- Name: artifact_object_upload; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.artifact_object_upload (
    tenant_id text NOT NULL,
    object_key text NOT NULL,
    artifact_id text NOT NULL,
    request_id text NOT NULL,
    content_digest text NOT NULL,
    content_size bigint NOT NULL,
    state text DEFAULT 'uploading'::text NOT NULL,
    protect_until timestamp with time zone NOT NULL,
    claim_owner text,
    claim_until timestamp with time zone,
    cleanup_attempt integer DEFAULT 0 NOT NULL,
    last_error_class text,
    quarantined_at timestamp with time zone,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT artifact_object_upload_artifact_id_check CHECK ((artifact_id ~ '^a1_[A-Za-z0-9_-]{43}$'::text)),
    CONSTRAINT artifact_object_upload_check CHECK ((((state = 'uploading'::text) AND (claim_owner IS NULL) AND (claim_until IS NULL) AND (quarantined_at IS NULL)) OR ((state = 'cleanup_claimed'::text) AND (claim_owner IS NOT NULL) AND (claim_until IS NOT NULL) AND (quarantined_at IS NULL)) OR ((state = 'quarantined'::text) AND (claim_owner IS NULL) AND (claim_until IS NULL) AND (quarantined_at IS NOT NULL) AND (last_error_class IS NOT NULL)))),
    CONSTRAINT artifact_object_upload_cleanup_attempt_check CHECK ((cleanup_attempt >= 0)),
    CONSTRAINT artifact_object_upload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT artifact_object_upload_content_size_check CHECK ((content_size > 0)),
    CONSTRAINT artifact_object_upload_object_key_check CHECK ((object_key ~ '^artifacts/v1/[A-Za-z0-9_-]{43}/a1_[A-Za-z0-9_-]{43}$'::text)),
    CONSTRAINT artifact_object_upload_state_check CHECK ((state = ANY (ARRAY['uploading'::text, 'cleanup_claimed'::text, 'quarantined'::text]))),
    CONSTRAINT artifact_object_upload_version_check CHECK ((version >= 1))
);


--
-- Name: artifact_reference; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.artifact_reference (
    tenant_id text NOT NULL,
    artifact_id text NOT NULL,
    reference_kind text NOT NULL,
    reference_id text NOT NULL,
    retain_until timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT artifact_reference_reference_id_check CHECK ((length(btrim(reference_id)) > 0)),
    CONSTRAINT artifact_reference_reference_kind_check CHECK ((reference_kind = 'prepared_payload'::text))
);


--
-- Name: audit_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_event (
    tenant_id text NOT NULL,
    audit_id text NOT NULL,
    schema_version integer NOT NULL,
    channel text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    request_id text DEFAULT ''::text NOT NULL,
    agent_app_id text DEFAULT ''::text NOT NULL,
    agent_app_revision bigint DEFAULT 0 NOT NULL,
    agent_name text DEFAULT ''::text NOT NULL,
    tool_name text DEFAULT ''::text NOT NULL,
    action text NOT NULL,
    decision text NOT NULL,
    reason_code text DEFAULT ''::text NOT NULL,
    latency_ms bigint DEFAULT 0 NOT NULL,
    error_type text DEFAULT ''::text NOT NULL,
    cost_micros bigint DEFAULT 0 NOT NULL,
    currency text DEFAULT ''::text NOT NULL,
    input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    config_version bigint DEFAULT 0 NOT NULL,
    policy_version bigint DEFAULT 0 NOT NULL,
    content_digest text DEFAULT ''::text NOT NULL,
    trace_id text DEFAULT ''::text NOT NULL,
    resource_refs jsonb DEFAULT '[]'::jsonb NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    event_digest text NOT NULL,
    exported_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT audit_event_action_check CHECK (((length(btrim(action)) >= 1) AND (length(btrim(action)) <= 128))),
    CONSTRAINT audit_event_agent_app_revision_check CHECK ((agent_app_revision >= 0)),
    CONSTRAINT audit_event_audit_id_check CHECK (((length(btrim(audit_id)) >= 1) AND (length(btrim(audit_id)) <= 256))),
    CONSTRAINT audit_event_config_version_check CHECK ((config_version >= 0)),
    CONSTRAINT audit_event_content_digest_check CHECK (((content_digest = ''::text) OR (content_digest ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT audit_event_cost_micros_check CHECK ((cost_micros >= 0)),
    CONSTRAINT audit_event_currency_check CHECK (((currency = ''::text) OR (currency ~ '^[A-Z]{3}$'::text))),
    CONSTRAINT audit_event_decision_check CHECK (((length(btrim(decision)) >= 1) AND (length(btrim(decision)) <= 128))),
    CONSTRAINT audit_event_event_digest_check CHECK ((event_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT audit_event_input_tokens_check CHECK ((input_tokens >= 0)),
    CONSTRAINT audit_event_latency_ms_check CHECK ((latency_ms >= 0)),
    CONSTRAINT audit_event_output_tokens_check CHECK ((output_tokens >= 0)),
    CONSTRAINT audit_event_policy_version_check CHECK ((policy_version >= 0)),
    CONSTRAINT audit_event_resource_refs_check CHECK ((jsonb_typeof(resource_refs) = 'array'::text)),
    CONSTRAINT audit_event_schema_version_check CHECK ((schema_version = 1)),
    CONSTRAINT audit_event_trace_id_check CHECK (((trace_id = ''::text) OR (trace_id ~ '^[0-9a-f]{32}$'::text)))
);


--
-- Name: backend_binding; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backend_binding (
    tenant_id text NOT NULL,
    config_version bigint NOT NULL,
    domain text NOT NULL,
    backend_profile_id text NOT NULL,
    backend_version bigint NOT NULL,
    required text[] DEFAULT '{}'::text[] NOT NULL,
    CONSTRAINT backend_binding_backend_version_check CHECK ((backend_version >= 1))
);


--
-- Name: backend_migration; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backend_migration (
    tenant_id text NOT NULL,
    migration_id text NOT NULL,
    domain text NOT NULL,
    epoch bigint NOT NULL,
    source_config_version bigint NOT NULL,
    source_backend_profile_id text NOT NULL,
    source_backend_version bigint NOT NULL,
    target_config_version bigint NOT NULL,
    target_backend_profile_id text NOT NULL,
    target_backend_version bigint NOT NULL,
    state text NOT NULL,
    snapshot_watermark text DEFAULT ''::text NOT NULL,
    dual_write_ref text DEFAULT ''::text NOT NULL,
    backfill_checkpoint text DEFAULT ''::text NOT NULL,
    next_batch_seq bigint DEFAULT 1 NOT NULL,
    backfill_count bigint DEFAULT 0 NOT NULL,
    backfill_complete boolean DEFAULT false NOT NULL,
    verify_source_count bigint,
    verify_target_count bigint,
    verify_source_digest text DEFAULT ''::text NOT NULL,
    verify_target_digest text DEFAULT ''::text NOT NULL,
    verify_source_watermark text DEFAULT ''::text NOT NULL,
    verify_target_watermark text DEFAULT ''::text NOT NULL,
    verify_sample_digest text DEFAULT ''::text NOT NULL,
    cutover_config_version bigint,
    cutover_at timestamp with time zone,
    observe_until timestamp with time zone,
    rollback_sync_watermark text DEFAULT ''::text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT backend_migration_backfill_count_check CHECK ((backfill_count >= 0)),
    CONSTRAINT backend_migration_check CHECK (((source_config_version <> target_config_version) OR (source_backend_profile_id <> target_backend_profile_id) OR (source_backend_version <> target_backend_version))),
    CONSTRAINT backend_migration_check1 CHECK (((length(snapshot_watermark) <= 512) AND (length(dual_write_ref) <= 512) AND (length(backfill_checkpoint) <= 512) AND (length(verify_source_watermark) <= 512) AND (length(verify_target_watermark) <= 512) AND (length(rollback_sync_watermark) <= 512))),
    CONSTRAINT backend_migration_check2 CHECK ((((verify_source_count IS NULL) OR (verify_source_count >= 0)) AND ((verify_target_count IS NULL) OR (verify_target_count >= 0)))),
    CONSTRAINT backend_migration_check3 CHECK ((((verify_source_digest = ''::text) OR (verify_source_digest ~ '^[0-9a-f]{64}$'::text)) AND ((verify_target_digest = ''::text) OR (verify_target_digest ~ '^[0-9a-f]{64}$'::text)) AND ((verify_sample_digest = ''::text) OR (verify_sample_digest ~ '^[0-9a-f]{64}$'::text)))),
    CONSTRAINT backend_migration_check4 CHECK ((updated_at >= created_at)),
    CONSTRAINT backend_migration_domain_check CHECK (((length(btrim(domain)) > 0) AND (length(domain) <= 32))),
    CONSTRAINT backend_migration_epoch_check CHECK ((epoch >= 1)),
    CONSTRAINT backend_migration_migration_id_check CHECK (((length(btrim(migration_id)) > 0) AND (length(migration_id) <= 128))),
    CONSTRAINT backend_migration_next_batch_seq_check CHECK ((next_batch_seq >= 1)),
    CONSTRAINT backend_migration_source_backend_version_check CHECK ((source_backend_version >= 1)),
    CONSTRAINT backend_migration_source_config_version_check CHECK ((source_config_version >= 1)),
    CONSTRAINT backend_migration_state_check CHECK ((state = ANY (ARRAY['planned'::text, 'snapshot'::text, 'dual_write'::text, 'backfill'::text, 'verify'::text, 'cutover'::text, 'observe'::text, 'cleanup'::text]))),
    CONSTRAINT backend_migration_target_backend_version_check CHECK ((target_backend_version >= 1)),
    CONSTRAINT backend_migration_target_config_version_check CHECK ((target_config_version >= 1)),
    CONSTRAINT backend_migration_version_check CHECK ((version >= 1))
);


--
-- Name: backend_migration_batch; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backend_migration_batch (
    tenant_id text NOT NULL,
    migration_id text NOT NULL,
    batch_seq bigint NOT NULL,
    batch_id text NOT NULL,
    epoch bigint NOT NULL,
    from_checkpoint text NOT NULL,
    to_checkpoint text NOT NULL,
    record_count bigint NOT NULL,
    content_digest text NOT NULL,
    complete boolean DEFAULT false NOT NULL,
    result_version bigint NOT NULL,
    committed_at timestamp with time zone NOT NULL,
    CONSTRAINT backend_migration_batch_batch_id_check CHECK (((length(btrim(batch_id)) > 0) AND (length(batch_id) <= 128))),
    CONSTRAINT backend_migration_batch_batch_seq_check CHECK ((batch_seq >= 1)),
    CONSTRAINT backend_migration_batch_check CHECK ((to_checkpoint <> from_checkpoint)),
    CONSTRAINT backend_migration_batch_check1 CHECK (((record_count > 0) OR complete)),
    CONSTRAINT backend_migration_batch_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT backend_migration_batch_epoch_check CHECK ((epoch >= 1)),
    CONSTRAINT backend_migration_batch_record_count_check CHECK ((record_count >= 0)),
    CONSTRAINT backend_migration_batch_result_version_check CHECK ((result_version >= 2)),
    CONSTRAINT backend_migration_batch_to_checkpoint_check CHECK (((length(to_checkpoint) > 0) AND (length(to_checkpoint) <= 512)))
);


--
-- Name: backend_migration_config_switch; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backend_migration_config_switch (
    tenant_id text NOT NULL,
    migration_id text NOT NULL,
    switch_id text NOT NULL,
    direction text NOT NULL,
    previous_config_version bigint NOT NULL,
    active_config_version bigint NOT NULL,
    migration_result_version bigint NOT NULL,
    previous_tenant_version bigint NOT NULL,
    tenant_result_version bigint NOT NULL,
    rollback_sync_watermark text DEFAULT ''::text NOT NULL,
    actor_id text NOT NULL,
    reason_code text NOT NULL,
    correlation_id text NOT NULL,
    trace_id text NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    CONSTRAINT backend_migration_config_switch_active_config_version_check CHECK ((active_config_version >= 1)),
    CONSTRAINT backend_migration_config_switch_actor_id_check CHECK (((length(btrim(actor_id)) >= 1) AND (length(btrim(actor_id)) <= 256))),
    CONSTRAINT backend_migration_config_switch_check CHECK ((tenant_result_version = (previous_tenant_version + 1))),
    CONSTRAINT backend_migration_config_switch_check1 CHECK (((length(btrim(correlation_id)) > 0) AND (length(btrim(trace_id)) > 0))),
    CONSTRAINT backend_migration_config_switch_check2 CHECK ((((direction = 'cutover'::text) AND (rollback_sync_watermark = ''::text)) OR ((direction = 'rollback'::text) AND (length(rollback_sync_watermark) > 0)))),
    CONSTRAINT backend_migration_config_switch_direction_check CHECK ((direction = ANY (ARRAY['cutover'::text, 'rollback'::text]))),
    CONSTRAINT backend_migration_config_switch_migration_result_version_check CHECK ((migration_result_version >= 1)),
    CONSTRAINT backend_migration_config_switch_previous_config_version_check CHECK ((previous_config_version >= 1)),
    CONSTRAINT backend_migration_config_switch_previous_tenant_version_check CHECK ((previous_tenant_version >= 1)),
    CONSTRAINT backend_migration_config_switch_reason_code_check CHECK (((length(btrim(reason_code)) >= 1) AND (length(btrim(reason_code)) <= 128))),
    CONSTRAINT backend_migration_config_switch_rollback_sync_watermark_check CHECK ((length(rollback_sync_watermark) <= 512)),
    CONSTRAINT backend_migration_config_switch_switch_id_check CHECK (((length(btrim(switch_id)) >= 1) AND (length(btrim(switch_id)) <= 128)))
);


--
-- Name: backend_profile; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backend_profile (
    tenant_id text NOT NULL,
    backend_profile_id text NOT NULL,
    display_name text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    row_version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    profile_key text NOT NULL,
    current_version bigint,
    CONSTRAINT backend_profile_key_check CHECK (((length(btrim(profile_key)) >= 1) AND (length(btrim(profile_key)) <= 128))),
    CONSTRAINT backend_profile_status_check CHECK ((status = ANY (ARRAY['active'::text, 'suspended'::text, 'disabled'::text]))),
    CONSTRAINT backend_profile_version_check CHECK ((row_version >= 1))
);


--
-- Name: backend_profile_revision; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.backend_profile_revision (
    tenant_id text NOT NULL,
    backend_profile_id text NOT NULL,
    profile_version bigint NOT NULL,
    schema_version integer NOT NULL,
    provider text NOT NULL,
    configuration jsonb DEFAULT '{}'::jsonb NOT NULL,
    credential_ref text,
    credential_version bigint,
    capabilities text[] DEFAULT '{}'::text[] NOT NULL,
    content_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT backend_profile_revision_check CHECK (((credential_ref IS NULL) = (credential_version IS NULL))),
    CONSTRAINT backend_profile_revision_configuration_check CHECK ((jsonb_typeof(configuration) = 'object'::text)),
    CONSTRAINT backend_profile_revision_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT backend_profile_revision_credential_version_check CHECK ((credential_version >= 1)),
    CONSTRAINT backend_profile_revision_profile_version_check CHECK ((profile_version >= 1)),
    CONSTRAINT backend_profile_revision_provider_check CHECK (((length(btrim(provider)) >= 1) AND (length(btrim(provider)) <= 128))),
    CONSTRAINT backend_profile_revision_schema_version_check CHECK ((schema_version >= 1))
);


--
-- Name: budget_reservation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.budget_reservation (
    tenant_id text NOT NULL,
    reservation_id text NOT NULL,
    request_id text NOT NULL,
    resource_id text NOT NULL,
    attempt_class text NOT NULL,
    policy_version bigint NOT NULL,
    pricing_version bigint,
    budget_period date NOT NULL,
    reserved_cost_micros bigint NOT NULL,
    reserved_tokens bigint NOT NULL,
    actual_cost_micros bigint DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    refund_reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT budget_reservation_actual_cost_micros_check CHECK ((actual_cost_micros >= 0)),
    CONSTRAINT budget_reservation_check CHECK ((actual_cost_micros <= reserved_cost_micros)),
    CONSTRAINT budget_reservation_check1 CHECK (((reserved_tokens = 0) OR ((input_tokens + output_tokens) <= reserved_tokens))),
    CONSTRAINT budget_reservation_input_tokens_check CHECK ((input_tokens >= 0)),
    CONSTRAINT budget_reservation_output_tokens_check CHECK ((output_tokens >= 0)),
    CONSTRAINT budget_reservation_reserved_cost_micros_check CHECK ((reserved_cost_micros >= 0)),
    CONSTRAINT budget_reservation_reserved_tokens_check CHECK ((reserved_tokens >= 0)),
    CONSTRAINT budget_reservation_state_check CHECK ((state = ANY (ARRAY['reserved'::text, 'settled'::text, 'refunded'::text]))),
    CONSTRAINT budget_reservation_version_check CHECK ((version >= 1))
);


--
-- Name: business_audit_purge_batch; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.business_audit_purge_batch (
    tenant_id text NOT NULL,
    batch_id text NOT NULL,
    state text DEFAULT 'planned'::text NOT NULL,
    cutoff_at timestamp with time zone NOT NULL,
    watermark_at timestamp with time zone,
    safe_cutoff_at timestamp with time zone NOT NULL,
    planned_events bigint DEFAULT 0 NOT NULL,
    planned_outbox bigint DEFAULT 0 NOT NULL,
    planned_digest text DEFAULT ''::text NOT NULL,
    deleted_events bigint DEFAULT 0 NOT NULL,
    deleted_outbox bigint DEFAULT 0 NOT NULL,
    delete_attempt integer DEFAULT 0 NOT NULL,
    last_error_class text DEFAULT ''::text NOT NULL,
    claim_owner text DEFAULT ''::text NOT NULL,
    claim_until timestamp with time zone,
    not_before timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT business_audit_purge_batch_batch_id_check CHECK (((length(btrim(batch_id)) >= 1) AND (length(btrim(batch_id)) <= 128))),
    CONSTRAINT business_audit_purge_batch_check CHECK ((updated_at >= created_at)),
    CONSTRAINT business_audit_purge_batch_delete_attempt_check CHECK ((delete_attempt >= 0)),
    CONSTRAINT business_audit_purge_batch_deleted_events_check CHECK ((deleted_events >= 0)),
    CONSTRAINT business_audit_purge_batch_deleted_outbox_check CHECK ((deleted_outbox >= 0)),
    CONSTRAINT business_audit_purge_batch_planned_digest_check CHECK (((planned_digest = ''::text) OR (planned_digest ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT business_audit_purge_batch_planned_events_check CHECK ((planned_events >= 0)),
    CONSTRAINT business_audit_purge_batch_planned_outbox_check CHECK ((planned_outbox >= 0)),
    CONSTRAINT business_audit_purge_batch_state_check CHECK ((state = ANY (ARRAY['planned'::text, 'executing'::text, 'completed'::text, 'failed'::text, 'quarantined'::text]))),
    CONSTRAINT business_audit_purge_batch_version_check CHECK ((version >= 1))
);


--
-- Name: business_audit_purge_certificate; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.business_audit_purge_certificate (
    tenant_id text NOT NULL,
    batch_id text NOT NULL,
    from_occurred_at timestamp with time zone NOT NULL,
    to_occurred_at timestamp with time zone NOT NULL,
    event_count bigint NOT NULL,
    outbox_count bigint NOT NULL,
    watermark_at timestamp with time zone,
    event_digest text NOT NULL,
    approved_by text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    CONSTRAINT business_audit_purge_certificate_event_digest_check CHECK ((event_digest ~ '^[0-9a-f]{64}$'::text))
);


--
-- Name: channel_binding; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_binding (
    tenant_id text NOT NULL,
    config_version bigint NOT NULL,
    binding_id text NOT NULL,
    channel text NOT NULL,
    external_account_id text NOT NULL,
    agent_app_id text NOT NULL,
    secret_ref text NOT NULL,
    secret_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    send_secret_ref text,
    send_secret_version bigint,
    CONSTRAINT channel_binding_secret_version_check CHECK ((secret_version >= 1)),
    CONSTRAINT channel_binding_send_secret_complete CHECK ((((send_secret_ref IS NULL) AND (send_secret_version IS NULL)) OR ((length(btrim(send_secret_ref)) > 0) AND (send_secret_version >= 1))))
);


--
-- Name: channel_binding_locator; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_binding_locator (
    opaque_binding_id text NOT NULL,
    tenant_id text NOT NULL,
    config_version bigint NOT NULL,
    binding_id text NOT NULL,
    identity_secret_ref text,
    identity_secret_version bigint,
    session_secret_ref text,
    session_secret_version bigint,
    CONSTRAINT channel_binding_locator_identity_secret_complete CHECK ((((identity_secret_ref IS NULL) AND (identity_secret_version IS NULL)) OR ((length(btrim(identity_secret_ref)) > 0) AND (identity_secret_version IS NOT NULL)))),
    CONSTRAINT channel_binding_locator_identity_secret_version_check CHECK ((identity_secret_version >= 1)),
    CONSTRAINT channel_binding_locator_session_secret_complete CHECK ((((session_secret_ref IS NULL) AND (session_secret_version IS NULL)) OR ((length(btrim(session_secret_ref)) > 0) AND (session_secret_version IS NOT NULL)))),
    CONSTRAINT channel_binding_locator_session_secret_version_check CHECK ((session_secret_version >= 1))
);


--
-- Name: channel_ingress_candidate; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_ingress_candidate (
    candidate_token_digest text NOT NULL,
    opaque_binding_id text NOT NULL,
    channel text NOT NULL,
    route_key_digest text NOT NULL,
    purpose text NOT NULL,
    binding_version bigint NOT NULL,
    state text NOT NULL,
    receipt_token_digest text,
    protocol_identity_digest text,
    issued_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    verified_at timestamp with time zone,
    version bigint DEFAULT 0 NOT NULL,
    CONSTRAINT channel_ingress_candidate_binding_version_check CHECK ((binding_version >= 1)),
    CONSTRAINT channel_ingress_candidate_candidate_token_digest_check CHECK (((length(btrim(candidate_token_digest)) >= 16) AND (length(btrim(candidate_token_digest)) <= 256))),
    CONSTRAINT channel_ingress_candidate_check CHECK ((expires_at > issued_at)),
    CONSTRAINT channel_ingress_candidate_check1 CHECK ((((state = ANY (ARRAY['issued'::text, 'verifier_acquired'::text, 'burned'::text])) AND (receipt_token_digest IS NULL) AND (protocol_identity_digest IS NULL) AND (verified_at IS NULL)) OR ((state = ANY (ARRAY['verified'::text, 'promoted'::text])) AND (receipt_token_digest IS NOT NULL) AND (protocol_identity_digest IS NOT NULL) AND (verified_at IS NOT NULL)))),
    CONSTRAINT channel_ingress_candidate_purpose_check CHECK ((purpose = 'channel_verify'::text)),
    CONSTRAINT channel_ingress_candidate_state_check CHECK ((state = ANY (ARRAY['issued'::text, 'verifier_acquired'::text, 'verified'::text, 'promoted'::text, 'burned'::text]))),
    CONSTRAINT channel_ingress_candidate_version_check CHECK ((version >= 0))
);


--
-- Name: channel_public_route; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_public_route (
    channel text NOT NULL,
    route_key_digest text NOT NULL,
    opaque_binding_id text NOT NULL,
    binding_version bigint NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_public_route_binding_version_check CHECK ((binding_version >= 1)),
    CONSTRAINT channel_public_route_channel_check CHECK (((length(btrim(channel)) >= 1) AND (length(btrim(channel)) <= 64))),
    CONSTRAINT channel_public_route_opaque_binding_id_check CHECK (((length(btrim(opaque_binding_id)) >= 16) AND (length(btrim(opaque_binding_id)) <= 256))),
    CONSTRAINT channel_public_route_route_key_digest_check CHECK (((length(btrim(route_key_digest)) >= 16) AND (length(btrim(route_key_digest)) <= 256)))
);


--
-- Name: config_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.config_snapshot (
    tenant_id text NOT NULL,
    config_version bigint NOT NULL,
    schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    content_digest text NOT NULL,
    state text NOT NULL,
    actor_id text NOT NULL,
    reason_code text NOT NULL,
    correlation_id text NOT NULL,
    trace_id text NOT NULL,
    published_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    policy_version bigint GENERATED ALWAYS AS (((payload ->> 'policy_version'::text))::bigint) STORED,
    CONSTRAINT config_snapshot_actor_id_check CHECK ((length(btrim(actor_id)) > 0)),
    CONSTRAINT config_snapshot_config_version_check CHECK ((config_version >= 1)),
    CONSTRAINT config_snapshot_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT config_snapshot_correlation_id_check CHECK ((length(btrim(correlation_id)) > 0)),
    CONSTRAINT config_snapshot_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT config_snapshot_reason_code_check CHECK ((length(btrim(reason_code)) > 0)),
    CONSTRAINT config_snapshot_schema_version_check CHECK ((schema_version = 1)),
    CONSTRAINT config_snapshot_state_check CHECK ((state = 'published'::text)),
    CONSTRAINT config_snapshot_trace_id_check CHECK ((length(btrim(trace_id)) > 0))
);


--
-- Name: confirmation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.confirmation (
    tenant_id text NOT NULL,
    confirmation_id text NOT NULL,
    request_id text NOT NULL,
    request_digest text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    input_seq bigint NOT NULL,
    subject_id text NOT NULL,
    channel_binding_id text NOT NULL,
    tool_id text NOT NULL,
    tool_version bigint NOT NULL,
    tool_call_id text NOT NULL,
    args_digest text NOT NULL,
    policy_version bigint NOT NULL,
    checkpoint_ref text NOT NULL,
    input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    cached_input_tokens bigint DEFAULT 0 NOT NULL,
    state text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    decision_at timestamp with time zone,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT confirmation_args_digest_check CHECK ((args_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT confirmation_check CHECK (((cached_input_tokens >= 0) AND (cached_input_tokens <= input_tokens))),
    CONSTRAINT confirmation_check1 CHECK ((((state = 'pending'::text) AND (decision_at IS NULL)) OR ((state <> 'pending'::text) AND (decision_at IS NOT NULL)))),
    CONSTRAINT confirmation_input_seq_check CHECK ((input_seq >= 1)),
    CONSTRAINT confirmation_input_tokens_check CHECK ((input_tokens >= 0)),
    CONSTRAINT confirmation_output_tokens_check CHECK ((output_tokens >= 0)),
    CONSTRAINT confirmation_request_digest_check CHECK ((request_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT confirmation_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'approved'::text, 'denied'::text, 'expired'::text, 'consumed'::text]))),
    CONSTRAINT confirmation_tool_version_check CHECK ((tool_version >= 1)),
    CONSTRAINT confirmation_version_check CHECK ((version >= 1))
);


--
-- Name: confirmation_grant; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.confirmation_grant (
    tenant_id text NOT NULL,
    grant_id text NOT NULL,
    confirmation_id text NOT NULL,
    request_id text NOT NULL,
    subject_id text NOT NULL,
    tool_id text NOT NULL,
    tool_version bigint NOT NULL,
    tool_call_id text NOT NULL,
    args_digest text NOT NULL,
    policy_version bigint NOT NULL,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    consumed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT confirmation_grant_args_digest_check CHECK ((args_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT confirmation_grant_check CHECK ((((state = 'available'::text) AND (consumed_at IS NULL)) OR ((state = 'consumed'::text) AND (consumed_at IS NOT NULL)))),
    CONSTRAINT confirmation_grant_state_check CHECK ((state = ANY (ARRAY['available'::text, 'consumed'::text]))),
    CONSTRAINT confirmation_grant_tool_version_check CHECK ((tool_version >= 1)),
    CONSTRAINT confirmation_grant_version_check CHECK ((version >= 1))
);


--
-- Name: delivery_ledger; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.delivery_ledger (
    tenant_id text NOT NULL,
    delivery_key text NOT NULL,
    segment_no integer NOT NULL,
    provider_message_id text,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    renderer_version text DEFAULT 'legacy-v1'::text NOT NULL,
    format_version text DEFAULT 'legacy-v1'::text NOT NULL,
    content_digest text DEFAULT repeat('0'::text, 64) NOT NULL,
    segment_count integer DEFAULT 1 NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    not_before timestamp with time zone DEFAULT now() NOT NULL,
    last_error_class text,
    client_request_id text NOT NULL,
    claim_owner text,
    claim_until timestamp with time zone,
    reconcile_attempt integer DEFAULT 0 NOT NULL,
    CONSTRAINT delivery_ledger_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT delivery_ledger_claim_check CHECK ((((state = 'sending'::text) AND (claim_owner IS NOT NULL) AND (length(btrim(claim_owner)) > 0) AND (claim_until IS NOT NULL)) OR ((state <> 'sending'::text) AND (claim_owner IS NULL) AND (claim_until IS NULL)))),
    CONSTRAINT delivery_ledger_client_request_id_check CHECK ((length(btrim(client_request_id)) > 0)),
    CONSTRAINT delivery_ledger_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT delivery_ledger_format_version_check CHECK ((length(btrim(format_version)) > 0)),
    CONSTRAINT delivery_ledger_reconcile_attempt_check CHECK ((reconcile_attempt >= 0)),
    CONSTRAINT delivery_ledger_renderer_version_check CHECK ((length(btrim(renderer_version)) > 0)),
    CONSTRAINT delivery_ledger_segment_count_check CHECK (((segment_count >= 1) AND (segment_no < segment_count))),
    CONSTRAINT delivery_ledger_segment_no_check CHECK ((segment_no >= 0)),
    CONSTRAINT delivery_ledger_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'sending'::text, 'sent'::text, 'ambiguous'::text, 'retry_wait'::text, 'failed'::text]))),
    CONSTRAINT delivery_ledger_version_check CHECK ((version >= 1))
);


--
-- Name: execution_cancel_intent; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.execution_cancel_intent (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    cancel_version bigint NOT NULL,
    actor_id text NOT NULL,
    reason_code text NOT NULL,
    traceparent text,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_cancel_intent_actor_id_check CHECK ((actor_id <> ''::text)),
    CONSTRAINT execution_cancel_intent_cancel_version_check CHECK ((cancel_version >= 1)),
    CONSTRAINT execution_cancel_intent_reason_code_check CHECK ((reason_code <> ''::text))
);


--
-- Name: execution_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.execution_record (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    tenant_version bigint NOT NULL,
    agent_app_id text NOT NULL,
    agent_app_version bigint NOT NULL,
    agent_app_revision bigint NOT NULL,
    agent_content_digest text NOT NULL,
    config_version bigint NOT NULL,
    policy_version bigint NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    channel text NOT NULL,
    input_seq bigint NOT NULL,
    payload_ref text NOT NULL,
    traceparent text,
    outcome text DEFAULT 'queued'::text NOT NULL,
    result_ref text,
    park_attempt integer DEFAULT 0 NOT NULL,
    not_before timestamp with time zone,
    version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    park_deadline timestamp with time zone,
    blocked_at timestamp with time zone,
    blocked_reason text,
    cancel_requested_at timestamp with time zone,
    cancel_version bigint DEFAULT 0 NOT NULL,
    CONSTRAINT execution_record_agent_content_digest_check CHECK ((agent_content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT execution_record_cancel_intent_check CHECK (((cancel_version >= 0) AND (((cancel_requested_at IS NULL) AND (cancel_version = 0)) OR ((cancel_requested_at IS NOT NULL) AND (cancel_version >= 1))))),
    CONSTRAINT execution_record_input_seq_check CHECK ((input_seq >= 1)),
    CONSTRAINT execution_record_outcome_check CHECK ((outcome = ANY (ARRAY['queued'::text, 'running'::text, 'pending'::text, 'blocked'::text, 'waiting_confirmation'::text, 'succeeded'::text, 'denied'::text, 'failed'::text, 'cancelled'::text, 'confirmation_denied'::text, 'confirmation_timeout'::text]))),
    CONSTRAINT execution_record_park_attempt_check CHECK ((park_attempt >= 0)),
    CONSTRAINT execution_record_park_state_check CHECK (((park_attempt >= 0) AND ((park_deadline IS NULL) OR (not_before IS NULL) OR (not_before <= park_deadline)) AND ((outcome <> 'blocked'::text) OR ((blocked_at IS NOT NULL) AND (blocked_reason = ANY (ARRAY['park_attempts_exhausted'::text, 'park_deadline_exceeded'::text])))))),
    CONSTRAINT execution_record_version_check CHECK ((version >= 0))
);


--
-- Name: governance_decision; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.governance_decision (
    tenant_id text NOT NULL,
    decision_id text NOT NULL,
    request_id text NOT NULL,
    stage text NOT NULL,
    action text NOT NULL,
    reason_code text NOT NULL,
    policy_version bigint NOT NULL,
    rule_ids jsonb DEFAULT '[]'::jsonb NOT NULL,
    reservation_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT governance_decision_action_check CHECK ((action = ANY (ARRAY['allow'::text, 'deny'::text, 'ask'::text, 'redact'::text, 'throttle'::text]))),
    CONSTRAINT governance_decision_rule_ids_check CHECK ((jsonb_typeof(rule_ids) = 'array'::text))
);


--
-- Name: inbound_payload; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.inbound_payload (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    payload_ref text NOT NULL,
    payload_ciphertext bytea NOT NULL,
    payload_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT inbound_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT inbound_payload_key_version_check CHECK ((key_version > 0))
);


--
-- Name: interaction_payload; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.interaction_payload (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    content_ref text NOT NULL,
    content_ciphertext bytea NOT NULL,
    content_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT interaction_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT interaction_payload_key_version_check CHECK ((key_version >= 1))
);


--
-- Name: knowledge_chunk; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_chunk (
    tenant_id text NOT NULL,
    knowledge_id text NOT NULL,
    knowledge_version bigint NOT NULL,
    chunk_id text NOT NULL,
    source_digest text NOT NULL,
    content_digest text NOT NULL,
    metadata_digest text NOT NULL,
    embedding_profile_id text NOT NULL,
    embedding_version bigint NOT NULL,
    vector_generation text NOT NULL,
    image_digest text NOT NULL,
    content text NOT NULL,
    metadata jsonb NOT NULL,
    vector jsonb NOT NULL,
    indexed_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT knowledge_chunk_chunk_id_check CHECK (((length(btrim(chunk_id)) >= 1) AND (length(btrim(chunk_id)) <= 512))),
    CONSTRAINT knowledge_chunk_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT knowledge_chunk_embedding_profile_id_check CHECK (((length(btrim(embedding_profile_id)) >= 1) AND (length(btrim(embedding_profile_id)) <= 256))),
    CONSTRAINT knowledge_chunk_embedding_version_check CHECK ((embedding_version >= 1)),
    CONSTRAINT knowledge_chunk_image_digest_check CHECK ((image_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT knowledge_chunk_knowledge_version_check CHECK ((knowledge_version >= 1)),
    CONSTRAINT knowledge_chunk_metadata_digest_check CHECK ((metadata_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT knowledge_chunk_source_digest_check CHECK ((source_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT knowledge_chunk_vector_generation_check CHECK (((length(btrim(vector_generation)) >= 1) AND (length(btrim(vector_generation)) <= 256)))
);


--
-- Name: knowledge_manifest; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_manifest (
    tenant_id text NOT NULL,
    knowledge_id text NOT NULL,
    version bigint NOT NULL,
    source_uri text NOT NULL,
    source_digest text NOT NULL,
    chunking_pipeline_version text NOT NULL,
    embedder_profile_id text NOT NULL,
    embedder_version bigint NOT NULL,
    vector_collection_generation text NOT NULL,
    metadata_schema jsonb NOT NULL,
    content_watermark text NOT NULL,
    state text DEFAULT 'staging'::text NOT NULL,
    chunk_total bigint,
    verification_digest text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    record_version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT knowledge_manifest_check CHECK ((updated_at >= created_at)),
    CONSTRAINT knowledge_manifest_chunk_total_check CHECK (((chunk_total IS NULL) OR (chunk_total >= 1))),
    CONSTRAINT knowledge_manifest_embedder_profile_id_check CHECK (((length(btrim(embedder_profile_id)) >= 1) AND (length(btrim(embedder_profile_id)) <= 256))),
    CONSTRAINT knowledge_manifest_embedder_version_check CHECK ((embedder_version >= 1)),
    CONSTRAINT knowledge_manifest_knowledge_id_check CHECK (((length(btrim(knowledge_id)) >= 1) AND (length(btrim(knowledge_id)) <= 256))),
    CONSTRAINT knowledge_manifest_record_version_check CHECK ((record_version >= 1)),
    CONSTRAINT knowledge_manifest_source_digest_check CHECK ((source_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT knowledge_manifest_source_uri_check CHECK (((length(btrim(source_uri)) >= 1) AND (length(btrim(source_uri)) <= 2048))),
    CONSTRAINT knowledge_manifest_state_check CHECK ((state = ANY (ARRAY['staging'::text, 'indexing'::text, 'verifying'::text, 'published'::text, 'failed'::text]))),
    CONSTRAINT knowledge_manifest_vector_collection_generation_check CHECK (((length(btrim(vector_collection_generation)) >= 1) AND (length(btrim(vector_collection_generation)) <= 256))),
    CONSTRAINT knowledge_manifest_verification_digest_check CHECK (((verification_digest = ''::text) OR (verification_digest ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT knowledge_manifest_version_check CHECK ((version >= 1))
);


--
-- Name: knowledge_migration_mutation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_migration_mutation (
    tenant_id text NOT NULL,
    migration_id text NOT NULL,
    mutation_id text NOT NULL,
    epoch bigint NOT NULL,
    knowledge_id text NOT NULL,
    knowledge_version bigint NOT NULL,
    chunk_id text NOT NULL,
    operation text NOT NULL,
    source_revision bigint NOT NULL,
    mutation_digest text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    lease_owner text DEFAULT ''::text NOT NULL,
    lease_until timestamp with time zone,
    not_before timestamp with time zone NOT NULL,
    last_error_class text DEFAULT ''::text NOT NULL,
    target_revision bigint,
    target_digest text DEFAULT ''::text NOT NULL,
    applied_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    direction text DEFAULT 'forward'::text NOT NULL,
    CONSTRAINT knowledge_migration_mutation_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT knowledge_migration_mutation_check CHECK (((target_revision IS NULL) OR (target_revision >= source_revision))),
    CONSTRAINT knowledge_migration_mutation_check1 CHECK ((updated_at >= created_at)),
    CONSTRAINT knowledge_migration_mutation_chunk_id_check CHECK (((length(btrim(chunk_id)) >= 1) AND (length(btrim(chunk_id)) <= 512))),
    CONSTRAINT knowledge_migration_mutation_direction_check CHECK ((direction = ANY (ARRAY['forward'::text, 'reverse'::text]))),
    CONSTRAINT knowledge_migration_mutation_epoch_check CHECK ((epoch >= 1)),
    CONSTRAINT knowledge_migration_mutation_knowledge_id_check CHECK (((length(btrim(knowledge_id)) >= 1) AND (length(btrim(knowledge_id)) <= 256))),
    CONSTRAINT knowledge_migration_mutation_knowledge_version_check CHECK ((knowledge_version >= 1)),
    CONSTRAINT knowledge_migration_mutation_last_error_class_check CHECK ((length(last_error_class) <= 64)),
    CONSTRAINT knowledge_migration_mutation_mutation_digest_check CHECK ((mutation_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT knowledge_migration_mutation_mutation_id_check CHECK (((length(btrim(mutation_id)) >= 1) AND (length(btrim(mutation_id)) <= 128))),
    CONSTRAINT knowledge_migration_mutation_operation_check CHECK ((operation = ANY (ARRAY['upsert'::text, 'delete'::text]))),
    CONSTRAINT knowledge_migration_mutation_source_revision_check CHECK ((source_revision >= 1)),
    CONSTRAINT knowledge_migration_mutation_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'applying'::text, 'applied'::text]))),
    CONSTRAINT knowledge_migration_mutation_target_digest_check CHECK (((target_digest = ''::text) OR (target_digest ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT knowledge_migration_mutation_version_check CHECK ((version >= 1))
);

-- Durable repair queue for a full (app_name,user_id) Memory image. Memory
-- backends do not expose a per-write revision, so a mutation carries the
-- source image digest; replaying the same mutation is idempotent while a
-- changed digest is a collision. The queue is intentionally separate from
-- session/knowledge ledgers: Memory migration must not couple to their data
-- models or locks.
CREATE TABLE public.memory_migration_mutation (
    tenant_id text NOT NULL,
    migration_id text NOT NULL,
    mutation_id text NOT NULL,
    epoch bigint NOT NULL,
    app_name text NOT NULL,
    user_id text NOT NULL,
    source_digest text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    lease_owner text DEFAULT ''::text NOT NULL,
    lease_until timestamp with time zone,
    not_before timestamp with time zone NOT NULL,
    last_error_class text DEFAULT ''::text NOT NULL,
    target_digest text DEFAULT ''::text NOT NULL,
    applied_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    direction text DEFAULT 'forward'::text NOT NULL,
    PRIMARY KEY (tenant_id,migration_id,app_name,user_id,mutation_id),
    UNIQUE (tenant_id,migration_id,mutation_id),
    CONSTRAINT memory_migration_mutation_attempt_check CHECK (attempt >= 0),
    CONSTRAINT memory_migration_mutation_updated_check CHECK (updated_at >= created_at),
    CONSTRAINT memory_migration_mutation_direction_check CHECK (direction = ANY (ARRAY['forward'::text, 'reverse'::text])),
    CONSTRAINT memory_migration_mutation_epoch_check CHECK (epoch >= 1),
    CONSTRAINT memory_migration_mutation_app_check CHECK (length(btrim(app_name)) >= 1 AND length(app_name) <= 255),
    CONSTRAINT memory_migration_mutation_user_check CHECK (length(btrim(user_id)) >= 1 AND length(user_id) <= 255),
    CONSTRAINT memory_migration_mutation_error_check CHECK (length(last_error_class) <= 64),
    CONSTRAINT memory_migration_mutation_source_digest_check CHECK (source_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT memory_migration_mutation_target_digest_check CHECK (target_digest = '' OR target_digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT memory_migration_mutation_id_check CHECK (length(btrim(mutation_id)) >= 1 AND length(mutation_id) <= 128),
    CONSTRAINT memory_migration_mutation_state_check CHECK (state = ANY (ARRAY['pending'::text, 'applying'::text, 'applied'::text])),
    CONSTRAINT memory_migration_mutation_version_check CHECK (version >= 1)
);


--
-- Name: knowledge_probe; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.knowledge_probe (
    tenant_id text NOT NULL,
    knowledge_id text NOT NULL,
    knowledge_version bigint NOT NULL,
    probe_id text NOT NULL,
    query text NOT NULL,
    expected_chunks jsonb NOT NULL,
    min_recall_ppm bigint NOT NULL,
    verified boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT knowledge_probe_knowledge_version_check CHECK ((knowledge_version >= 1)),
    CONSTRAINT knowledge_probe_min_recall_ppm_check CHECK (((min_recall_ppm >= 1) AND (min_recall_ppm <= 1000000))),
    CONSTRAINT knowledge_probe_probe_id_check CHECK (((length(btrim(probe_id)) >= 1) AND (length(btrim(probe_id)) <= 128))),
    CONSTRAINT knowledge_probe_query_check CHECK (((length(btrim(query)) >= 1) AND (length(btrim(query)) <= 4096)))
);


--
-- Name: media_artifact; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.media_artifact (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    artifact_id text NOT NULL,
    artifact_ref text NOT NULL,
    ordinal integer NOT NULL,
    source_digest text NOT NULL,
    content_digest text NOT NULL,
    media_type text NOT NULL,
    kind text NOT NULL,
    content bytea,
    malware_scan_version text NOT NULL,
    dlp_version text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    storage_kind text DEFAULT 'postgres_bytea'::text NOT NULL,
    object_key text,
    content_size bigint NOT NULL,
    retention_managed boolean DEFAULT false NOT NULL,
    lifecycle_state text DEFAULT 'active'::text NOT NULL,
    claim_owner text,
    claim_until timestamp with time zone,
    delete_attempt integer DEFAULT 0 NOT NULL,
    last_error_class text,
    quarantined_at timestamp with time zone,
    lifecycle_version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT media_artifact_content_check CHECK ((octet_length(content) > 0)),
    CONSTRAINT media_artifact_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT media_artifact_content_size_check CHECK ((content_size > 0)),
    CONSTRAINT media_artifact_delete_attempt_check CHECK ((delete_attempt >= 0)),
    CONSTRAINT media_artifact_kind_check CHECK ((kind = ANY (ARRAY['image'::text, 'file'::text]))),
    CONSTRAINT media_artifact_lifecycle_shape_check CHECK ((((lifecycle_state = 'active'::text) AND (claim_owner IS NULL) AND (claim_until IS NULL) AND (quarantined_at IS NULL)) OR ((lifecycle_state = 'delete_claimed'::text) AND (claim_owner IS NOT NULL) AND (claim_until IS NOT NULL) AND (quarantined_at IS NULL)) OR ((lifecycle_state = 'quarantined'::text) AND (claim_owner IS NULL) AND (claim_until IS NULL) AND (quarantined_at IS NOT NULL) AND (last_error_class IS NOT NULL)))),
    CONSTRAINT media_artifact_lifecycle_state_check CHECK ((lifecycle_state = ANY (ARRAY['active'::text, 'delete_claimed'::text, 'quarantined'::text]))),
    CONSTRAINT media_artifact_lifecycle_version_check CHECK ((lifecycle_version >= 1)),
    CONSTRAINT media_artifact_ordinal_check CHECK ((ordinal >= 0)),
    CONSTRAINT media_artifact_source_digest_check CHECK ((source_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT media_artifact_storage_kind_check CHECK ((storage_kind = ANY (ARRAY['postgres_bytea'::text, 'object'::text]))),
    CONSTRAINT media_artifact_storage_shape_check CHECK ((((storage_kind = 'postgres_bytea'::text) AND (content IS NOT NULL) AND (object_key IS NULL) AND (content_size = octet_length(content))) OR ((storage_kind = 'object'::text) AND (content IS NULL) AND (object_key ~ '^artifacts/v1/[A-Za-z0-9_-]{43}/a1_[A-Za-z0-9_-]{43}$'::text))))
);


--
-- Name: model_profile; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.model_profile (
    tenant_id text NOT NULL,
    model_profile_id text NOT NULL,
    profile_key text NOT NULL,
    display_name text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    current_version bigint,
    row_version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT model_profile_profile_key_check CHECK (((length(btrim(profile_key)) >= 1) AND (length(btrim(profile_key)) <= 128))),
    CONSTRAINT model_profile_row_version_check CHECK ((row_version >= 1)),
    CONSTRAINT model_profile_status_check CHECK ((status = ANY (ARRAY['active'::text, 'suspended'::text, 'disabled'::text])))
);


--
-- Name: model_profile_revision; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.model_profile_revision (
    tenant_id text NOT NULL,
    model_profile_id text NOT NULL,
    profile_version bigint NOT NULL,
    schema_version integer NOT NULL,
    provider text NOT NULL,
    model_name text NOT NULL,
    endpoint text DEFAULT ''::text NOT NULL,
    options jsonb DEFAULT '{}'::jsonb NOT NULL,
    secret_ref text,
    secret_version bigint,
    generation jsonb DEFAULT '{}'::jsonb NOT NULL,
    content_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT model_profile_revision_check CHECK (((secret_ref IS NULL) = (secret_version IS NULL))),
    CONSTRAINT model_profile_revision_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT model_profile_revision_generation_check CHECK ((jsonb_typeof(generation) = 'object'::text)),
    CONSTRAINT model_profile_revision_model_name_check CHECK (((length(btrim(model_name)) >= 1) AND (length(btrim(model_name)) <= 256))),
    CONSTRAINT model_profile_revision_options_check CHECK ((jsonb_typeof(options) = 'object'::text)),
    CONSTRAINT model_profile_revision_profile_version_check CHECK ((profile_version >= 1)),
    CONSTRAINT model_profile_revision_provider_check CHECK (((length(btrim(provider)) >= 1) AND (length(btrim(provider)) <= 128))),
    CONSTRAINT model_profile_revision_schema_version_check CHECK ((schema_version >= 1)),
    CONSTRAINT model_profile_revision_secret_version_check CHECK ((secret_version >= 1))
);


--
-- Name: outbox; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outbox (
    tenant_id text NOT NULL,
    outbox_id text NOT NULL,
    kind text NOT NULL,
    aggregate_id text NOT NULL,
    event_seq bigint NOT NULL,
    idempotency_key text NOT NULL,
    payload_ref text NOT NULL,
    traceparent text,
    state text DEFAULT 'pending'::text NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    claim_owner text,
    claim_until timestamp with time zone,
    published_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT outbox_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT outbox_event_seq_check CHECK ((event_seq >= 0)),
    CONSTRAINT outbox_kind_check CHECK ((kind = ANY (ARRAY['audit'::text, 'tenant-control'::text, 'config-invalidation'::text, 'dispatch'::text, 'reply'::text, 'wakeup'::text, 'execution-control'::text]))),
    CONSTRAINT outbox_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'claimed'::text, 'retry_wait'::text, 'published'::text, 'dead_letter'::text]))),
    CONSTRAINT outbox_version_check CHECK ((version >= 0))
);


--
-- Name: policy_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.policy_snapshot (
    tenant_id text NOT NULL,
    policy_version bigint NOT NULL,
    schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    content_digest text NOT NULL,
    pricing_version bigint,
    state text NOT NULL,
    published_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT policy_snapshot_check CHECK ((((COALESCE((((payload -> 'budget'::text) ->> 'max_cost_micros_per_run'::text))::bigint, (0)::bigint) = 0) AND (pricing_version IS NULL)) OR ((COALESCE((((payload -> 'budget'::text) ->> 'max_cost_micros_per_run'::text))::bigint, (0)::bigint) > 0) AND (pricing_version IS NOT NULL)))),
    CONSTRAINT policy_snapshot_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT policy_snapshot_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT policy_snapshot_policy_version_check CHECK ((policy_version >= 1)),
    CONSTRAINT policy_snapshot_schema_version_check CHECK ((schema_version = 1)),
    CONSTRAINT policy_snapshot_state_check CHECK ((state = 'published'::text))
);


--
-- Name: prepared_payload; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prepared_payload (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    payload_ref text NOT NULL,
    source_payload_ref text NOT NULL,
    payload_ciphertext bytea NOT NULL,
    payload_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    artifact_retention_seconds bigint DEFAULT 0 NOT NULL,
    CONSTRAINT prepared_payload_artifact_retention_seconds_check CHECK ((artifact_retention_seconds >= 0)),
    CONSTRAINT prepared_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT prepared_payload_key_version_check CHECK ((key_version > 0))
);


--
-- Name: preprocess_job; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.preprocess_job (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    job_id text NOT NULL,
    tenant_version bigint NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    user_id text NOT NULL,
    channel text NOT NULL,
    payload_ref text NOT NULL,
    traceparent text DEFAULT ''::text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    lease_owner text,
    lease_until timestamp with time zone,
    not_before timestamp with time zone DEFAULT now() NOT NULL,
    reject_reason text DEFAULT ''::text NOT NULL,
    dispatched_at timestamp with time zone,
    version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    prepared_payload_ref text DEFAULT ''::text NOT NULL,
    channel_binding_id text NOT NULL,
    config_version bigint NOT NULL,
    CONSTRAINT preprocess_job_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT preprocess_job_check CHECK ((((lease_owner IS NULL) AND (lease_until IS NULL) AND (state <> 'running'::text)) OR ((lease_owner IS NOT NULL) AND (lease_until IS NOT NULL) AND (state = ANY (ARRAY['running'::text, 'ready'::text]))))),
    CONSTRAINT preprocess_job_check1 CHECK (((dispatched_at IS NULL) OR (state = 'ready'::text))),
    CONSTRAINT preprocess_job_config_version_check CHECK ((config_version >= 1)),
    CONSTRAINT preprocess_job_prepared_ref_complete CHECK ((((state <> 'ready'::text) AND (prepared_payload_ref = ''::text)) OR ((state = 'ready'::text) AND ((prepared_payload_ref = ''::text) OR (length(btrim(prepared_payload_ref)) > 0))))),
    CONSTRAINT preprocess_job_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'ready'::text, 'rejected'::text, 'retry_wait'::text]))),
    CONSTRAINT preprocess_job_tenant_version_check CHECK ((tenant_version >= 1)),
    CONSTRAINT preprocess_job_version_check CHECK ((version >= 0))
);


--
-- Name: pricing_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pricing_snapshot (
    tenant_id text NOT NULL,
    pricing_version bigint NOT NULL,
    schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    content_digest text NOT NULL,
    state text NOT NULL,
    published_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT pricing_snapshot_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT pricing_snapshot_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT pricing_snapshot_pricing_version_check CHECK ((pricing_version >= 1)),
    CONSTRAINT pricing_snapshot_schema_version_check CHECK ((schema_version = 1)),
    CONSTRAINT pricing_snapshot_state_check CHECK ((state = 'published'::text))
);


--
-- Name: result_payload; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.result_payload (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    result_ref text NOT NULL,
    result_ciphertext bytea NOT NULL,
    result_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT result_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT result_payload_key_version_check CHECK ((key_version >= 1))
);


--
-- Name: session_commit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.session_commit (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    commit_id text NOT NULL,
    request_id text NOT NULL,
    request_digest text NOT NULL,
    input_seq bigint NOT NULL,
    stage text NOT NULL,
    outcome text NOT NULL,
    fence bigint NOT NULL,
    session_version bigint NOT NULL,
    reply_cursor text,
    result_ref text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT session_commit_fence_check CHECK ((fence >= 1)),
    CONSTRAINT session_commit_input_seq_check CHECK ((input_seq >= 1)),
    CONSTRAINT session_commit_outcome_check CHECK ((outcome = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'waiting_confirmation'::text, 'succeeded'::text, 'denied'::text, 'failed'::text, 'cancelled'::text, 'confirmation_denied'::text, 'confirmation_timeout'::text]))),
    CONSTRAINT session_commit_request_digest_check CHECK ((request_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT session_commit_session_version_check CHECK ((session_version >= 1))
);


--
-- Name: session_event; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.session_event (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    session_seq bigint NOT NULL,
    request_id text NOT NULL,
    input_seq bigint NOT NULL,
    event_seq bigint NOT NULL,
    event_id text NOT NULL,
    event_type text NOT NULL,
    payload_ref text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    event_payload jsonb,
    CONSTRAINT session_event_event_seq_check CHECK ((event_seq >= 1)),
    CONSTRAINT session_event_input_seq_check CHECK ((input_seq >= 1)),
    CONSTRAINT session_event_session_seq_check CHECK ((session_seq >= 1))
);


--
-- Name: session_head; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.session_head (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    last_fence bigint DEFAULT 0 NOT NULL,
    last_session_seq bigint DEFAULT 0 NOT NULL,
    next_input_seq bigint DEFAULT 1 NOT NULL,
    last_allocated_input_seq bigint DEFAULT 0 NOT NULL,
    state_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    summary_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT session_head_last_allocated_input_seq_check CHECK ((last_allocated_input_seq >= 0)),
    CONSTRAINT session_head_last_fence_check CHECK ((last_fence >= 0)),
    CONSTRAINT session_head_last_session_seq_check CHECK ((last_session_seq >= 0)),
    CONSTRAINT session_head_next_input_seq_check CHECK ((next_input_seq >= 1)),
    CONSTRAINT session_head_version_check CHECK ((version >= 0))
);


--
-- Name: session_migration_mutation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.session_migration_mutation (
    tenant_id text NOT NULL,
    migration_id text NOT NULL,
    mutation_id text NOT NULL,
    epoch bigint NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    source_version bigint NOT NULL,
    mutation_digest text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    lease_owner text DEFAULT ''::text NOT NULL,
    lease_until timestamp with time zone,
    not_before timestamp with time zone NOT NULL,
    last_error_class text DEFAULT ''::text NOT NULL,
    target_version bigint,
    target_digest text DEFAULT ''::text NOT NULL,
    applied_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    direction text DEFAULT 'forward'::text NOT NULL,
    CONSTRAINT session_migration_mutation_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT session_migration_mutation_check CHECK (((target_version IS NULL) OR (target_version >= source_version))),
    CONSTRAINT session_migration_mutation_check1 CHECK ((updated_at >= created_at)),
    CONSTRAINT session_migration_mutation_direction_check CHECK ((direction = ANY (ARRAY['forward'::text, 'reverse'::text]))),
    CONSTRAINT session_migration_mutation_epoch_check CHECK ((epoch >= 1)),
    CONSTRAINT session_migration_mutation_last_error_class_check CHECK ((length(last_error_class) <= 64)),
    CONSTRAINT session_migration_mutation_mutation_digest_check CHECK ((mutation_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT session_migration_mutation_mutation_id_check CHECK ((length(btrim(mutation_id)) > 0)),
    CONSTRAINT session_migration_mutation_source_version_check CHECK ((source_version >= 1)),
    CONSTRAINT session_migration_mutation_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'applying'::text, 'applied'::text]))),
    CONSTRAINT session_migration_mutation_target_digest_check CHECK (((target_digest = ''::text) OR (target_digest ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT session_migration_mutation_version_check CHECK ((version >= 1))
);

-- Idempotency receipt for applying an opaque trpc-agent-go Session snapshot
-- to a target PostgreSQL binding. This is intentionally separate from the
-- source-side repair queue above: source and target can be different DBs.
CREATE TABLE public.session_sdk_migration_apply (
    tenant_id text NOT NULL,
    migration_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    mutation_id text NOT NULL,
    epoch bigint NOT NULL,
    source_version bigint NOT NULL,
    snapshot_digest text NOT NULL,
    target_version bigint NOT NULL,
    applied_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, migration_id, agent_app_id, session_id, mutation_id),
    CONSTRAINT session_sdk_migration_apply_epoch_check CHECK (epoch >= 1),
    CONSTRAINT session_sdk_migration_apply_source_version_check CHECK (source_version >= 0),
    CONSTRAINT session_sdk_migration_apply_target_version_check CHECK (target_version >= 0),
    CONSTRAINT session_sdk_migration_apply_digest_check CHECK (snapshot_digest ~ '^[0-9a-f]{64}$')
);


--
-- Name: session_summary; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.session_summary (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    summary_id text NOT NULL,
    base_session_seq bigint NOT NULL,
    last_event_id text NOT NULL,
    cutoff_at timestamp with time zone NOT NULL,
    content_ref text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT session_summary_base_session_seq_check CHECK ((base_session_seq >= 1))
);


--
-- Name: session_summary_content; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.session_summary_content (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    session_id text NOT NULL,
    summary_id text NOT NULL,
    content_ref text NOT NULL,
    content_digest text NOT NULL,
    content bytea NOT NULL,
    state text DEFAULT 'active'::text NOT NULL,
    superseded_by_summary_id text,
    frozen boolean DEFAULT false NOT NULL,
    claim_owner text,
    claim_until timestamp with time zone,
    not_before timestamp with time zone DEFAULT now() NOT NULL,
    delete_attempt integer DEFAULT 0 NOT NULL,
    last_error_class text,
    record_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    superseded_at timestamp with time zone,
    CONSTRAINT session_summary_content_check CHECK ((((state = 'active'::text) AND (superseded_by_summary_id IS NULL) AND (superseded_at IS NULL) AND (claim_owner IS NULL) AND (claim_until IS NULL)) OR ((state = 'superseded'::text) AND (superseded_by_summary_id IS NOT NULL) AND (superseded_at IS NOT NULL) AND (claim_owner IS NULL) AND (claim_until IS NULL)) OR ((state = 'delete_claimed'::text) AND (superseded_by_summary_id IS NOT NULL) AND (superseded_at IS NOT NULL) AND (claim_owner IS NOT NULL) AND (claim_until IS NOT NULL)))),
    CONSTRAINT session_summary_content_content_check CHECK (((octet_length(content) > 0) AND (octet_length(content) <= 1048576))),
    CONSTRAINT session_summary_content_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT session_summary_content_delete_attempt_check CHECK ((delete_attempt >= 0)),
    CONSTRAINT session_summary_content_record_version_check CHECK ((record_version >= 0)),
    CONSTRAINT session_summary_content_state_check CHECK ((state = ANY (ARRAY['active'::text, 'superseded'::text, 'delete_claimed'::text])))
);


--
-- Name: skill_catalog; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.skill_catalog (
    tenant_id text NOT NULL,
    skill_id text NOT NULL,
    skill_version bigint NOT NULL,
    content_digest text NOT NULL,
    relative_path text NOT NULL,
    state text NOT NULL,
    record_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    CONSTRAINT skill_catalog_check CHECK ((((state = 'published'::text) AND (published_at IS NOT NULL)) OR ((state = ANY (ARRAY['staged'::text, 'failed'::text])) AND (published_at IS NULL)))),
    CONSTRAINT skill_catalog_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT skill_catalog_record_version_check CHECK ((record_version >= 0)),
    CONSTRAINT skill_catalog_relative_path_check CHECK (((relative_path !~ '^/'::text) AND (relative_path !~ '(^|/)\\.\\.?(/|$)'::text))),
    CONSTRAINT skill_catalog_skill_id_check CHECK (((skill_id !~ '[\\/]'::text) AND (skill_id <> ALL (ARRAY['.'::text, '..'::text])))),
    CONSTRAINT skill_catalog_skill_version_check CHECK ((skill_version >= 1)),
    CONSTRAINT skill_catalog_state_check CHECK ((state = ANY (ARRAY['staged'::text, 'published'::text, 'failed'::text])))
);


--
-- Name: tenant; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenant (
    tenant_id text NOT NULL,
    tenant_key text NOT NULL,
    display_name text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    request_limit_per_minute bigint,
    max_concurrent_executions integer,
    monthly_token_budget bigint,
    monthly_cost_budget_micros bigint,
    billing_currency character(3) DEFAULT 'USD'::bpchar NOT NULL,
    audit_retention_days integer DEFAULT 180 NOT NULL,
    audit_payload_mode text DEFAULT 'redacted'::text NOT NULL,
    log_masking_level text DEFAULT 'basic'::text NOT NULL,
    trace_sampling_rate numeric(5,4) DEFAULT 0.0100 NOT NULL,
    default_agent_app_id text,
    default_backend_profile_id text,
    active_config_version bigint,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT tenant_active_config_version_ck CHECK (((active_config_version IS NULL) OR (active_config_version >= 1))),
    CONSTRAINT tenant_audit_payload_mode_ck CHECK ((audit_payload_mode = ANY (ARRAY['metadata_only'::text, 'redacted'::text, 'encrypted_reference'::text]))),
    CONSTRAINT tenant_audit_retention_ck CHECK ((audit_retention_days >= 1)),
    CONSTRAINT tenant_concurrency_limit_ck CHECK (((max_concurrent_executions IS NULL) OR (max_concurrent_executions >= 0))),
    CONSTRAINT tenant_cost_budget_ck CHECK (((monthly_cost_budget_micros IS NULL) OR (monthly_cost_budget_micros >= 0))),
    CONSTRAINT tenant_currency_ck CHECK ((billing_currency ~ '^[A-Z]{3}$'::text)),
    CONSTRAINT tenant_display_name_ck CHECK (((length(btrim(display_name)) >= 1) AND (length(btrim(display_name)) <= 200))),
    CONSTRAINT tenant_id_format_ck CHECK ((tenant_id ~ '^t_[0-7][0-9A-HJKMNP-TV-Z]{25}$'::text)),
    CONSTRAINT tenant_key_format_ck CHECK (((tenant_key = lower(tenant_key)) AND ((length(tenant_key) >= 2) AND (length(tenant_key) <= 64)) AND (tenant_key ~ '^[a-z][a-z0-9-]{1,63}$'::text))),
    CONSTRAINT tenant_log_masking_level_ck CHECK ((log_masking_level = ANY (ARRAY['none'::text, 'basic'::text, 'strict'::text]))),
    CONSTRAINT tenant_request_limit_ck CHECK (((request_limit_per_minute IS NULL) OR (request_limit_per_minute >= 0))),
    CONSTRAINT tenant_status_ck CHECK ((status = ANY (ARRAY['active'::text, 'suspended'::text, 'disabled'::text]))),
    CONSTRAINT tenant_token_budget_ck CHECK (((monthly_token_budget IS NULL) OR (monthly_token_budget >= 0))),
    CONSTRAINT tenant_trace_sampling_ck CHECK (((trace_sampling_rate >= (0)::numeric) AND (trace_sampling_rate <= (1)::numeric))),
    CONSTRAINT tenant_version_ck CHECK ((version >= 1))
);


--
-- Name: tenant_status_change; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenant_status_change (
    tenant_id text NOT NULL,
    event_id bigint NOT NULL,
    previous_status text NOT NULL,
    next_status text NOT NULL,
    previous_version bigint NOT NULL,
    next_version bigint NOT NULL,
    actor_type text NOT NULL,
    actor_id text NOT NULL,
    reason_code text NOT NULL,
    reason_text_ref text,
    correlation_id text NOT NULL,
    trace_id text NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tenant_status_change_actor_id_check CHECK (((length(btrim(actor_id)) >= 1) AND (length(btrim(actor_id)) <= 256))),
    CONSTRAINT tenant_status_change_actor_type_check CHECK (((length(btrim(actor_type)) >= 1) AND (length(btrim(actor_type)) <= 64))),
    CONSTRAINT tenant_status_change_check CHECK ((next_version = (previous_version + 1))),
    CONSTRAINT tenant_status_change_check1 CHECK ((((((previous_status = 'active'::text) AND (next_status = 'suspended'::text)) OR ((previous_status = 'active'::text) AND (next_status = 'disabled'::text))) OR ((previous_status = 'suspended'::text) AND (next_status = 'active'::text))) OR ((previous_status = 'suspended'::text) AND (next_status = 'disabled'::text)))),
    CONSTRAINT tenant_status_change_correlation_id_check CHECK ((length(btrim(correlation_id)) > 0)),
    CONSTRAINT tenant_status_change_previous_version_check CHECK ((previous_version >= 1)),
    CONSTRAINT tenant_status_change_reason_code_check CHECK (((length(btrim(reason_code)) >= 1) AND (length(btrim(reason_code)) <= 128))),
    CONSTRAINT tenant_status_change_reason_text_ref_check CHECK (((reason_text_ref IS NULL) OR (length(btrim(reason_text_ref)) > 0))),
    CONSTRAINT tenant_status_change_trace_id_check CHECK ((length(btrim(trace_id)) > 0))
);


--
-- Name: tenant_status_change_event_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.tenant_status_change ALTER COLUMN event_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.tenant_status_change_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tool_attempt; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tool_attempt (
    tenant_id text NOT NULL,
    grant_id text NOT NULL,
    request_id text NOT NULL,
    tool_call_id text NOT NULL,
    state text NOT NULL,
    result_ref text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tool_attempt_state_check CHECK ((state = ANY (ARRAY['effect_unknown'::text, 'succeeded'::text, 'failed'::text])))
);


--
-- Name: tool_result_payload; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tool_result_payload (
    tenant_id text NOT NULL,
    grant_id text NOT NULL,
    request_id text NOT NULL,
    result_ref text NOT NULL,
    result_ciphertext bytea NOT NULL,
    result_nonce bytea NOT NULL,
    content_digest text NOT NULL,
    key_version bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT tool_result_payload_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT tool_result_payload_key_version_check CHECK ((key_version >= 1))
);


--
-- Name: usage_ledger; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_ledger (
    tenant_id text NOT NULL,
    request_id text NOT NULL,
    stage text NOT NULL,
    usage_kind text NOT NULL,
    reservation_id text NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    cached_input_tokens bigint NOT NULL,
    cost_micros bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT usage_ledger_check CHECK (((cached_input_tokens >= 0) AND (cached_input_tokens <= input_tokens))),
    CONSTRAINT usage_ledger_cost_micros_check CHECK ((cost_micros >= 0)),
    CONSTRAINT usage_ledger_input_tokens_check CHECK ((input_tokens >= 0)),
    CONSTRAINT usage_ledger_output_tokens_check CHECK ((output_tokens >= 0))
);


--
-- Name: webui_message; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.webui_message (
    tenant_id text NOT NULL,
    config_version bigint NOT NULL,
    channel_binding_id text NOT NULL,
    external_account_id text NOT NULL,
    external_user_id text NOT NULL,
    external_chat_id text NOT NULL,
    request_id text NOT NULL,
    client_request_id text NOT NULL,
    provider_message_id text NOT NULL,
    content_ref text NOT NULL,
    content_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT webui_message_client_request_id_check CHECK (((length(btrim(client_request_id)) >= 1) AND (length(btrim(client_request_id)) <= 256))),
    CONSTRAINT webui_message_config_version_check CHECK ((config_version >= 1)),
    CONSTRAINT webui_message_content_digest_check CHECK ((content_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT webui_message_external_account_id_check CHECK (((length(btrim(external_account_id)) >= 1) AND (length(btrim(external_account_id)) <= 512))),
    CONSTRAINT webui_message_external_chat_id_check CHECK (((length(btrim(external_chat_id)) >= 1) AND (length(btrim(external_chat_id)) <= 512))),
    CONSTRAINT webui_message_external_user_id_check CHECK (((length(btrim(external_user_id)) >= 1) AND (length(btrim(external_user_id)) <= 512))),
    CONSTRAINT webui_message_provider_message_id_check CHECK (((length(btrim(provider_message_id)) >= 1) AND (length(btrim(provider_message_id)) <= 256)))
);


--
-- Name: agent_app_change agent_app_change_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_change
    ADD CONSTRAINT agent_app_change_pkey PRIMARY KEY (tenant_id, agent_app_id, event_id);


--
-- Name: agent_app_change agent_app_change_tenant_id_agent_app_id_next_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_change
    ADD CONSTRAINT agent_app_change_tenant_id_agent_app_id_next_version_key UNIQUE (tenant_id, agent_app_id, next_version);


--
-- Name: agent_app agent_app_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app
    ADD CONSTRAINT agent_app_pkey PRIMARY KEY (tenant_id, agent_app_id);


--
-- Name: agent_app_revision_child agent_app_revision_child_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_child
    ADD CONSTRAINT agent_app_revision_child_pkey PRIMARY KEY (tenant_id, agent_app_id, revision, node_key);


--
-- Name: agent_app_revision_child agent_app_revision_child_tenant_id_agent_app_id_revision_or_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_child
    ADD CONSTRAINT agent_app_revision_child_tenant_id_agent_app_id_revision_or_key UNIQUE (tenant_id, agent_app_id, revision, ordinal);


--
-- Name: agent_app_revision_knowledge agent_app_revision_knowledge_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_knowledge
    ADD CONSTRAINT agent_app_revision_knowledge_pkey PRIMARY KEY (tenant_id, agent_app_id, revision, knowledge_id);


--
-- Name: agent_app_revision agent_app_revision_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision
    ADD CONSTRAINT agent_app_revision_pkey PRIMARY KEY (tenant_id, agent_app_id, revision);


--
-- Name: agent_app_revision_skill agent_app_revision_skill_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_skill
    ADD CONSTRAINT agent_app_revision_skill_pkey PRIMARY KEY (tenant_id, agent_app_id, revision, skill_id);


--
-- Name: agent_app_revision_tool agent_app_revision_tool_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_tool
    ADD CONSTRAINT agent_app_revision_tool_pkey PRIMARY KEY (tenant_id, agent_app_id, revision, tool_id);


--
-- Name: agent_app agent_app_tenant_id_agent_app_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app
    ADD CONSTRAINT agent_app_tenant_id_agent_app_key_key UNIQUE (tenant_id, agent_app_key);


--
-- Name: artifact_object_upload artifact_object_upload_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_object_upload
    ADD CONSTRAINT artifact_object_upload_pkey PRIMARY KEY (tenant_id, object_key);


--
-- Name: artifact_object_upload artifact_object_upload_tenant_id_artifact_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_object_upload
    ADD CONSTRAINT artifact_object_upload_tenant_id_artifact_id_key UNIQUE (tenant_id, artifact_id);


--
-- Name: artifact_reference artifact_reference_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_reference
    ADD CONSTRAINT artifact_reference_pkey PRIMARY KEY (tenant_id, artifact_id, reference_kind, reference_id);


--
-- Name: audit_event audit_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_event
    ADD CONSTRAINT audit_event_pkey PRIMARY KEY (tenant_id, audit_id);


--
-- Name: backend_binding backend_binding_migration_coordinate_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_binding
    ADD CONSTRAINT backend_binding_migration_coordinate_key UNIQUE (tenant_id, config_version, domain, backend_profile_id, backend_version);


--
-- Name: backend_binding backend_binding_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_binding
    ADD CONSTRAINT backend_binding_pkey PRIMARY KEY (tenant_id, config_version, domain);


--
-- Name: backend_migration_batch backend_migration_batch_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration_batch
    ADD CONSTRAINT backend_migration_batch_pkey PRIMARY KEY (tenant_id, migration_id, batch_seq);


--
-- Name: backend_migration_batch backend_migration_batch_tenant_id_migration_id_batch_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration_batch
    ADD CONSTRAINT backend_migration_batch_tenant_id_migration_id_batch_id_key UNIQUE (tenant_id, migration_id, batch_id);


--
-- Name: backend_migration_config_switch backend_migration_config_swit_tenant_id_migration_id_direct_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration_config_switch
    ADD CONSTRAINT backend_migration_config_swit_tenant_id_migration_id_direct_key UNIQUE (tenant_id, migration_id, direction);


--
-- Name: backend_migration_config_switch backend_migration_config_switch_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration_config_switch
    ADD CONSTRAINT backend_migration_config_switch_pkey PRIMARY KEY (tenant_id, migration_id, switch_id);


--
-- Name: backend_migration backend_migration_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration
    ADD CONSTRAINT backend_migration_pkey PRIMARY KEY (tenant_id, migration_id);


--
-- Name: backend_migration backend_migration_tenant_id_domain_epoch_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration
    ADD CONSTRAINT backend_migration_tenant_id_domain_epoch_key UNIQUE (tenant_id, domain, epoch);


--
-- Name: backend_profile backend_profile_key_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_profile
    ADD CONSTRAINT backend_profile_key_unique UNIQUE (tenant_id, profile_key);


--
-- Name: backend_profile backend_profile_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_profile
    ADD CONSTRAINT backend_profile_pkey PRIMARY KEY (tenant_id, backend_profile_id);


--
-- Name: backend_profile_revision backend_profile_revision_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_profile_revision
    ADD CONSTRAINT backend_profile_revision_pkey PRIMARY KEY (tenant_id, backend_profile_id, profile_version);


--
-- Name: budget_reservation budget_reservation_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.budget_reservation
    ADD CONSTRAINT budget_reservation_pkey PRIMARY KEY (tenant_id, reservation_id);


--
-- Name: budget_reservation budget_reservation_tenant_id_request_id_resource_id_attempt_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.budget_reservation
    ADD CONSTRAINT budget_reservation_tenant_id_request_id_resource_id_attempt_key UNIQUE (tenant_id, request_id, resource_id, attempt_class);


--
-- Name: business_audit_purge_batch business_audit_purge_batch_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.business_audit_purge_batch
    ADD CONSTRAINT business_audit_purge_batch_pkey PRIMARY KEY (tenant_id, batch_id);


--
-- Name: business_audit_purge_certificate business_audit_purge_certificate_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.business_audit_purge_certificate
    ADD CONSTRAINT business_audit_purge_certificate_pkey PRIMARY KEY (tenant_id, batch_id);


--
-- Name: channel_binding_locator channel_binding_locator_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_binding_locator
    ADD CONSTRAINT channel_binding_locator_pkey PRIMARY KEY (opaque_binding_id);


--
-- Name: channel_binding channel_binding_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_binding
    ADD CONSTRAINT channel_binding_pkey PRIMARY KEY (tenant_id, config_version, binding_id);


--
-- Name: channel_binding channel_binding_tenant_id_config_version_channel_external_a_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_binding
    ADD CONSTRAINT channel_binding_tenant_id_config_version_channel_external_a_key UNIQUE (tenant_id, config_version, channel, external_account_id);


--
-- Name: channel_ingress_candidate channel_ingress_candidate_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ingress_candidate
    ADD CONSTRAINT channel_ingress_candidate_pkey PRIMARY KEY (candidate_token_digest);


--
-- Name: channel_public_route channel_public_route_opaque_binding_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_public_route
    ADD CONSTRAINT channel_public_route_opaque_binding_id_key UNIQUE (opaque_binding_id);


--
-- Name: channel_public_route channel_public_route_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_public_route
    ADD CONSTRAINT channel_public_route_pkey PRIMARY KEY (channel, route_key_digest);


--
-- Name: config_snapshot config_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.config_snapshot
    ADD CONSTRAINT config_snapshot_pkey PRIMARY KEY (tenant_id, config_version);


--
-- Name: config_snapshot config_snapshot_tenant_id_content_digest_config_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.config_snapshot
    ADD CONSTRAINT config_snapshot_tenant_id_content_digest_config_version_key UNIQUE (tenant_id, content_digest, config_version);


--
-- Name: confirmation_grant confirmation_grant_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.confirmation_grant
    ADD CONSTRAINT confirmation_grant_pkey PRIMARY KEY (tenant_id, grant_id);


--
-- Name: confirmation_grant confirmation_grant_tenant_id_confirmation_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.confirmation_grant
    ADD CONSTRAINT confirmation_grant_tenant_id_confirmation_id_key UNIQUE (tenant_id, confirmation_id);


--
-- Name: confirmation confirmation_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.confirmation
    ADD CONSTRAINT confirmation_pkey PRIMARY KEY (tenant_id, confirmation_id);


--
-- Name: confirmation confirmation_tenant_id_request_id_tool_call_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.confirmation
    ADD CONSTRAINT confirmation_tenant_id_request_id_tool_call_id_key UNIQUE (tenant_id, request_id, tool_call_id);


--
-- Name: delivery_ledger delivery_ledger_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delivery_ledger
    ADD CONSTRAINT delivery_ledger_pkey PRIMARY KEY (tenant_id, delivery_key, segment_no);


--
-- Name: execution_cancel_intent execution_cancel_intent_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_cancel_intent
    ADD CONSTRAINT execution_cancel_intent_pkey PRIMARY KEY (tenant_id, request_id, cancel_version);


--
-- Name: execution_record execution_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_pkey PRIMARY KEY (tenant_id, request_id);


--
-- Name: execution_record execution_record_tenant_id_agent_app_id_session_id_input_se_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_tenant_id_agent_app_id_session_id_input_se_key UNIQUE (tenant_id, agent_app_id, session_id, input_seq);


--
-- Name: governance_decision governance_decision_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.governance_decision
    ADD CONSTRAINT governance_decision_pkey PRIMARY KEY (tenant_id, decision_id);


--
-- Name: inbound_payload inbound_payload_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inbound_payload
    ADD CONSTRAINT inbound_payload_pkey PRIMARY KEY (tenant_id, request_id);


--
-- Name: inbound_payload inbound_payload_tenant_id_payload_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inbound_payload
    ADD CONSTRAINT inbound_payload_tenant_id_payload_ref_key UNIQUE (tenant_id, payload_ref);


--
-- Name: inbox inbox_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_pkey PRIMARY KEY (tenant_id, channel, external_account_id, external_message_id);


--
-- Name: inbox inbox_tenant_id_request_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_tenant_id_request_id_key UNIQUE (tenant_id, request_id);


--
-- Name: interaction_payload interaction_payload_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.interaction_payload
    ADD CONSTRAINT interaction_payload_pkey PRIMARY KEY (tenant_id, request_id, content_ref);


--
-- Name: knowledge_chunk knowledge_chunk_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_chunk
    ADD CONSTRAINT knowledge_chunk_pkey PRIMARY KEY (tenant_id, knowledge_id, knowledge_version, chunk_id);


--
-- Name: knowledge_manifest knowledge_manifest_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_manifest
    ADD CONSTRAINT knowledge_manifest_pkey PRIMARY KEY (tenant_id, knowledge_id, version);


--
-- Name: knowledge_migration_mutation knowledge_migration_mutation_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_migration_mutation
    ADD CONSTRAINT knowledge_migration_mutation_pkey PRIMARY KEY (tenant_id, migration_id, knowledge_id, knowledge_version, chunk_id, mutation_id);


--
-- Name: knowledge_migration_mutation knowledge_migration_mutation_tenant_id_migration_id_mutatio_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_migration_mutation
    ADD CONSTRAINT knowledge_migration_mutation_tenant_id_migration_id_mutatio_key UNIQUE (tenant_id, migration_id, mutation_id);


--
-- Name: knowledge_probe knowledge_probe_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_probe
    ADD CONSTRAINT knowledge_probe_pkey PRIMARY KEY (tenant_id, knowledge_id, knowledge_version, probe_id);


--
-- Name: media_artifact media_artifact_object_key_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_artifact
    ADD CONSTRAINT media_artifact_object_key_unique UNIQUE (tenant_id, object_key);


--
-- Name: media_artifact media_artifact_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_artifact
    ADD CONSTRAINT media_artifact_pkey PRIMARY KEY (tenant_id, artifact_id);


--
-- Name: media_artifact media_artifact_tenant_id_artifact_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_artifact
    ADD CONSTRAINT media_artifact_tenant_id_artifact_ref_key UNIQUE (tenant_id, artifact_ref);


--
-- Name: model_profile model_profile_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_profile
    ADD CONSTRAINT model_profile_pkey PRIMARY KEY (tenant_id, model_profile_id);


--
-- Name: model_profile_revision model_profile_revision_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_profile_revision
    ADD CONSTRAINT model_profile_revision_pkey PRIMARY KEY (tenant_id, model_profile_id, profile_version);


--
-- Name: model_profile model_profile_tenant_id_profile_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_profile
    ADD CONSTRAINT model_profile_tenant_id_profile_key_key UNIQUE (tenant_id, profile_key);


--
-- Name: outbox outbox_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox
    ADD CONSTRAINT outbox_pkey PRIMARY KEY (tenant_id, outbox_id);


--
-- Name: outbox outbox_tenant_id_kind_idempotency_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outbox
    ADD CONSTRAINT outbox_tenant_id_kind_idempotency_key_key UNIQUE (tenant_id, kind, idempotency_key);


--
-- Name: policy_snapshot policy_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.policy_snapshot
    ADD CONSTRAINT policy_snapshot_pkey PRIMARY KEY (tenant_id, policy_version);


--
-- Name: prepared_payload prepared_payload_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prepared_payload
    ADD CONSTRAINT prepared_payload_pkey PRIMARY KEY (tenant_id, request_id);


--
-- Name: prepared_payload prepared_payload_tenant_id_payload_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prepared_payload
    ADD CONSTRAINT prepared_payload_tenant_id_payload_ref_key UNIQUE (tenant_id, payload_ref);


--
-- Name: preprocess_job preprocess_job_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_pkey PRIMARY KEY (tenant_id, job_id);


--
-- Name: preprocess_job preprocess_job_tenant_id_request_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_tenant_id_request_id_key UNIQUE (tenant_id, request_id);


--
-- Name: pricing_snapshot pricing_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pricing_snapshot
    ADD CONSTRAINT pricing_snapshot_pkey PRIMARY KEY (tenant_id, pricing_version);


--
-- Name: result_payload result_payload_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.result_payload
    ADD CONSTRAINT result_payload_pkey PRIMARY KEY (tenant_id, request_id);


--
-- Name: result_payload result_payload_tenant_id_result_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.result_payload
    ADD CONSTRAINT result_payload_tenant_id_result_ref_key UNIQUE (tenant_id, result_ref);


--
-- Name: session_commit session_commit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_commit
    ADD CONSTRAINT session_commit_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id, commit_id);


--
-- Name: session_commit session_commit_tenant_id_agent_app_id_session_id_input_seq__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_commit
    ADD CONSTRAINT session_commit_tenant_id_agent_app_id_session_id_input_seq__key UNIQUE (tenant_id, agent_app_id, session_id, input_seq, stage);


--
-- Name: session_event session_event_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_event
    ADD CONSTRAINT session_event_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id, session_seq);


--
-- Name: session_event session_event_tenant_id_agent_app_id_session_id_event_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_event
    ADD CONSTRAINT session_event_tenant_id_agent_app_id_session_id_event_id_key UNIQUE (tenant_id, agent_app_id, session_id, event_id);


--
-- Name: session_event session_event_tenant_id_request_id_event_seq_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_event
    ADD CONSTRAINT session_event_tenant_id_request_id_event_seq_key UNIQUE (tenant_id, request_id, event_seq);


--
-- Name: session_head session_head_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_head
    ADD CONSTRAINT session_head_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id);


--
-- Name: session_migration_mutation session_migration_mutation_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_migration_mutation
    ADD CONSTRAINT session_migration_mutation_pkey PRIMARY KEY (tenant_id, migration_id, agent_app_id, session_id, mutation_id);


--
-- Name: session_summary_content session_summary_content_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_summary_content
    ADD CONSTRAINT session_summary_content_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id, summary_id);


--
-- Name: session_summary_content session_summary_content_tenant_id_agent_app_id_session_id_c_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_summary_content
    ADD CONSTRAINT session_summary_content_tenant_id_agent_app_id_session_id_c_key UNIQUE (tenant_id, agent_app_id, session_id, content_ref);


--
-- Name: session_summary session_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_summary
    ADD CONSTRAINT session_summary_pkey PRIMARY KEY (tenant_id, agent_app_id, session_id, summary_id);


--
-- Name: session_summary session_summary_tenant_id_agent_app_id_session_id_base_sess_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_summary
    ADD CONSTRAINT session_summary_tenant_id_agent_app_id_session_id_base_sess_key UNIQUE (tenant_id, agent_app_id, session_id, base_session_seq);


--
-- Name: skill_catalog skill_catalog_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_catalog
    ADD CONSTRAINT skill_catalog_pkey PRIMARY KEY (tenant_id, skill_id, skill_version);


--
-- Name: skill_catalog skill_catalog_tenant_id_relative_path_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_catalog
    ADD CONSTRAINT skill_catalog_tenant_id_relative_path_key UNIQUE (tenant_id, relative_path);


--
-- Name: tenant tenant_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant
    ADD CONSTRAINT tenant_pkey PRIMARY KEY (tenant_id);


--
-- Name: tenant_status_change tenant_status_change_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_status_change
    ADD CONSTRAINT tenant_status_change_pkey PRIMARY KEY (tenant_id, event_id);


--
-- Name: tenant_status_change tenant_status_change_tenant_id_next_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_status_change
    ADD CONSTRAINT tenant_status_change_tenant_id_next_version_key UNIQUE (tenant_id, next_version);


--
-- Name: tenant tenant_tenant_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant
    ADD CONSTRAINT tenant_tenant_key_key UNIQUE (tenant_key);


--
-- Name: tool_attempt tool_attempt_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tool_attempt
    ADD CONSTRAINT tool_attempt_pkey PRIMARY KEY (tenant_id, grant_id);


--
-- Name: tool_attempt tool_attempt_tenant_id_request_id_tool_call_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tool_attempt
    ADD CONSTRAINT tool_attempt_tenant_id_request_id_tool_call_id_key UNIQUE (tenant_id, request_id, tool_call_id);


--
-- Name: tool_result_payload tool_result_payload_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tool_result_payload
    ADD CONSTRAINT tool_result_payload_pkey PRIMARY KEY (tenant_id, grant_id);


--
-- Name: tool_result_payload tool_result_payload_tenant_id_result_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tool_result_payload
    ADD CONSTRAINT tool_result_payload_tenant_id_result_ref_key UNIQUE (tenant_id, result_ref);


--
-- Name: usage_ledger usage_ledger_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_ledger
    ADD CONSTRAINT usage_ledger_pkey PRIMARY KEY (tenant_id, request_id, stage, usage_kind);


--
-- Name: webui_message webui_message_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webui_message
    ADD CONSTRAINT webui_message_pkey PRIMARY KEY (tenant_id, client_request_id);


--
-- Name: webui_message webui_message_tenant_id_provider_message_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webui_message
    ADD CONSTRAINT webui_message_tenant_id_provider_message_id_key UNIQUE (tenant_id, provider_message_id);


--
-- Name: artifact_object_upload_cleanup_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX artifact_object_upload_cleanup_idx ON public.artifact_object_upload USING btree (state, protect_until, claim_until, created_at);


--
-- Name: artifact_reference_retention_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX artifact_reference_retention_idx ON public.artifact_reference USING btree (retain_until, tenant_id, artifact_id);


--
-- Name: audit_event_request_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX audit_event_request_idx ON public.audit_event USING btree (tenant_id, request_id, occurred_at) WHERE (request_id <> ''::text);


--
-- Name: audit_event_tenant_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX audit_event_tenant_time_idx ON public.audit_event USING btree (tenant_id, occurred_at DESC, audit_id);


--
-- Name: backend_migration_active_domain_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX backend_migration_active_domain_idx ON public.backend_migration USING btree (tenant_id, domain) WHERE (state <> 'cleanup'::text);


--
-- Name: budget_reservation_period_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX budget_reservation_period_idx ON public.budget_reservation USING btree (tenant_id, budget_period, state);


--
-- Name: channel_ingress_candidate_expiry_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX channel_ingress_candidate_expiry_idx ON public.channel_ingress_candidate USING btree (state, expires_at);


--
-- Name: delivery_ledger_claim_expiry_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX delivery_ledger_claim_expiry_idx ON public.delivery_ledger USING btree (claim_until) WHERE (state = 'sending'::text);


--
-- Name: delivery_ledger_retry_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX delivery_ledger_retry_idx ON public.delivery_ledger USING btree (state, not_before, updated_at) WHERE (state = ANY (ARRAY['pending'::text, 'retry_wait'::text, 'ambiguous'::text]));


--
-- Name: execution_park_ready_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX execution_park_ready_idx ON public.execution_record USING btree (tenant_id, agent_app_id, session_id, input_seq, not_before) WHERE (outcome = 'pending'::text);


--
-- Name: knowledge_manifest_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX knowledge_manifest_state_idx ON public.knowledge_manifest USING btree (tenant_id, state);


--
-- Name: knowledge_migration_mutation_direction_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX knowledge_migration_mutation_direction_idx ON public.knowledge_migration_mutation USING btree (tenant_id, migration_id, direction, state);


--
-- Name: knowledge_migration_mutation_repair_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX knowledge_migration_mutation_repair_idx ON public.knowledge_migration_mutation USING btree (tenant_id, migration_id, not_before, created_at) WHERE (state <> 'applied'::text);


--
-- Name: media_artifact_object_lifecycle_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX media_artifact_object_lifecycle_idx ON public.media_artifact USING btree (tenant_id, created_at, object_key) WHERE (storage_kind = 'object'::text);


--
-- Name: media_artifact_request_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX media_artifact_request_idx ON public.media_artifact USING btree (tenant_id, request_id, ordinal);


--
-- Name: media_artifact_retention_claim_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX media_artifact_retention_claim_idx ON public.media_artifact USING btree (lifecycle_state, claim_until, created_at, tenant_id, artifact_id);


--
-- Name: outbox_claim_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX outbox_claim_idx ON public.outbox USING btree (kind, state, next_attempt_at, created_at);


--
-- Name: preprocess_job_claim_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX preprocess_job_claim_idx ON public.preprocess_job USING btree (state, not_before, lease_until, created_at);


--
-- Name: preprocess_job_ready_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX preprocess_job_ready_idx ON public.preprocess_job USING btree (created_at) WHERE ((state = 'ready'::text) AND (dispatched_at IS NULL));


--
-- Name: session_commit_terminal_input_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX session_commit_terminal_input_idx ON public.session_commit USING btree (tenant_id, agent_app_id, session_id, input_seq) WHERE (outcome = ANY (ARRAY['succeeded'::text, 'denied'::text, 'failed'::text, 'cancelled'::text, 'confirmation_denied'::text, 'confirmation_timeout'::text]));


--
-- Name: session_migration_mutation_direction_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX session_migration_mutation_direction_idx ON public.session_migration_mutation USING btree (tenant_id, migration_id, direction, state);


--
-- Name: session_migration_mutation_repair_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX session_migration_mutation_repair_idx ON public.session_migration_mutation USING btree (tenant_id, migration_id, not_before, created_at) WHERE (state <> 'applied'::text);


--
-- Name: session_summary_content_compact_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX session_summary_content_compact_idx ON public.session_summary_content USING btree (state, superseded_at, tenant_id, agent_app_id, session_id, summary_id) WHERE ((state = ANY (ARRAY['superseded'::text, 'delete_claimed'::text])) AND (NOT frozen));


--
-- Name: webui_message_mailbox_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX webui_message_mailbox_idx ON public.webui_message USING btree (tenant_id, channel_binding_id, external_account_id, external_user_id, external_chat_id, created_at, provider_message_id);


--
-- Name: agent_app agent_app_current_revision_published; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_current_revision_published BEFORE UPDATE ON public.agent_app FOR EACH ROW EXECUTE FUNCTION public.guard_agent_app_current_revision();


--
-- Name: agent_app agent_app_identity_version_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_identity_version_guard BEFORE UPDATE ON public.agent_app FOR EACH ROW EXECUTE FUNCTION public.reject_agent_app_identity_change();


--
-- Name: agent_app_revision_child agent_app_revision_child_draft_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_revision_child_draft_only BEFORE INSERT OR DELETE OR UPDATE ON public.agent_app_revision_child FOR EACH ROW EXECUTE FUNCTION public.guard_revision_child_write();


--
-- Name: agent_app_revision_child agent_app_revision_child_target_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_revision_child_target_guard BEFORE INSERT OR UPDATE ON public.agent_app_revision_child FOR EACH ROW EXECUTE FUNCTION public.guard_agent_app_revision_child();


--
-- Name: agent_app_revision_knowledge agent_app_revision_knowledge_draft_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_revision_knowledge_draft_only BEFORE INSERT OR DELETE OR UPDATE ON public.agent_app_revision_knowledge FOR EACH ROW EXECUTE FUNCTION public.guard_revision_child_write();


--
-- Name: agent_app_revision agent_app_revision_knowledge_published_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_revision_knowledge_published_guard BEFORE UPDATE OF state ON public.agent_app_revision FOR EACH ROW EXECUTE FUNCTION public.guard_agent_app_revision_knowledge_published();


--
-- Name: agent_app_revision agent_app_revision_published_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_revision_published_immutable BEFORE DELETE OR UPDATE ON public.agent_app_revision FOR EACH ROW EXECUTE FUNCTION public.reject_published_revision_change();


--
-- Name: agent_app_revision_skill agent_app_revision_skill_draft_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_revision_skill_draft_only BEFORE INSERT OR DELETE OR UPDATE ON public.agent_app_revision_skill FOR EACH ROW EXECUTE FUNCTION public.guard_revision_child_write();


--
-- Name: agent_app_revision_tool agent_app_revision_tool_draft_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_app_revision_tool_draft_only BEFORE INSERT OR DELETE OR UPDATE ON public.agent_app_revision_tool FOR EACH ROW EXECUTE FUNCTION public.guard_revision_child_write();


--
-- Name: agent_app_revision agent_model_profile_publish_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER agent_model_profile_publish_guard BEFORE UPDATE OF state ON public.agent_app_revision FOR EACH ROW EXECUTE FUNCTION public.guard_agent_model_profile_publish();


--
-- Name: audit_event audit_event_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_event_immutable BEFORE DELETE OR UPDATE ON public.audit_event FOR EACH ROW EXECUTE FUNCTION public.reject_audit_event_change();


--
-- Name: backend_migration_batch backend_migration_batch_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER backend_migration_batch_immutable BEFORE DELETE OR UPDATE ON public.backend_migration_batch FOR EACH ROW EXECUTE FUNCTION public.reject_backend_migration_batch_change();


--
-- Name: backend_migration_config_switch backend_migration_config_switch_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER backend_migration_config_switch_immutable BEFORE DELETE OR UPDATE ON public.backend_migration_config_switch FOR EACH ROW EXECUTE FUNCTION public.reject_backend_migration_config_switch_change();


--
-- Name: backend_migration backend_migration_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER backend_migration_guard BEFORE UPDATE ON public.backend_migration FOR EACH ROW EXECUTE FUNCTION public.guard_backend_migration_update();


--
-- Name: backend_migration backend_migration_knowledge_repair_gate; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER backend_migration_knowledge_repair_gate BEFORE UPDATE ON public.backend_migration FOR EACH ROW EXECUTE FUNCTION public.guard_knowledge_migration_authority_update();


--
-- Name: backend_migration backend_migration_session_repair_gate; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER backend_migration_session_repair_gate BEFORE UPDATE ON public.backend_migration FOR EACH ROW EXECUTE FUNCTION public.guard_session_migration_authority_update();


--
-- Name: backend_profile backend_profile_identity_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER backend_profile_identity_guard BEFORE UPDATE ON public.backend_profile FOR EACH ROW EXECUTE FUNCTION public.guard_profile_identity();


--
-- Name: backend_profile_revision backend_profile_revision_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER backend_profile_revision_immutable BEFORE DELETE OR UPDATE ON public.backend_profile_revision FOR EACH ROW EXECUTE FUNCTION public.guard_profile_revision_immutable();


--
-- Name: business_audit_purge_batch business_audit_purge_batch_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER business_audit_purge_batch_guard BEFORE DELETE OR UPDATE ON public.business_audit_purge_batch FOR EACH ROW EXECUTE FUNCTION public.guard_business_audit_purge_batch_update();


--
-- Name: business_audit_purge_certificate business_audit_purge_certificate_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER business_audit_purge_certificate_immutable BEFORE DELETE OR UPDATE ON public.business_audit_purge_certificate FOR EACH ROW EXECUTE FUNCTION public.reject_business_audit_purge_certificate_change();


--
-- Name: channel_binding channel_binding_populate_send_secret; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER channel_binding_populate_send_secret BEFORE INSERT ON public.channel_binding FOR EACH ROW EXECUTE FUNCTION public.populate_channel_send_secret();


--
-- Name: config_snapshot config_snapshot_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER config_snapshot_immutable BEFORE DELETE OR UPDATE ON public.config_snapshot FOR EACH ROW EXECUTE FUNCTION public.reject_published_config_change();


--
-- Name: config_snapshot config_snapshot_policy_default; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER config_snapshot_policy_default BEFORE INSERT ON public.config_snapshot FOR EACH ROW EXECUTE FUNCTION public.ensure_config_policy_snapshot();


--
-- Name: execution_record execution_cancel_intent_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER execution_cancel_intent_guard BEFORE UPDATE OF outcome ON public.execution_record FOR EACH ROW EXECUTE FUNCTION public.guard_execution_cancel_intent();


--
-- Name: execution_record execution_requires_preprocess; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER execution_requires_preprocess BEFORE INSERT ON public.execution_record FOR EACH ROW EXECUTE FUNCTION public.reject_unpreprocessed_execution();


--
-- Name: knowledge_manifest knowledge_manifest_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER knowledge_manifest_guard BEFORE UPDATE ON public.knowledge_manifest FOR EACH ROW EXECUTE FUNCTION public.guard_knowledge_manifest_update();


--
-- Name: knowledge_migration_mutation knowledge_migration_mutation_direction_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER knowledge_migration_mutation_direction_guard BEFORE UPDATE ON public.knowledge_migration_mutation FOR EACH ROW EXECUTE FUNCTION public.guard_knowledge_migration_direction_update();


--
-- Name: knowledge_migration_mutation knowledge_migration_mutation_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER knowledge_migration_mutation_guard BEFORE UPDATE ON public.knowledge_migration_mutation FOR EACH ROW EXECUTE FUNCTION public.guard_knowledge_migration_mutation_update();


--
-- Name: model_profile model_profile_identity_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER model_profile_identity_guard BEFORE UPDATE ON public.model_profile FOR EACH ROW EXECUTE FUNCTION public.guard_profile_identity();


--
-- Name: model_profile_revision model_profile_revision_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER model_profile_revision_immutable BEFORE DELETE OR UPDATE ON public.model_profile_revision FOR EACH ROW EXECUTE FUNCTION public.guard_profile_revision_immutable();


--
-- Name: outbox outbox_idempotency_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER outbox_idempotency_guard BEFORE INSERT ON public.outbox FOR EACH ROW EXECUTE FUNCTION public.guard_outbox_idempotency();


--
-- Name: policy_snapshot policy_snapshot_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER policy_snapshot_immutable BEFORE DELETE OR UPDATE ON public.policy_snapshot FOR EACH ROW EXECUTE FUNCTION public.reject_governance_snapshot_change();


--
-- Name: pricing_snapshot pricing_snapshot_immutable; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER pricing_snapshot_immutable BEFORE DELETE OR UPDATE ON public.pricing_snapshot FOR EACH ROW EXECUTE FUNCTION public.reject_governance_snapshot_change();


--
-- Name: session_commit session_commit_capture_migration; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER session_commit_capture_migration AFTER INSERT ON public.session_commit FOR EACH ROW EXECUTE FUNCTION public.capture_session_migration_mutation();


--
-- Name: session_event session_event_unpack_payload; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER session_event_unpack_payload BEFORE INSERT ON public.session_event FOR EACH ROW EXECUTE FUNCTION public.unpack_session_event_payload();


--
-- Name: session_head session_head_wakeup_next_parked; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER session_head_wakeup_next_parked AFTER UPDATE OF next_input_seq ON public.session_head FOR EACH ROW EXECUTE FUNCTION public.enqueue_next_parked_wakeup();


--
-- Name: session_migration_mutation session_migration_mutation_direction_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER session_migration_mutation_direction_guard BEFORE UPDATE ON public.session_migration_mutation FOR EACH ROW EXECUTE FUNCTION public.guard_session_migration_direction_update();


--
-- Name: session_migration_mutation session_migration_mutation_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER session_migration_mutation_guard BEFORE UPDATE ON public.session_migration_mutation FOR EACH ROW EXECUTE FUNCTION public.guard_session_migration_mutation_update();


--
-- Name: session_summary_content session_summary_content_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER session_summary_content_guard BEFORE INSERT OR DELETE OR UPDATE ON public.session_summary_content FOR EACH ROW EXECUTE FUNCTION public.guard_session_summary_content();


--
-- Name: skill_catalog skill_catalog_guard; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER skill_catalog_guard BEFORE INSERT OR DELETE OR UPDATE ON public.skill_catalog FOR EACH ROW EXECUTE FUNCTION public.guard_skill_catalog();


--
-- Name: tenant tenant_maintain_update_trg; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER tenant_maintain_update_trg BEFORE UPDATE ON public.tenant FOR EACH ROW EXECUTE FUNCTION public.maintain_tenant_update();


--
-- Name: agent_app_change agent_app_change_tenant_id_agent_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_change
    ADD CONSTRAINT agent_app_change_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);


--
-- Name: agent_app agent_app_current_revision_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app
    ADD CONSTRAINT agent_app_current_revision_fk FOREIGN KEY (tenant_id, agent_app_id, current_revision) REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision) DEFERRABLE;


--
-- Name: agent_app_revision_child agent_app_revision_child_tenant_id_agent_app_id_revision_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_child
    ADD CONSTRAINT agent_app_revision_child_tenant_id_agent_app_id_revision_fkey FOREIGN KEY (tenant_id, agent_app_id, revision) REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE;


--
-- Name: agent_app_revision_child agent_app_revision_child_tenant_id_child_agent_app_id_chil_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_child
    ADD CONSTRAINT agent_app_revision_child_tenant_id_child_agent_app_id_chil_fkey FOREIGN KEY (tenant_id, child_agent_app_id, child_revision) REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision);


--
-- Name: agent_app_revision_knowledge agent_app_revision_knowledge_tenant_id_agent_app_id_revisi_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_knowledge
    ADD CONSTRAINT agent_app_revision_knowledge_tenant_id_agent_app_id_revisi_fkey FOREIGN KEY (tenant_id, agent_app_id, revision) REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE;


--
-- Name: agent_app_revision_skill agent_app_revision_skill_tenant_id_agent_app_id_revision_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_skill
    ADD CONSTRAINT agent_app_revision_skill_tenant_id_agent_app_id_revision_fkey FOREIGN KEY (tenant_id, agent_app_id, revision) REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE;


--
-- Name: agent_app_revision agent_app_revision_tenant_id_agent_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision
    ADD CONSTRAINT agent_app_revision_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);


--
-- Name: agent_app_revision_tool agent_app_revision_tool_tenant_id_agent_app_id_revision_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision_tool
    ADD CONSTRAINT agent_app_revision_tool_tenant_id_agent_app_id_revision_fkey FOREIGN KEY (tenant_id, agent_app_id, revision) REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE;


--
-- Name: agent_app agent_app_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app
    ADD CONSTRAINT agent_app_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: agent_app_revision agent_revision_model_profile_version_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agent_app_revision
    ADD CONSTRAINT agent_revision_model_profile_version_fk FOREIGN KEY (tenant_id, model_profile_id, model_profile_version) REFERENCES public.model_profile_revision(tenant_id, model_profile_id, profile_version);


--
-- Name: artifact_object_upload artifact_object_upload_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_object_upload
    ADD CONSTRAINT artifact_object_upload_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: artifact_object_upload artifact_object_upload_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_object_upload
    ADD CONSTRAINT artifact_object_upload_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id);


--
-- Name: artifact_reference artifact_reference_tenant_id_artifact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.artifact_reference
    ADD CONSTRAINT artifact_reference_tenant_id_artifact_id_fkey FOREIGN KEY (tenant_id, artifact_id) REFERENCES public.media_artifact(tenant_id, artifact_id) ON DELETE CASCADE;


--
-- Name: audit_event audit_event_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_event
    ADD CONSTRAINT audit_event_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: backend_binding backend_binding_profile_version_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_binding
    ADD CONSTRAINT backend_binding_profile_version_fk FOREIGN KEY (tenant_id, backend_profile_id, backend_version) REFERENCES public.backend_profile_revision(tenant_id, backend_profile_id, profile_version);


--
-- Name: backend_binding backend_binding_tenant_id_backend_profile_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_binding
    ADD CONSTRAINT backend_binding_tenant_id_backend_profile_id_fkey FOREIGN KEY (tenant_id, backend_profile_id) REFERENCES public.backend_profile(tenant_id, backend_profile_id);


--
-- Name: backend_binding backend_binding_tenant_id_config_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_binding
    ADD CONSTRAINT backend_binding_tenant_id_config_version_fkey FOREIGN KEY (tenant_id, config_version) REFERENCES public.config_snapshot(tenant_id, config_version);


--
-- Name: backend_migration_batch backend_migration_batch_tenant_id_migration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration_batch
    ADD CONSTRAINT backend_migration_batch_tenant_id_migration_id_fkey FOREIGN KEY (tenant_id, migration_id) REFERENCES public.backend_migration(tenant_id, migration_id);


--
-- Name: backend_migration_config_switch backend_migration_config_swit_tenant_id_active_config_vers_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration_config_switch
    ADD CONSTRAINT backend_migration_config_swit_tenant_id_active_config_vers_fkey FOREIGN KEY (tenant_id, active_config_version) REFERENCES public.config_snapshot(tenant_id, config_version);


--
-- Name: backend_migration_config_switch backend_migration_config_switch_tenant_id_migration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration_config_switch
    ADD CONSTRAINT backend_migration_config_switch_tenant_id_migration_id_fkey FOREIGN KEY (tenant_id, migration_id) REFERENCES public.backend_migration(tenant_id, migration_id);


--
-- Name: backend_migration backend_migration_tenant_id_cutover_config_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration
    ADD CONSTRAINT backend_migration_tenant_id_cutover_config_version_fkey FOREIGN KEY (tenant_id, cutover_config_version) REFERENCES public.config_snapshot(tenant_id, config_version);


--
-- Name: backend_migration backend_migration_tenant_id_source_config_version_domain_s_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration
    ADD CONSTRAINT backend_migration_tenant_id_source_config_version_domain_s_fkey FOREIGN KEY (tenant_id, source_config_version, domain, source_backend_profile_id, source_backend_version) REFERENCES public.backend_binding(tenant_id, config_version, domain, backend_profile_id, backend_version);


--
-- Name: backend_migration backend_migration_tenant_id_target_config_version_domain_t_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_migration
    ADD CONSTRAINT backend_migration_tenant_id_target_config_version_domain_t_fkey FOREIGN KEY (tenant_id, target_config_version, domain, target_backend_profile_id, target_backend_version) REFERENCES public.backend_binding(tenant_id, config_version, domain, backend_profile_id, backend_version);


--
-- Name: backend_profile backend_profile_current_version_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_profile
    ADD CONSTRAINT backend_profile_current_version_fk FOREIGN KEY (tenant_id, backend_profile_id, current_version) REFERENCES public.backend_profile_revision(tenant_id, backend_profile_id, profile_version) DEFERRABLE;


--
-- Name: backend_profile_revision backend_profile_revision_tenant_id_backend_profile_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_profile_revision
    ADD CONSTRAINT backend_profile_revision_tenant_id_backend_profile_id_fkey FOREIGN KEY (tenant_id, backend_profile_id) REFERENCES public.backend_profile(tenant_id, backend_profile_id);


--
-- Name: backend_profile backend_profile_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.backend_profile
    ADD CONSTRAINT backend_profile_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: budget_reservation budget_reservation_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.budget_reservation
    ADD CONSTRAINT budget_reservation_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: budget_reservation budget_reservation_tenant_id_policy_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.budget_reservation
    ADD CONSTRAINT budget_reservation_tenant_id_policy_version_fkey FOREIGN KEY (tenant_id, policy_version) REFERENCES public.policy_snapshot(tenant_id, policy_version);


--
-- Name: budget_reservation budget_reservation_tenant_id_pricing_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.budget_reservation
    ADD CONSTRAINT budget_reservation_tenant_id_pricing_version_fkey FOREIGN KEY (tenant_id, pricing_version) REFERENCES public.pricing_snapshot(tenant_id, pricing_version);


--
-- Name: business_audit_purge_certificate business_audit_purge_certificate_tenant_id_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.business_audit_purge_certificate
    ADD CONSTRAINT business_audit_purge_certificate_tenant_id_batch_id_fkey FOREIGN KEY (tenant_id, batch_id) REFERENCES public.business_audit_purge_batch(tenant_id, batch_id);


--
-- Name: channel_binding_locator channel_binding_locator_opaque_binding_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_binding_locator
    ADD CONSTRAINT channel_binding_locator_opaque_binding_id_fkey FOREIGN KEY (opaque_binding_id) REFERENCES public.channel_public_route(opaque_binding_id);


--
-- Name: channel_binding_locator channel_binding_locator_tenant_id_config_version_binding_i_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_binding_locator
    ADD CONSTRAINT channel_binding_locator_tenant_id_config_version_binding_i_fkey FOREIGN KEY (tenant_id, config_version, binding_id) REFERENCES public.channel_binding(tenant_id, config_version, binding_id);


--
-- Name: channel_binding channel_binding_tenant_id_agent_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_binding
    ADD CONSTRAINT channel_binding_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);


--
-- Name: channel_binding channel_binding_tenant_id_config_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_binding
    ADD CONSTRAINT channel_binding_tenant_id_config_version_fkey FOREIGN KEY (tenant_id, config_version) REFERENCES public.config_snapshot(tenant_id, config_version);


--
-- Name: channel_ingress_candidate channel_ingress_candidate_opaque_binding_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ingress_candidate
    ADD CONSTRAINT channel_ingress_candidate_opaque_binding_id_fkey FOREIGN KEY (opaque_binding_id) REFERENCES public.channel_public_route(opaque_binding_id);


--
-- Name: config_snapshot config_snapshot_policy_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.config_snapshot
    ADD CONSTRAINT config_snapshot_policy_fk FOREIGN KEY (tenant_id, policy_version) REFERENCES public.policy_snapshot(tenant_id, policy_version);


--
-- Name: config_snapshot config_snapshot_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.config_snapshot
    ADD CONSTRAINT config_snapshot_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: confirmation_grant confirmation_grant_tenant_id_confirmation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.confirmation_grant
    ADD CONSTRAINT confirmation_grant_tenant_id_confirmation_id_fkey FOREIGN KEY (tenant_id, confirmation_id) REFERENCES public.confirmation(tenant_id, confirmation_id);


--
-- Name: confirmation confirmation_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.confirmation
    ADD CONSTRAINT confirmation_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: confirmation confirmation_tenant_id_policy_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.confirmation
    ADD CONSTRAINT confirmation_tenant_id_policy_version_fkey FOREIGN KEY (tenant_id, policy_version) REFERENCES public.policy_snapshot(tenant_id, policy_version);


--
-- Name: confirmation confirmation_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.confirmation
    ADD CONSTRAINT confirmation_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id);


--
-- Name: execution_cancel_intent execution_cancel_intent_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_cancel_intent
    ADD CONSTRAINT execution_cancel_intent_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id);


--
-- Name: execution_record execution_record_tenant_id_agent_app_id_agent_app_revision_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_tenant_id_agent_app_id_agent_app_revision_fkey FOREIGN KEY (tenant_id, agent_app_id, agent_app_revision) REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision);


--
-- Name: execution_record execution_record_tenant_id_config_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_tenant_id_config_version_fkey FOREIGN KEY (tenant_id, config_version) REFERENCES public.config_snapshot(tenant_id, config_version);


--
-- Name: execution_record execution_record_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.execution_record
    ADD CONSTRAINT execution_record_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id);


--
-- Name: governance_decision governance_decision_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.governance_decision
    ADD CONSTRAINT governance_decision_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: governance_decision governance_decision_tenant_id_policy_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.governance_decision
    ADD CONSTRAINT governance_decision_tenant_id_policy_version_fkey FOREIGN KEY (tenant_id, policy_version) REFERENCES public.policy_snapshot(tenant_id, policy_version);


--
-- Name: governance_decision governance_decision_tenant_id_reservation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.governance_decision
    ADD CONSTRAINT governance_decision_tenant_id_reservation_id_fkey FOREIGN KEY (tenant_id, reservation_id) REFERENCES public.budget_reservation(tenant_id, reservation_id);


--
-- Name: inbound_payload inbound_payload_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inbound_payload
    ADD CONSTRAINT inbound_payload_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: inbox inbox_tenant_id_agent_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inbox
    ADD CONSTRAINT inbox_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);


--
-- Name: interaction_payload interaction_payload_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.interaction_payload
    ADD CONSTRAINT interaction_payload_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id);


--
-- Name: knowledge_chunk knowledge_chunk_tenant_id_knowledge_id_knowledge_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_chunk
    ADD CONSTRAINT knowledge_chunk_tenant_id_knowledge_id_knowledge_version_fkey FOREIGN KEY (tenant_id, knowledge_id, knowledge_version) REFERENCES public.knowledge_manifest(tenant_id, knowledge_id, version);


--
-- Name: knowledge_manifest knowledge_manifest_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_manifest
    ADD CONSTRAINT knowledge_manifest_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: knowledge_migration_mutation knowledge_migration_mutation_tenant_id_migration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_migration_mutation
    ADD CONSTRAINT knowledge_migration_mutation_tenant_id_migration_id_fkey FOREIGN KEY (tenant_id, migration_id) REFERENCES public.backend_migration(tenant_id, migration_id);


--
-- Name: knowledge_probe knowledge_probe_tenant_id_knowledge_id_knowledge_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.knowledge_probe
    ADD CONSTRAINT knowledge_probe_tenant_id_knowledge_id_knowledge_version_fkey FOREIGN KEY (tenant_id, knowledge_id, knowledge_version) REFERENCES public.knowledge_manifest(tenant_id, knowledge_id, version);


--
-- Name: media_artifact media_artifact_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_artifact
    ADD CONSTRAINT media_artifact_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: media_artifact media_artifact_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_artifact
    ADD CONSTRAINT media_artifact_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id);


--
-- Name: model_profile model_profile_current_version_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_profile
    ADD CONSTRAINT model_profile_current_version_fk FOREIGN KEY (tenant_id, model_profile_id, current_version) REFERENCES public.model_profile_revision(tenant_id, model_profile_id, profile_version) DEFERRABLE;


--
-- Name: model_profile_revision model_profile_revision_tenant_id_model_profile_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_profile_revision
    ADD CONSTRAINT model_profile_revision_tenant_id_model_profile_id_fkey FOREIGN KEY (tenant_id, model_profile_id) REFERENCES public.model_profile(tenant_id, model_profile_id);


--
-- Name: model_profile model_profile_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.model_profile
    ADD CONSTRAINT model_profile_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: policy_snapshot policy_snapshot_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.policy_snapshot
    ADD CONSTRAINT policy_snapshot_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: policy_snapshot policy_snapshot_tenant_id_pricing_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.policy_snapshot
    ADD CONSTRAINT policy_snapshot_tenant_id_pricing_version_fkey FOREIGN KEY (tenant_id, pricing_version) REFERENCES public.pricing_snapshot(tenant_id, pricing_version);


--
-- Name: prepared_payload prepared_payload_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prepared_payload
    ADD CONSTRAINT prepared_payload_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: prepared_payload prepared_payload_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prepared_payload
    ADD CONSTRAINT prepared_payload_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id);


--
-- Name: preprocess_job preprocess_job_channel_binding_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_channel_binding_fk FOREIGN KEY (tenant_id, config_version, channel_binding_id) REFERENCES public.channel_binding(tenant_id, config_version, binding_id);


--
-- Name: preprocess_job preprocess_job_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.preprocess_job
    ADD CONSTRAINT preprocess_job_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.inbox(tenant_id, request_id);


--
-- Name: pricing_snapshot pricing_snapshot_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pricing_snapshot
    ADD CONSTRAINT pricing_snapshot_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: result_payload result_payload_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.result_payload
    ADD CONSTRAINT result_payload_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: result_payload result_payload_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.result_payload
    ADD CONSTRAINT result_payload_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id);


--
-- Name: session_commit session_commit_tenant_id_agent_app_id_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_commit
    ADD CONSTRAINT session_commit_tenant_id_agent_app_id_session_id_fkey FOREIGN KEY (tenant_id, agent_app_id, session_id) REFERENCES public.session_head(tenant_id, agent_app_id, session_id);


--
-- Name: session_event session_event_tenant_id_agent_app_id_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_event
    ADD CONSTRAINT session_event_tenant_id_agent_app_id_session_id_fkey FOREIGN KEY (tenant_id, agent_app_id, session_id) REFERENCES public.session_head(tenant_id, agent_app_id, session_id);


--
-- Name: session_head session_head_tenant_id_agent_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_head
    ADD CONSTRAINT session_head_tenant_id_agent_app_id_fkey FOREIGN KEY (tenant_id, agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id);


--
-- Name: session_migration_mutation session_migration_mutation_tenant_id_migration_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_migration_mutation
    ADD CONSTRAINT session_migration_mutation_tenant_id_migration_id_fkey FOREIGN KEY (tenant_id, migration_id) REFERENCES public.backend_migration(tenant_id, migration_id);


--
-- Name: session_summary_content session_summary_content_tenant_id_agent_app_id_session_id__fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_summary_content
    ADD CONSTRAINT session_summary_content_tenant_id_agent_app_id_session_id__fkey FOREIGN KEY (tenant_id, agent_app_id, session_id, summary_id) REFERENCES public.session_summary(tenant_id, agent_app_id, session_id, summary_id) ON DELETE RESTRICT;


--
-- Name: session_summary_content session_summary_content_tenant_id_agent_app_id_session_id_fkey1; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_summary_content
    ADD CONSTRAINT session_summary_content_tenant_id_agent_app_id_session_id_fkey1 FOREIGN KEY (tenant_id, agent_app_id, session_id, superseded_by_summary_id) REFERENCES public.session_summary(tenant_id, agent_app_id, session_id, summary_id) ON DELETE RESTRICT;


--
-- Name: session_summary session_summary_tenant_id_agent_app_id_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.session_summary
    ADD CONSTRAINT session_summary_tenant_id_agent_app_id_session_id_fkey FOREIGN KEY (tenant_id, agent_app_id, session_id) REFERENCES public.session_head(tenant_id, agent_app_id, session_id);


--
-- Name: skill_catalog skill_catalog_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.skill_catalog
    ADD CONSTRAINT skill_catalog_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: tenant tenant_active_config_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant
    ADD CONSTRAINT tenant_active_config_fk FOREIGN KEY (tenant_id, active_config_version) REFERENCES public.config_snapshot(tenant_id, config_version) DEFERRABLE;


--
-- Name: tenant tenant_default_agent_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant
    ADD CONSTRAINT tenant_default_agent_app_fk FOREIGN KEY (tenant_id, default_agent_app_id) REFERENCES public.agent_app(tenant_id, agent_app_id) DEFERRABLE;


--
-- Name: tenant tenant_default_backend_profile_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant
    ADD CONSTRAINT tenant_default_backend_profile_fk FOREIGN KEY (tenant_id, default_backend_profile_id) REFERENCES public.backend_profile(tenant_id, backend_profile_id) DEFERRABLE;


--
-- Name: tenant_status_change tenant_status_change_tenant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_status_change
    ADD CONSTRAINT tenant_status_change_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id);


--
-- Name: tool_attempt tool_attempt_tenant_id_grant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tool_attempt
    ADD CONSTRAINT tool_attempt_tenant_id_grant_id_fkey FOREIGN KEY (tenant_id, grant_id) REFERENCES public.confirmation_grant(tenant_id, grant_id);


--
-- Name: tool_result_payload tool_result_payload_tenant_id_grant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tool_result_payload
    ADD CONSTRAINT tool_result_payload_tenant_id_grant_id_fkey FOREIGN KEY (tenant_id, grant_id) REFERENCES public.tool_attempt(tenant_id, grant_id);


--
-- Name: tool_result_payload tool_result_payload_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tool_result_payload
    ADD CONSTRAINT tool_result_payload_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id);


--
-- Name: usage_ledger usage_ledger_tenant_id_reservation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_ledger
    ADD CONSTRAINT usage_ledger_tenant_id_reservation_id_fkey FOREIGN KEY (tenant_id, reservation_id) REFERENCES public.budget_reservation(tenant_id, reservation_id);


--
-- Name: webui_message webui_message_tenant_id_config_version_channel_binding_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webui_message
    ADD CONSTRAINT webui_message_tenant_id_config_version_channel_binding_id_fkey FOREIGN KEY (tenant_id, config_version, channel_binding_id) REFERENCES public.channel_binding(tenant_id, config_version, binding_id);


--
-- Name: webui_message webui_message_tenant_id_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webui_message
    ADD CONSTRAINT webui_message_tenant_id_request_id_fkey FOREIGN KEY (tenant_id, request_id) REFERENCES public.execution_record(tenant_id, request_id);


--
-- Name: FUNCTION begin_knowledge_backend_observation(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_at timestamp with time zone, p_observe_until timestamp with time zone); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.begin_knowledge_backend_observation(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_at timestamp with time zone, p_observe_until timestamp with time zone) FROM PUBLIC;


--
-- Name: FUNCTION begin_session_backend_observation(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_at timestamp with time zone, p_observe_until timestamp with time zone); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.begin_session_backend_observation(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_at timestamp with time zone, p_observe_until timestamp with time zone) FROM PUBLIC;


--
-- Name: FUNCTION business_audit_watermark(p_tenant text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.business_audit_watermark(p_tenant text) FROM PUBLIC;


--
-- Name: FUNCTION capture_session_migration_mutation(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.capture_session_migration_mutation() FROM PUBLIC;


--
-- Name: FUNCTION claim_channel_inbox(p_tenant_id text, p_channel text, p_external_account_id text, p_external_message_id text, p_request_id text, p_agent_app_id text, p_session_id text, p_external_chat_id text, p_external_user_id text, p_payload_ref text, p_payload_digest text, p_key_version bigint, p_initial_state text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.claim_channel_inbox(p_tenant_id text, p_channel text, p_external_account_id text, p_external_message_id text, p_request_id text, p_agent_app_id text, p_session_id text, p_external_chat_id text, p_external_user_id text, p_payload_ref text, p_payload_digest text, p_key_version bigint, p_initial_state text) FROM PUBLIC;


--
-- Name: FUNCTION claim_inbox(p_tenant_id text, p_channel text, p_external_account_id text, p_external_message_id text, p_request_id text, p_agent_app_id text, p_session_id text, p_payload_ref text, p_payload_digest text, p_key_version bigint, p_initial_state text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.claim_inbox(p_tenant_id text, p_channel text, p_external_account_id text, p_external_message_id text, p_request_id text, p_agent_app_id text, p_session_id text, p_payload_ref text, p_payload_digest text, p_key_version bigint, p_initial_state text) FROM PUBLIC;


--
-- Name: FUNCTION cleanup_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.cleanup_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone) FROM PUBLIC;


--
-- Name: FUNCTION cleanup_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.cleanup_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone) FROM PUBLIC;


--
-- Name: FUNCTION commit_turn(p_tenant_id text, p_agent_app_id text, p_session_id text, p_request_id text, p_commit_id text, p_request_digest text, p_stage text, p_input_seq bigint, p_fence bigint, p_expected_version bigint, p_outcome text, p_events jsonb, p_state_delta jsonb, p_summary jsonb, p_result_ref text, p_reply_cursor text, p_outbox jsonb); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.commit_turn(p_tenant_id text, p_agent_app_id text, p_session_id text, p_request_id text, p_commit_id text, p_request_digest text, p_stage text, p_input_seq bigint, p_fence bigint, p_expected_version bigint, p_outcome text, p_events jsonb, p_state_delta jsonb, p_summary jsonb, p_result_ref text, p_reply_cursor text, p_outbox jsonb) FROM PUBLIC;


--
-- Name: FUNCTION cutover_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_source_count bigint, p_target_count bigint, p_source_digest text, p_target_digest text, p_source_watermark text, p_target_watermark text, p_sample_digest text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.cutover_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_source_count bigint, p_target_count bigint, p_source_digest text, p_target_digest text, p_source_watermark text, p_target_watermark text, p_sample_digest text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION cutover_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_source_count bigint, p_target_count bigint, p_source_digest text, p_target_digest text, p_source_watermark text, p_target_watermark text, p_sample_digest text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.cutover_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_source_count bigint, p_target_count bigint, p_source_digest text, p_target_digest text, p_source_watermark text, p_target_watermark text, p_sample_digest text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION decide_confirmation(p_tenant_id text, p_confirmation_id text, p_subject_id text, p_approve boolean, p_expected_version bigint, p_decided_at timestamp with time zone); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.decide_confirmation(p_tenant_id text, p_confirmation_id text, p_subject_id text, p_approve boolean, p_expected_version bigint, p_decided_at timestamp with time zone) FROM PUBLIC;


--
-- Name: FUNCTION enqueue_next_parked_wakeup(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.enqueue_next_parked_wakeup() FROM PUBLIC;


--
-- Name: FUNCTION execute_business_audit_purge(p_tenant text, p_batch text, p_owner text, p_chunk bigint); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.execute_business_audit_purge(p_tenant text, p_batch text, p_owner text, p_chunk bigint) FROM PUBLIC;
GRANT ALL ON FUNCTION public.execute_business_audit_purge(p_tenant text, p_batch text, p_owner text, p_chunk bigint) TO audit_retention_purger;


--
-- Name: FUNCTION expire_confirmations(p_now timestamp with time zone, p_limit integer); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.expire_confirmations(p_now timestamp with time zone, p_limit integer) FROM PUBLIC;


--
-- Name: FUNCTION guard_agent_app_revision_knowledge_published(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_agent_app_revision_knowledge_published() FROM PUBLIC;


--
-- Name: FUNCTION guard_backend_migration_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_backend_migration_update() FROM PUBLIC;


--
-- Name: FUNCTION guard_business_audit_purge_batch_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_business_audit_purge_batch_update() FROM PUBLIC;


--
-- Name: FUNCTION guard_execution_cancel_intent(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_execution_cancel_intent() FROM PUBLIC;


--
-- Name: FUNCTION guard_knowledge_manifest_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_knowledge_manifest_update() FROM PUBLIC;


--
-- Name: FUNCTION guard_knowledge_migration_authority_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_knowledge_migration_authority_update() FROM PUBLIC;


--
-- Name: FUNCTION guard_knowledge_migration_direction_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_knowledge_migration_direction_update() FROM PUBLIC;


--
-- Name: FUNCTION guard_knowledge_migration_mutation_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_knowledge_migration_mutation_update() FROM PUBLIC;


--
-- Name: FUNCTION guard_session_migration_authority_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_session_migration_authority_update() FROM PUBLIC;


--
-- Name: FUNCTION guard_session_migration_direction_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_session_migration_direction_update() FROM PUBLIC;


--
-- Name: FUNCTION guard_session_migration_mutation_update(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.guard_session_migration_mutation_update() FROM PUBLIC;


--
-- Name: FUNCTION inspect_execution_wakeup(p_tenant_id text, p_request_id text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.inspect_execution_wakeup(p_tenant_id text, p_request_id text) FROM PUBLIC;


--
-- Name: FUNCTION park_execution(p_tenant_id text, p_request_id text, p_input_seq bigint, p_base_delay_seconds bigint, p_max_delay_seconds bigint, p_deadline_seconds bigint, p_max_attempts integer); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.park_execution(p_tenant_id text, p_request_id text, p_input_seq bigint, p_base_delay_seconds bigint, p_max_delay_seconds bigint, p_deadline_seconds bigint, p_max_attempts integer) FROM PUBLIC;


--
-- Name: FUNCTION plan_business_audit_purge(p_tenant text, p_cutoff timestamp with time zone, p_actor text, p_reason text, p_now timestamp with time zone); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.plan_business_audit_purge(p_tenant text, p_cutoff timestamp with time zone, p_actor text, p_reason text, p_now timestamp with time zone) FROM PUBLIC;
GRANT ALL ON FUNCTION public.plan_business_audit_purge(p_tenant text, p_cutoff timestamp with time zone, p_actor text, p_reason text, p_now timestamp with time zone) TO audit_retention_purger;


--
-- Name: FUNCTION populate_channel_send_secret(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.populate_channel_send_secret() FROM PUBLIC;


--
-- Name: FUNCTION prepare_dispatch(p_tenant_id text, p_expected_tenant_version bigint, p_agent_app_id text, p_expected_app_version bigint, p_agent_app_revision bigint, p_agent_content_digest text, p_config_version bigint, p_policy_version bigint, p_request_id text, p_session_id text, p_user_id text, p_channel text, p_payload_ref text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.prepare_dispatch(p_tenant_id text, p_expected_tenant_version bigint, p_agent_app_id text, p_expected_app_version bigint, p_agent_app_revision bigint, p_agent_content_digest text, p_config_version bigint, p_policy_version bigint, p_request_id text, p_session_id text, p_user_id text, p_channel text, p_payload_ref text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION publish_agent_app_revision(p_tenant_id text, p_agent_app_id text, p_revision bigint, p_expected_app_version bigint, p_expected_draft_version bigint, p_content_digest text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.publish_agent_app_revision(p_tenant_id text, p_agent_app_id text, p_revision bigint, p_expected_app_version bigint, p_expected_draft_version bigint, p_content_digest text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION publish_config_snapshot(p_tenant_id text, p_expected_tenant_version bigint, p_schema_version integer, p_payload jsonb, p_content_digest text, p_default_agent_app_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.publish_config_snapshot(p_tenant_id text, p_expected_tenant_version bigint, p_schema_version integer, p_payload jsonb, p_content_digest text, p_default_agent_app_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION quarantine_business_audit_purge(p_tenant text, p_batch text, p_owner text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.quarantine_business_audit_purge(p_tenant text, p_batch text, p_owner text) FROM PUBLIC;
GRANT ALL ON FUNCTION public.quarantine_business_audit_purge(p_tenant text, p_batch text, p_owner text) TO audit_retention_purger;


--
-- Name: FUNCTION reject_audit_event_change(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.reject_audit_event_change() FROM PUBLIC;


--
-- Name: FUNCTION reject_backend_migration_batch_change(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.reject_backend_migration_batch_change() FROM PUBLIC;


--
-- Name: FUNCTION reject_backend_migration_config_switch_change(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.reject_backend_migration_config_switch_change() FROM PUBLIC;


--
-- Name: FUNCTION reject_business_audit_purge_certificate_change(); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.reject_business_audit_purge_certificate_change() FROM PUBLIC;


--
-- Name: FUNCTION request_cancel_execution(p_tenant_id text, p_request_id text, p_expected_version bigint, p_actor_id text, p_reason_code text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.request_cancel_execution(p_tenant_id text, p_request_id text, p_expected_version bigint, p_actor_id text, p_reason_code text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION rollback_agent_app_revision(p_tenant_id text, p_agent_app_id text, p_target_revision bigint, p_expected_app_version bigint, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.rollback_agent_app_revision(p_tenant_id text, p_agent_app_id text, p_target_revision bigint, p_expected_app_version bigint, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION rollback_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.rollback_knowledge_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION rollback_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.rollback_session_backend_migration(p_tenant_id text, p_migration_id text, p_expected_tenant_version bigint, p_expected_migration_version bigint, p_rollback_sync_watermark text, p_at timestamp with time zone, p_switch_id text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION suspend_turn(p_tenant_id text, p_agent_app_id text, p_session_id text, p_request_id text, p_commit_id text, p_request_digest text, p_input_seq bigint, p_fence bigint, p_expected_version bigint, p_events jsonb, p_state_delta jsonb, p_confirmation_id text, p_subject_id text, p_channel_binding_id text, p_tool_id text, p_tool_version bigint, p_tool_call_id text, p_args_digest text, p_policy_version bigint, p_checkpoint_ref text, p_input_tokens bigint, p_output_tokens bigint, p_cached_input_tokens bigint, p_expires_at timestamp with time zone, p_outbox jsonb); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.suspend_turn(p_tenant_id text, p_agent_app_id text, p_session_id text, p_request_id text, p_commit_id text, p_request_digest text, p_input_seq bigint, p_fence bigint, p_expected_version bigint, p_events jsonb, p_state_delta jsonb, p_confirmation_id text, p_subject_id text, p_channel_binding_id text, p_tool_id text, p_tool_version bigint, p_tool_call_id text, p_args_digest text, p_policy_version bigint, p_checkpoint_ref text, p_input_tokens bigint, p_output_tokens bigint, p_cached_input_tokens bigint, p_expires_at timestamp with time zone, p_outbox jsonb) FROM PUBLIC;


--
-- Name: FUNCTION transition_agent_app_status(p_tenant_id text, p_agent_app_id text, p_expected_app_version bigint, p_next_status text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.transition_agent_app_status(p_tenant_id text, p_agent_app_id text, p_expected_app_version bigint, p_next_status text, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION transition_tenant_status(p_tenant_id text, p_expected_version bigint, p_next_status text, p_actor_type text, p_actor_id text, p_reason_code text, p_reason_text_ref text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.transition_tenant_status(p_tenant_id text, p_expected_version bigint, p_next_status text, p_actor_type text, p_actor_id text, p_reason_code text, p_reason_text_ref text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- Name: FUNCTION update_tenant_configuration(p_tenant_id text, p_expected_version bigint, p_display_name text, p_request_limit_per_minute bigint, p_max_concurrent_executions integer, p_monthly_token_budget bigint, p_monthly_cost_budget_micros bigint, p_billing_currency character, p_audit_retention_days integer, p_audit_payload_mode text, p_log_masking_level text, p_trace_sampling_rate numeric, p_default_agent_app_id text, p_default_backend_profile_id text, p_active_config_version bigint, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text); Type: ACL; Schema: public; Owner: -
--

REVOKE ALL ON FUNCTION public.update_tenant_configuration(p_tenant_id text, p_expected_version bigint, p_display_name text, p_request_limit_per_minute bigint, p_max_concurrent_executions integer, p_monthly_token_budget bigint, p_monthly_cost_budget_micros bigint, p_billing_currency character, p_audit_retention_days integer, p_audit_payload_mode text, p_log_masking_level text, p_trace_sampling_rate numeric, p_default_agent_app_id text, p_default_backend_profile_id text, p_active_config_version bigint, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) FROM PUBLIC;


--
-- PostgreSQL database dump complete
--



-- First-delivery baseline: all final contract additions are kept in this one
-- transaction so an empty database never observes an intermediate schema.
CREATE TABLE public.config_release (
 release_id text PRIMARY KEY, state text NOT NULL, percentage integer NOT NULL, salt text NOT NULL,
 version bigint NOT NULL DEFAULT 1, actor_id text NOT NULL, reason_code text NOT NULL,
 correlation_id text NOT NULL, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 CONSTRAINT config_release_state_check CHECK (state IN ('active','rolled_back')),
 CONSTRAINT config_release_percentage_check CHECK (percentage BETWEEN 0 AND 100), CONSTRAINT config_release_version_check CHECK (version >= 1),
 CONSTRAINT config_release_identifier_check CHECK (length(btrim(release_id)) > 0 AND length(release_id) <= 256),
 CONSTRAINT config_release_salt_check CHECK (length(btrim(salt)) >= 16 AND length(salt) <= 512),
 CONSTRAINT config_release_metadata_check CHECK (length(btrim(actor_id)) > 0 AND length(btrim(reason_code)) > 0 AND length(btrim(correlation_id)) > 0 AND length(btrim(trace_id)) > 0)
);
CREATE TABLE public.config_release_target (
 release_id text NOT NULL REFERENCES public.config_release(release_id), tenant_id text NOT NULL REFERENCES public.tenant(tenant_id),
 baseline_config_version bigint NOT NULL, candidate_config_version bigint NOT NULL, allowlisted boolean NOT NULL DEFAULT false,
 PRIMARY KEY (release_id, tenant_id), CONSTRAINT config_release_target_versions_check CHECK (baseline_config_version >= 1 AND candidate_config_version >= 1 AND baseline_config_version <> candidate_config_version),
 CONSTRAINT config_release_target_baseline_fk FOREIGN KEY (tenant_id, baseline_config_version) REFERENCES public.config_snapshot(tenant_id, config_version),
 CONSTRAINT config_release_target_candidate_fk FOREIGN KEY (tenant_id, candidate_config_version) REFERENCES public.config_snapshot(tenant_id, config_version)
);
CREATE INDEX config_release_target_effective_idx ON public.config_release_target(tenant_id, baseline_config_version, release_id);

ALTER TABLE public.outbox DROP CONSTRAINT outbox_kind_check;
ALTER TABLE public.outbox ADD CONSTRAINT outbox_kind_check CHECK (kind = ANY (ARRAY['audit'::text, 'tenant-control'::text, 'config-invalidation'::text, 'dispatch'::text, 'reply'::text, 'wakeup'::text, 'execution-control'::text]));

ALTER TABLE public.result_payload ADD COLUMN content_type text NOT NULL DEFAULT 'text/plain', ADD CONSTRAINT result_payload_content_type_check CHECK (content_type IN ('text/plain', 'application/vnd.trpc.card+json') OR content_type ~ '^image/[a-z0-9.+-]+$');
ALTER TABLE public.interaction_payload ADD COLUMN content_type text NOT NULL DEFAULT 'text/plain', ADD CONSTRAINT interaction_payload_content_type_check CHECK (content_type IN ('text/plain', 'application/vnd.trpc.card+json') OR content_type ~ '^image/[a-z0-9.+-]+$');
ALTER TABLE public.agent_app_revision ADD COLUMN fallback_model_refs jsonb NOT NULL DEFAULT '[]'::jsonb, ADD CONSTRAINT agent_app_revision_fallback_model_refs_check CHECK (jsonb_typeof(fallback_model_refs) = 'array' AND (agent_kind = 'llm'::text OR fallback_model_refs = '[]'::jsonb));
ALTER TABLE public.agent_app_revision ADD COLUMN max_llm_calls integer NOT NULL DEFAULT 0, ADD COLUMN max_tool_calls integer NOT NULL DEFAULT 0, ADD COLUMN max_parallel_tools integer NOT NULL DEFAULT 0, ADD COLUMN execution_timeout_seconds integer NOT NULL DEFAULT 0, ADD CONSTRAINT agent_app_revision_execution_budget_check CHECK (max_llm_calls BETWEEN 0 AND 10000 AND max_tool_calls BETWEEN 0 AND 10000 AND max_parallel_tools BETWEEN 0 AND 1000 AND execution_timeout_seconds BETWEEN 0 AND 86400);
ALTER TABLE public.execution_record ADD COLUMN max_llm_calls integer NOT NULL DEFAULT 0, ADD COLUMN max_tool_calls integer NOT NULL DEFAULT 0, ADD COLUMN max_parallel_tools integer NOT NULL DEFAULT 0, ADD COLUMN execution_timeout_seconds integer NOT NULL DEFAULT 0, ADD CONSTRAINT execution_record_execution_budget_check CHECK (max_llm_calls BETWEEN 0 AND 10000 AND max_tool_calls BETWEEN 0 AND 10000 AND max_parallel_tools BETWEEN 0 AND 1000 AND execution_timeout_seconds BETWEEN 0 AND 86400);
CREATE FUNCTION public.hydrate_execution_budget() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog' AS $$
DECLARE budget public.agent_app_revision%ROWTYPE;
BEGIN
 SELECT * INTO budget FROM public.agent_app_revision WHERE tenant_id=NEW.tenant_id AND agent_app_id=NEW.agent_app_id AND revision=NEW.agent_app_revision AND state='published' AND content_digest=NEW.agent_content_digest;
 IF NOT FOUND THEN RAISE EXCEPTION 'published revision budget not found' USING ERRCODE='40001'; END IF;
 NEW.max_llm_calls := budget.max_llm_calls; NEW.max_tool_calls := budget.max_tool_calls; NEW.max_parallel_tools := budget.max_parallel_tools; NEW.execution_timeout_seconds := budget.execution_timeout_seconds;
 RETURN NEW;
END;
$$;
CREATE TRIGGER execution_record_hydrate_execution_budget BEFORE INSERT ON public.execution_record FOR EACH ROW EXECUTE FUNCTION public.hydrate_execution_budget();
REVOKE ALL ON FUNCTION public.hydrate_execution_budget() FROM PUBLIC;

-- Framework-owned agent long-term memory (trpc-agent-go/memory/postgres). The
-- worker runs the service with WithSkipDBInit(true), so the schema is owned by
-- this baseline; column types match the framework's verifySchema expectation.
CREATE TABLE public.memories (
 memory_id text PRIMARY KEY,
 app_name text NOT NULL,
 user_id text NOT NULL,
 memory_data jsonb NOT NULL,
 created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 deleted_at timestamp NULL DEFAULT NULL
);
CREATE INDEX idx_memories_app_user ON public.memories(app_name, user_id);
CREATE INDEX idx_memories_updated_at ON public.memories(updated_at DESC);
CREATE INDEX idx_memories_deleted_at ON public.memories(deleted_at);

-- Framework-owned Session/Event/State persistence
-- (trpc-agent-go/session/postgres). The platform keeps only ordering, fence,
-- terminal and outbox coordination in its own session_* tables; these tables
-- are the sole source of conversational session state and event history.
-- Like memory, role composition uses WithSkipDBInit(true), so all schema
-- changes remain visible and reviewable in the service migration baseline.
CREATE TABLE public.session_states (
 id BIGSERIAL PRIMARY KEY,
 app_name VARCHAR(255) NOT NULL,
 user_id VARCHAR(255) NOT NULL,
 session_id VARCHAR(255) NOT NULL,
 state jsonb DEFAULT NULL,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 expires_at TIMESTAMP DEFAULT NULL,
 deleted_at TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX idx_session_states_unique_active ON public.session_states(app_name, user_id, session_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_session_states_expires ON public.session_states(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE public.session_events (
 id BIGSERIAL PRIMARY KEY,
 app_name VARCHAR(255) NOT NULL,
 user_id VARCHAR(255) NOT NULL,
 session_id VARCHAR(255) NOT NULL,
 event JSONB NOT NULL,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 expires_at TIMESTAMP DEFAULT NULL,
 deleted_at TIMESTAMP DEFAULT NULL
);
CREATE INDEX idx_session_events_lookup ON public.session_events(app_name, user_id, session_id, created_at);
CREATE INDEX idx_session_events_expires ON public.session_events(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE public.session_track_events (
 id BIGSERIAL PRIMARY KEY,
 app_name VARCHAR(255) NOT NULL,
 user_id VARCHAR(255) NOT NULL,
 session_id VARCHAR(255) NOT NULL,
 track VARCHAR(255) NOT NULL,
 event JSONB NOT NULL,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 expires_at TIMESTAMP DEFAULT NULL,
 deleted_at TIMESTAMP DEFAULT NULL
);
CREATE INDEX idx_session_track_events_lookup ON public.session_track_events(app_name, user_id, session_id, track, created_at);
CREATE INDEX idx_session_track_events_expires ON public.session_track_events(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE public.session_summaries (
 id BIGSERIAL PRIMARY KEY,
 app_name VARCHAR(255) NOT NULL,
 user_id VARCHAR(255) NOT NULL,
 session_id VARCHAR(255) NOT NULL,
 filter_key VARCHAR(255) NOT NULL DEFAULT '',
 summary JSONB DEFAULT NULL,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 expires_at TIMESTAMP DEFAULT NULL,
 deleted_at TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX idx_session_summaries_unique_active ON public.session_summaries(app_name, user_id, session_id, filter_key) WHERE deleted_at IS NULL;
CREATE INDEX idx_session_summaries_expires ON public.session_summaries(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE public.app_states (
 id BIGSERIAL PRIMARY KEY,
 app_name VARCHAR(255) NOT NULL,
 key VARCHAR(255) NOT NULL,
 value TEXT DEFAULT NULL,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 expires_at TIMESTAMP DEFAULT NULL,
 deleted_at TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX idx_app_states_unique_active ON public.app_states(app_name, key) WHERE deleted_at IS NULL;
CREATE INDEX idx_app_states_expires ON public.app_states(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE public.user_states (
 id BIGSERIAL PRIMARY KEY,
 app_name VARCHAR(255) NOT NULL,
 user_id VARCHAR(255) NOT NULL,
 key VARCHAR(255) NOT NULL,
 value TEXT DEFAULT NULL,
 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 expires_at TIMESTAMP DEFAULT NULL,
 deleted_at TIMESTAMP DEFAULT NULL
);
CREATE UNIQUE INDEX idx_user_states_unique_active ON public.user_states(app_name, user_id, key) WHERE deleted_at IS NULL;
CREATE INDEX idx_user_states_expires ON public.user_states(expires_at) WHERE expires_at IS NOT NULL;

-- Runner plugins are immutable revision capabilities. The database repeats the
-- reviewed allow-list so direct SQL cannot publish arbitrary framework hooks.
CREATE TABLE public.agent_app_revision_plugin (
 tenant_id text NOT NULL,
 agent_app_id text NOT NULL,
 revision bigint NOT NULL,
 plugin_id text NOT NULL,
 plugin_version bigint NOT NULL,
 PRIMARY KEY (tenant_id, agent_app_id, revision, plugin_id),
 CONSTRAINT agent_app_revision_plugin_version_check CHECK (plugin_version = 1),
 CONSTRAINT agent_app_revision_plugin_allowlist_check CHECK (plugin_id IN ('tool_call_id', 'message_merger', 'tool_search', 'await_user_reply')),
 CONSTRAINT agent_app_revision_plugin_revision_fk FOREIGN KEY (tenant_id, agent_app_id, revision)
   REFERENCES public.agent_app_revision(tenant_id, agent_app_id, revision) ON DELETE CASCADE
);
CREATE TRIGGER agent_app_revision_plugin_draft_only
 BEFORE INSERT OR DELETE OR UPDATE ON public.agent_app_revision_plugin
 FOR EACH ROW EXECUTE FUNCTION public.guard_revision_child_write();


-- Consolidated first-delivery controls, redaction rules, and phase journal.

ALTER TABLE public.backend_migration
  ADD COLUMN paused_from_state text NOT NULL DEFAULT '';

ALTER TABLE public.backend_migration
  DROP CONSTRAINT backend_migration_state_check,
  ADD CONSTRAINT backend_migration_state_check CHECK (state = ANY (ARRAY[
    'planned'::text, 'snapshot'::text, 'dual_write'::text, 'backfill'::text,
    'verify'::text, 'cutover'::text, 'observe'::text, 'cleanup'::text,
    'paused'::text, 'aborted'::text
  ])),
  ADD CONSTRAINT backend_migration_paused_from_state_check CHECK (
    (state='paused' AND paused_from_state = ANY (ARRAY['planned'::text, 'snapshot'::text, 'dual_write'::text, 'backfill'::text, 'verify'::text])) OR
    (state<>'paused' AND paused_from_state='')
  );

DROP INDEX public.backend_migration_active_domain_idx;
CREATE UNIQUE INDEX backend_migration_active_domain_idx ON public.backend_migration USING btree (tenant_id, domain)
  WHERE state NOT IN ('cleanup'::text, 'aborted'::text);

CREATE TABLE public.backend_migration_control (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  control_version bigint NOT NULL,
  action text NOT NULL,
  from_state text NOT NULL,
  to_state text NOT NULL,
  actor_id text NOT NULL,
  reason_code text NOT NULL,
  correlation_id text NOT NULL,
  trace_id text NOT NULL,
  traceparent text NOT NULL DEFAULT '',
  occurred_at timestamp with time zone NOT NULL,
  PRIMARY KEY (tenant_id, migration_id, control_version),
  CONSTRAINT backend_migration_control_action_check CHECK (action = ANY (ARRAY['pause'::text, 'resume'::text, 'abort'::text])),
  CONSTRAINT backend_migration_control_actor_id_check CHECK (length(btrim(actor_id)) BETWEEN 1 AND 256),
  CONSTRAINT backend_migration_control_reason_code_check CHECK (length(btrim(reason_code)) BETWEEN 1 AND 128),
  CONSTRAINT backend_migration_control_correlation_id_check CHECK (length(btrim(correlation_id)) BETWEEN 1 AND 256),
  CONSTRAINT backend_migration_control_trace_id_check CHECK (length(btrim(trace_id)) BETWEEN 1 AND 128),
  CONSTRAINT backend_migration_control_traceparent_check CHECK (length(traceparent) <= 512),
  CONSTRAINT backend_migration_control_version_check CHECK (control_version >= 2),
  FOREIGN KEY (tenant_id, migration_id) REFERENCES public.backend_migration(tenant_id, migration_id)
);

CREATE FUNCTION public.reject_backend_migration_control_change() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path TO 'pg_catalog'
AS $$
BEGIN
  RAISE EXCEPTION 'backend migration controls are immutable' USING ERRCODE='23000';
END;
$$;

CREATE TRIGGER backend_migration_control_immutable
  BEFORE DELETE OR UPDATE ON public.backend_migration_control
  FOR EACH ROW EXECUTE FUNCTION public.reject_backend_migration_control_change();

CREATE OR REPLACE FUNCTION public.guard_backend_migration_update() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path TO 'pg_catalog', 'public'
AS $$
DECLARE
  expected_state text;
  pausable boolean;
  abortable boolean;
BEGIN
  IF (NEW.tenant_id,NEW.migration_id,NEW.domain,NEW.epoch,
      NEW.source_config_version,NEW.source_backend_profile_id,NEW.source_backend_version,
      NEW.target_config_version,NEW.target_backend_profile_id,NEW.target_backend_version,NEW.created_at)
     IS DISTINCT FROM
     (OLD.tenant_id,OLD.migration_id,OLD.domain,OLD.epoch,
      OLD.source_config_version,OLD.source_backend_profile_id,OLD.source_backend_version,
      OLD.target_config_version,OLD.target_backend_profile_id,OLD.target_backend_version,OLD.created_at) THEN
    RAISE EXCEPTION 'backend migration identity is immutable' USING ERRCODE='23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'backend migration version must advance exactly once' USING ERRCODE='40001';
  END IF;

  expected_state := CASE OLD.state
    WHEN 'planned' THEN 'snapshot' WHEN 'snapshot' THEN 'dual_write'
    WHEN 'dual_write' THEN 'backfill' WHEN 'backfill' THEN 'verify'
    WHEN 'verify' THEN 'cutover' WHEN 'cutover' THEN 'observe'
    WHEN 'observe' THEN 'cleanup' ELSE NULL END;
  pausable := OLD.state IN ('planned','snapshot','dual_write','backfill','verify');
  abortable := OLD.state IN ('planned','snapshot') OR
    (OLD.state='paused' AND OLD.paused_from_state IN ('planned','snapshot'));

  IF NEW.state = OLD.state THEN
    IF OLD.state='backfill' AND NOT OLD.backfill_complete AND
       NEW.next_batch_seq=OLD.next_batch_seq+1 AND NEW.backfill_count>=OLD.backfill_count AND
       NEW.backfill_checkpoint<>OLD.backfill_checkpoint AND NEW.paused_from_state=OLD.paused_from_state AND
       (NEW.snapshot_watermark,NEW.dual_write_ref,
        NEW.verify_source_count,NEW.verify_target_count,
        NEW.verify_source_digest,NEW.verify_target_digest,
        NEW.verify_source_watermark,NEW.verify_target_watermark,NEW.verify_sample_digest,
        NEW.cutover_config_version,NEW.cutover_at,NEW.observe_until,NEW.rollback_sync_watermark)
         IS NOT DISTINCT FROM
       (OLD.snapshot_watermark,OLD.dual_write_ref,
        OLD.verify_source_count,OLD.verify_target_count,
        OLD.verify_source_digest,OLD.verify_target_digest,
        OLD.verify_source_watermark,OLD.verify_target_watermark,OLD.verify_sample_digest,
        OLD.cutover_config_version,OLD.cutover_at,OLD.observe_until,OLD.rollback_sync_watermark) THEN
      RETURN NEW;
    ELSIF OLD.state='verify' AND NEW.paused_from_state='' AND
       (NEW.snapshot_watermark,NEW.dual_write_ref,NEW.backfill_checkpoint,NEW.next_batch_seq,NEW.backfill_count,NEW.backfill_complete,
        NEW.cutover_config_version,NEW.cutover_at,NEW.observe_until,NEW.rollback_sync_watermark)
         IS NOT DISTINCT FROM
       (OLD.snapshot_watermark,OLD.dual_write_ref,OLD.backfill_checkpoint,OLD.next_batch_seq,OLD.backfill_count,OLD.backfill_complete,
        OLD.cutover_config_version,OLD.cutover_at,OLD.observe_until,OLD.rollback_sync_watermark) THEN
      RETURN NEW;
    END IF;
    RAISE EXCEPTION 'backend migration update is invalid' USING ERRCODE='23514';
  END IF;

  IF (NEW.backfill_checkpoint,NEW.next_batch_seq,NEW.backfill_count,NEW.backfill_complete)
       IS DISTINCT FROM
     (OLD.backfill_checkpoint,OLD.next_batch_seq,OLD.backfill_count,OLD.backfill_complete) THEN
    RAISE EXCEPTION 'state transition cannot mutate backfill progress' USING ERRCODE='23514';
  END IF;

  IF NEW.state='paused' AND pausable AND NEW.paused_from_state=OLD.state THEN
    NULL;
  ELSIF OLD.state='paused' AND NEW.state=OLD.paused_from_state AND NEW.paused_from_state='' THEN
    NULL;
  ELSIF NEW.state='aborted' AND abortable AND NEW.paused_from_state='' THEN
    NULL;
  ELSIF NEW.state=expected_state AND OLD.paused_from_state='' AND NEW.paused_from_state='' THEN
    NULL;
  ELSE
    RAISE EXCEPTION 'illegal backend migration transition' USING ERRCODE='23514';
  END IF;

  IF NEW.state='snapshot' AND length(NEW.snapshot_watermark)=0 THEN
    RAISE EXCEPTION 'snapshot watermark is required' USING ERRCODE='23514';
  END IF;
  IF NEW.state='dual_write' AND length(NEW.dual_write_ref)=0 THEN
    RAISE EXCEPTION 'dual write authority is required' USING ERRCODE='23514';
  END IF;
  IF NEW.state='verify' AND NOT NEW.backfill_complete THEN
    RAISE EXCEPTION 'backfill must complete before verification' USING ERRCODE='23514';
  END IF;
  IF NEW.state IN ('cutover','observe','cleanup') AND (
    NEW.verify_source_count IS NULL OR NEW.verify_target_count IS NULL OR
    NEW.verify_source_count<>NEW.verify_target_count OR
    NEW.verify_source_digest !~ '^[0-9a-f]{64}$' OR NEW.verify_target_digest<>NEW.verify_source_digest OR
    length(NEW.verify_source_watermark)=0 OR NEW.verify_target_watermark<>NEW.verify_source_watermark OR
    NEW.verify_sample_digest !~ '^[0-9a-f]{64}$' OR
    NEW.cutover_config_version IS NULL OR NEW.cutover_config_version<>NEW.target_config_version OR
    NEW.cutover_at IS NULL) THEN
    RAISE EXCEPTION 'verification evidence is incomplete' USING ERRCODE='23514';
  END IF;
  IF NEW.state IN ('observe','cleanup') AND (NEW.observe_until IS NULL OR NEW.observe_until <= NEW.cutover_at) THEN
    RAISE EXCEPTION 'observation window is invalid' USING ERRCODE='23514';
  END IF;
  IF NEW.state='cleanup' AND (NEW.updated_at < NEW.observe_until OR NEW.rollback_sync_watermark <> NEW.verify_target_watermark) THEN
    RAISE EXCEPTION 'rollback sync is incomplete' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;



ALTER TABLE public.tenant
  ADD COLUMN redaction_rules jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD CONSTRAINT tenant_redaction_rules_array_check CHECK (jsonb_typeof(redaction_rules) = 'array');

DROP FUNCTION public.update_tenant_configuration(text, bigint, text, bigint, integer, bigint, bigint, character, integer, text, text, numeric, text, text, bigint, text, text, text, text, text);

CREATE FUNCTION public.update_tenant_configuration(p_tenant_id text, p_expected_version bigint, p_display_name text, p_request_limit_per_minute bigint, p_max_concurrent_executions integer, p_monthly_token_budget bigint, p_monthly_cost_budget_micros bigint, p_billing_currency character, p_audit_retention_days integer, p_audit_payload_mode text, p_log_masking_level text, p_redaction_rules jsonb, p_trace_sampling_rate numeric, p_default_agent_app_id text, p_default_backend_profile_id text, p_active_config_version bigint, p_actor_id text, p_reason_code text, p_correlation_id text, p_trace_id text, p_traceparent text) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
AS $$
DECLARE
  v_status text;
  v_version bigint;
  v_next_version bigint;
BEGIN
  IF p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0
     OR p_reason_code IS NULL OR length(btrim(p_reason_code)) = 0
     OR p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0
     OR p_trace_id IS NULL OR length(btrim(p_trace_id)) = 0 THEN
    RAISE EXCEPTION 'tenant configuration metadata is incomplete' USING ERRCODE = '22023';
  END IF;
  IF jsonb_typeof(p_redaction_rules) <> 'array' OR jsonb_array_length(p_redaction_rules) > 32 THEN
    RAISE EXCEPTION 'tenant redaction rules are invalid' USING ERRCODE = '22023';
  END IF;
  SELECT status, version INTO v_status, v_version FROM public.tenant
    WHERE tenant_id = p_tenant_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist' USING ERRCODE = 'P0002'; END IF;
  IF v_status = 'disabled' THEN RAISE EXCEPTION 'disabled tenant is immutable' USING ERRCODE = '55000'; END IF;
  IF v_version <> p_expected_version THEN RAISE EXCEPTION 'tenant version conflict' USING ERRCODE = '40001'; END IF;
  v_next_version := v_version + 1;
  UPDATE public.tenant SET
    display_name = p_display_name,
    request_limit_per_minute = p_request_limit_per_minute,
    max_concurrent_executions = p_max_concurrent_executions,
    monthly_token_budget = p_monthly_token_budget,
    monthly_cost_budget_micros = p_monthly_cost_budget_micros,
    billing_currency = p_billing_currency,
    audit_retention_days = p_audit_retention_days,
    audit_payload_mode = p_audit_payload_mode,
    log_masking_level = p_log_masking_level,
    redaction_rules = p_redaction_rules,
    trace_sampling_rate = p_trace_sampling_rate,
    default_agent_app_id = p_default_agent_app_id,
    default_backend_profile_id = p_default_backend_profile_id,
    active_config_version = p_active_config_version,
    version = v_next_version
  WHERE tenant_id = p_tenant_id;
  INSERT INTO public.outbox(tenant_id, outbox_id, kind, aggregate_id, event_seq, idempotency_key, payload_ref, traceparent)
  VALUES
    (p_tenant_id, format('tenant-config-audit:%s:%s', p_tenant_id, v_next_version), 'audit', p_tenant_id, v_next_version, format('tenant-config:%s:%s:audit', p_tenant_id, v_next_version), format('tenant-config://%s/%s', p_tenant_id, v_next_version), p_traceparent),
    (p_tenant_id, format('tenant-config-control:%s:%s', p_tenant_id, v_next_version), 'tenant-control', p_tenant_id, v_next_version, format('tenant-config:%s:%s:control', p_tenant_id, v_next_version), format('tenant-config://%s/%s', p_tenant_id, v_next_version), p_traceparent);
  RETURN v_next_version;
END;
$$;



CREATE TABLE public.backend_migration_phase_intent (
  tenant_id text NOT NULL,
  migration_id text NOT NULL,
  intent_id text NOT NULL,
  request jsonb NOT NULL,
  request_digest text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  result_version bigint NOT NULL DEFAULT 0,
  created_at timestamp with time zone NOT NULL,
  completed_at timestamp with time zone,
  PRIMARY KEY (tenant_id, migration_id, intent_id),
  CONSTRAINT backend_migration_phase_intent_id_check CHECK (length(btrim(intent_id)) BETWEEN 1 AND 128),
  CONSTRAINT backend_migration_phase_intent_request_check CHECK (jsonb_typeof(request) = 'object'),
  CONSTRAINT backend_migration_phase_intent_digest_check CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  CONSTRAINT backend_migration_phase_intent_status_check CHECK (status = ANY (ARRAY['pending'::text, 'completed'::text])),
  CONSTRAINT backend_migration_phase_intent_completion_check CHECK (
    (status = 'pending' AND result_version = 0 AND completed_at IS NULL) OR
    (status = 'completed' AND result_version >= 2 AND completed_at IS NOT NULL)
  ),
  FOREIGN KEY (tenant_id, migration_id) REFERENCES public.backend_migration(tenant_id, migration_id)
);

CREATE INDEX backend_migration_phase_intent_pending_idx
  ON public.backend_migration_phase_intent (tenant_id, migration_id, created_at, intent_id)
  WHERE status = 'pending';

CREATE FUNCTION public.guard_backend_migration_phase_intent_update() RETURNS trigger
  LANGUAGE plpgsql
  SET search_path TO 'pg_catalog'
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'backend migration phase intents are append-only' USING ERRCODE='23000';
  END IF;
  IF (NEW.tenant_id, NEW.migration_id, NEW.intent_id, NEW.request, NEW.request_digest, NEW.created_at)
     IS DISTINCT FROM
     (OLD.tenant_id, OLD.migration_id, OLD.intent_id, OLD.request, OLD.request_digest, OLD.created_at) THEN
    RAISE EXCEPTION 'backend migration phase intent identity is immutable' USING ERRCODE='23000';
  END IF;
  IF OLD.status <> 'pending' OR NEW.status <> 'completed' OR NEW.result_version < 2 OR NEW.completed_at IS NULL THEN
    RAISE EXCEPTION 'backend migration phase intent completion is invalid' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER backend_migration_phase_intent_guard
  BEFORE UPDATE OR DELETE ON public.backend_migration_phase_intent
  FOR EACH ROW EXECUTE FUNCTION public.guard_backend_migration_phase_intent_update();


COMMIT;
