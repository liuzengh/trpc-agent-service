"""WV-31/36/37: actual dual-process scheduling across immutable DB deadlines.

Only real Gateway input admits Runs. No fixture rewrites business timestamps,
status, Manifest, accepted head or execution token. Short windows are explicit
Worker configuration copied at intake. Every wait asserts database facts, not
elapsed Python sleep. Restore a standard Worker in each scenario's finally.
"""
from __future__ import annotations

from datetime import timedelta
import json
from pathlib import Path
import uuid

from faults import (attempts, candidates, head, kill_holder, model_calls, one,
                    outboxes, parse_time, rows, submit, wait_success)
from scope_scenarios import assert_transcript
from session_scenarios import session_login_outage


def run_fact(h, run_id):
    return one(h, "SELECT tenant_id,run_id,session_id,session_sequence,status,wait_reason,"
               "attempts,current_attempt_id,accepted_at,run_deadline,reply_deadline,execution_deadline,retry_at,"
               "policy_json,request_json->'Input'->>'ReceivedAt' AS received_at,clock_timestamp() AS db_now,"
               "run_deadline<=clock_timestamp() AS run_expired,"
               "COALESCE(execution_deadline<=clock_timestamp(),false) AS execution_expired "
               "FROM worker.execution_runs WHERE run_id="+h.quote(run_id))


def completion_facts(h, run_id):
    return rows(h, "SELECT completion_id,attempt_id,kind,status,candidate_ref,candidate_digest,"
                "final_intent_id,reply_disposition,reason,completed_at FROM worker.execution_completions "
                "WHERE run_id="+h.quote(run_id))


def assert_fixed_window(before, after):
    for field in ('run_id','session_id','session_sequence','received_at','accepted_at','run_deadline',
                  'execution_deadline','reply_deadline','policy_json'):
        assert before[field] == after[field], 'retry/restart changed fixed Run field '+field


def assert_queued_expired(record, attempt_rows):
    assert record['status']=='QUEUED' and record['attempts']==0 and not attempt_rows
    assert record['execution_deadline'] is None, 'never-claimed queue item has execution deadline'
    assert record['run_expired'] is True and parse_time(record['db_now'])>=parse_time(record['run_deadline'])


def assert_system_terminal(record, completion_rows, candidate_rows, final_rows, before, after):
    assert record['status']=='FAILED' and len(completion_rows)==1
    completion=completion_rows[0]
    assert completion['kind']=='SYSTEM_TERMINATION' and completion['status']=='FAILED'
    assert completion['reason']=='DEADLINE_EXPIRED' and completion['reply_disposition']=='NONE'
    assert not completion['candidate_ref'] and not completion['candidate_digest'] and not completion['final_intent_id']
    assert candidate_rows==[] and final_rows==[]
    deadline=min(parse_time(record[field]) for field in ('run_deadline','execution_deadline') if record[field] is not None)
    assert parse_time(completion['completed_at'])>=deadline
    assert before['accepted_ref']==after['accepted_ref'] and before['accepted_digest']==after['accepted_digest']
    assert after['settled_sequence']==record['session_sequence']


def policy(max_age, retry_backoff='200ms'):
    return {'policy':{'max_run_age':max_age,'max_reply_age':'3m','max_attempts':4,'retry_backoff':retry_backoff}}


def pair(h, overrides):
    h.stop_fault_workers()
    workers={name:h.start_worker(name,overrides) for name in ('worker-one','worker-two')}
    assert len({process.pid for process in workers.values()})==2
    assert all(process.poll() is None for process in workers.values())
    return workers


def restore_standard(h):
    h.stop_fault_workers()
    process=h.start_worker()
    return {'worker_id':'worker-one','pid':process.pid,'configuration':'standard harness defaults','ready':process.poll() is None}


def observe_one_live(h, run_id):
    record=run_fact(h,run_id)
    live=one(h,"SELECT count(*) AS n FROM worker.execution_attempts a JOIN worker.execution_runs r "
             "ON r.tenant_id=a.tenant_id AND r.run_id=a.run_id WHERE r.tenant_id="+h.quote(record['tenant_id'])+
             " AND r.session_id="+h.quote(record['session_id'])+" AND a.status IN ('PREPARING','EXECUTING') AND a.lease_until>clock_timestamp()")
    assert live['n']==1 and record['status']=='RUNNING'
    return {'run':record,'same_session_live_attempt_count':live['n'],'attempts':attempts(h,run_id)}


def manifest_window(h):
    result=one(h,"SELECT content_digest,convert_from(envelope,'UTF8')::json->'content'->'execution' AS execution "
               "FROM worker.runtime_manifests WHERE tenant_id="+h.quote(h.tenant_id)+' AND manifest_id='+h.quote(h.manifest_id))
    assert result['content_digest']==h.manifest_digest and result['execution']['max_run_seconds']>0
    return {'manifest_id':h.manifest_id,'manifest_digest':h.manifest_digest,'max_run_seconds':result['execution']['max_run_seconds']}


def assert_initial_deadline(record, attempt, manifest):
    received=parse_time(record['received_at'])
    assert parse_time(record['run_deadline'])==received+timedelta(microseconds=record['policy_json']['MaxRunAge']//1000)
    expected=min(parse_time(record['run_deadline']),parse_time(attempt['created_at'])+timedelta(seconds=manifest['max_run_seconds']))
    assert parse_time(record['execution_deadline'])==expected
    assert parse_time(attempt['lease_until'])<=expected


def wait_status(h,run_id,status,timeout=35):
    return h.wait(lambda:(lambda value:value if value['status']==status else None)(run_fact(h,run_id)),
                  'real durable '+status+' for '+run_id,timeout=timeout)


def scheduling_records(h, run_id):
    found=[]
    for path in sorted(Path(h.artifacts).glob('worker-*.log')):
        for line in path.read_text(errors='replace').splitlines():
            try:value=json.loads(line)
            except ValueError:continue
            if value.get('msg')=='worker.operation' and value.get('run_id')==run_id and value.get('operation') in ('claim','terminalize'):
                found.append(value)
    return found


def terminal_snapshot(h, run_id, before):
    record=wait_status(h,run_id,'FAILED')
    completion=completion_facts(h,run_id);candidate=candidates(h,run_id);final=outboxes(h,run_id);after=head(h,record)
    assert_system_terminal(record,completion,candidate,final,before,after)
    trace=h.wait(lambda:(lambda values:values if any(v.get('operation')=='terminalize' and v.get('result')=='ok' for v in values) else None)(scheduling_records(h,run_id)), 'actual typed SystemTerminalizer success after deadline')
    successful_claims=[v for v in trace if v.get('operation')=='claim' and v.get('result')=='ok']
    terminals=[v for v in trace if v.get('operation')=='terminalize' and v.get('result')=='ok']
    assert len(successful_claims)==record['attempts'] and len(terminals)==1
    deadline=min(parse_time(record[field]) for field in ('run_deadline','execution_deadline') if record[field] is not None)
    assert all(parse_time(v['time'])<deadline for v in successful_claims)
    return {'run':record,'completion':completion[0],'attempts':attempts(h,run_id),'head':after,
            'candidate_count':len(candidate),'final_count':len(final),'scheduling_trace':trace}


def fresh_follower(h, text, conversation, expected_users, parent):
    run_id=submit(h,text,conversation)
    result=wait_success(h,run_id);delivery=h.wait_delivery(run_id)
    calls=model_calls(h,text);assert len(calls)==1
    assert_transcript(calls[0],expected_users+[text])
    assert result['candidate']['parent_ref']==parent['accepted_ref']
    assert result['candidate']['parent_digest']==parent['accepted_digest']
    return {'run_id':run_id,'run':run_fact(h,run_id),'completion':result['completion'],
            'candidate':result['candidate'],'delivery':delivery,'sdk_request':calls[0]}


def seed(h,text,conversation):
    run_id=submit(h,text,conversation)
    result=wait_success(h,run_id);delivery=h.wait_delivery(run_id)
    return {'run_id':run_id,'head':head(h,run_fact(h,run_id)),'candidate':result['candidate'],'delivery':delivery}


def retry_fixed_deadline(h,record):
    manifest=manifest_window(h)
    overrides=policy(str(manifest['max_run_seconds']+30)+'s');workers=pair(h,overrides)
    original_hold_timeout=h.model.hold_timeout_seconds
    h.model.hold_timeout_seconds=manifest['max_run_seconds']+30
    marker='deadline-retry-'+uuid.uuid4().hex[:10];conversation=str(2600000000+int(uuid.uuid4().hex[:7],16))
    seed_text,victim,follower=marker+'-seed',marker+'-victim',marker+'-follower'
    record.update(model_hold_timeout_seconds=h.model.hold_timeout_seconds,scenario='cross-attempt-fixed-deadline-and-live-lease-expiry',invariants=['WV-31','WV-36','WV-37'],processes={k:p.pid for k,p in workers.items()})
    try:
        accepted=seed(h,seed_text,conversation);record['seed']=accepted
        h.model.hold(victim);victim_run=submit(h,victim,conversation);h.model.wait_entered(victim)
        before=observe_one_live(h,victim_run);assert len(before['attempts'])==1
        assert_initial_deadline(before['run'],before['attempts'][0],manifest)
        assert parse_time(before['run']['execution_deadline'])<parse_time(before['run']['run_deadline']), 'execution window must be independently observable, not hidden by RunDeadline clamp'
        record.update(victim_run_id=victim_run,manifest=manifest,first=before)
        killed=kill_holder(h,workers,victim_run,before['attempts'][0]);record['kill']=killed
        h.wait(lambda:len(model_calls(h,victim))==2,'second actual Worker SDK request within fixed execution deadline',timeout=10)
        retry=observe_one_live(h,victim_run);record['retry']=retry
        assert_fixed_window(before['run'],retry['run']);assert len(retry['attempts'])==2
        old,new=retry['attempts'];assert old['status']=='ABORTED' and old['reason']=='LEASE_EXPIRED'
        assert new['worker_id']==killed['survivor_worker_id'] and new['status']=='EXECUTING'
        assert parse_time(new['created_at'])>=parse_time(killed['lease_until_at_process_exit'])
        assert new['generation']==old['generation']+1 and new['lease_epoch']==old['lease_epoch']+1
        assert parse_time(new['lease_until'])<=parse_time(before['run']['execution_deadline'])
        restarted=h.start_worker(killed['killed_worker_id'],overrides)
        record['competing_processes']={k:p.pid for k,p in h.workers.items()};assert restarted.poll() is None
        assert all(p.poll() is None for p in h.workers.values())
        # The renewed lease must itself reach the immutable execution deadline,
        # so this tests simultaneous lease/execution-window expiry, not only a stale dead PID.
        current=run_fact(h,victim_run)
        remaining=max(0,(parse_time(current['execution_deadline'])-parse_time(current['db_now'])).total_seconds())
        near=h.wait(lambda:(lambda aa:aa[-1] if aa and aa[-1]['status']=='EXECUTING' and
                        parse_time(aa[-1]['lease_until'])==parse_time(before['run']['execution_deadline']) else None)(attempts(h,victim_run)),
                    'actual renewal clamped to fixed execution deadline',timeout=remaining+10)
        record['last_live_lease_at_deadline']=near
        ended=terminal_snapshot(h,victim_run,accepted['head']);record['terminal']=ended
        assert_fixed_window(before['run'],ended['run'])
        assert ended['run']['execution_expired'] and not ended['run']['run_expired']
        assert len(ended['attempts'])==2 and ended['attempts'][-1]['status']=='ABORTED'
        assert ended['attempts'][-1]['reason']=='DEADLINE_EXPIRED'
        assert len(model_calls(h,victim))==2
        h.model.release(victim)
        record['follower']=fresh_follower(h,follower,conversation,[seed_text],accepted['head'])
        record['sdk_calls']=2;record['result']='PASS'
    finally:
        h.model.release(victim)
        h.model.hold_timeout_seconds=original_hold_timeout
        record['restoration']=restore_standard(h)
        record['restoration']['model_hold_timeout_seconds']=original_hold_timeout


def retry_wait_deadline(h,record):
    workers=pair(h,policy('8s','20s'))
    marker='deadline-backoff-'+uuid.uuid4().hex[:10];conversation=str(2900000000+int(uuid.uuid4().hex[:7],16))
    seed_text,victim,follower=marker+'-seed',marker+'-victim',marker+'-follower'
    record.update(scenario='retry-wait-expires-before-backoff',invariants=['WV-31','WV-36','WV-37'],processes={k:p.pid for k,p in workers.items()},events=[])
    try:
        accepted=seed(h,seed_text,conversation);record['seed']=accepted
        with session_login_outage(h,record['events']):
            victim_run=submit(h,victim,conversation)
            waiting=wait_status(h,victim_run,'RETRY_WAIT',timeout=5)
            aa=attempts(h,victim_run);assert len(aa)==1 and aa[0]['status']=='FAILED' and aa[0]['reason']=='DEPENDENCY_UNAVAILABLE'
            assert aa[0]['agent_started_at'] is None and model_calls(h,victim)==[]
            assert parse_time(waiting['retry_at'])>parse_time(waiting['run_deadline'])
            assert waiting['policy_json']['RetryBackoff']==20000000000
            assert_initial_deadline(waiting,aa[0],manifest_window(h))
            record.update(victim_run_id=victim_run,retry_wait=waiting,failed_attempt=aa[0])
        record['session_login_restored']=h.sql("SELECT rolcanlogin FROM pg_roles WHERE rolname='session_runtime'")==[['t']]
        assert record['session_login_restored']
        ended=terminal_snapshot(h,victim_run,accepted['head']);record['terminal']=ended
        assert_fixed_window(waiting,ended['run'])
        assert parse_time(ended['completion']['completed_at'])<parse_time(waiting['retry_at'])
        assert len(ended['attempts'])==1 and model_calls(h,victim)==[]
        record['follower']=fresh_follower(h,follower,conversation,[seed_text],accepted['head'])
        record['sdk_calls']=0;record['result']='PASS'
    finally:
        record['restoration']=restore_standard(h)


def queued_deadline(h,record):
    workers=pair(h,policy('30s'))
    marker='deadline-queue-'+uuid.uuid4().hex[:10];conversation=str(3200000000+int(uuid.uuid4().hex[:7],16))
    seed_text,blocker,expired,follower=marker+'-seed',marker+'-blocker',marker+'-expired',marker+'-follower'
    record.update(scenario='queued-run-expires-behind-live-session-head',invariants=['WV-31','WV-36','WV-37'])
    try:
        accepted=seed(h,seed_text,conversation);record['seed']=accepted
        h.model.hold(blocker);blocker_run=submit(h,blocker,conversation);h.model.wait_entered(blocker)
        original=observe_one_live(h,blocker_run);record['blocker_original']=original
        killed=kill_holder(h,workers,blocker_run,original['attempts'][0]);record['kill']=killed
        short=pair(h,policy('6s'));record['competing_processes']={k:p.pid for k,p in short.items()}
        h.wait(lambda:len(model_calls(h,blocker))==2,'same older Run resumes under new process config',timeout=10)
        resumed=observe_one_live(h,blocker_run);record['blocker_resumed']=resumed
        assert_fixed_window(original['run'],resumed['run'])
        assert resumed['run']['policy_json']['MaxRunAge']==30000000000
        expired_run=submit(h,expired,conversation);queued=run_fact(h,expired_run)
        assert queued['status']=='QUEUED' and queued['attempts']==0 and queued['execution_deadline'] is None
        assert queued['policy_json']['MaxRunAge']==6000000000
        assert parse_time(queued['run_deadline'])<parse_time(resumed['run']['execution_deadline'])
        record.update(expired_run_id=expired_run,queued_before_expiry=queued)
        crossed=h.wait(lambda:(lambda r:r if r['run_expired'] else None)(run_fact(h,expired_run)),
                       'database observes queued Run crossing its own fixed deadline',timeout=10)
        assert_queued_expired(crossed,attempts(h,expired_run));record['queued_after_deadline']=crossed
        active=observe_one_live(h,blocker_run);record['blocker_still_live_after_queue_expiry']=active
        assert not active['run']['execution_expired']
        assert completion_facts(h,expired_run)==[] and model_calls(h,expired)==[]
        h.model.release(blocker)
        success=wait_success(h,blocker_run);blocker_delivery=h.wait_delivery(blocker_run)
        # Claim and SystemTerminalizer now see an eligible queue head, but its
        # own deadline is already past. Neither Worker may create its Attempt.
        parent={'accepted_ref':success['candidate']['candidate_ref'],'accepted_digest':success['candidate']['content_digest']}
        ended=terminal_snapshot(h,expired_run,parent);record['terminal']=ended
        assert_fixed_window(queued,ended['run']);assert ended['run']['attempts']==0 and ended['attempts']==[]
        assert ended['run']['execution_deadline'] is None and model_calls(h,expired)==[]
        record.update(blocker_completion=success['completion'],blocker_delivery=blocker_delivery)
        record['follower']=fresh_follower(h,follower,conversation,[seed_text,blocker],parent)
        record['expired_sdk_calls']=0;record['result']='PASS'
    finally:
        h.model.release(blocker)
        record['restoration']=restore_standard(h)


def run_deadline(h):
    evidence={'version':'worker-v1-deadlines/v1','scenarios':[],
              'business_timestamp_status_writes':False,'manifest_or_token_changes':False,
              'clock':'PostgreSQL clock_timestamp and immutable Run/Attempt facts'}
    path=Path(h.artifacts)/'worker-deadline-scenarios.json'
    try:
        for scenario in (retry_fixed_deadline,retry_wait_deadline,queued_deadline):
            record={};evidence['scenarios'].append(record)
            scenario(h,record)
            print('WORKER_DEADLINE_CASE=PASS '+record['scenario'],flush=True)
        evidence['result']='PASS'
        print('WORKER_DEADLINE_MATRIX=PASS immutable windows across real retry; dual-process deadline terminalization; RETRY_WAIT deadline before backoff; queued expiry without Attempt; unchanged accepted history and fresh followers',flush=True)
        return evidence
    except BaseException:
        evidence['result']='FAIL'
        raise
    finally:
        path.write_text(json.dumps(evidence,indent=2)+'\n')
        assert json.loads(path.read_text())==evidence
