"""WV-14: real Control live credentials, initialized Attempts, and fixed targets.

Call run_credentials(h) LAST, with a ready real Worker and exclusive use of the
published Profile. Live clear is intentionally terminal; the caller owns cleanup.
Only h.api writes business state. SQL reads durable execution evidence. The
external model hook records non-secret authentication generations, never keys.

Required ModelFixture additions to the existing Harness:
* rotate_key(new_key): atomically select the next accepted key generation.
* authentication_events(text): owned list of {accepted: bool, key_generation:int}
  for ALL received requests for this text, including authentication rejection.
An already-authenticated held request keeps its original generation after rotate.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import re
from urllib.parse import urlsplit, urlunsplit
import uuid


def credential_update(view, category, resource, purpose, action, value=None):
    """Build the public live-update DTO, not a write into a mutable Draft."""
    state = view['credential_states'][category][resource][purpose]
    if (state['status'] != 'active' or not state['configured'] or
            type(state['credential_revision']) is not int or state['credential_revision'] < 1 or
            type(view['revision_number']) is not int or view['revision_number'] < 1 or
            not re.fullmatch(r'[0-9a-f]{64}', state['association_token'])):
        raise ValueError('live update requires a current active public association')
    if action not in ('replace', 'clear') or (action == 'clear' and value is not None):
        raise ValueError('live update action/value differs')
    if action == 'replace' and (not isinstance(value, str) or not value):
        raise ValueError('live replacement requires a value')
    body = {'target': {'profile_revision_number': view['revision_number'],
        'category': category, 'resource_name': resource, 'purpose_field': purpose,
        'association_token': state['association_token']}, 'action': action,
        'expected_credential_revision': state['credential_revision']}
    if action == 'replace':
        body['value'] = value
    return body


def changed_target_dsn(dsn):
    """Use a syntactically valid different database; retain all other URI fields."""
    parsed = urlsplit(dsn)
    if parsed.scheme not in ('postgres', 'postgresql') or not parsed.hostname or not parsed.path:
        raise ValueError('fixture DSN is not a PostgreSQL destination')
    return urlunsplit(parsed._replace(path=parsed.path + '_wv14_changed'))


def public_snapshot(view):
    """Evidence excludes association capabilities and all supplied secret values."""
    states = {}
    for category, resources in view['credential_states'].items():
        states[category] = {}
        for resource, purposes in resources.items():
            states[category][resource] = {purpose: {field: state[field] for field in
                ('configured', 'status', 'credential_revision')} for purpose, state in purposes.items()}
    return {'profile_id': view['profile_id'], 'revision_number': view['revision_number'],
            'spec_digest': view['spec_digest'], 'config': copy.deepcopy(view['config']),
            'credential_states': states}


def _rows(h, query):
    raw = h.sql("SELECT COALESCE(json_agg(row_to_json(wv14_q)), '[]'::json)::text FROM (" + query + ') wv14_q')
    if len(raw) != 1 or len(raw[0]) != 1:
        raise AssertionError('WV-14 observation must be one JSON row aggregate')
    result = json.loads(raw[0][0])
    if not isinstance(result, list):
        raise AssertionError('WV-14 observation must be a row list')
    return result


def _one(h, query):
    result = _rows(h, query)
    if len(result) != 1:
        raise AssertionError('WV-14 expected exactly one durable fact')
    return result[0]


def _run(h, run_id):
    return _one(h, 'SELECT tenant_id,run_id,session_id,session_sequence,status,attempts,current_attempt_id '
                'FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))


def _attempts(h, run_id):
    return _rows(h, 'SELECT attempt_id,worker_id,generation,status,reason,agent_started_at '
                 'FROM worker.execution_attempts WHERE run_id=' + h.quote(run_id) + ' ORDER BY generation')


def _head(h, run_id):
    return _one(h, 'SELECT s.accepted_ref,s.accepted_digest,s.settled_sequence FROM '
                'worker.execution_sessions s JOIN worker.execution_runs r USING(tenant_id,session_id) '
                'WHERE r.run_id=' + h.quote(run_id))


def _completion(h, run_id):
    return _one(h, 'SELECT completion_id,attempt_id,kind,status,candidate_ref,candidate_digest,'
                'final_intent_id,reply_disposition,reason FROM worker.execution_completions '
                'WHERE run_id=' + h.quote(run_id))


def _submit(h, text, conversation):
    run_id = h.send_text(text, conversation_id=conversation)
    h.wait(lambda: bool(_rows(h, 'SELECT run_id FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))),
           'WV-14 Worker durable intake', timeout=30)
    return run_id


def _authenticated_once(h, text, generation=None):
    events = h.model.authentication_events(text)
    if len(events) != 1 or events[0].get('accepted') is not True:
        raise AssertionError('WV-14 expected exactly one accepted model authentication')
    observed = events[0].get('key_generation')
    if type(observed) is not int or observed < 1 or (generation is not None and observed != generation):
        raise AssertionError('WV-14 model used an unexpected credential generation')
    # Do not copy optional fixture internals into the artifact.
    return {'accepted': True, 'key_generation': observed}


def initialized_attempt(run, attempts):
    """Model entry proves initialization only for the current executing Attempt."""
    assert run['status'] == 'RUNNING', 'Run lifecycle differs from initialized Attempt'
    assert len(attempts) == 1 and attempts[0]['status'] == 'EXECUTING'
    assert attempts[0]['agent_started_at'] is not None
    assert run['current_attempt_id'] == attempts[0]['attempt_id']
    return copy.deepcopy(attempts[0])


def _initialized(h, run_id, text):
    h.model.wait_entered(text)
    return initialized_attempt(_run(h, run_id), _attempts(h, run_id))


def _success(h, run_id, text, generation, original_attempt=None):
    delivery = h.wait_delivery(run_id)
    assert delivery['delivery_state'] == 'ACCEPTED'
    completion = _completion(h, run_id)
    attempts = _attempts(h, run_id)
    assert completion['status'] == 'SUCCEEDED' and completion['kind'] == 'ATTEMPT'
    assert completion['reply_disposition'] == 'FINAL' and completion['candidate_ref']
    assert len(attempts) == 1 and attempts[0]['status'] == 'SUCCEEDED'
    if original_attempt is not None:
        assert completion['attempt_id'] == original_attempt['attempt_id'], 'rotation replaced the initialized Attempt'
    auth = _authenticated_once(h, text, generation)
    return {'run_id': run_id, 'attempt': attempts[0], 'completion': completion,
            'authentication': auth, 'delivery': delivery}


def _unchanged_public(before, after):
    for field in ('profile_id', 'revision_number', 'spec_digest', 'config'):
        assert before[field] == after[field], 'live credential mutation changed published configuration'


def run_credentials(h):
    """Actual public rotate / fixed-destination reject / terminal clear scenarios."""
    base = '/v1/tenants/' + h.tenant_id + '/runtime-profiles/' + h.profile_id
    # seed() publishes Profile revision 1; Deployment revision_number is a distinct
    # contract and is deliberately not used as the Profile revision here.
    profile_revision = 1
    read_path = base + '/revisions/' + str(profile_revision)
    marker = 'credential-' + uuid.uuid4().hex[:12]
    conversation = str(1400000000 + int(uuid.uuid4().hex[:7], 16))
    old_text, new_text, clear_text, denied_text = [marker + '-' + suffix for suffix in
                                               ('old-initialized', 'new-after-rotate', 'clear-initialized', 'after-clear')]
    evidence = {'version': 'worker-v1-live-credentials/v1', 'criterion': 'WV-14',
                'profile_id': h.profile_id, 'profile_revision': profile_revision,
                'manifest_id': h.manifest_id, 'manifest_digest': h.manifest_digest,
                'scenarios': [], 'events': []}
    path = Path(h.artifacts) / 'worker-live-credentials.json'

    def read():
        view = h.api('GET', read_path)
        assert view['profile_id'] == h.profile_id and view['revision_number'] == profile_revision
        return view

    def update(view, category, resource, purpose, action, value=None, status=200):
        body = credential_update(view, category, resource, purpose, action, value)
        result = h.api('POST', base + '/credentials/update', body, status=status,
                       idem=marker + '-' + str(len(evidence['events'])))
        entry = {'operation': action, 'category': category, 'resource': resource,
                 'purpose': purpose, 'expected_credential_revision': body['expected_credential_revision'],
                 'http_status': status}
        if status == 200:
            expected_status = 'active' if action == 'replace' else 'cleared'
            assert result == {'credential_revision': body['expected_credential_revision'] + 1,
                              'status': expected_status}
            entry.update(result)
        else:
            assert result['error']['code'] == 'CREDENTIAL_ASSOCIATION_CONFLICT'
            entry['error_code'] = result['error']['code']
        evidence['events'].append(entry)
        return result

    try:
        before = read()
        evidence['published_before'] = public_snapshot(before)
        h.model.hold(old_text)
        old_run = _submit(h, old_text, conversation)
        initialized = _initialized(h, old_run, old_text)
        old_auth = _authenticated_once(h, old_text)
        next_key = h.secret()
        update(before, 'models', 'primary', 'api_key', 'replace', next_key)
        h.model.rotate_key(next_key)
        rotated = read()
        _unchanged_public(before, rotated)
        assert rotated['credential_states']['models']['primary']['api_key']['credential_revision'] == \
               before['credential_states']['models']['primary']['api_key']['credential_revision'] + 1
        assert _attempts(h, old_run)[0]['attempt_id'] == initialized['attempt_id']
        assert _run(h, old_run)['status'] == 'RUNNING'
        h.model.release(old_text)
        old_result = _success(h, old_run, old_text, old_auth['key_generation'], initialized)
        evidence['scenarios'].append({'scenario': 'initialized-attempt-retains-authorized-batch-on-rotate',
            'before': public_snapshot(before), 'after': public_snapshot(rotated),
            'initialized_before_update': initialized, **old_result, 'result': 'PASS'})

        # A live replacement may change the password, never the published DSN
        # destination. The changed database is syntactically valid and receives
        # association conflict, not malformed-input rejection.
        rejected = update(rotated, 'storage', 'session', 'dsn', 'replace',
                          changed_target_dsn(h.dsns['session_runtime']), status=409)
        after_rejection = read()
        assert public_snapshot(rotated) == public_snapshot(after_rejection), 'rejected DSN update mutated current state'
        new_run = _submit(h, new_text, conversation)
        new_result = _success(h, new_run, new_text, old_auth['key_generation'] + 1)
        evidence['scenarios'].append({'scenario': 'new-attempt-uses-rotated-key-and-fixed-session-target',
            'dsn_target_change_http_status': 409, 'dsn_target_change_error': rejected['error']['code'],
            'public_state_unchanged_by_rejection': True, **new_result, 'result': 'PASS'})

        h.model.hold(clear_text)
        clear_run = _submit(h, clear_text, conversation)
        clear_attempt = _initialized(h, clear_run, clear_text)
        _authenticated_once(h, clear_text, old_auth['key_generation'] + 1)
        update(after_rejection, 'models', 'primary', 'api_key', 'clear')
        # Both credentials belonged to the same already-initialized batch. The
        # storage clear also tests the existing pool's later Session candidate
        # write, not merely a model HTTP response authenticated before clear.
        update(read(), 'storage', 'session', 'dsn', 'clear')
        cleared = read()
        _unchanged_public(before, cleared)
        for category, resource, purpose in [('models', 'primary', 'api_key'), ('storage', 'session', 'dsn')]:
            state = cleared['credential_states'][category][resource][purpose]
            assert state['status'] == 'cleared' and state['configured'] is False
        assert _run(h, clear_run)['status'] == 'RUNNING'
        h.model.release(clear_text)
        clear_result = _success(h, clear_run, clear_text, old_auth['key_generation'] + 1, clear_attempt)
        evidence['scenarios'].append({'scenario': 'initialized-attempt-completes-after-both-credentials-cleared',
            'after': public_snapshot(cleared), 'initialized_before_update': clear_attempt,
            **clear_result, 'result': 'PASS'})

        accepted_before_denial = _head(h, clear_run)
        denied_run = _submit(h, denied_text, conversation)
        h.wait(lambda: _run(h, denied_run)['status'] in ('SUCCEEDED', 'FAILED'),
               'WV-14 post-clear permanent credential denial', timeout=40)
        denied_completion = _completion(h, denied_run)
        denied_attempts = _attempts(h, denied_run)
        assert _run(h, denied_run)['status'] == 'FAILED'
        assert denied_completion['kind'] == 'ATTEMPT' and denied_completion['status'] == 'FAILED'
        assert denied_completion['reason'] == 'CREDENTIAL_DENIED'
        assert not denied_completion['candidate_ref'] and not denied_completion['candidate_digest']
        assert len(denied_attempts) == 1 and denied_attempts[0]['agent_started_at'] is None
        assert h.model.authentication_events(denied_text) == [], 'cleared credential reached external model'
        candidates = _one(h, 'SELECT count(*) AS n FROM runtime_session.session_candidates WHERE run_id=' + h.quote(denied_run))
        assert candidates['n'] == 0
        accepted_after_denial = _head(h, denied_run)
        for field in ('accepted_ref', 'accepted_digest'):
            assert accepted_before_denial[field] == accepted_after_denial[field], 'credential denial changed accepted Session'
        assert accepted_after_denial['settled_sequence'] == accepted_before_denial['settled_sequence'] + 1
        # Existing Worker contract permits one platform-fixed error Final for an
        # authentic failed Attempt. It is not a fabricated model answer, and is
        # distinct from the no-Attempt system terminalizer's NONE disposition.
        assert denied_completion['reply_disposition'] == 'FINAL'
        failed_delivery = h.gateway.wait_delivery(denied_run)
        assert failed_delivery['final_text'] == '本次执行未完成，请稍后重试。'
        evidence['scenarios'].append({'scenario': 'new-attempt-denied-after-clear-without-model-or-session-write',
            'run_id': denied_run, 'attempt': denied_attempts[0], 'completion': denied_completion,
            'model_http_attempt_count': 0, 'candidate_count': 0,
            'accepted_head_before': accepted_before_denial, 'accepted_head_after': accepted_after_denial,
            'delivery': failed_delivery, 'result': 'PASS'})
        evidence['published_after'] = public_snapshot(read())
        evidence['result'] = 'PASS'
        print('WORKER_LIVE_CREDENTIALS=PASS actual Control rotate/clear/DSN target rejection; '
              'initialized batches retained; new Attempt denied with fixed error Final; zero secret evidence', flush=True)
        return evidence
    except BaseException:
        evidence['result'] = 'FAIL'
        raise
    finally:
        h.model.release(old_text)
        h.model.release(clear_text)
        path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
        if json.loads(path.read_text()) != evidence:
            raise AssertionError('WV-14 evidence readback mismatch')
