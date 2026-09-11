"""Read-only correlation of a real deployment's IM/provider evidence and Trace.

This verifier does not send Telegram updates, invoke a model, or write business
rows. Evidence files must come from the running real ingress/provider relay;
unit fixtures validate assertions only and are never live acceptance evidence.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import re
from pathlib import Path
import subprocess
from urllib.parse import urlsplit


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def credential_values(path):
    """Read explicitly supplied dotenv secrets without evaluating shell syntax."""
    values = []
    for line in Path(path).read_text().splitlines():
        match = re.match(r"\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)", line)
        if match and any(word in match[1].upper() for word in ('KEY','TOKEN','PASSWORD','SECRET','API')):
            value = match[2].strip().strip("\"'")
            if len(value) >= 8:
                values.append(value)
    return values


def external_evidence(directory, text):
    directory = Path(directory)
    deployment = json.loads((directory / 'deployment.json').read_text())
    origin = urlsplit(deployment['real_provider'])
    assert deployment['real_telegram'] is True
    assert origin.scheme == 'https' and origin.hostname and origin.hostname not in ('localhost', '127.0.0.1', '::1')
    requests = json.loads((directory / 'provider-requests.json').read_text())
    responses = json.loads((directory / 'provider-responses.json').read_text())
    assert len(requests) == len(responses), 'provider call still in flight or unmatched evidence'
    matches = []
    for index, request in enumerate(requests):
        users = [m.get('content') for m in request.get('messages', []) if m.get('role') == 'user']
        if users and users[-1] == text:
            matches.append(index)
    assert len(matches) == 1, 'expected exactly one real provider call for this input'
    index = matches[0]
    request, response = requests[index], responses[index]
    assert request['model'] == deployment['model'] and request['stream'] is True
    assert response['http_status'] == 200 and response['bytes'] > 0
    assert response['final_text'] and len(response['sha256']) == 64
    usage = response['usage']
    assert usage['prompt_tokens'] > 0 and usage['completion_tokens'] > 0
    incoming = json.loads((directory / 'telegram-ingress.json').read_text())
    matching = [e for e in incoming if e.get('update', {}).get('message', {}).get('text') == text]
    assert matching and all(e['gateway_status'] == 200 for e in matching)
    assert len({e['update']['update_id'] for e in matching}) == 1, 'ambiguous Telegram input'
    first = matching[0]
    result = {'real_provider_host': origin.hostname, 'model': request['model'], 'provider_http_status': 200,
              'provider_call_index': index, 'provider_call_count': 1, 'provider_sse_sha256': response['sha256'],
              'usage': {k: usage[k] for k in ('prompt_tokens', 'completion_tokens', 'total_tokens')},
              'telegram_update_id': first['update']['update_id'], 'telegram_message_id': first['update']['message']['message_id'],
              'ingress_raw_sha256': first['raw_sha256'], 'input_sha256': digest(text), 'final_sha256': digest(response['final_text'])}
    return result, response['final_text'], deployment


class ReadOnlyDatabase:
    def __init__(self, container, artifacts, secrets):
        self.container, self.artifacts, self.secrets = container, Path(artifacts), secrets

    @staticmethod
    def quote(value):
        return "'" + str(value).replace("'", "''") + "'"

    def sql(self, query):
        if not query.lstrip().upper().startswith('SELECT '):
            raise ValueError('evidence queries must be SELECT')
        cmd = ['docker', 'exec', '-e', 'PGOPTIONS=-c default_transaction_read_only=on', self.container,
               'psql', '-X', '-A', '-t', '-F', '\t', '-v', 'ON_ERROR_STOP=1', '-U', 'platform_admin', '-d', 'agent_platform', '-c', query]
        result = subprocess.run(cmd, text=True, capture_output=True, timeout=15)
        if result.returncode:
            raise RuntimeError('read-only database evidence query failed')
        return [line.split('\t') for line in result.stdout.splitlines() if line]


def verify_business(db, text, final):
    rows = db.sql("SELECT run_id,status FROM worker.execution_runs WHERE request_json->'Input'->>'Text'=" + db.quote(text))
    assert len(rows) == 1 and rows[0][1] == 'SUCCEEDED'
    run = rows[0][0]; q = db.quote(run)
    formal = db.sql("SELECT c.candidate_ref,c.candidate_digest,s.accepted_ref,s.accepted_digest,c.kind,c.status,c.reply_disposition,encode(sha256(k.content),'hex'),k.content_digest FROM worker.execution_completions c JOIN worker.execution_runs r USING(tenant_id,run_id) JOIN worker.execution_sessions s USING(tenant_id,session_id) JOIN runtime_session.session_candidates k ON k.tenant_id=c.tenant_id AND k.candidate_ref=c.candidate_ref AND k.run_id=c.run_id AND k.attempt_id=c.attempt_id WHERE c.run_id=" + q)
    assert len(formal) == 1
    row = formal[0]
    assert row[0] and row[0:2] == row[2:4], 'test run is not current formally accepted Session head'
    assert row[4:7] == ['ATTEMPT', 'SUCCEEDED', 'FINAL']
    assert row[1] == row[8] == 'sha256:' + row[7], 'formal candidate digest mismatch'
    counts = db.sql("SELECT (SELECT count(*) FROM runtime_session.session_candidates WHERE run_id="+q+"),(SELECT count(*) FROM worker.execution_completions WHERE run_id="+q+"),(SELECT count(*) FROM worker.execution_reply_outbox WHERE run_id="+q+")")
    assert counts == [['1','1','1']]
    parts = db.sql("SELECT p.state,encode(convert_to(p.body,'UTF8'),'hex'),a.result->>'Certainty',a.result->>'ProviderMessageID' FROM gateway.gateway_delivery_intents i JOIN gateway.gateway_delivery_parts p USING(intent_id) JOIN gateway.gateway_delivery_attempts a ON a.attempt_id=p.current_attempt_id WHERE i.run_id=" + q + ' ORDER BY p.part_index')
    assert parts and all(p[0] == p[2] == 'ACCEPTED' and p[3] for p in parts)
    assert ''.join(bytes.fromhex(p[1]).decode() for p in parts) == final, 'delivered text differs from real provider Final'
    return run, {'formal_candidate_ref': row[0], 'formal_candidate_digest': row[1], 'accepted_session_head': True,
                 'candidate_count': 1, 'completion_count': 1, 'reply_count': 1,
                 'provider_message_ids': [p[3] for p in parts], 'all_parts_accepted': True, 'final_matches_provider': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--pg-container', required=True)
    parser.add_argument('--input-text', required=True)
    parser.add_argument('--tempo-url', required=True)
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--secrets-env', type=Path, help='explicit dotenv file; values are scanned in exported Trace and never recorded')
    args = parser.parse_args()
    external, final, deployment = external_evidence(args.evidence_dir, args.input_text)
    args.artifacts.mkdir(parents=True, exist_ok=True)
    secrets = credential_values(args.secrets_env) if args.secrets_env else []
    db = ReadOnlyDatabase(args.pg_container, args.artifacts, [args.input_text, final, *secrets])
    run, business = verify_business(db, args.input_text, final)
    source = Path(__file__).resolve().parents[1] / 'test-im-tracing-v1.py'
    spec = importlib.util.spec_from_file_location('im_tracing_process_gate', source)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    trace = module.verify_run(db, args.tempo_url, run)
    result = {'result': 'PASS', 'gate': 'T15', 'run_id': run, 'external': external, 'business': business, 'trace': trace,
              'credential_scan': {'provided': args.secrets_env is not None, 'values_checked': len(secrets), 'values_recorded': False},
              'business_writes': False, 'evidence_origin': 'operator-owned real ingress/provider relay; not generated by this verifier'}
    (args.artifacts / 'tracing-live-result.json').write_text(json.dumps(result, indent=2) + '\n')
    print('TRACING_LIVE=PASS real_telegram_ingress=true real_provider_http200=true formal_session=true accepted_reply=true same_trace=true parentage=true body_absent=true')


if __name__ == '__main__':
    main()
