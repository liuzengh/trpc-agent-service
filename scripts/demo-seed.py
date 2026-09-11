#!/usr/bin/env python3
"""Provision one reviewable tenant on a running reviewer demo stack.

Creates a second operator account, one tenant, one published Agent version, one
published Runtime Profile revision, one published Deployment revision, one
Channel Lab bot, and an enabled channel account plus binding. It never reuses or
rewrites an existing tenant, and it refuses to run twice for the same state.

Model choice: by default the Agent runs against the Channel Lab deterministic
`lab-echo` model, so a reviewer needs no external credentials. Set
DEMO_MODEL_BASE_URL, DEMO_MODEL_NAME and DEMO_MODEL_API_KEY to point at a real
OpenAI-compatible model instead.
"""
import argparse
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import stat
import sys
import urllib.error
import urllib.request


class Failure(Exception):
    pass


def private_json(path):
    path = Path(path)
    if stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise Failure('private state file permissions are too open')
    return json.loads(path.read_text())


def save_private(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.tmp')
    handle = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(handle, 'w') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def secret_file(path):
    value = Path(path).read_text().removesuffix('\n')
    if not value or '\n' in value or '\r' in value:
        raise Failure('invalid secret file')
    return value


class API:
    """Cookie-jar HTTP client; failures report status only, never a body."""

    def __init__(self, base):
        self.base = base.rstrip('/')
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
            urllib.request.ProxyHandler({}),
        )

    def call(self, method, path, body=None, expected=(200,), idem=None):
        encoded = None if body is None else json.dumps(body).encode()
        headers = {'Content-Type': 'application/json'}
        if idem:
            headers['Idempotency-Key'] = idem
        request = urllib.request.Request(self.base + path, data=encoded, method=method, headers=headers)
        try:
            response = self.opener.open(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read(64 * 1024)
            status = response.status
        if status not in expected:
            raise Failure(method + ' ' + path + ': ' + str(status) + ' expected ' + str(expected))
        return json.loads(raw) if raw else None


def lab_bot(lab_url, name):
    """Create one bot through the Lab's public HTTP API, which is Origin-checked."""
    origin = lab_url.rstrip('/')
    request = urllib.request.Request(
        origin + '/lab/bots',
        data=json.dumps({'name': name}).encode(),
        method='POST',
        headers={'Content-Type': 'application/json', 'Origin': origin},
    )
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=30) as response:
        return json.loads(response.read())


def lab_model_key(lab_url):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(lab_url.rstrip('/') + '/lab/state', timeout=30) as response:
        return json.loads(response.read())['model_key']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--control-url')
    parser.add_argument('--lab-url', default='http://127.0.0.1:18090')
    parser.add_argument('--phase', choices=['all', 'tenant', 'publish'], default='all',
                        help='tenant creates the owner and tenant; publish fills it in')
    arguments = parser.parse_args()
    state = arguments.state_dir.expanduser().resolve()
    credentials_path = state / 'review-credentials.json'
    metadata = private_json(state / 'metadata.json')
    control_url = arguments.control_url or metadata['urls']['control']
    api = API(control_url)

    # `tenant` and `publish` run as separate processes on purpose: the platform
    # catalog has to be re-granted in between, and the release contract digest
    # is derived from that catalog. Publishing first would pin every artifact to
    # a digest the restarted Worker no longer accepts.
    if arguments.phase in ('all', 'tenant'):
        if credentials_path.exists():
            previous = private_json(credentials_path)
            if previous.get('seeded') or previous.get('tenant_id'):
                print('DEMO_SEED=SKIP already seeded tenant=' + str(previous.get('tenant_id')))
                return 0
            raise Failure('a previous seed stopped before finishing; archive ' + str(state)
                          + ' and run just demo again')
        # 1. Rotate the bootstrap administrator, as the product requires.
        admin_user = metadata['bootstrap']['username']
        admin_password = secret_file(metadata['bootstrap']['password_file'])
        api.call('POST', '/v1/auth/login', {'username': admin_user, 'password': admin_password})
        me = api.call('GET', '/v1/me')
        if me.get('password_change_required') or me.get('must_change_password') or me.get('restricted'):
            rotated = secrets.token_urlsafe(24)
            api.call('POST', '/v1/me/change-password',
                     {'current_password': admin_password, 'new_password': rotated}, expected=(204,))
            admin_password = rotated
        # 2. Create the demo owner and tenant.
        owner_user = 'demo-owner'
        owner_temporary = secrets.token_urlsafe(24)
        user = api.call('POST', '/v1/admin/users',
                        {'username': owner_user, 'display_name': 'Demo Owner',
                         'temporary_password': owner_temporary}, expected=(201,))
        tenant = api.call('POST', '/v1/admin/tenants',
                          {'slug': 'demo', 'name': 'Demo Tenant', 'owner_user_id': user['id']}, expected=(201,))
        tenant_id = tenant['id']
        api.call('POST', '/v1/auth/login', {'username': owner_user, 'password': owner_temporary})
        owner_password = secrets.token_urlsafe(24)
        api.call('POST', '/v1/me/change-password',
                 {'current_password': owner_temporary, 'new_password': owner_password}, expected=(204,))
        # Persist before anything else can fail: the rotated passwords exist
        # nowhere else once this process exits.
        save_private(credentials_path, {
            'control_url': control_url,
            'channel_lab_url': arguments.lab_url,
            'admin': {'username': admin_user, 'password': admin_password},
            'owner': {'username': owner_user, 'password': owner_password},
            'tenant_id': tenant_id,
            'seeded': False,
        })
        if arguments.phase == 'tenant':
            print('DEMO_SEED=TENANT tenant=' + tenant_id)
            return 0
    else:
        previous = private_json(credentials_path)
        if previous.get('seeded'):
            print('DEMO_SEED=SKIP already seeded tenant=' + str(previous.get('tenant_id')))
            return 0
        tenant_id = previous['tenant_id']
        admin_user = previous['admin']['username']
        admin_password = previous['admin']['password']
        owner_user = previous['owner']['username']
        owner_password = previous['owner']['password']
        api.call('POST', '/v1/auth/login', {'username': owner_user, 'password': owner_password})
    base = '/v1/tenants/' + tenant_id

    # 3. Publish one Agent version.
    agent = api.call('POST', base + '/agents', {'name': 'Demo Assistant'}, expected=(201,))['agent']
    spec = {
        'schema_version': 'v1',
        'root': 'assistant',
        'requirements': {
            'models': {'primary': {'capabilities': ['chat', 'tool_call']}},
            'tools': {},
            'knowledge': {},
            'executors': {'sandbox': {'capability': 'workspace'}},
        },
        'nodes': {'assistant': {
            'kind': 'llm',
            'name': 'Demo Assistant',
            'instruction': 'Answer the latest user message in one short paragraph.',
            'model_slot': 'primary',
            'tool_slots': [],
            'knowledge_slots': [],
            'memory': {'tools': ['memory_add', 'memory_search', 'memory_load'], 'preload_limit': 5},
            'artifact': {'enabled': True},
            'workspace': {'executor_slot': 'sandbox',
                          'tools': ['workspace_exec', 'workspace_save_artifact']},
            'add_session_summary': True,
        }},
        'runtime': {'summary': {'enabled': True, 'model_slot': 'primary', 'event_threshold': 20}},
    }
    api.call('PUT', base + '/agents/' + agent['id'] + '/draft', {'expected_revision': 1, 'spec': spec})
    api.call('POST', base + '/agents/' + agent['id'] + '/versions', {'expected_revision': 2}, expected=(201,))

    # 4. Publish one Runtime Profile revision bound to the chosen model.
    external_key = os.environ.get('DEMO_MODEL_API_KEY')
    if external_key:
        model = {
            'kind': 'openai_compatible',
            'model': os.environ.get('DEMO_MODEL_NAME') or 'gpt-4o',
            'base_url': (os.environ.get('DEMO_MODEL_BASE_URL') or 'https://api.openai.com/v1').rstrip('/'),
            'capabilities': ['chat', 'tool_call'],
        }
        model_key, model_source = external_key, '外部 OpenAI 兼容模型'
    else:
        model = {'kind': 'openai_compatible', 'model': 'lab-echo',
                 'base_url': 'http://channel-lab:8080/v1', 'capabilities': ['chat', 'tool_call']}
        model_key, model_source = lab_model_key(arguments.lab_url), 'Channel Lab 内置确定性模型（无需外部密钥）'

    database = 'agent_platform'
    session_password = secret_file(state / 'secrets' / 'pg_session')
    session_dsn = ('postgres://session_runtime:' + session_password + '@postgres:5432/'
                   + database + '?sslmode=disable')
    memory_password = secret_file(state / 'secrets' / 'pg_memory')
    artifact_user = secret_file(state / 'secrets' / 'minio_app_user')
    artifact_secret = secret_file(state / 'secrets' / 'minio_app_password')
    profile = api.call('POST', base + '/runtime-profiles', {'name': 'Demo Profile'}, expected=(201,))['profile']
    config = {
        'models': {'primary': model},
        'tools': {},
        'knowledge': {},
        'executors': {'sandbox': {'kind': 'sdk_sandbox'}},
        'storage': {
            'session': {'kind': 'postgres_state', 'destination': {
                'host': 'postgres', 'port': 5432, 'database': database,
                'username': 'session_runtime', 'sslmode': 'disable'}},
            'memory': {'kind': 'managed_memory', 'backend_id': 'memory-pg', 'backend_revision': 1},
            'artifact': {'kind': 'managed_artifact', 'backend_id': 'artifact-s3', 'backend_revision': 1},
        },
    }
    profile_credentials = {
        'models': {'primary': {'api_key': {'action': 'replace', 'value': model_key}}},
        # Managed memory and artifact still carry their own credentials: the
        # catalog fixes the target, the credential proves the caller may use it.
        'storage': {
            'session': {'dsn': {'action': 'replace', 'value': session_dsn}},
            # Managed resources carry a password, not a DSN: the platform
            # resolves host, port, database and user from the catalog target,
            # and only the secret half comes from the credential.
            'memory': {'dsn_password': {'action': 'replace', 'value': memory_password}},
            'artifact': {
                'access_key_id': {'action': 'replace', 'value': artifact_user},
                'secret_access_key': {'action': 'replace', 'value': artifact_secret},
            },
        },
    }
    api.call('PUT', base + '/runtime-profiles/' + profile['id'] + '/draft',
             {'expected_draft_revision': 1, 'credential_protocol_version': 'v1',
              'config': config, 'credentials': profile_credentials}, idem='demo-profile-draft')
    api.call('POST', base + '/runtime-profiles/' + profile['id'] + '/revisions',
             {'expected_revision': 2}, expected=(201,))

    # 5. Publish one Deployment revision.
    deployment = api.call('POST', base + '/deployments', {'name': 'Demo Deployment'},
                          expected=(200, 201), idem='demo-deployment-create')['deployment']
    source = {'schema_version': 'v1',
              'agent': {'agent_id': agent['id'], 'version_number': 1},
              'profile': {'profile_id': profile['id'], 'revision_number': 1}}
    result = api.call('POST', base + '/deployments/' + deployment['id'] + '/validate', source)
    if not result.get('valid'):
        raise Failure('deployment validation rejected the pair: '
                      + json.dumps(result, ensure_ascii=False)[:900])
    publication = api.call('POST', base + '/deployments/' + deployment['id'] + '/revisions',
                           {'expected_latest_revision_number': None, 'input': source},
                           expected=(200, 201), idem='demo-deployment-publish')
    revision_number = publication['revision']['revision_number']

    # 6. Bind a Channel Lab bot to the published revision.
    bot = lab_bot(arguments.lab_url, 'Demo Lab Bot')
    account = api.call('POST', base + '/channel-accounts', {
        'provider': 'telegram',
        'provider_account_id': str(bot['id']),
        'name': 'Demo Lab Bot',
        'description': 'Channel Lab simulator bound to the demo deployment',
        'config': {'receive_mode': 'long_polling', 'endpoint_profile': 'test'},
        'credentials': {'telegram.bot_token': {'action': 'replace', 'value': bot['token']}},
    }, expected=(200, 201), idem='demo-account-create')['account']
    binding = api.call('POST', base + '/channel-bindings', {
        'account_id': account['account_id'],
        'target': {'deployment_id': deployment['id'], 'revision_number': revision_number},
    }, expected=(200, 201), idem='demo-binding-create')['binding']
    # Left disabled on purpose. Enabling needs a channel preflight first, and
    # an enabled account the Gateway cannot reach makes its readiness fail,
    # which in turn makes `docker compose up --wait` fail for the whole stack.
    # Enable these from the console once the connection has been preflighted.

    save_private(credentials_path, {
        'control_url': control_url,
        'channel_lab_url': arguments.lab_url,
        'seeded': True,
        'admin': {'username': admin_user, 'password': admin_password},
        'owner': {'username': owner_user, 'password': owner_password},
        'tenant_id': tenant_id,
        'agent_id': agent['id'],
        'profile_id': profile['id'],
        'deployment_id': deployment['id'],
        'deployment_revision': revision_number,
        'account_id': account['account_id'],
        'binding_id': binding['binding_id'],
        'lab_bot_id': bot['id'],
        'model': model['model'],
        'model_source': model_source,
    })
    print('DEMO_SEED=PASS tenant=' + tenant_id + ' deployment_revision=' + str(revision_number))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Failure as error:
        print('DEMO_SEED=FAIL ' + str(error), file=sys.stderr)
        sys.exit(1)
