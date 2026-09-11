#!/usr/bin/env python3
"""Bounded secret-file-only deployment initialization and persistence probes."""
import argparse
import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


class Failure(Exception):
    pass


def secret(path):
    value = Path(path).read_text().removesuffix('\n')
    if not value or '\r' in value or '\n' in value or '\x00' in value:
        raise Failure('invalid secret file')
    return value


def identifier(value):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,62}', value):
        raise Failure('invalid resource identifier')
    return value


def endpoint(value):
    p = urllib.parse.urlsplit(value)
    if p.scheme not in ('http', 'https') or not p.hostname or p.username or p.password or p.path not in ('', '/') or p.query or p.fragment:
        raise Failure('invalid endpoint')
    return value.rstrip('/')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def http(url, method='GET', body=None, headers=None, timeout=5):
    request = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    except (OSError, urllib.error.URLError):
        raise Failure('dependency unavailable') from None
    with response:
        status = response.status
        # Errors are never returned or printed: providers can echo credentials.
        if not 200 <= status < 300:
            return status, b''
        raw = response.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise Failure('dependency response too large')
        return status, raw


def wait_ready(check, seconds):
    deadline = time.monotonic() + seconds
    while True:
        try:
            if check(): return
        except Failure:
            pass
        if time.monotonic() >= deadline:
            raise Failure('dependency readiness timeout')
        time.sleep(min(.2, max(0, deadline - time.monotonic())))


def redis_acl(args):
    values = [secret(args.admin_password_file), secret(args.memory_password_file), secret(args.session_password_file)]
    if len(set(values)) != 3:
        raise Failure('dedicated Redis credentials must differ')
    hashes = [hashlib.sha256(s.encode()).hexdigest() for s in values]
    text = '\n'.join(['user default off',
        'user deployment_admin on #' + hashes[0] + ' ~* +@all',
        'user memory_runtime on #' + hashes[1] + ' ~runtime_memory:* +@connection +mget +get +type +pttl +eval +mset',
        'user session_runtime on #' + hashes[2] + ' ~runtime_session:* +@connection +get +type +pttl +eval +set', ''])
    uid, gid = getattr(args, "owner_uid", None), getattr(args, "owner_gid", None)
    if (uid is None) != (gid is None): raise Failure("ACL owner requires uid and gid")
    if uid is not None and (uid < 0 or gid < 0): raise Failure("invalid ACL owner")
    path = Path(args.output)
    fd, temporary = tempfile.mkstemp(prefix='.redis-acl-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as out:
            os.fchmod(out.fileno(), 0o600)
            if uid is not None: os.fchown(out.fileno(), uid, gid)
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
    print('REDIS_ACL=PASS users=deployment_admin,memory_runtime,session_runtime default=off')


def redis_reply(reader):
    line = reader.readline(1048577)
    if not line.endswith(b'\r\n') or len(line) > 1048576: raise Failure('invalid Redis response')
    tag, value = line[:1], line[1:-2]
    if tag == b'-': raise Failure('Redis command rejected')
    if tag == b'+': return value.decode()
    if tag == b':': return int(value)
    if tag == b'$':
        length = int(value)
        if length == -1: return None
        if not 0 <= length <= 1048576: raise Failure('Redis response too large')
        raw = reader.read(length + 2)
        if len(raw) != length + 2 or not raw.endswith(b'\r\n'): raise Failure('invalid Redis response')
        return raw[:-2].decode()
    if tag == b'*':
        count = int(value)
        if not 0 <= count <= 128: raise Failure('invalid Redis response')
        return [redis_reply(reader) for _ in range(count)]
    raise Failure('invalid Redis response')


def redis(args, *command):
    def send(sock, reader, items):
        encoded = [str(v).encode() for v in items]
        sock.sendall(b'*' + str(len(encoded)).encode() + b'\r\n' + b''.join(b'$' + str(len(v)).encode() + b'\r\n' + v + b'\r\n' for v in encoded))
        return redis_reply(reader)
    try:
        with socket.create_connection((args.host, args.port), timeout=args.timeout) as raw:
            sock = ssl.create_default_context().wrap_socket(raw, server_hostname=args.host) if args.tls else raw
            with sock:
                with sock.makefile('rb') as reader:
                    if send(sock, reader, ['AUTH', args.username, secret(args.password_file)]) != 'OK': raise Failure('Redis authentication rejected')
                    return send(sock, reader, command)
    except OSError:
        raise Failure('Redis dependency unavailable') from None


def qdrant(args, method, path, value=None):
    return http(endpoint(args.endpoint) + path, method, None if value is None else json.dumps(value).encode(), {'api-key': secret(args.api_key_file), 'Content-Type': 'application/json'}, args.timeout)


def collection_matches(raw, name, dimensions, distance):
    config = json.loads(raw)['result']['config']['params']['vectors']
    if not isinstance(config, dict) or name not in config or config[name].get('size') != dimensions or config[name].get('distance', '').lower() != distance.lower():
        raise Failure('Qdrant collection vector contract conflict')


def qdrant_init(args):
    if args.dimensions is None:
        print('QDRANT_COLLECTION=SKIPPED reason=no_explicit_dimensions embedding_semantics=unconfigured')
        return
    if args.dimensions <= 0: raise Failure('positive explicit dimensions required')
    name, vector = identifier(args.collection), identifier(args.vector_name)
    wait_ready(lambda: qdrant(args, 'GET', '/collections')[0] == 200, args.wait_seconds)
    path = '/collections/' + name
    status, raw = qdrant(args, 'GET', path)
    if status == 404:
        status, _ = qdrant(args, 'PUT', path, {'vectors': {vector: {'size': args.dimensions, 'distance': args.distance}}})
        if status not in (200, 409): raise Failure('Qdrant collection creation rejected')
        status, raw = qdrant(args, 'GET', path)
    if status != 200: raise Failure('Qdrant collection inspection rejected')
    collection_matches(raw, vector, args.dimensions, args.distance)
    print('QDRANT_COLLECTION=PASS embedding_semantics=not_verified')


def s3(args, method, key=None, query=None, body=b''):
    base = endpoint(args.endpoint)
    bucket = args.bucket
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', bucket): raise Failure('invalid bucket name')
    path = '/' + bucket + ('' if key is None else '/' + urllib.parse.quote(key, safe='/~'))
    query = urllib.parse.urlencode(sorted((query or {}).items()), quote_via=urllib.parse.quote)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    day = stamp[:8]
    digest = hashlib.sha256(body).hexdigest()
    host = urllib.parse.urlsplit(base).netloc
    canonical_headers = 'host:' + host + '\nx-amz-content-sha256:' + digest + '\nx-amz-date:' + stamp + '\n'
    names = 'host;x-amz-content-sha256;x-amz-date'
    canonical = '\n'.join((method, path, query, canonical_headers, names, digest))
    scope = day + '/' + args.region + '/s3/aws4_request'
    sign = '\n'.join(('AWS4-HMAC-SHA256', stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()))
    signing_key = ('AWS4' + secret(args.secret_key_file)).encode()
    for value in (day, args.region, 's3', 'aws4_request'): signing_key = hmac.new(signing_key, value.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing_key, sign.encode(), hashlib.sha256).hexdigest()
    headers = {'Host': host, 'X-Amz-Content-Sha256': digest, 'X-Amz-Date': stamp,
               'Authorization': 'AWS4-HMAC-SHA256 Credential=' + secret(args.access_key_file) + '/' + scope + ', SignedHeaders=' + names + ', Signature=' + signature}
    return http(base + path + ('?' + query if query else ''), method, body if method in ('PUT', 'POST') else None, headers, args.timeout)


def minio_init(args):
    # Authenticated S3 read, not only MinIO's early health endpoint, must be ready.
    def ready():
        status, _ = s3(args, 'HEAD')
        return status in (200, 404)
    wait_ready(ready, args.wait_seconds)
    status, _ = s3(args, 'HEAD')
    if status == 404:
        body = b'' if args.region == 'us-east-1' else ('<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>' + args.region + '</LocationConstraint></CreateBucketConfiguration>').encode()
        status, _ = s3(args, 'PUT', body=body)
        if status not in (200, 409): raise Failure('S3 bucket creation rejected')
    if s3(args, 'HEAD')[0] != 200: raise Failure('S3 bucket inspection rejected')
    status, raw = s3(args, 'GET', query={'versioning': ''})
    if status != 200: raise Failure('S3 bucket versioning inspection rejected')
    root = ET.fromstring(raw)
    state = root.findtext('{*}Status')
    if state not in (None, ''): raise Failure('S3 bucket versioning must remain disabled')
    print('MINIO_BUCKET=PASS versioning=disabled')



def artifact_policy(bucket):
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', bucket): raise Failure('invalid bucket name')
    return {'Version': '2012-10-17', 'Statement': [
        {'Effect': 'Allow', 'Action': ['s3:GetBucketLocation', 's3:ListBucket'], 'Resource': ['arn:aws:s3:::' + bucket]},
        {'Effect': 'Allow', 'Action': ['s3:GetObject', 's3:PutObject', 's3:DeleteObject'], 'Resource': ['arn:aws:s3:::' + bucket + '/*']}]}


def minio_account_init(args):
    # mc owns the admin protocol; temporary config is private and discarded.
    root_user, root_password = secret(args.access_key_file), secret(args.secret_key_file)
    app_user, app_password = secret(args.app_access_key_file), secret(args.app_secret_key_file)
    if app_user == root_user or app_password == root_password: raise Failure('dedicated S3 credentials required')
    policy = artifact_policy(args.bucket)
    url = endpoint(args.endpoint)
    with tempfile.TemporaryDirectory(prefix='minio-init-') as directory:
        policy_file = Path(directory) / 'policy.json'
        policy_file.write_text(json.dumps(policy))
        env = {k: v for k, v in os.environ.items() if 'proxy' not in k.lower() and not k.startswith('MC_')}
        def mc(*command):
            try:
                result = subprocess.run([args.mc_binary, '--config-dir', directory, '--json', *command],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env, timeout=args.timeout, check=False)
            except (OSError, subprocess.TimeoutExpired): raise Failure('MinIO admin unavailable') from None
            if result.returncode: raise Failure('MinIO admin command rejected')
        mc('alias', 'set', 'owned', url, root_user, root_password, '--api', 'S3v4')
        # This named deployment-owned user and policy are reconciled to the
        # mounted secrets/config. Buckets and objects are never replaced.
        policy_name = 'worker-artifact-' + hashlib.sha256(args.bucket.encode()).hexdigest()[:16]
        mc('admin', 'user', 'add', 'owned', app_user, app_password)
        mc('admin', 'policy', 'create', 'owned', policy_name, str(policy_file))
        mc('admin', 'policy', 'attach', 'owned', policy_name, '--user', app_user)
        mc('alias', 'set', 'application', url, app_user, app_password, '--api', 'S3v4')
        mc('stat', 'application/' + args.bucket)
    print('MINIO_ACCOUNT=PASS dedicated=true bucket_policy=true')


def health(args):
    if args.service == 'redis':
        def ready():
            try: return redis(args, 'PING') == 'PONG'
            except Failure: return False
    elif args.service == 'qdrant':
        ready = lambda: qdrant(args, 'GET', '/collections')[0] == 200
    else:
        ready = lambda: http(endpoint(args.endpoint) + '/minio/health/ready', timeout=args.timeout)[0] == 200
    wait_ready(ready, args.wait_seconds)
    print('DATA_BACKEND_HEALTH=PASS service=' + args.service)


def smoke(args):
    probe = identifier(args.probe_id)
    marker = 'deployment-persistence-probe-v1:' + probe
    if args.service == 'redis':
        key = 'deployment_smoke:' + probe
        config = redis(args, 'CONFIG', 'GET', 'appendonly', 'appendfsync', 'maxmemory-policy')
        config = dict(zip(config[::2], config[1::2]))
        if config != {'appendonly': 'yes', 'appendfsync': 'always', 'maxmemory-policy': 'noeviction'}: raise Failure('Redis persistence configuration conflict')
        if args.action == 'write':
            old = redis(args, 'GET', key)
            if old is not None and old != marker: raise Failure('smoke key collision')
            if old is None: redis(args, 'SET', key, marker, 'NX')
        if args.action == 'delete':
            old = redis(args, 'GET', key)
            if old is not None and old != marker: raise Failure('Redis smoke collision')
            redis(args, 'DEL', key)
        elif redis(args, 'GET', key) != marker or redis(args, 'PTTL', key) != -1: raise Failure('Redis persistence probe missing or changed')
    elif args.service == 'minio':
        key = '_deployment_smoke/' + probe
        status, raw = s3(args, 'GET', key)
        if args.action == 'write':
            if status == 404:
                if s3(args, 'PUT', key, body=marker.encode())[0] != 200: raise Failure('S3 smoke write failed')
                status, raw = s3(args, 'GET', key)
        if args.action == 'delete':
            if status == 200 and raw != marker.encode(): raise Failure('S3 smoke collision')
            if s3(args, 'DELETE', key)[0] not in (200, 204): raise Failure('S3 smoke cleanup failed')
        elif status != 200 or raw != marker.encode(): raise Failure('S3 persistence probe missing or changed')
    else:
        # A dedicated one-dimensional infrastructure probe is NOT an embedding
        # configuration and never modifies the application's collection.
        collection = 'deployment_smoke_' + probe
        path = '/collections/' + collection
        status, raw = qdrant(args, 'GET', path)
        if args.action == 'write' and status == 404:
            status, _ = qdrant(args, 'PUT', path, {'vectors': {'probe': {'size': 1, 'distance': 'Dot'}}})
            if status != 200: raise Failure('Qdrant smoke collection creation failed')
            status, raw = qdrant(args, 'GET', path)
        if status != 200: raise Failure('Qdrant smoke collection missing')
        collection_matches(raw, 'probe', 1, 'Dot')
        point_path = path + '/points/1'
        found, point = qdrant(args, 'GET', point_path)
        if args.action == 'write' and found == 404:
            status, _ = qdrant(args, 'PUT', path + '/points?wait=true', {'points': [{'id': 1, 'vector': {'probe': [1]}, 'payload': {'deployment_probe': marker}}]})
            if status != 200: raise Failure('Qdrant smoke write failed')
            found, point = qdrant(args, 'GET', point_path)
        if found != 200 or json.loads(point)['result']['payload'] != {'deployment_probe': marker}: raise Failure('Qdrant persistence probe missing or changed')
        if args.action == 'delete':
            status, count = qdrant(args, 'POST', path + '/points/count', {'exact': True})
            if status != 200 or json.loads(count)['result']['count'] != 1: raise Failure('Qdrant smoke collection collision')
            if qdrant(args, 'DELETE', path)[0] != 200: raise Failure('Qdrant smoke cleanup failed')
    print('DATA_BACKEND_SMOKE=PASS service=' + args.service + ' action=' + args.action)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    commands = p.add_subparsers(dest='command', required=True)
    acl = commands.add_parser('redis-acl')
    for role in ('admin', 'memory', 'session'): acl.add_argument('--' + role + '-password-file', required=True)
    acl.add_argument('--output', required=True)
    acl.add_argument('--owner-uid', type=int); acl.add_argument('--owner-gid', type=int)
    for command in ('minio-init', 'minio-account-init', 'qdrant-init', 'health', 'smoke'):
        sub = commands.add_parser(command)
        sub.add_argument('--timeout', type=float, default=5)
        sub.add_argument('--wait-seconds', type=float, default=60)
        sub.add_argument('--endpoint')
        sub.add_argument('--api-key-file')
        sub.add_argument('--access-key-file'); sub.add_argument('--secret-key-file')
        sub.add_argument('--bucket'); sub.add_argument('--region', default='us-east-1')
        sub.add_argument('--host', default='redis'); sub.add_argument('--port', type=int, default=6379)
        sub.add_argument('--username', default='deployment_admin'); sub.add_argument('--password-file'); sub.add_argument('--tls', action='store_true')
        if command == 'minio-account-init':
            sub.add_argument('--app-access-key-file', required=True); sub.add_argument('--app-secret-key-file', required=True)
            sub.add_argument('--mc-binary', default='mc')
        if command == 'qdrant-init':
            sub.add_argument('--collection'); sub.add_argument('--vector-name'); sub.add_argument('--dimensions', type=int)
            sub.add_argument('--distance', choices=['Cosine', 'Dot', 'Euclid', 'Manhattan'], default='Cosine')
        if command in ('health', 'smoke'): sub.add_argument('--service', required=True, choices=['redis', 'minio', 'qdrant'])
        if command == 'smoke':
            sub.add_argument('--action', required=True, choices=['write', 'read', 'delete'])
            sub.add_argument('--probe-id', required=True)
    return p


def main():
    args = parser().parse_args()
    try:
        if hasattr(args, 'timeout') and (not 0 < args.timeout <= 300 or not 0 < args.wait_seconds <= 600): raise Failure('invalid bounded timeout')
        {'redis-acl': redis_acl, 'minio-init': minio_init, 'minio-account-init': minio_account_init, 'qdrant-init': qdrant_init, 'health': health, 'smoke': smoke}[args.command](args)
    except Exception:
        # Deliberately omit provider bodies, URLs, credential-file paths and raw
        # exceptions. The finite command/service/status marker is enough for ops.
        print('DATA_BACKEND_OPERATION=FAIL command=' + args.command, file=sys.stderr)
        return 1
    return 0

if __name__ == '__main__': sys.exit(main())
