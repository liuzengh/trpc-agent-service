"""Private mTLS response relay for real Control credential uncertainty gates.

Both TLS legs validate the fixture CA and use the same allowlisted client URI.
The owner receives one request. Default forwarding preserves its exact bytes;
a closed proof-negative mode changes only the manifest digest before the real
owner checks it. No HTTP success, proof, identity header, or credential value is
manufactured. Closed batch-negative modes mutate a complete real owner 200,
never a cached or synthesized batch. Final-negative modes change one identity
field only after a complete valid owner proof matches the original request.
Observations keep owner metadata distinct from these response mutations.
Bodies and execution capabilities are never part of
observations; ephemeral HMAC correlation is exposed only as a local integer.
"""
from __future__ import annotations

import copy
import hmac
import http.client
import http.server
import json
import os
import re
import socket
import ssl
import threading
import time
from urllib.parse import urlsplit

RESOLVE_PATH = '/internal/v1/runtime-profiles/credentials/resolve'
ATTEMPT_PATH = '/internal/v1/execution/attempts:verify'
FINAL_PATH = '/internal/v1/execution/finals:verify'
# Evidence is narrower than api/runtime/execution/v1 finalID's generic 128-byte
# wire grammar: these are the actual owners' production-generated IDs used by
# this private joint gate, not arbitrary strings smuggled into identity fields.
# Sources: Gateway admission/application/service.go newID; Worker
# postgresadapter/lease.go; Control tenant/runtimeprofile/deployment id.go.
_METADATA_IDS = {
    'run_id': re.compile(r'^[0-9a-f]{32}$'),
    'attempt_id': re.compile(r'^att_[0-9a-f]{32}$'),
    'tenant_id': re.compile(r'^tnt_[A-Za-z0-9_-]{24}$'),
    'profile_id': re.compile(r'^rpf_[A-Za-z0-9_-]{24}$'),
    'manifest_id': re.compile(r'^rmf_[A-Za-z0-9_-]{24}$'),
}


_BATCH_MODES = frozenset(('missing_use', 'extra_use', 'duplicate_use', 'identity_mismatch'))
_BATCH_FIELDS = frozenset(('tenant_id', 'profile_id', 'profile_revision_number', 'run_id',
    'attempt_id', 'worker_id', 'lease_epoch', 'manifest_id', 'manifest_digest', 'credentials'))
_CREDENTIAL_FIELDS = frozenset(('credential_id', 'purpose', 'audience_digest', 'credential_revision', 'value'))
_DIGEST = re.compile(r'^sha256:[0-9a-f]{64}$')
_CREDENTIAL_ID = re.compile(r'^crd_[0-9a-f]{32}$')
_FINAL_MODES = frozenset(('final_tenant_mismatch', 'final_manifest_mismatch'))
_FINAL_REQUEST_FIELDS = frozenset(('intent_id', 'digest', 'admission_id', 'run_id',
    'attempt_id', 'completion_id', 'execution_generation', 'sequence'))
_FINAL_FIELDS = _FINAL_REQUEST_FIELDS | {'tenant_id', 'manifest_digest'}
_FINAL_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$')
_FINAL_METADATA_IDS = {key: _METADATA_IDS[key] for key in ('run_id', 'attempt_id', 'tenant_id')}
_FINAL_METADATA_IDS.update({'intent_id': re.compile(r'^fin_[0-9a-f]{64}$'),
    'admission_id': re.compile(r'^[0-9a-f]{32}$'),
    'completion_id': re.compile(r'^cmp_[0-9a-f]{64}$')})


def _decode_final(raw, response):
    # Mirrors the public Final codec, not a forgiving generic JSON repair. This
    # prevents a malformed or duplicate-key owner body becoming a valid proof.
    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('Final fixture duplicate field')
            value[key] = item
        return value
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 4096:
        raise ValueError('Final fixture body bound')
    value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique_object)
    fields = _FINAL_FIELDS if response else _FINAL_REQUEST_FIELDS
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError('Final fixture field contract')
    for field in fields - {'execution_generation', 'sequence'}:
        item = value[field]
        pattern = _DIGEST if field in ('digest', 'manifest_digest') else _FINAL_ID
        if not isinstance(item, str) or not pattern.fullmatch(item):
            raise ValueError('Final fixture string contract')
    if (type(value['execution_generation']) is not int or
            not 1 <= value['execution_generation'] <= 9007199254740991 or
            type(value['sequence']) is not int or value['sequence'] != 1):
        raise ValueError('Final fixture integer contract')
    return value


def mutate_final(raw, mode, request):
    """One valid identity field changes; original owner request proof must match."""
    if mode not in _FINAL_MODES:
        raise ValueError('unknown Final negative fixture')
    value = _decode_final(raw, True)
    expected = _decode_final(request, False)
    if {key: value[key] for key in _FINAL_REQUEST_FIELDS} != expected:
        raise ValueError('Final fixture owner request mismatch')
    field = 'tenant_id' if mode == 'final_tenant_mismatch' else 'manifest_digest'
    original = value[field]
    value[field] = original[:-1]+('1' if original[-1] == '0' else '0')
    result = json.dumps(value, separators=(',', ':')).encode()
    _decode_final(result, True)
    return result, field


def safe_final_metadata(raw):
    """Original owner fields only; generated ID shapes prevent arbitrary text."""
    try:
        value = _decode_final(raw, True)
    except (ValueError, UnicodeError):
        return {}
    result = {key: value[key] for key, pattern in _FINAL_METADATA_IDS.items()
              if pattern.fullmatch(value[key])}
    result.update({key: value[key] for key in
                   ('digest', 'manifest_digest', 'execution_generation', 'sequence')})
    return result



def mutate_batch(raw, mode):
    """Closed V1 two-use negative fixture, never accepts arbitrary patch input.

    Returned bytes are ephemeral wire data, not evidence. Values always originate
    in the one complete real owner batch and are never created or persisted.
    """
    if mode not in _BATCH_MODES:
        raise ValueError('unknown batch negative fixture')
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) != _BATCH_FIELDS:
        raise ValueError('batch fixture top-level contract')
    items = value['credentials']
    if not isinstance(items, list) or len(items) != 2:
        raise ValueError('batch fixture requires two real V1 uses')
    for item in items:
        if (not isinstance(item, dict) or set(item) != _CREDENTIAL_FIELDS or
                not isinstance(item['credential_id'], str) or not _CREDENTIAL_ID.fullmatch(item['credential_id']) or
                not isinstance(item['value'], str) or not item['value']):
            raise ValueError('batch fixture credential contract')
    if not isinstance(value['attempt_id'], str) or not _METADATA_IDS['attempt_id'].fullmatch(value['attempt_id']):
        raise ValueError('batch fixture Attempt identity contract')
    if mode == 'missing_use':
        value['credentials'] = items[:-1]
    elif mode == 'extra_use':
        extra = dict(items[0])
        existing = {item['credential_id'] for item in items}
        # Choose an unused correctly-shaped ID, not a new credential value.
        extra['credential_id'] = next('crd_'+format(i, '032x') for i in range(3)
                                      if 'crd_'+format(i, '032x') not in existing)
        value['credentials'] = items+[extra]
    elif mode == 'duplicate_use':
        value['credentials'] = [items[0], dict(items[0])]
    else:
        original = value['attempt_id']
        value['attempt_id'] = original[:-1]+('1' if original[-1] == '0' else '0')
    result = json.dumps(value, separators=(',', ':')).encode()
    counts = {'credential_count_before': len(items),
              'credential_count_after': len(value['credentials'])}
    del value, items
    return result, counts


def mutate_proof_digest(raw):
    """Only this one valid digest mismatch; capability and identity stay intact."""
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) != {'execution_token', 'manifest_id', 'manifest_digest', 'workload_identity'}:
        raise ValueError('proof fixture request contract')
    digest = value['manifest_digest']
    if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
        raise ValueError('proof fixture digest contract')
    value['manifest_digest'] = digest[:-1]+('1' if digest[-1] == '0' else '0')
    result = json.dumps(value, separators=(',', ':')).encode()
    del value, digest
    return result


class GrantGroups:
    """Process-local equality labels, not stored tokens or token fingerprints."""
    def __init__(self):
        self._key = os.urandom(32)
        self._groups = {}
        self._lock = threading.Lock()

    def label(self, body):
        try:
            value = json.loads(body)
            token = value.get('execution_token') if isinstance(value, dict) else None
            if not isinstance(token, str) or not 1 <= len(token) <= 8192:
                return None
            fingerprint = hmac.digest(self._key, token.encode(), 'sha256')
            del token, value
        except (ValueError, UnicodeError):
            return None
        with self._lock:
            if fingerprint not in self._groups:
                if len(self._groups) >= 4096:
                    raise RuntimeError('fixture correlation capacity')
                self._groups[fingerprint] = len(self._groups) + 1
            return self._groups[fingerprint]


def safe_metadata(raw):
    """Closed output vocabulary: no generic JSON fields enter evidence."""
    result = {}
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError):
        return result
    if not isinstance(value, dict):
        return result
    for field, pattern in _METADATA_IDS.items():
        item = value.get(field)
        if isinstance(item, str) and pattern.fullmatch(item):
            result[field] = item
    if value.get('worker_id') in ('worker-one', 'worker-two'):
        result['worker_id'] = value['worker_id']
    for field in ('lease_epoch', 'profile_revision_number'):
        if type(value.get(field)) is int and 0 < value[field] <= 9007199254740991:
            result[field] = value[field]
    return result


class ResolveFaultProxy:
    """A bounded closed-set fault relay; one explicitly armed request at a time."""
    def __init__(self, upstream, certs, peers, groups=None, name='resolve',
                 host='127.0.0.1', port=0):
        target = urlsplit(upstream)
        if target.scheme != 'https' or target.hostname != '127.0.0.1' or not target.port:
            raise ValueError('relay upstream must be fixed private loopback HTTPS')
        if target.username or target.password or target.path or target.query or target.fragment:
            raise ValueError('relay upstream must be an origin')
        self.upstream = upstream
        self.host, self.port, self.name = host, port, name
        self._target = target
        self._groups = groups or GrantGroups()
        self._certs, self._peers = certs, dict(peers)
        self._lock = threading.Lock()
        self._records, self._connections = [], set()
        self._armed, self._fault = None, None
        self._server = self._thread = None
        self._closed = False
        self._closing = threading.Event()
        self._release = threading.Event()
        self._contexts = {}
        for uri, name in self._peers.items():
            context = ssl.create_default_context(cafile=certs['ca'])
            context.minimum_version = ssl.TLSVersion.TLSv1_3
            context.load_cert_chain(certs[name+'_cert'], certs[name+'_key'])
            self._contexts[uri] = context

    def start(self):
        if self._server is not None or self._closed:
            raise RuntimeError('relay lifecycle')
        owner = self
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(self._certs['server_cert'], self._certs['server_key'])
        context.load_verify_locations(self._certs['ca'])
        context.verify_mode = ssl.CERT_REQUIRED

        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = True
            block_on_close = False
            def get_request(self):
                raw, address = super().get_request()
                connection = raw
                try:
                    raw.settimeout(5)
                    connection = context.wrap_socket(raw, server_side=True,
                                                     do_handshake_on_connect=False)
                    with owner._lock:
                        owner._connections.add(connection)
                    if owner._closing.is_set():
                        raise OSError('fixture relay closing')
                    connection.do_handshake()
                    return connection, address
                except BaseException:
                    with owner._lock:
                        owner._connections.discard(connection)
                    connection.close()
                    raise
            def shutdown_request(self, request):
                try:
                    super().shutdown_request(request)
                finally:
                    with owner._lock:
                        owner._connections.discard(request)
            def handle_error(self, request, client_address):
                # Library exception text may contain protocol input. Never log it.
                pass

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_):
                pass
            def do_GET(self):
                owner._handle(self)
            def do_POST(self):
                owner._handle(self)

        server = Server((self.host, self.port), Handler)
        self._server = server
        self.port = server.server_address[1]
        self._thread = threading.Thread(target=server.serve_forever,
            kwargs={'poll_interval': .05}, daemon=True)
        self._thread.start()
        return self

    @property
    def url(self):
        return 'https://'+self.host+':'+str(self.port)

    def arm(self, mode, path=RESOLVE_PATH):
        allowed = {RESOLVE_PATH: _BATCH_MODES | {'drop_after_owner_200', 'hold_after_owner_200'},
                   ATTEMPT_PATH: {'drop_after_owner_200', 'manifest_digest_mismatch'},
                   FINAL_PATH: _FINAL_MODES}
        if path not in allowed or mode not in allowed[path]:
            raise ValueError('unknown closed-set path/fault pair')
        with self._lock:
            if self._armed is not None or (self._fault and not self._fault.get('finished_ns')):
                raise RuntimeError('fixture already has an active fault')
            self._release.clear()
            self._fault = None
            self._armed = {'mode': mode, 'path': path}

    def release(self):
        self._release.set()

    def events(self):
        with self._lock:
            return copy.deepcopy(self._records)

    def fault(self):
        with self._lock:
            return copy.deepcopy(self._fault)

    def evidence(self):
        with self._lock:
            return {'version': 'worker-resolve-mtls-relay/v1', 'name': self.name,
                    'closed': self._closed, 'live_connections': len(self._connections),
                    'events': copy.deepcopy(self._records),
                    'credential_bodies_recorded': False, 'capabilities_recorded': False,
                    'capability_fingerprints_recorded': False,
                    'owner_requests_retried': False, 'upstream_peer_identity_preserved': True}

    def _update(self, record, **values):
        with self._lock:
            record.update(values)

    def _handle(self, handler):
        handler.close_connection = True
        handler.connection.settimeout(30)
        peer = handler.connection.getpeercert()
        uris = [value for kind, value in peer.get('subjectAltName', ()) if kind == 'URI']
        # Certificate validation occurs at TLS handshake; URI authorization is
        # local explicit mapping, never a user supplied header.
        if len(uris) != 1 or uris[0] not in self._contexts:
            handler.send_response(403)
            handler.send_header('Content-Length', '0')
            handler.send_header('Connection', 'close')
            handler.end_headers()
            return
        upstream = None
        upstream_socket = None
        response = None
        record = None
        body = raw = final_request = None
        try:
            if handler.headers.get('Transfer-Encoding'):
                raise ValueError('fixture expects bounded known-length HTTP input')
            length = int(handler.headers.get('Content-Length', '0'))
            if not 0 <= length <= 1024 * 1024:
                raise ValueError('fixture request bound')
            body = handler.rfile.read(length)
            if len(body) != length:
                raise EOFError('request incomplete')
            path = urlsplit(handler.path).path
            group = self._groups.label(body) if path in (RESOLVE_PATH, ATTEMPT_PATH) else None
            with self._lock:
                if len(self._records) >= 8192:
                    raise RuntimeError('fixture record capacity')
                record = {'request_ordinal': len(self._records)+1, 'relay': self.name,
                          'method': handler.command, 'path': path,
                          'principal_uri': uris[0], 'started_ns': time.monotonic_ns(),
                          'downstream_body_bytes_forwarded': 0}
                if group is not None:
                    record['grant_group'] = group
                if self._armed and handler.command == 'POST' and path == self._armed['path']:
                    record['fault'] = self._armed['mode']
                    self._armed, self._fault = None, record
                self._records.append(record)
            if record.get('fault') == 'manifest_digest_mismatch':
                body = mutate_proof_digest(body)
                length = len(body)
                self._update(record, mutation='manifest_digest_mismatch',
                             mutation_phase='request_before_owner')
            if record.get('fault') in _FINAL_MODES:
                final_request = body
            upstream = http.client.HTTPSConnection(self._target.hostname,
                self._target.port, context=self._contexts[uris[0]], timeout=10)
            headers = {'Content-Length': str(length), 'Connection': 'close'}
            if handler.headers.get('Content-Type'):
                headers['Content-Type'] = handler.headers['Content-Type']
            upstream.connect()
            with self._lock:
                upstream_socket = upstream.sock
                self._connections.add(upstream_socket)
            upstream.request(handler.command, handler.path, body=body, headers=headers)
            body = None
            response = upstream.getresponse()
            raw = response.read(8 * 1024 * 1024 + 1)
            if len(raw) > 8 * 1024 * 1024:
                raise ValueError('fixture response bound')
            if response.length not in (None, 0):
                # read(amt) does not raise for a short Content-Length response.
                # Such a response is not a complete owner 200 and cannot arm a
                # successful-response-loss claim. Keep partial bytes out of errs.
                raise http.client.IncompleteRead(b'', response.length)
            metadata = (safe_final_metadata(raw) if path == FINAL_PATH else
                        safe_metadata(raw) if path in (RESOLVE_PATH, ATTEMPT_PATH) else {})
            self._update(record, upstream_status=response.status,
                         owner_response_bytes=len(raw), owner_response_ns=time.monotonic_ns(),
                         **metadata)
            if record.get('fault') in ('drop_after_owner_200', 'hold_after_owner_200') and response.status == 200:
                # True complete owner 200 has already arrived. Drop all credential
                # bytes before waiting: timeout injection retains no batch body.
                raw = None
                if record['fault'] == 'hold_after_owner_200':
                    self._release.wait(15)
                self._update(record, downstream_action='owner_200_suppressed',
                             released_by_test=self._release.is_set())
                return
            downstream_action = 'forwarded'
            if record.get('fault') in _BATCH_MODES and response.status == 200:
                raw, counts = mutate_batch(raw, record['fault'])
                self._update(record, mutation=record['fault'],
                             mutation_phase='complete_owner_200_response', **counts)
                downstream_action = 'mutated_owner_200'
            if record.get('fault') in _FINAL_MODES and response.status == 200:
                raw, field = mutate_final(raw, record['fault'], final_request)
                self._update(record, mutation=record['fault'], mutation_field=field,
                             mutation_phase='complete_owner_200_response')
                downstream_action = 'mutated_owner_200'
            handler.send_response(response.status)
            handler.send_header('Content-Type', response.getheader('Content-Type', 'application/json'))
            handler.send_header('Content-Length', str(len(raw)))
            handler.send_header('Connection', 'close')
            handler.end_headers()
            handler.wfile.write(raw)
            handler.wfile.flush()
            self._update(record, downstream_status=response.status,
                         downstream_body_bytes_forwarded=len(raw), downstream_action=downstream_action)
        except (OSError, ValueError, EOFError, http.client.HTTPException, RuntimeError) as error:
            if record is not None:
                self._update(record, transport_result=type(error).__name__)
            # Connection loss stays connection loss, never fabricated owner HTTP.
        finally:
            body = raw = final_request = None
            with self._lock:
                if upstream_socket is not None:
                    self._connections.discard(upstream_socket)
            if response is not None:
                response.close()
            if upstream is not None:
                upstream.close()
            if record is not None:
                self._update(record, finished_ns=time.monotonic_ns())

    def close(self):
        self._release.set()
        self._closing.set()
        server, self._server = self._server, None
        with self._lock:
            connections = list(self._connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        if server is not None:
            server.shutdown()
            server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=3)
            if self._thread.is_alive():
                raise RuntimeError('fixture relay thread did not stop')
        deadline = time.monotonic()+3
        while time.monotonic() < deadline:
            with self._lock:
                if not self._connections:
                    self._closed = True
                    return
            time.sleep(.01)
        raise RuntimeError('fixture relay connections did not stop')
