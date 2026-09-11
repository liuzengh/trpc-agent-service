"""Real loopback TLS unit tests of plumbing, NOT substitutes for owner gates."""
from concurrent.futures import ThreadPoolExecutor
import http.client
import http.server
import json
from pathlib import Path
import shutil
import ssl
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock

from harness import Harness
from resolve_fault_proxy import ATTEMPT_PATH, RESOLVE_PATH, GrantGroups, ResolveFaultProxy, safe_metadata
import resolve_fault_proxy as relay

RUN = '3fbfcb9a6283f76d7e602719134edea6'
ATTEMPT = 'att_0d3a12bb6955431e7433815f72059e22'
TOKEN = 'unit-execution-capability-do-not-record'
SECRET = 'unit-credential-value-do-not-record'
REQUEST = json.dumps({'execution_token': TOKEN, 'uses': []}).encode()
FINAL_PATH = '/internal/v1/execution/finals:verify'
FINAL = {'intent_id': 'fin_'+'1'*64, 'digest': 'sha256:'+'2'*64,
    'admission_id': '4'*32, 'run_id': RUN, 'attempt_id': ATTEMPT,
    'completion_id': 'cmp_'+'3'*64, 'execution_generation': 1, 'sequence': 1,
    'tenant_id': 'tnt_TozdDU85IZrcXYnI013cBNal', 'manifest_digest': 'sha256:'+'a'*64}
FINAL_REQUEST = json.dumps({k:v for k,v in FINAL.items()
    if k not in ('tenant_id', 'manifest_digest')}).encode()
BODY = json.dumps({'run_id': RUN, 'attempt_id': ATTEMPT, 'worker_id': 'worker-one',
    'lease_epoch': 1, 'credentials': [{'value': SECRET}]}).encode()


class Owner:
    def __init__(self, certs):
        self.calls = []
        self.status = 200
        self.body = BODY
        self.declared_length = None
        self.expected_request = REQUEST
        self.reject_digest_mismatch = False
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_): pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                uris = [v for k,v in self.connection.getpeercert().get('subjectAltName', ()) if k == 'URI']
                owner.calls.append({'uris': uris, 'request_unchanged': body == owner.expected_request,
                                    'identity_header': self.headers.get('X-Worker-ID')})
                status = owner.status
                if owner.reject_digest_mismatch and json.loads(body).get('manifest_digest') != json.loads(owner.expected_request).get('manifest_digest'):
                    status = 403
                raw = owner.body if status == 200 else b'{"error":"denied"}'
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(owner.declared_length or len(raw)))
                self.send_header('Connection', 'close')
                self.end_headers(); self.wfile.write(raw)
                self.close_connection = True
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1',0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(certs['server_cert'], certs['server_key'])
        context.load_verify_locations(certs['ca']); context.verify_mode = ssl.CERT_REQUIRED
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.url = 'https://127.0.0.1:'+str(self.server.server_address[1])
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={'poll_interval': .05}, daemon=True)
        self.thread.start()
    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(3)


class ResolveTLSProxyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifacts = tempfile.mkdtemp(prefix='resolve-relay-unit-artifacts-')
        cls.h = Harness(Path(__file__).resolve().parents[2], cls.artifacts)
        cls.h.make_pki()
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.h.work); shutil.rmtree(cls.artifacts)
    def setUp(self):
        self.owner = Owner(self.h.certs)
        self.proxy = ResolveFaultProxy(self.owner.url, self.h.certs,
            {'spiffe://agent-platform/worker/one': 'worker',
             'spiffe://agent-platform/worker/two': 'worker_two'}).start()
    def tearDown(self):
        self.proxy.close(); self.owner.close()
        evidence = self.proxy.evidence()
        self.assertTrue(evidence['closed']); self.assertEqual(evidence['live_connections'], 0)
        encoded = json.dumps(evidence)
        self.assertNotIn(TOKEN, encoded); self.assertNotIn(SECRET, encoded)
        self.assertNotIn('\"credentials\":', encoded)
    def request(self, client='worker', timeout=2, path=RESOLVE_PATH, body=REQUEST):
        context = ssl.create_default_context(cafile=self.h.certs['ca'])
        if client:
            context.load_cert_chain(self.h.certs[client+'_cert'], self.h.certs[client+'_key'])
        connection = http.client.HTTPSConnection('127.0.0.1', self.proxy.port,
                                                 context=context, timeout=timeout)
        try:
            connection.request('POST', path, body, {'Content-Type': 'application/json'})
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()
    def wait_fault(self):
        deadline = time.monotonic()+3
        while time.monotonic() < deadline:
            event = self.proxy.fault()
            if event and event.get('upstream_status') == 200: return event
            time.sleep(.005)
        self.fail('fixture owner 200 not observed')
    def wait_finished_fault(self):
        deadline = time.monotonic()+3
        while time.monotonic() < deadline:
            value = self.proxy.fault()
            if value and value.get('finished_ns'):
                return value
            time.sleep(.005)
        self.fail('fixture response bookkeeping did not finish')

    def test_batch_mutations_are_closed_set_and_keep_original_owner_metadata(self):
        batch = {'tenant_id': 'tnt_TozdDU85IZrcXYnI013cBNal',
            'profile_id': 'rpf_sP3LUNDKOyXLcmCOyUUCUetx', 'profile_revision_number': 1,
            'run_id': RUN, 'attempt_id': ATTEMPT, 'worker_id': 'worker-one', 'lease_epoch': 1,
            'manifest_id': 'rmf_U6hGMJwbnULS2YV8-w0R3NeX', 'manifest_digest': 'sha256:'+'a'*64,
            'credentials': [{'credential_id': 'crd_'+'1'*32, 'purpose': 'api_key',
                'audience_digest': 'sha256:'+'2'*64, 'credential_revision': 1, 'value': SECRET},
                {'credential_id': 'crd_'+'3'*32, 'purpose': 'dsn',
                'audience_digest': 'sha256:'+'4'*64, 'credential_revision': 1, 'value': SECRET}]}
        self.owner.body = json.dumps(batch).encode()
        for mode in ('missing_use', 'extra_use', 'duplicate_use', 'identity_mismatch'):
            with self.subTest(mode=mode):
                self.proxy.arm(mode)
                status, raw = self.request()
                self.assertEqual(status, 200)
                value = json.loads(raw)
                event = self.wait_finished_fault()
                self.assertEqual(event['upstream_status'], 200)
                self.assertEqual(event['downstream_status'], 200)
                self.assertEqual(event['mutation'], mode)
                self.assertEqual(event['attempt_id'], ATTEMPT)
                self.assertEqual(event['credential_count_before'], 2)
                self.assertEqual(event['credential_count_after'], len(value['credentials']))
                self.assertEqual(event['downstream_action'], 'mutated_owner_200')
                self.assertEqual(len(value['credentials']), {'missing_use':1,'extra_use':3,'duplicate_use':2,'identity_mismatch':2}[mode])
                self.assertTrue(all(c['value'] == SECRET for c in value['credentials']))
                self.assertEqual(set(value), set(batch))
                for field in set(batch)-{'credentials','attempt_id'}:
                    self.assertEqual(value[field], batch[field])
                if mode == 'identity_mismatch':
                    self.assertNotEqual(value['attempt_id'], ATTEMPT)
                    self.assertEqual(value['credentials'], batch['credentials'])
                elif mode == 'duplicate_use':
                    self.assertEqual(value['credentials'][0], value['credentials'][1])
                elif mode == 'extra_use':
                    self.assertEqual(value['credentials'][:2], batch['credentials'])
                    self.assertNotIn(value['credentials'][2]['credential_id'],
                                     [x['credential_id'] for x in batch['credentials']])
        self.assertEqual(len(self.owner.calls), 4)
        for mode,path in [('arbitrary_patch',RESOLVE_PATH),('missing_use',ATTEMPT_PATH),
                          ('hold_after_owner_200',ATTEMPT_PATH),('manifest_digest_mismatch',RESOLVE_PATH)]:
            with self.assertRaises(ValueError): self.proxy.arm(mode,path=path)

    def test_final_valid_identity_mutations_preserve_original_owner_metadata(self):
        self.assertEqual(relay.FINAL_PATH, FINAL_PATH)
        # The peer is the real Gateway principal on both authenticated TLS legs.
        self.proxy.close()
        self.proxy = ResolveFaultProxy(self.owner.url, self.h.certs,
            {'spiffe://agent-platform/channel-gateway': 'gateway'}).start()
        self.owner.body = json.dumps(FINAL).encode()
        self.owner.expected_request = FINAL_REQUEST
        for mode, field in (('final_tenant_mismatch', 'tenant_id'),
                            ('final_manifest_mismatch', 'manifest_digest')):
            with self.subTest(mode=mode):
                self.proxy.arm(mode, path=FINAL_PATH)
                status, raw = self.request('gateway', path=FINAL_PATH, body=FINAL_REQUEST)
                self.assertEqual(status, 200)
                actual = json.loads(raw)
                self.assertEqual(set(actual), set(FINAL))
                self.assertEqual([k for k in FINAL if actual[k] != FINAL[k]], [field])
                if field == 'tenant_id':
                    self.assertRegex(actual[field], r'^tnt_[A-Za-z0-9_-]{24}$')
                else:
                    self.assertRegex(actual[field], r'^sha256:[0-9a-f]{64}$')
                event = self.wait_finished_fault()
                self.assertEqual({k:event[k] for k in FINAL}, FINAL)
                self.assertEqual(event['mutation'], mode)
                self.assertEqual(event['mutation_field'], field)
                self.assertEqual(event['mutation_phase'], 'complete_owner_200_response')
                self.assertEqual(event['upstream_status'], 200)
                self.assertEqual(event['downstream_status'], 200)
                self.assertEqual(event['downstream_action'], 'mutated_owner_200')
                self.assertEqual(event['owner_response_bytes'], len(self.owner.body))
                self.assertEqual(event['downstream_body_bytes_forwarded'], len(raw))
                self.assertNotIn('grant_group', event)
                self.assertTrue(self.owner.calls[-1]['request_unchanged'])
                self.assertEqual(self.owner.calls[-1]['uris'], ['spiffe://agent-platform/channel-gateway'])
        self.assertEqual(len(self.owner.calls), 2)
        # Fault is one-shot; a later real request is forwarded byte-for-byte.
        self.assertEqual(self.request('gateway', path=FINAL_PATH, body=FINAL_REQUEST),
                         (200, self.owner.body))

    def test_final_fault_does_not_synthesize_owner_success_or_repair_invalid_proof(self):
        self.owner.expected_request = FINAL_REQUEST
        self.owner.body = json.dumps(FINAL).encode()
        for mode in ('final_tenant_mismatch', 'final_manifest_mismatch'):
            for owner_status in (403, 409, 503):
                with self.subTest(mode=mode, owner_status=owner_status):
                    self.owner.status = owner_status
                    self.proxy.arm(mode, path=FINAL_PATH)
                    self.assertEqual(self.request(path=FINAL_PATH, body=FINAL_REQUEST),
                                     (owner_status, b'{"error":"denied"}'))
                    event = self.wait_finished_fault()
                    self.assertEqual(event['upstream_status'], owner_status)
                    self.assertEqual(event['downstream_status'], owner_status)
                    self.assertEqual(event['downstream_action'], 'forwarded')
                    self.assertNotIn('mutation', event)
        self.owner.status = 200
        bad_proofs = [dict(FINAL, run_id='5'*32), dict(FINAL, execution_token=TOKEN),
                      dict(FINAL, tenant_id=None), dict(FINAL, manifest_digest=SECRET)]
        for proof in bad_proofs:
            self.owner.body = json.dumps(proof).encode()
            self.proxy.arm('final_tenant_mismatch', path=FINAL_PATH)
            with self.assertRaises((http.client.RemoteDisconnected, ConnectionError)):
                self.request(path=FINAL_PATH, body=FINAL_REQUEST)
            event = self.wait_finished_fault()
            self.assertEqual(event['upstream_status'], 200)
            self.assertEqual(event['transport_result'], 'ValueError')
            self.assertEqual(event['downstream_body_bytes_forwarded'], 0)
            self.assertNotIn('downstream_status', event)
            self.assertNotIn('mutation', event)
        self.owner.body = json.dumps(FINAL).encode()
        self.owner.declared_length = len(self.owner.body)+10
        self.proxy.arm('final_manifest_mismatch', path=FINAL_PATH)
        with self.assertRaises((http.client.RemoteDisconnected, ConnectionError)):
            self.request(path=FINAL_PATH, body=FINAL_REQUEST)
        event = self.wait_finished_fault()
        self.assertEqual(event['transport_result'], 'IncompleteRead')
        self.assertNotIn('upstream_status', event)
        self.assertNotIn('mutation', event)

    def test_final_modes_are_exclusive_to_final_post_and_keep_other_paths_unchanged(self):
        for mode, path in (('final_tenant_mismatch', RESOLVE_PATH),
                           ('final_manifest_mismatch', ATTEMPT_PATH),
                           ('missing_use', FINAL_PATH),
                           ('drop_after_owner_200', FINAL_PATH),
                           ('manifest_digest_mismatch', FINAL_PATH)):
            with self.assertRaises(ValueError):
                self.proxy.arm(mode, path=path)
        self.proxy.arm('final_tenant_mismatch', path=FINAL_PATH)
        self.assertEqual(self.request(), (200, BODY))
        self.assertIsNone(self.proxy.fault())
        self.owner.expected_request = FINAL_REQUEST
        self.owner.body = json.dumps(FINAL).encode()
        self.assertEqual(self.request(path=FINAL_PATH, body=FINAL_REQUEST)[0], 200)
        self.assertEqual(self.wait_finished_fault()['mutation'], 'final_tenant_mismatch')

    def test_proof_drop_requires_real_complete_owner_200(self):
        self.proxy.arm('drop_after_owner_200', path=ATTEMPT_PATH)
        with self.assertRaises((http.client.RemoteDisconnected, ConnectionError)):
            self.request(path=ATTEMPT_PATH)
        event = self.wait_finished_fault()
        self.assertEqual(event['upstream_status'], 200)
        self.assertEqual(event['downstream_body_bytes_forwarded'], 0)
        self.assertNotIn('downstream_status', event)
        self.assertEqual(len(self.owner.calls), 1)

    def test_manifest_digest_mismatch_is_request_mutation_with_actual_owner_403(self):
        body = json.dumps({'execution_token': TOKEN, 'manifest_digest':'sha256:'+'a'*64,
            'manifest_id':'rmf_U6hGMJwbnULS2YV8-w0R3NeX','workload_identity':'worker-one'}).encode()
        self.owner.expected_request = body
        self.owner.reject_digest_mismatch = True
        self.proxy.arm('manifest_digest_mismatch', path=ATTEMPT_PATH)
        self.assertEqual(self.request(path=ATTEMPT_PATH,body=body), (403,b'{"error":"denied"}'))
        event = self.wait_finished_fault()
        self.assertEqual(event['upstream_status'], 403)
        self.assertEqual(event['downstream_status'], 403)
        self.assertEqual(event['mutation'], 'manifest_digest_mismatch')
        self.assertEqual(event['mutation_phase'], 'request_before_owner')
        self.assertFalse(self.owner.calls[0]['request_unchanged'])
        self.assertNotIn('manifest_digest', event)
        self.assertEqual(self.request(path=ATTEMPT_PATH,body=body), (200,BODY))
        self.assertTrue(self.owner.calls[1]['request_unchanged'])

    def test_idle_authenticated_tls_connection_is_counted_and_closed(self):
        context = ssl.create_default_context(cafile=self.h.certs['ca'])
        context.load_cert_chain(self.h.certs['worker_cert'], self.h.certs['worker_key'])
        client = context.wrap_socket(socket.create_connection(('127.0.0.1', self.proxy.port)),
                                     server_hostname='127.0.0.1')
        try:
            deadline = time.monotonic()+.5
            while time.monotonic() < deadline and self.proxy.evidence()['live_connections'] == 0:
                time.sleep(.005)
            self.assertEqual(self.proxy.evidence()['live_connections'], 1)
            self.proxy.close()
            client.settimeout(.5)
            self.assertEqual(client.recv(1), b'')
            self.assertEqual(self.proxy.evidence()['live_connections'], 0)
        finally:
            client.close()

    def test_oversize_response_closes_real_httpresponse_even_after_rejection(self):
        self.owner.body = b'x' * (8*1024*1024+2)
        captured = []
        original = http.client.HTTPSConnection.getresponse
        owner_port = self.owner.server.server_address[1]
        def tracking(connection):
            response = original(connection)
            if connection.port == owner_port:
                captured.append(response)
            return response
        try:
            with mock.patch.object(http.client.HTTPSConnection, 'getresponse', tracking):
                with self.assertRaises((http.client.RemoteDisconnected, ConnectionError)):
                    self.request()
            self.assertEqual(len(captured), 1)
            self.assertTrue(captured[0].isclosed(), 'rejected response retained a live reader')
        finally:
            for response in captured:
                response.close()

    def test_truncated_owner_body_is_transport_failure_not_complete_200(self):
        self.owner.body = b'{"partial":'
        self.owner.declared_length = 1000
        self.proxy.arm('drop_after_owner_200')
        with self.assertRaises((http.client.RemoteDisconnected, ConnectionError)):
            self.request()
        event = self.proxy.fault()
        self.assertEqual(event.get('transport_result'), 'IncompleteRead')
        self.assertNotIn('upstream_status', event)
        self.assertNotIn('downstream_status', event)

    def test_real_tls_both_legs_preserve_request_peer_and_response(self):
        self.assertEqual(self.request('worker_two'), (200, BODY))
        self.assertEqual(self.owner.calls, [{'uris': ['spiffe://agent-platform/worker/two'],
            'request_unchanged': True, 'identity_header': None}])
        # The client can receive the final bytes before the proxy records its
        # post-flush observation. Wait for that phase, then compare exact bytes.
        deadline = time.monotonic() + 3
        while True:
            event = self.proxy.events()[0]
            if 'downstream_status' in event or time.monotonic() >= deadline:
                break
            time.sleep(.005)
        self.assertEqual(event.get('downstream_status'), 200)
        self.assertEqual(event['attempt_id'], ATTEMPT)
        self.assertEqual(event['upstream_status'], 200)
        self.assertEqual(event['downstream_body_bytes_forwarded'], len(BODY))
    def test_drop_follows_exactly_one_complete_actual_upstream_response(self):
        self.proxy.arm('drop_after_owner_200')
        with self.assertRaises((http.client.RemoteDisconnected, ConnectionError)):
            self.request()
        fault = self.wait_fault()
        self.assertEqual(fault['upstream_status'], 200)
        self.assertEqual(fault['owner_response_bytes'], len(BODY))
        self.assertEqual(fault['downstream_body_bytes_forwarded'], 0)
        self.assertEqual(len(self.owner.calls), 1)
        self.assertEqual(self.request(), (200, BODY))
        self.assertEqual(len(self.owner.calls), 2)
    def test_hold_is_caller_timeout_not_synthetic_http_status(self):
        self.proxy.arm('hold_after_owner_200')
        with ThreadPoolExecutor(1) as pool:
            pending = pool.submit(self.request, 'worker', .2)
            event = self.wait_fault()
            self.assertNotIn('finished_ns', event)
            with self.assertRaises(TimeoutError): pending.result(2)
            self.assertNotIn('downstream_status', self.proxy.fault())
            self.proxy.release()
        self.assertEqual(len(self.owner.calls), 1)
    def test_existing_owner_denial_is_forwarded_not_replaced_by_fault(self):
        self.owner.status = 403
        self.proxy.arm('drop_after_owner_200')
        self.assertEqual(self.request(), (403, b'{"error":"denied"}'))
        self.assertEqual(self.proxy.fault()['upstream_status'], 403)
    def test_missing_client_certificate_fails_before_owner(self):
        with self.assertRaises((ssl.SSLError, ConnectionError)):
            self.request(None)
        self.assertEqual(self.owner.calls, [])
    def test_valid_but_unmapped_client_is_denied_before_owner(self):
        self.assertEqual(self.request('gateway'), (403, b''))
        self.assertEqual(self.owner.calls, [])


class ResolveMetadataTests(unittest.TestCase):
    def test_actual_binary_generated_identity_shapes_are_preserved_field_by_field(self):
        # Actual failed gate IDs came from its public publication and Worker log,
        # not a saved credential response. Shapes are pinned to owner generators:
        # Gateway 16-byte hex; Worker att_16-byte hex; Control 18-byte raw URL64.
        ids = {'run_id': RUN, 'attempt_id': ATTEMPT,
               'tenant_id': 'tnt_TozdDU85IZrcXYnI013cBNal',
               'profile_id': 'rpf_sP3LUNDKOyXLcmCOyUUCUetx',
               'manifest_id': 'rmf_U6hGMJwbnULS2YV8-w0R3NeX'}
        self.assertEqual(safe_metadata(json.dumps(ids).encode()), ids)
        for field in ids:
            for bad in ('', 'secret with spaces', ids[field]+'x', RUN if field != 'run_id' else ATTEMPT):
                altered = dict(ids, **{field: bad})
                self.assertNotIn(field, safe_metadata(json.dumps(altered).encode()))

    def test_closed_metadata_allowlist_discards_secret_fields_and_bad_types(self):
        self.assertEqual(safe_metadata(json.dumps({'run_id': SECRET, 'attempt_id': ATTEMPT,
            'execution_token': TOKEN, 'worker_id': SECRET, 'lease_epoch': True,
            'credentials': [{'value': SECRET}], 'profile_revision_number': 1}).encode()),
            {'attempt_id': ATTEMPT, 'profile_revision_number': 1})
    def test_final_metadata_keeps_original_closed_fields_not_secret_bodies(self):
        self.assertEqual(relay.safe_final_metadata(json.dumps(FINAL).encode()), FINAL)
        for key in ('intent_id', 'admission_id', 'run_id', 'attempt_id', 'completion_id', 'tenant_id'):
            # Valid generic wire ID is still too broad for persisted observations.
            proof = dict(FINAL, **{key: SECRET})
            self.assertNotIn(key, relay.safe_final_metadata(json.dumps(proof).encode()))
        for key in ('execution_token', 'credentials', 'api_key', 'dsn'):
            self.assertEqual(relay.safe_final_metadata(json.dumps(dict(FINAL, **{key:TOKEN})).encode()), {})
        with self.assertRaises(ValueError):
            relay.mutate_final(json.dumps(FINAL).encode(), 'arbitrary_patch', FINAL_REQUEST)

    def test_final_mutation_rejects_wrong_types_duplicates_bounds_and_original_mismatch(self):
        raw = json.dumps(FINAL).encode()
        invalid = [raw+b' {}', raw+b' '*4096,
            raw[:-1]+b',"tenant_id":"tnt_TozdDU85IZrcXYnI013cBNal"}',
            raw.decode().encode('utf-16'), b'[]']
        for key, value in (('sequence', True), ('sequence', 1.0), ('sequence', 2),
                           ('execution_generation', 0), ('execution_generation', 9007199254740992),
                           ('manifest_digest', SECRET), ('tenant_id', ''), ('run_id', None)):
            invalid.append(json.dumps(dict(FINAL, **{key:value})).encode())
        for body in invalid:
            with self.assertRaises((ValueError, UnicodeError)):
                relay.mutate_final(body, 'final_tenant_mismatch', FINAL_REQUEST)
        request = json.loads(FINAL_REQUEST)
        request['run_id'] = '5'*32
        with self.assertRaises(ValueError):
            relay.mutate_final(raw, 'final_manifest_mismatch', json.dumps(request).encode())

    def test_group_equality_crosses_relays_without_returning_fingerprint(self):
        groups = GrantGroups()
        first = groups.label(REQUEST)
        second = groups.label(json.dumps({'extra': 1, 'execution_token': TOKEN}).encode())
        self.assertEqual(first, second); self.assertIs(type(first), int)
        self.assertNotEqual(first, groups.label(b'{"execution_token":"different"}'))
        self.assertIsNone(groups.label(b'not JSON'))


if __name__ == '__main__': unittest.main()
