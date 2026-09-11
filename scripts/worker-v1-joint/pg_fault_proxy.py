"""Disposable plaintext PostgreSQL-v3 commit-response-loss fixture.

Bytes are never changed, queries are never injected, and no authentication,
parameters, rows, error bodies or BackendKeyData are recorded. The one-shot
fault suppresses a candidate INSERT's replies, observes CommandComplete plus
idle ReadyForQuery from the real server, then closes the socket. This narrow
fixture supports the Worker's ordinary extended-query/autocommit path only;
an SSL/GSS negotiation request is rejected, not silently downgraded.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
import select
import socket
import socketserver
import struct
import threading
import time

MAX_FRAME = 8 * 1024 * 1024
MAX_STARTUP = 64 * 1024
MAX_NAMES = 256


def _time():
    return {'at': datetime.now(timezone.utc).isoformat(), 'monotonic_ns': time.monotonic_ns()}


def _cstring(payload, offset=0):
    end = payload.find(b'\0', offset)
    if end < 0:
        raise ValueError('invalid_protocol_string')
    return payload[offset:end], end + 1


def _classify(query):
    normalized = b' '.join(query.strip().lower().split())
    if normalized.startswith(b'insert into runtime_session.session_candidates ') and b';' not in normalized:
        return 'candidate_insert'
    if normalized.startswith(b'select ') and b' from runtime_session.session_candidates ' in normalized:
        return 'candidate_read'
    return 'other'


class PGCommitFaultProxy:
    def __init__(self, upstream_host, upstream_port, host='127.0.0.1', port=0):
        self.upstream = (upstream_host, int(upstream_port))
        self.host, self.port = host, int(port)
        self._lock = threading.Lock()
        self._server = None
        self._thread = None
        self._sockets = set()
        self._handlers = set()
        self._next_connection = 0
        self._armed = False
        self._claimed = False
        self._closed = False
        self._fault = None
        self._done = threading.Event()
        self._server_ready = threading.Event()
        self._release_drop = threading.Event()
        self._ready_record = None
        self._pause_drop = False
        self._reads = []
        self._connections = []

    def start(self):
        if self._server is not None or self._closed:
            raise RuntimeError('proxy_already_started_or_closed')
        owner = self
        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True
            block_on_close = False
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                owner._relay(self.request)
        self._server = Server((self.host, self.port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        kwargs={'poll_interval': .02}, daemon=True)
        self._thread.start()
        return self

    def arm_next_insert(self, *, pause_before_disconnect=False):
        with self._lock:
            if self._armed or self._claimed or self._closed:
                raise RuntimeError('commit_fault_is_one_shot')
            self._armed = True
            self._pause_drop = pause_before_disconnect

    def _claim(self, connection):
        with self._lock:
            if not self._armed or self._claimed:
                return None
            self._claimed = True
            self._armed = False
        return {'connection_id': connection, 'phase': 'candidate_bind_observed',
                'commit_observed': False, 'execute_observed': False, 'sync_observed': False,
                'command_tag': None, 'ready_status': None, 'error_response_observed': False,
                'suppressed_message_types': [], 'suppressed_bytes': 0,
                'successful_response_bytes_forwarded': 0, 'bind': _time()}

    def _finish(self, record, reason):
        if record is None:
            return
        record['phase'] = 'server_commit_observed_response_dropped' if record['commit_observed'] else 'commit_not_proven'
        record['end_reason'] = reason
        record['client_disconnect'] = _time()
        with self._lock:
            if self._fault is None:
                self._fault = copy.deepcopy(record)
                self._done.set()

    def _read(self, connection):
        with self._lock:
            # Categories/timing only. Neither SQL strings nor parameter values
            # enter evidence. The immutable SQL readback ties the row to RunID.
            self._reads.append({'connection_id': connection, **_time()})

    def wait_server_commit(self, timeout=30):
        if not self._server_ready.wait(timeout):
            raise TimeoutError('no_server_commit_proof_observed')
        with self._lock:
            return copy.deepcopy(self._ready_record)

    def release_drop(self):
        self._release_drop.set()

    def wait_fault(self, timeout=30):
        if not self._done.wait(timeout):
            raise TimeoutError('no_candidate_commit_response_fault_observed')
        with self._lock:
            return copy.deepcopy(self._fault)

    def evidence(self):
        with self._lock:
            return {'fault': copy.deepcopy(self._fault),
                    'candidate_reads': copy.deepcopy(self._reads),
                    'connections': copy.deepcopy(self._connections),
                    'live_connections': len(self._handlers), 'closed': self._closed,
                    'payloads_recorded': False}

    def _relay(self, client):
        upstream, fault = None, None
        thread = threading.current_thread()
        with self._lock:
            if self._closed:
                return
            self._next_connection += 1
            number = self._next_connection
            self._handlers.add(thread)
            self._sockets.add(client)
            self._connections.append({'connection_id': number, 'event': 'opened', **_time()})
        reason = 'peer_eof_before_commit_proof'
        try:
            upstream = socket.create_connection(self.upstream, timeout=3)
            upstream.settimeout(3)
            client.settimeout(3)
            with self._lock:
                self._sockets.add(upstream)
            frontend, backend = bytearray(), bytearray()
            statements, portals = {}, {}
            startup = True
            while True:
                readable, _, _ = select.select([client, upstream], [], [], .25)
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        return
                    buffer = frontend if source is client else backend
                    buffer.extend(data)
                    while True:
                        if source is client and startup:
                            if len(buffer) < 4:
                                break
                            size = struct.unpack('!I', buffer[:4])[0]
                            if size < 8 or size > MAX_STARTUP:
                                raise ValueError('invalid_startup_size')
                            if len(buffer) < size:
                                break
                            protocol = struct.unpack('!I', buffer[4:8])[0]
                            if protocol != 196608 and not (protocol == 80877102 and size == 16):
                                # This fixture never sends an SSL fallback byte.
                                # CancelRequest uses a separate short-lived socket;
                                # forward its private backend key without recording it.
                                raise ValueError('plaintext_v3_startup_required')
                            upstream.sendall(buffer[:size])
                            del buffer[:size]
                            startup = False
                            continue
                        if len(buffer) < 5:
                            break
                        size = struct.unpack('!I', buffer[1:5])[0]
                        if size < 4 or size > MAX_FRAME:
                            raise ValueError('invalid_protocol_frame_size')
                        if len(buffer) < size + 1:
                            break
                        packet = bytes(buffer[:size + 1])
                        del buffer[:size + 1]
                        kind, payload = packet[:1], packet[5:]
                        if source is client:
                            if kind == b'P':
                                name, offset = _cstring(payload)
                                query, _ = _cstring(payload, offset)
                                if len(statements) >= MAX_NAMES and name not in statements:
                                    raise ValueError('statement_map_capacity')
                                statements[name] = _classify(query)
                            elif kind == b'B':
                                portal, offset = _cstring(payload)
                                name, _ = _cstring(payload, offset)
                                if len(portals) >= MAX_NAMES and portal not in portals:
                                    raise ValueError('portal_map_capacity')
                                category = statements.get(name, 'other')
                                portals[portal] = category
                                if category == 'candidate_insert' and fault is None:
                                    fault = self._claim(number)
                            elif kind == b'E':
                                portal, _ = _cstring(payload)
                                category = portals.get(portal, 'other')
                                if category == 'candidate_insert' and fault is not None:
                                    fault['execute_observed'] = True
                                if category == 'candidate_read':
                                    self._read(number)
                            elif kind == b'S' and fault is not None:
                                fault['sync_observed'] = True
                            elif kind == b'Q':
                                query, _ = _cstring(payload)
                                if _classify(query) == 'candidate_read':
                                    self._read(number)
                            elif kind == b'C' and payload:
                                name, _ = _cstring(payload, 1)
                                (statements if payload[:1] == b'S' else portals).pop(name, None)
                            upstream.sendall(packet)
                        elif fault is None:
                            client.sendall(packet)
                        else:
                            # No success bytes from the targeted query have ever
                            # reached the client. Do not save the payload itself.
                            fault['suppressed_message_types'].append(kind.decode('ascii', 'replace'))
                            fault['suppressed_bytes'] += len(packet)
                            if kind == b'C':
                                if payload == b'INSERT 0 1\0' and fault['command_tag'] is None:
                                    fault['command_tag'] = 'INSERT 0 1'
                                else:
                                    fault['command_tag'] = 'unexpected_or_multiple_command'
                            elif kind == b'E':
                                fault['error_response_observed'] = True
                            elif kind == b'Z':
                                fault['ready_status'] = payload.decode('ascii') if payload in (b'I', b'T', b'E') else 'invalid'
                                fault['server_ready'] = _time()
                                fault['commit_observed'] = (fault['execute_observed'] and fault['sync_observed'] and
                                    fault['command_tag'] == 'INSERT 0 1' and payload == b'I' and
                                    not fault['error_response_observed'])
                                if fault['commit_observed']:
                                    fault['phase'] = 'server_commit_observed_before_disconnect'
                                    with self._lock:
                                        self._ready_record = copy.deepcopy(fault)
                                    self._server_ready.set()
                                    if self._pause_drop:
                                        fault['disconnect_released_by_test'] = self._release_drop.wait(3)
                                reason = 'server_ready_then_disconnect'
                                return
        except (OSError, ValueError):
            # Raw exceptions may carry sensitive upstream data; categories only.
            reason = 'transport_or_protocol_error'
        finally:
            if fault is not None:
                fault['disconnect_started'] = _time()
            for connection in (client, upstream):
                if connection is not None:
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    connection.close()
            self._finish(fault, reason)
            with self._lock:
                self._sockets.discard(client)
                self._sockets.discard(upstream)
                self._handlers.discard(thread)
                self._connections.append({'connection_id': number, 'event': 'closed', **_time()})

    def close(self):
        self.release_drop()
        with self._lock:
            self._closed = True
        server, self._server = self._server, None
        if server is not None:
            server.shutdown()
            server.server_close()
        with self._lock:
            sockets, handlers = list(self._sockets), list(self._handlers)
        for connection in sockets:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        for handler in handlers:
            handler.join(timeout=4)
            if handler.is_alive():
                raise RuntimeError('protocol_proxy_handler_did_not_stop')
        if self._thread is not None:
            self._thread.join(timeout=3)
            if self._thread.is_alive():
                raise RuntimeError('protocol_proxy_listener_did_not_stop')
            self._thread = None
