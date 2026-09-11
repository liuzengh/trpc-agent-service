"""Socket-level protocol fixture checks; these do not claim a real DB commit."""
import socket
import struct
import threading
import unittest

from pg_fault_proxy import PGCommitFaultProxy


def frame(kind, payload=b''):
    return kind.encode() + struct.pack('!I', len(payload) + 4) + payload


def read_exact(sock, count):
    result = b''
    while len(result) < count:
        data = sock.recv(count - len(result))
        if not data:
            raise EOFError('protocol peer closed')
        result += data
    return result


def read_frame(sock):
    kind = read_exact(sock, 1)
    length = struct.unpack('!I', read_exact(sock, 4))[0]
    return kind, read_exact(sock, length - 4)


INSERT = b'INSERT INTO runtime_session.session_candidates (candidate_ref) VALUES ($1) ON CONFLICT DO NOTHING'


class ScriptedPostgres:
    def __init__(self, fn):
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.listener.settimeout(3)
        self.port = self.listener.getsockname()[1]
        self.error = None
        self.fn = fn
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            conn, _ = self.listener.accept()
            with conn:
                conn.settimeout(3)
                length = struct.unpack('!I', read_exact(conn, 4))[0]
                startup = read_exact(conn, length - 4)
                assert startup == struct.pack('!I', 196608) + b'user\0session_runtime\0\0'
                conn.sendall(frame('R', struct.pack('!I', 0)) + frame('Z', b'I'))
                self.fn(conn)
        except Exception as error:
            self.error = error

    def close(self):
        self.thread.join(timeout=4)
        self.listener.close()
        assert not self.thread.is_alive()
        if self.error is not None:
            raise self.error


class ProxyTest(unittest.TestCase):
    def connect(self, proxy):
        client = socket.create_connection(('127.0.0.1', proxy.port), timeout=3)
        startup = struct.pack('!I', 196608) + b'user\0session_runtime\0\0'
        client.sendall(struct.pack('!I', len(startup) + 4) + startup)
        self.assertEqual(read_frame(client), (b'R', struct.pack('!I', 0)))
        self.assertEqual(read_frame(client), (b'Z', b'I'))
        return client

    def prepare(self, client, sql=INSERT):
        client.sendall(frame('P', b'candidate\0' + sql + b'\0\0\0') + frame('D', b'Scandidate\0') + frame('S'))
        self.assertEqual(read_frame(client), (b'1', b''))
        self.assertEqual(read_frame(client), (b'n', b''))
        self.assertEqual(read_frame(client), (b'Z', b'I'))

    def exercise(self, response, expected_commit, execute=True, eof=False):
        forwarded = []
        def script(conn):
            for expected in (b'P', b'D', b'S'):
                self.assertEqual(read_frame(conn)[0], expected)
            conn.sendall(frame('1') + frame('n') + frame('Z', b'I'))
            for expected in ((b'B', b'E', b'S') if execute else (b'B', b'S')):
                message = read_frame(conn)
                self.assertEqual(message[0], expected)
                forwarded.append(message)
            conn.sendall(response)
            if not eof:
                self.assertEqual(conn.recv(1), b'')
        pg = ScriptedPostgres(script)
        proxy = PGCommitFaultProxy('127.0.0.1', pg.port).start()
        try:
            with self.connect(proxy) as client:
                proxy.arm_next_insert()
                self.prepare(client)
                self.assertIsNone(proxy.evidence()['fault'])
                bind = frame('B', b'\0candidate\0' + b'\0\0\0\1' + struct.pack('!i', 13) + b'PRIVATE-VALUE' + b'!\0\0')
                wire = bind + (frame('E', b'\0' + struct.pack('!I', 0)) if execute else b'') + frame('S')
                # Fragmentation must not expose even BindComplete downstream.
                for offset in range(0, len(wire), 3):
                    client.sendall(wire[offset:offset+3])
                self.assertEqual(client.recv(1), b'')
                receipt = proxy.wait_fault(timeout=2)
                self.assertEqual(receipt['commit_observed'], expected_commit)
                self.assertEqual(receipt['successful_response_bytes_forwarded'], 0)
                self.assertNotIn('PRIVATE', str(proxy.evidence()))
                self.assertNotIn(INSERT.decode(), str(proxy.evidence()))
                if expected_commit:
                    self.assertEqual(receipt['command_tag'], 'INSERT 0 1')
                    self.assertEqual(receipt['ready_status'], 'I')
                    self.assertGreater(receipt['suppressed_bytes'], 0)
                self.assertEqual(forwarded[0][1], bind[5:])
        finally:
            proxy.close()
            proxy.close()
            pg.close()
        self.assertEqual(proxy.evidence()['live_connections'], 0)

    def test_extended_protocol_success_is_withheld_through_idle_ready(self):
        self.exercise(frame('2') + frame('C', b'INSERT 0 1\0') + frame('Z', b'I'), True)

    def test_commit_barrier_keeps_client_open_without_success_bytes_until_release(self):
        def script(conn):
            for expected in (b'P', b'D', b'S'):
                self.assertEqual(read_frame(conn)[0], expected)
            conn.sendall(frame('1') + frame('n') + frame('Z', b'I'))
            for expected in (b'B', b'E', b'S'):
                self.assertEqual(read_frame(conn)[0], expected)
            conn.sendall(frame('2') + frame('C', b'INSERT 0 1\0') + frame('Z', b'I'))
            self.assertEqual(conn.recv(1), b'')
        pg = ScriptedPostgres(script)
        proxy = PGCommitFaultProxy('127.0.0.1', pg.port).start()
        try:
            with self.connect(proxy) as client:
                self.prepare(client)
                proxy.arm_next_insert(pause_before_disconnect=True)
                client.sendall(frame('B', b'\0candidate\0\0\0\0\0\0\0') + frame('E', b'\0' + struct.pack('!I', 0)) + frame('S'))
                receipt = proxy.wait_server_commit(timeout=2)
                self.assertTrue(receipt['commit_observed'])
                self.assertEqual(receipt['phase'], 'server_commit_observed_before_disconnect')
                self.assertIsNone(proxy.evidence()['fault'])
                client.settimeout(.05)
                with self.assertRaises(socket.timeout):
                    client.recv(1)
                proxy.release_drop()
                client.settimeout(2)
                self.assertEqual(client.recv(1), b'')
                dropped = proxy.wait_fault(timeout=2)
                self.assertTrue(dropped['disconnect_released_by_test'])
                self.assertTrue(dropped['commit_observed'])
        finally:
            proxy.close()
            pg.close()

    def test_cancel_request_is_byte_identical_and_does_not_record_backend_key(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0)); listener.listen(); listener.settimeout(2)
        proxy = PGCommitFaultProxy('127.0.0.1', listener.getsockname()[1]).start()
        packet = struct.pack('!IIII', 16, 80877102, 1234567, 987654321)
        try:
            proxy.arm_next_insert()
            with socket.create_connection(('127.0.0.1', proxy.port), timeout=2) as client:
                client.sendall(packet)
                upstream, _ = listener.accept()
                with upstream:
                    upstream.settimeout(2)
                    self.assertEqual(read_exact(upstream, 16), packet)
                self.assertEqual(client.recv(1), b'')
            self.assertIsNone(proxy.evidence()['fault'])
            self.assertNotIn('987654321', str(proxy.evidence()))
        finally:
            proxy.close(); listener.close()

    def test_command_complete_without_ready_is_not_commit(self):
        self.exercise(frame('2') + frame('C', b'INSERT 0 1\0'), False, eof=True)

    def test_error_even_with_idle_ready_is_not_commit(self):
        self.exercise(frame('2') + frame('E', b'SERROR\0Mprivate-error\0\0') + frame('Z', b'I'), False)

    def test_open_transaction_is_not_commit(self):
        self.exercise(frame('2') + frame('C', b'INSERT 0 1\0') + frame('Z', b'T'), False)

    def test_rollback_is_not_commit(self):
        self.exercise(frame('2') + frame('C', b'ROLLBACK\0') + frame('Z', b'I'), False)

    def test_conflicting_replay_is_not_fresh_insert_commit(self):
        self.exercise(frame('2') + frame('C', b'INSERT 0 0\0') + frame('Z', b'I'), False)

    def test_bind_without_execute_is_not_commit(self):
        self.exercise(frame('2') + frame('C', b'INSERT 0 1\0') + frame('Z', b'I'), False, execute=False)

    def test_no_candidate_write_never_proves_commit_and_bytes_are_transparent(self):
        payload = frame('T', b'opaque descriptor') + frame('D', b'opaque row') + frame('C', b'SELECT 1\0') + frame('Z', b'I')
        def script(conn):
            self.assertEqual(read_frame(conn), (b'Q', b'SELECT 1\0'))
            conn.sendall(payload)
            self.assertEqual(conn.recv(1), b'')
        pg = ScriptedPostgres(script)
        proxy = PGCommitFaultProxy('127.0.0.1', pg.port).start()
        try:
            with self.connect(proxy) as client:
                proxy.arm_next_insert()
                client.sendall(frame('Q', b'SELECT 1\0'))
                self.assertEqual(read_exact(client, len(payload)), payload)
                with self.assertRaises(TimeoutError):
                    proxy.wait_fault(timeout=.05)
                self.assertIsNone(proxy.evidence()['fault'])
        finally:
            proxy.close()
            pg.close()


if __name__ == '__main__':
    unittest.main()
