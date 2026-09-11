"""Fast L4 helper checks; process acceptance remains the joint runner's job."""
import socket
import socketserver
import threading
import unittest

from faults import ProofSwitch, submit


class EchoServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class Echo(socketserver.BaseRequestHandler):
    def handle(self):
        while data := self.request.recv(8192):
            self.request.sendall(data)


class ProofSwitchTest(unittest.TestCase):
    def test_raw_bytes_fall_through_dead_backend_and_close(self):
        echo = EchoServer(('127.0.0.1', 0), Echo)
        thread = threading.Thread(target=echo.serve_forever,
                                  kwargs={'poll_interval': 0.02}, daemon=True)
        thread.start()
        relay = ProofSwitch(port=0).start()
        try:
            closed = socket.socket()
            closed.bind(('127.0.0.1', 0))
            dead_port = closed.getsockname()[1]
            closed.close()
            relay.add('killed', '127.0.0.1', dead_port)
            relay.add('survivor', '127.0.0.1', echo.server_address[1])
            for _ in range(2):
                with socket.create_connection(('127.0.0.1', relay.port), timeout=2) as client:
                    payload = bytes(range(256)) * 32
                    client.sendall(payload)
                    result = b''
                    while len(result) < len(payload):
                        result += client.recv(len(payload) - len(result))
                    self.assertEqual(result, payload)
            with socket.create_connection(('127.0.0.1', relay.port), timeout=2) as client:
                client.sendall(b'before')
                self.assertEqual(client.recv(6), b'before')
                self.assertGreaterEqual(relay.disconnect(), 2)
                self.assertEqual(client.recv(1), b'')
            with socket.create_connection(('127.0.0.1', relay.port), timeout=2) as client:
                client.sendall(b'after')
                self.assertEqual(client.recv(5), b'after')
            relay.remove('survivor')
            with socket.create_connection(('127.0.0.1', relay.port), timeout=2) as client:
                self.assertEqual(client.recv(1), b'')
        finally:
            relay.close()
            relay.close()
            echo.shutdown()
            echo.server_close()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())


class IntakeObservationTest(unittest.TestCase):
    def test_gateway_admission_does_not_imply_immediate_worker_row(self):
        class Observations:
            reads = 0
            def send_text(self, text, conversation_id):
                self.request = (text, conversation_id)
                return 'run-delayed'
            def sql(self, query):
                self.reads += 1
                return [['[]' if self.reads == 1 else '[{"run_id":"run-delayed"}]']]
            def wait(self, predicate, description, timeout):
                self.asserted_delay = not predicate()
                assert predicate()
        observation = Observations()
        self.assertEqual(submit(observation, 'text', '123'), 'run-delayed')
        self.assertTrue(observation.asserted_delay)
        self.assertEqual(observation.reads, 2)
        self.assertEqual(observation.request, ('text', '123'))


if __name__ == '__main__':
    unittest.main()
