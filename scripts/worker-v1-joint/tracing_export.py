"""T14: failing/slow OTLP and a truly full stdout pipe on real processes.

No business timestamps/rows or live deployment configuration are changed. Runtime
counter/drop semantics are additionally tested by the named Go saturation test.
"""
from contextlib import contextmanager
import array
import fcntl
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import termios
import threading
import time
from unittest.mock import patch
import uuid
from faults import candidates, completions, model_calls, outboxes


class ExportFault:
    def __init__(self, h, mode):
        self.mode, self.h = mode, h
        self.records, self.lock = [], threading.Lock()
        self.release = threading.Event()
        fixture = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_POST(self):
                if self.path != '/v1/traces':
                    self.send_error(404); return
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= 4 << 20:
                    self.send_error(413); return
                raw = self.rfile.read(size)
                record = {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw), 'received_at': time.monotonic(), 'mode': fixture.mode,
                          'canary_present': b'TRACE_BODY_CANARY' in raw or any(s.encode() in raw for s in h.secrets), 'response': None}
                with fixture.lock:
                    fixture.records.append(record)
                if fixture.mode == 'slow':
                    fixture.release.wait(30)
                body = b'TRACE_BODY_CANARY collector unavailable'
                try:
                    self.send_response(503)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    with fixture.lock:
                        record['response'] = 503
                except (BrokenPipeError, ConnectionResetError):
                    with fixture.lock:
                        record['response'] = 'client_closed'
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = 'http://127.0.0.1:' + str(self.server.server_port) + '/v1/traces'
    def snapshot(self):
        with self.lock:
            return [dict(r) for r in self.records]
    def close(self):
        self.release.set()
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=3)
        assert not self.thread.is_alive()


class FullPipe:
    def __init__(self):
        self.reader, self.writer = os.pipe()
        os.set_blocking(self.writer, False)
        self.filled = 0
        while True:
            try:
                self.filled += os.write(self.writer, b'P' * 4096)
            except BlockingIOError:
                break
        assert self.filled > 0
        # Set blocking before inheritance; never mutate descriptor flags while
        # the child writes. No reader drains this pipe during the process test.
        os.set_blocking(self.writer, True)
    def snapshot(self):
        value = array.array('i', [0])
        fcntl.ioctl(self.reader, termios.FIONREAD, value, True)
        return {'prefilled_bytes': self.filled, 'unread_bytes': value[0], 'reader_drained': False}
    def close(self):
        os.close(self.writer); os.close(self.reader)


@contextmanager
def blocked_stdout(h):
    sinks = {}
    real_popen = subprocess.Popen
    def spawn(*args, **kwargs):
        argv = args[0] if args else kwargs.get('args', [])
        name = Path(str(argv[0])).name if argv else ''
        if kwargs.get('start_new_session') and name in ('agent-worker', 'channel-gateway'):
            assert name not in sinks
            sink = FullPipe(); sinks[name] = sink
            kwargs['stderr'] = kwargs['stdout']  # stderr stays independently observable
            kwargs['stdout'] = sink.writer
        return real_popen(*args, **kwargs)
    try:
        with patch('harness.subprocess.Popen', side_effect=spawn):
            yield sinks
    finally:
        # Stop children before closing an undrained pipe, including assertion failures.
        if h.worker and h.worker.poll() is None:
            h.stop_worker()
        if h.gateway.process and h.gateway.process.poll() is None:
            h.gateway.stop()
        for sink in sinks.values():
            sink.close()


def configure(h, config):
    h.tracing = dict(config)
    Path(h.trace_config).write_text(json.dumps(config))


def stop_pair(h):
    records = []
    for role, process, stop, bound in [('worker', h.worker, h.stop_worker, 25), ('gateway', h.gateway.process, h.gateway.stop, 45)]:
        started = time.monotonic(); stop(); duration = time.monotonic() - started
        assert process.returncode == 0 and duration < bound
        records.append({'role': role, 'pid': process.pid, 'exit': process.returncode, 'seconds': duration, 'bound_seconds': bound})
    return records


def rounds(h, prefix, count):
    results = []
    for n in range(count):
        text = 'TRACE_BODY_CANARY export-' + prefix + '-' + str(n) + '-' + uuid.uuid4().hex[:8]
        run = h.send_text(text)
        delivery = h.wait_delivery(run)
        assert len(model_calls(h, text)) == len(candidates(h, run)) == len(completions(h, run)) == len(outboxes(h, run)) == 1
        traceparent = h.sql('SELECT traceparent FROM worker.execution_runs WHERE run_id=' + h.quote(run))[0][0]
        assert traceparent.endswith('-01')
        results.append({'run_id': run, 'traceparent': traceparent, 'delivery': delivery, 'model_calls': 1})
    return results


def run_traced_export(h, backend, verify_run):
    normal = dict(h.tracing); results = {}
    stop_pair(h)
    try:
        # A bound but non-listening owned TCP port produces real connect refusal.
        with socket.socket() as refused:
            refused.bind(('127.0.0.1', 0))
            config = dict(normal, traces_endpoint='http://127.0.0.1:' + str(refused.getsockname()[1]) + '/v1/traces', export_timeout='100ms')
            configure(h, config); h.start_worker(); h.gateway.start()
            runs = rounds(h, 'refused', 1)
            results['connection_refused'] = {'runs': runs, 'shutdown': stop_pair(h), 'endpoint': 'owned bound non-listening TCP socket'}
        fault = ExportFault(h, '503')
        try:
            configure(h, dict(normal, traces_endpoint=fault.endpoint, export_timeout='500ms'))
            h.start_worker(); h.gateway.start()
            runs = rounds(h, '503', 2)
            h.wait(lambda: any(r['response'] == 503 for r in fault.snapshot()), 'actual failed OTLP HTTP response')
            stopped = stop_pair(h); records = fault.snapshot()
            assert records and not any(r['canary_present'] for r in records)
            assert len({r['sha256'] for r in records}) == len(records), 'same OTLP batch retried'
            results['http_503'] = {'runs': runs, 'shutdown': stopped, 'requests': records}
        finally:
            fault.close()
        fault = ExportFault(h, 'slow')
        try:
            configure(h, dict(normal, traces_endpoint=fault.endpoint, export_timeout='10s', max_queue_size=1, max_export_batch_size=1))
            with blocked_stdout(h) as sinks:
                h.start_worker(); h.gateway.start()
                runs = rounds(h, 'slow-full-pipe', 3)
                records = fault.snapshot()
                assert len(records) >= 2 and all(r['response'] is None for r in records)
                assert not any(r['canary_present'] for r in records)
                # Each completed sampled Run has at least callback/create/process/
                # Runner/Session commit/send: 6 finished spans. With two process
                # queues of 1, batches of 1, and every exported request still held,
                # received batches plus <=6 buffered/in-flight spans cannot fit
                # this lower bound. This is loss evidence, not an exact drop count.
                minimum_finished = 6 * len(runs)
                assert minimum_finished > len(records) + 6
                pipes = {name: sink.snapshot() for name, sink in sinks.items()}
                assert len(pipes) == 2 and all(p['unread_bytes'] == p['prefilled_bytes'] > 0 for p in pipes.values())
                stopped = stop_pair(h)
                results['slow_queue_stdout'] = {'runs': runs, 'requests_before_stop': records, 'pipes': pipes, 'shutdown': stopped, 'sampled_finished_span_lower_bound': minimum_finished, 'max_unexported_capacity_upper_bound': 6, 'queue_size_per_process': 1, 'batch_size': 1, 'exact_drop_count': 'not claimed'}
        finally:
            fault.close()
    finally:
        # Restore only private processes/configuration. Keep sinks alive until
        # their processes stop, preventing a closed pipe from changing behavior.
        if h.worker and h.worker.poll() is None: h.stop_worker()
        if h.gateway.process and h.gateway.process.poll() is None: h.gateway.stop()
        configure(h, normal); h.start_worker(); h.gateway.start()
    recovered = rounds(h, 'restored', 1)[0]
    recovered['trace'] = verify_run(h, backend, recovered['run_id'])
    results['restored'] = recovered
    result = {'result': 'PASS', 'scenarios': results, 'runtime_counter_evidence': 'named Go TestExportOutageAndQueueRemainBounded; external health metrics remain deferred', 'business_row_writes': False}
    (h.artifacts / 'tracing-export-matrix.json').write_text(json.dumps(result, indent=2) + '\n')
    print('TRACING_EXPORT=PASS refused=true HTTP503=true slow_queue_loss=true full_stdout_pipe=true business_commits=true shutdown_bounded=true no_batch_retry=true recovery_trace=true', flush=True)
    return result
