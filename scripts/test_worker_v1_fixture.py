"""Docker publication lifecycle, not blind retries on an opaque CLI failure."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('worker_fixture_gate', Path(__file__).with_name('test-worker-v1.py'))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def state(status='running', port=None, requested=True):
    return {'state': {'Status': status, 'Running': status == 'running', 'ExitCode': 2 if status == 'exited' else 0},
            'requested': {'4222/tcp': [{'HostIp': '127.0.0.1', 'HostPort': ''}]} if requested else {},
            'ports': {'4222/tcp': [{'HostIp': '127.0.0.1', 'HostPort': str(port)}]} if port else {}}


class PublishedPortTest(unittest.TestCase):
    def fake(self, observations):
        calls=[]
        def command(argv):
            calls.append(argv)
            if argv[1] == 'port':
                raise RuntimeError("No public port '4222/tcp' published for fixture")
            if argv[1] == 'logs':
                return 'nats-server: configuration parse failure'
            self.assertEqual(argv[1], 'inspect')
            return json.dumps(observations.pop(0))
        return command,calls

    def test_running_requested_mapping_may_be_published_after_first_observation(self):
        command,calls=self.fake([state(),state(port=32888)])
        with patch.object(gate.time,'sleep') as sleep:
            self.assertEqual(gate.published_port(command,'fixture',4222),'32888')
        self.assertEqual(sum(call[1]=='inspect' for call in calls),2)
        self.assertEqual(sleep.call_count,1)

    def test_exited_server_is_not_retried_and_retains_startup_diagnostic(self):
        command,calls=self.fake([state('exited')])
        with patch.object(gate.time,'sleep') as sleep,self.assertRaisesRegex(RuntimeError,'exited.*exit=2.*configuration parse failure'):
            gate.published_port(command,'fixture',4222)
        sleep.assert_not_called()
        self.assertEqual([call[1] for call in calls],['inspect','logs'])

    def test_missing_requested_binding_is_a_configuration_failure_not_a_race(self):
        command,calls=self.fake([state(requested=False)])
        with patch.object(gate.time,'sleep') as sleep,self.assertRaisesRegex(RuntimeError,'not requested'):
            gate.published_port(command,'fixture',4222)
        sleep.assert_not_called()

    def test_running_without_mapping_has_bounded_timeout(self):
        command,calls=self.fake([state()])
        with patch.object(gate.time,'sleep') as sleep,self.assertRaisesRegex(RuntimeError,'publication timeout.*running'):
            gate.published_port(command,'fixture',4222,timeout=0)
        sleep.assert_not_called()


if __name__=='__main__':unittest.main()
