import unittest
from pathlib import Path
from intake_scenarios import ACK_PERMISSION, without_run_ack, broker_request


class IntakeFixtureTests(unittest.TestCase):
    def test_remove_only_worker_run_ack_in_actual_generated_config(self):
        root = Path(__file__).resolve().parents[2]
        original = (root/'deploy/nats/server.conf').read_text()
        changed = without_run_ack(original)
        self.assertNotIn('"'+ACK_PERMISSION+'"', changed)
        self.assertEqual(changed.replace('"$JS.API.CONSUMER.MSG.NEXT.RUN_REQUESTS_V1.agent-worker-runs-v1"',
                                        '"$JS.API.CONSUMER.MSG.NEXT.RUN_REQUESTS_V1.agent-worker-runs-v1", "'+ACK_PERMISSION+'"'), original)
        self.assertIn('"$JS.ACK.RUNTIME_MANIFESTS_V1.worker-manifests-v1.>"', changed)
        self.assertIn('"$JS.ACK.REPLY_INTENTS_V1.channel-gateway-replies-v1.>"', changed)

    def test_missing_or_duplicate_ack_fails_before_edit(self):
        with self.assertRaises(ValueError): without_run_ack('publish: []')
        with self.assertRaises(ValueError): without_run_ack(', "'+ACK_PERMISSION+'", "'+ACK_PERMISSION+'"')

    def test_observation_never_accepts_broker_mutations(self):
        for operation in ('delete', 'purge', 'update', 'publish'):
            with self.assertRaises(ValueError): broker_request(None, operation)

if __name__ == '__main__': unittest.main()
