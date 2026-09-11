import json
from pathlib import Path
import tempfile
import unittest
from capacity_scenarios import capacity_observation

class CapacityTests(unittest.TestCase):
    def test_only_actual_intake_capacity_for_exact_run_is_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'worker.log'
            self.assertIsNone(capacity_observation(path, 'run'))
            path.write_text('not json\n' + json.dumps({'operation': 'claim', 'result': 'capacity', 'run_id': 'run'}) + '\n' + json.dumps({'operation': 'intake', 'result': 'capacity', 'run_id': 'another'}) + '\n')
            self.assertIsNone(capacity_observation(path, 'run'))
            with path.open('a') as out:
                out.write(json.dumps({'operation': 'intake', 'result': 'capacity', 'run_id': 'run', 'extra': 'not evidence'}) + '\n')
            self.assertEqual(capacity_observation(path, 'run'), {'operation': 'intake', 'result': 'capacity', 'run_id': 'run'})

if __name__ == '__main__': unittest.main()
