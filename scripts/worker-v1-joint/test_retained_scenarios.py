"""Only fixture config contracts; durable behavior is tested with real processes."""
import unittest
from retained_scenarios import limits_for_retained


class RetainedConfigTest(unittest.TestCase):
    def test_count_is_an_explicit_retained_limit_not_active_or_token_limit(self):
        self.assertEqual(limits_for_retained(3), {'limits': {'max_retained_runs': 3}})
        self.assertEqual(limits_for_retained(4), {'limits': {'max_retained_runs': 4}})

    def test_absent_history_and_non_integer_limits_are_not_silent_defaults(self):
        for count in (None, 0, -1, True, 1.5, '3'):
            with self.subTest(count=count), self.assertRaises(ValueError):
                limits_for_retained(count)


if __name__ == '__main__':
    unittest.main()
