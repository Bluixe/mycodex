import unittest
from mycodex.evaluation import evaluate_records, evaluate_run


class EvaluationTests(unittest.TestCase):
    def test_false_success_and_unsupported_claim_are_caught(self):
        report = evaluate_records([
            {"id": 1, "status": "completed", "raw_json": {"result": {"turn": {"status": "failed"}}}},
            {"id": 2, "status": "completed", "claims": [{"status": "supported", "verification_status": "legacy_unverified"}]},
            {"id": 3, "status": "completed", "raw_json": {"result": {"turn": {"status": "completed"}}}},
        ])
        self.assertEqual(report["evaluated"], 3)
        self.assertEqual(report["failed"], 2)
        self.assertIsNone(evaluate_records([])["pass_rate"])

    def test_missing_observations_are_unknown_not_zero_or_success(self):
        report = evaluate_records([{'status': 'completed', 'max_total_tokens': 8000}])
        self.assertEqual(report['unknown'], 1)
        self.assertEqual(report['failed'], 0)
        self.assertIsNone(report['pass_rate'])
        self.assertIsNone(report['metrics']['total_tokens']['total'])
        self.assertEqual(report['metrics']['total_tokens']['unknown'], 1)
        self.assertIsNone(evaluate_run({})['passed'])

    def test_succeeded_alias_cannot_hide_server_failure(self):
        report = evaluate_records([{'status': 'succeeded', 'raw_json': {'result': {'turn': {'status': 'failed'}}}}])
        self.assertEqual(report['failed'], 1)
        self.assertEqual(report['metrics']['erroneous_completions'], 1)

    def test_malformed_nested_trace_is_unknown_and_does_not_crash(self):
        result = evaluate_run({'status': 'completed', 'raw_json': '{invalid', 'claims': []})
        self.assertEqual(result['state'], 'unknown')
        result = evaluate_run({'status': 'completed', 'raw_json': {'result': [], 'token_usage': []}})
        self.assertEqual(result['state'], 'unknown')

    def test_available_budget_and_metrics_are_measured(self):
        report = evaluate_records([{'status': 'completed', 'raw_json': {'turn': {'status': 'completed'}},
            'budget_usage': {'total_tokens': 9000, 'max_total_tokens': 8000},
            'total_tokens': 9000, 'elapsed_seconds': 3.2, 'evidence_coverage': 0.5, 'user_interventions': 1}])
        self.assertEqual(report['failed'], 1)
        self.assertEqual(report['metrics']['total_tokens']['total'], 9000)
        self.assertEqual(report['metrics']['user_interventions']['total'], 1)

    def test_wrong_citation_is_separate_from_receipt_marker(self):
        result = evaluate_run({'status': 'failed', 'claims': [{'status': 'supported',
            'verification_status': 'passage_verified', 'citation_matches_source': False}]})
        self.assertFalse(result['passed'])
        self.assertEqual(result['checks'][-1]['name'], 'citation_matches_source')
