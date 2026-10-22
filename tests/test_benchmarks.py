from __future__ import annotations

import hashlib
import json
import tempfile
import subprocess
import unittest
from pathlib import Path

from mycodex.benchmarks import (_queue_budget, _terminal, compare_results, gate_report, load_cases,
    run_benchmarks, run_live_material_pair, score_material_answers)

ROOT = Path(__file__).resolve().parents[1]


class BenchmarkTests(unittest.TestCase):
    def test_case_provenance_does_not_claim_constructed_inputs_are_raw_history(self):
        cases = load_cases(ROOT)
        self.assertGreaterEqual(len(cases), 10)
        self.assertTrue(all(row['provenance']['type'] == 'constructed_from_incident' for row in cases))
        self.assertTrue(any(row['probe'] == 'terminal_success' for row in cases))

    def test_historical_trace_label_requires_matching_raw_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'benchmarks').mkdir()
            manifest = {'cases': [{'id': 'history', 'probe': 'terminal_failed', 'provenance': {
                'type': 'observed_historical_failure', 'trace_sha256': 'a' * 64, 'trace_path': 'trace.json'}}]}
            (root / 'benchmarks/cases.json').write_text(json.dumps(manifest), encoding='utf-8')
            with self.assertRaises(ValueError):
                load_cases(root)
            trace = b'{"retained_original_record":true}'
            (root / 'trace.json').write_bytes(trace)
            manifest['cases'][0]['provenance']['trace_sha256'] = hashlib.sha256(trace).hexdigest()
            (root / 'benchmarks/cases.json').write_text(json.dumps(manifest), encoding='utf-8')
            self.assertEqual(len(load_cases(root)), 1)

    def test_materials_have_pinned_original_urls_hashes_and_gold_answers(self):
        material = json.loads((ROOT / 'benchmarks/materials/sqlite.json').read_text(encoding='utf-8'))
        self.assertEqual(len(material['questions']), 4)
        for source in material['sources']:
            self.assertTrue(source['url'].startswith('https://www.sqlite.org/'))
            self.assertEqual(len(source['raw_sha256']), 64)
            self.assertEqual(hashlib.sha256(source['text'].encode()).hexdigest(), source['excerpt_sha256'])
            self.assertIn(source['evidence_marker'], source['text'])

    def test_missing_capability_is_excluded_from_shared_denominator(self):
        def version(ref, states):
            return {'ref': ref, 'commit': ref, 'source_tree_hash': 'hash', 'cases': [
                {'case_id': key, 'state': state, 'elapsed_seconds': 1.0, 'observation': {}}
                for key, state in states.items()]}
        result = compare_results([version('one', {'shared': 'passed', 'new': 'unsupported'}),
                                  version('two', {'shared': 'failed', 'new': 'passed'})])
        self.assertEqual(result['shared_evaluable_cases'], ['shared'])
        self.assertEqual(result['versions'][0]['shared_case_pass_rate'], 1)
        self.assertEqual(result['versions'][1]['shared_case_pass_rate'], 0)
        self.assertIsNone(result['causal_claim'])

    def test_unavailable_revision_is_explicit_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = run_benchmarks(ROOT, Path(tmp) / 'run', refs=['this-reference-intentionally-does-not-exist'])
        summary = result['comparison']['versions'][0]
        self.assertEqual(summary['availability'], 'missing_ref')
        self.assertIsNone(summary['known_check_pass_rate'])
        self.assertEqual(summary['passed'], 0)

    def test_snapshot_subprocess_cannot_fall_back_to_editable_current_modules(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = run_benchmarks(ROOT, Path(tmp) / 'run', refs=['working-tree'])
            version = result['versions'][0]
            self.assertTrue(version['cases'])
            self.assertTrue(all(Path(path).resolve().is_relative_to((Path(tmp) / 'run/version-0/snapshot').resolve())
                for path in version['imported_modules'].values() if path))
            self.assertEqual(version['platform'], result['versions'][0]['platform'])
            self.assertEqual(len(version['source_tree_hash']), 64)

    def test_old_snapshot_missing_module_is_unsupported_not_current_fallback(self):
        if subprocess.run(['git', 'cat-file', '-e', 'ed16415^{commit}'], cwd=ROOT, capture_output=True).returncode:
            self.skipTest('public baseline commit unavailable in this clone; missing-ref behavior tested separately')
        with tempfile.TemporaryDirectory() as tmp:
            result = run_benchmarks(ROOT, Path(tmp) / 'run', refs=['ed16415'])
        states = {row['case_id']: row['state'] for row in result['versions'][0]['cases']}
        self.assertEqual(states['conflicting-evidence'], 'unsupported')
        self.assertEqual(states['bounded-review-dag'], 'unsupported')

    def test_gold_checker_rejects_right_answer_with_hidden_or_wrong_citation(self):
        material = json.loads((ROOT / 'benchmarks/materials/sqlite.json').read_text(encoding='utf-8'))
        wal = next(row for row in material['sources'] if row['id'] == 'wal')
        answer = {'answers': [{'id': 'Q1', 'status': 'contradicted', 'source_url': wal['url'],
                               'quote': wal['text'].split('\n')[-1]}]}
        hidden = score_material_answers(answer, material, {'sources': []})
        self.assertFalse(hidden['questions'][0]['visible_exact_quote'])
        visible = score_material_answers(answer, material, {'evidence_passages': [wal]})
        self.assertTrue(visible['questions'][0]['visible_exact_quote'])
        answer['answers'][0]['source_url'] = 'https://unrelated.example/'
        self.assertFalse(score_material_answers(answer, material, {'evidence_passages': [wal]})['questions'][0]['visible_exact_quote'])

    def test_multiline_quote_is_compared_with_decoded_context(self):
        material = json.loads((ROOT / 'benchmarks/materials/sqlite.json').read_text(encoding='utf-8'))
        source = next(row for row in material['sources'] if row['id'] == 'backup')
        answer = {'answers': [{'id': 'Q3', 'status': 'supported', 'source_url': source['url'], 'quote': source['text']}]}
        scored = score_material_answers(answer, material, {'evidence_passages': [{'text': source['text']}]})
        self.assertTrue(scored['questions'][2]['visible_exact_quote'])
        self.assertFalse(scored['answer_schema_valid'])  # Other question answers are missing.

    def test_live_material_bounds_reject_before_any_runtime_request(self):
        with self.assertRaises(ValueError):
            run_live_material_pair('unused-path', token_budget=30001)
        with self.assertRaises(ValueError):
            run_live_material_pair('unused-path', timeout=181)

    def test_queue_budget_blocks_dependency_even_when_native_server_completed(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = _queue_budget(Path(tmp))
        self.assertTrue(result['passed'])
        self.assertEqual(result['native_run_status'], 'completed')
        self.assertEqual(result['queue_status'], 'blocked')
        self.assertFalse(result['dependency_continued'])
        self.assertEqual(result['observed_tokens'], 25)

    def test_native_completion_race_preserves_server_truth_separately(self):
        result = _terminal(budget=True)
        self.assertTrue(result['passed'])
        self.assertEqual(result['observed_status'], 'completed')
        self.assertTrue(result['interrupted'])

    def test_gate_requires_nonempty_all_passing_target_ignoring_old_failures(self):
        good = {'ref': 'working-tree', 'cases': [{'state': 'passed'}]}
        old = {'ref': 'baseline', 'cases': [{'state': 'failed'}]}
        self.assertTrue(gate_report({'versions': [old, good]})['passed'])
        for state in ('failed', 'error', 'unsupported', 'unknown'):
            self.assertFalse(gate_report({'versions': [{'ref': 'working-tree', 'cases': [{'state': state}]}]})['passed'])
        for target in ({'ref': 'working-tree', 'cases': []},
                       {'ref': 'working-tree', 'availability': 'missing_ref', 'cases': [{'state': 'passed'}]}):
            self.assertFalse(gate_report({'versions': [target]})['passed'])
        self.assertFalse(gate_report({'versions': []})['passed'])

    def test_module_gate_returns_nonzero_for_missing_target_without_inference(self):
        import sys
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run([sys.executable, '-m', 'mycodex.benchmarks', '--output', str(Path(tmp)/'run'),
                '--ref', 'intentionally-missing-benchmark-ref', '--gate-ref', 'working-tree'], cwd=ROOT, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'"passed": false', result.stdout)

    def test_main_cli_gate_returns_nonzero_for_missing_target_without_inference(self):
        import sys
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run([sys.executable, '-m', 'mycodex', 'benchmarks',
                '--output', str(Path(tmp)/'run'), '--ref', 'intentionally-missing-benchmark-ref',
                '--gate-ref', 'working-tree'], cwd=ROOT, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'benchmark gate failed', result.stderr)
        self.assertIn(b'"passed": false', result.stdout)


if __name__ == '__main__':
    unittest.main()
