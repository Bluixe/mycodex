"""Recorded-state evaluation. Unknown evidence stays unknown, never a success."""
from __future__ import annotations

import json
import math

SUCCESS = {'completed', 'succeeded', 'done'}


def _object(value) -> dict:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def _number(value):
    return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def evaluate_run(record: dict) -> dict:
    if not isinstance(record, dict):
        raise ValueError('a run record must be an object')
    checks = []
    raw = _object(record.get('raw_json'))
    runtime = _object(raw.get('result'))
    server = _object(runtime.get('turn')).get('status') or _object(raw.get('turn')).get('status')
    local_success = record.get('status') in SUCCESS
    consistency = (None if record.get('status') is None else True if not local_success or record.get('kind') == 'sync_sessions'
                   else None if server is None else server == 'completed')
    checks.append({"name": "terminal_status_consistency",
                   "passed": consistency,
                   "observed": {"local": record.get("status"), "server": server}})
    usage = _object(raw.get('token_usage') or runtime.get('token_usage'))
    measured = _object(record.get('budget_usage') or usage.get('turn') or usage)
    total = _number(measured.get('total_tokens', measured.get('totalTokens')))
    budget = _number(record.get('max_total_tokens', measured.get('max_total_tokens')))
    if budget is not None:
        checks.append({'name': 'token_budget', 'passed': None if total is None else total <= budget,
                       'observed': total, 'budget': budget})
    if record.get("require_output"):
        checks.append({"name": "nonempty_output", "passed": bool(str(record.get("output_text") or "").strip())})
    claims = record.get('claims') or []
    for claim in claims if isinstance(claims, list) else []:
        if not isinstance(claim, dict):
            checks.append({'name': 'claim_has_verified_evidence', 'passed': False, 'observed': 'malformed_claim'})
            continue
        if claim.get("status") in {"supported", "contradicted"}:
            status = claim.get('verification_status') or _object(claim.get('metadata_json') or claim.get('metadata')).get('verification_status')
            checks.append({'name': 'claim_has_verified_evidence', 'passed': status == 'passage_verified',
                           'observed': status, 'scope': 'verification receipt only; not semantic entailment'})
        if 'citation_matches_source' in claim:
            checks.append({'name': 'citation_matches_source', 'passed': claim['citation_matches_source'] is True})
    known = [check['passed'] for check in checks if check['passed'] is not None]
    state = 'failed' if False in known else 'unknown' if any(check['passed'] is None for check in checks) else 'passed'
    return {'run_id': record.get('id'), 'passed': False if state == 'failed' else True if state == 'passed' else None,
            'state': state, 'declared_completed': local_success,
            'erroneous_completion': local_success and state == 'failed', 'checks': checks}


def summarize_metrics(records: list[dict], results: list[dict] | None = None) -> dict:
    results = results or [evaluate_run(record) for record in records]
    completed = sum(row.get('declared_completed', False) for row in results)
    errors = sum(row.get('erroneous_completion', False) for row in results)
    output = {'declared_completions': completed, 'completion_rate': completed / len(records) if records else None,
              'erroneous_completions': errors, 'erroneous_completion_rate': errors / completed if completed else None}
    for field in ('evidence_coverage', 'elapsed_seconds', 'total_tokens', 'user_interventions'):
        observed = [_number(record.get(field)) for record in records]
        values = [item for item in observed if item is not None]
        output[field] = {'observed': len(values), 'unknown': len(records) - len(values),
                         'mean': sum(values) / len(values) if values else None,
                         'total': sum(values) if values else None}
    return output


def evaluate_records(records: list[dict]) -> dict:
    if not isinstance(records, list):
        raise ValueError('records must be a list')
    results = [evaluate_run(record) for record in records]
    passed = sum(result['state'] == 'passed' for result in results)
    failed = sum(result['state'] == 'failed' for result in results)
    unknown = len(results) - passed - failed
    return {'version': 2, 'evaluated': len(results), 'passed': passed, 'failed': failed, 'unknown': unknown,
            'pass_rate': passed / (passed + failed) if passed + failed else None,
            'results': results, 'metrics': summarize_metrics(records, results),
            'scope': 'Recorded-state checks only; missing observations are unknown. No scientific-quality inference.'}
