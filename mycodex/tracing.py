"""Metadata-only job/run traces, explicitly exported via OTLP/HTTP JSON.

Wire format: https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
No prompts, source text, outputs, paths, environment or credentials are exported.
"""
from __future__ import annotations

import hashlib
import json
import time
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .jobs import JobQueue


def _attr(key, value):
    kind = 'boolValue' if isinstance(value, bool) else 'intValue' if isinstance(value, int) else 'doubleValue' if isinstance(value, float) else 'stringValue'
    return {'key': key, 'value': {kind: str(value) if kind == 'intValue' else value}}


def job_trace(store, job_id):
    queue = JobQueue(store)
    job = queue.get(job_id)
    if job is None:
        raise ValueError('unknown job')
    identity = hashlib.sha256((str(store.path) + ':' + str(job_id)).encode()).hexdigest()
    trace_id, parent = identity[:32], identity[32:48]
    end = job['completed_at'] or int(time.time())
    start = job['created_at']
    attrs = [_attr('mycodex.job.id', job_id), _attr('mycodex.job.kind', job['kind']),
             _attr('mycodex.job.status', job['status']), _attr('mycodex.job.attempts', job['attempts']),
             _attr('mycodex.job.revision', job['revision'])]
    observed = (job['result'].get('budget_usage') or {}).get('total_tokens')
    if observed is not None:
        attrs.append(_attr('mycodex.observed_tokens', observed))
    root = {'traceId': trace_id, 'spanId': parent, 'name': 'mycodex.job.' + job['kind'], 'kind': 1,
            'startTimeUnixNano': str(start * 10**9), 'endTimeUnixNano': str(max(start, end) * 10**9),
            'attributes': attrs, 'status': {'code': 2 if job['status'] in {'failed','blocked'} else 1 if job['status'] == 'succeeded' else 0},
            'events': [{'timeUnixNano': str(event['created_at'] * 10**9), 'name': event['event_type']}
                       for event in reversed(queue.events(job_id, limit=1000))]}
    event_count = store.conn.execute('SELECT COUNT(*) FROM job_events WHERE job_id=?',(job_id,)).fetchone()[0]
    root['droppedEventsCount'] = max(0, event_count - len(root['events']))
    spans = [root]
    # The service stores correlation in raw_json, including interrupted attempts.
    rows = store.conn.execute("""SELECT * FROM runs WHERE project_id=?
        AND json_extract(CASE WHEN json_valid(raw_json) THEN raw_json ELSE '{}' END,'$.trace_id')=?
        ORDER BY started_at,id""", (job['project_id'], f'job:{job_id}'))
    for row in rows:
        try:
            raw = json.loads(row['raw_json'] or '{}')
        except (TypeError, ValueError):
            continue
        if raw.get('trace_id') != f'job:{job_id}':
            continue
        run = dict(row)
        run_start = run['started_at']
        run_end = run.get('completed_at') or end
        usage = (raw.get('token_usage') or {}).get('turn') or {}
        attributes = [_attr('mycodex.run.id', run['id']), _attr('mycodex.run.status', run['status'])]
        for native, label in [('inputTokens','input_tokens'),('outputTokens','output_tokens'),('totalTokens','total_tokens')]:
            if isinstance(usage.get(native), int):
                attributes.append(_attr('mycodex.' + label, usage[native]))
        spans.append({'traceId': trace_id, 'spanId': hashlib.sha256(f'{identity}:run:{run["id"]}'.encode()).hexdigest()[:16],
                      'parentSpanId': parent, 'name': 'mycodex.codex.turn', 'kind': 3,
                      'startTimeUnixNano': str(run_start * 10**9), 'endTimeUnixNano': str(max(run_start,run_end) * 10**9),
                      'attributes': attributes, 'status': {'code': 1 if run['status'] == 'completed'
                                                          else 0 if run['status'] == 'running' else 2}})
    return {'resourceSpans': [{'resource': {'attributes': [_attr('service.name','mycodex')]},
                              'scopeSpans': [{'scope': {'name': 'mycodex.control-plane'}, 'spans': spans}]}]}


def export_trace(payload, endpoint, *, timeout=10):
    """One explicit bounded POST. Caller owns retry decisions; no hidden exporter."""
    parsed = urlparse(endpoint)
    if parsed.scheme not in {'http','https'} or not parsed.hostname or parsed.username or parsed.password or parsed.fragment or parsed.query:
        raise ValueError('provide an HTTP(S) collector endpoint without embedded credentials/query')
    target = endpoint.rstrip('/')
    if not target.endswith('/v1/traces'):
        target += '/v1/traces'
    request = Request(target, data=json.dumps(payload).encode(), method='POST',
                      headers={'Content-Type':'application/json'})
    with urlopen(request, timeout=timeout) as response:
        body = response.read(1024 * 1024 + 1)
        if len(body) > 1024 * 1024:
            raise ValueError('collector response exceeds size limit')
        result = json.loads(body) if body else {}
        partial = result.get('partialSuccess') or {}
        if int(partial.get('rejectedSpans') or 0) > 0:
            raise ValueError('collector rejected spans: ' + str(partial.get('errorMessage') or 'partial success'))
        return {'accepted': True, 'http_status': response.status, 'response': result,
                'scope': 'Collector transport acknowledgement, not backend persistence verification.'}
