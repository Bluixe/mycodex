"""Owner-requested task controls; ambiguous running work is never silently retried."""
from __future__ import annotations

import json
import time


def _read_for_update(queue, job_id, expected_revision=None):
    job = queue.get(job_id)
    if job is None:
        raise ValueError(f"unknown job: {job_id}")
    if expected_revision is not None and (type(expected_revision) is not int or job['revision'] != expected_revision):
        raise ValueError("task changed; reload before applying this edit")
    return job


def pause_job(queue, job_id):
    try:
        queue.conn.execute('BEGIN IMMEDIATE')
        job = _read_for_update(queue, job_id)
        if job['status'] not in {'queued', 'leased', 'running', 'paused'}:
            raise ValueError('only queued or active tasks can be paused')
        if job['status'] == 'running' and job['kind'] not in {'prompt', 'daily_summary', 'review_synthesis'}:
            raise ValueError('this active operation has no interruptible checkpoint; inspect its result before resubmission')
        if job['cancel_requested_at']:
            raise ValueError('cancellation is already pending')
        if job['status'] != 'paused':
            ts = int(time.time())
            queue.conn.execute("""UPDATE jobs SET pause_requested_at=?, updated_at=?, revision=revision+1,
                status=CASE WHEN status IN ('queued','leased') THEN 'paused' ELSE status END,
                lease_owner=CASE WHEN status IN ('queued','leased') THEN NULL ELSE lease_owner END,
                lease_expires_at=CASE WHEN status IN ('queued','leased') THEN NULL ELSE lease_expires_at END
                WHERE id=?""", (ts, ts, job_id))
            queue._event(job_id, 'pause_requested', {'previous_status': job['status']})
        queue.conn.commit()
        return queue.get(job_id)
    except Exception:
        queue.conn.rollback()
        raise


def resume_job(queue, job_id, *, expected_revision=None):
    try:
        queue.conn.execute('BEGIN IMMEDIATE')
        job = _read_for_update(queue, job_id, expected_revision)
        if job['status'] != 'paused':
            raise ValueError('only a confirmed paused task can resume; reconcile blocked work separately')
        result = job['result']
        checkpoint = dict(job['checkpoint'])
        if result:
            if result.get('not_started') is not True and result.get('interruption_confirmed') is not True:
                raise ValueError('runtime interruption is not confirmed; reconcile effects before resubmission')
            checkpoint.update({key: result[key] for key in ('thread_id', 'run_id', 'status') if key in result})
            checkpoint['goal_revision'] = job['revision']
            checkpoint['consumed_tokens'] = (result.get('budget_usage') or {}).get('total_tokens')
            checkpoint['elapsed_seconds'] = result.get('elapsed_seconds', 0)
            if result.get('not_started'):
                checkpoint['consumed_tokens'] = 0
            budget = job['payload'].get('max_total_tokens')
            consumed = checkpoint['consumed_tokens']
            if budget is not None and (consumed is None or consumed >= budget):
                raise ValueError('token budget is exhausted or prior usage is unknown; inspect before submitting new work')
            if checkpoint['elapsed_seconds'] >= job['payload'].get('timeout', 1800):
                raise ValueError('task time budget is exhausted')
            if not checkpoint.get('thread_id') and result.get('not_started') is not True:
                raise ValueError('interrupted task has no thread checkpoint; inspect before creating a replacement')
        resource = 'thread:' + checkpoint['thread_id'] if checkpoint.get('thread_id') else job['resource_key']
        queue.conn.execute("""UPDATE jobs SET status='queued', pause_requested_at=NULL,
            lease_owner=NULL,lease_expires_at=NULL,completed_at=NULL, error='', result_json='{}',
            checkpoint_json=?,resource_key=?,updated_at=?,revision=revision+1 WHERE id=?""",
            (json.dumps(checkpoint), resource, int(time.time()), job_id))
        queue._event(job_id, 'resumed', {'checkpoint': checkpoint, 'previous_result_status': result.get('status')})
        queue.conn.commit()
        return queue.get(job_id)
    except Exception:
        queue.conn.rollback()
        raise


def revise_job(queue, job_id, prompt, *, expected_revision):
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 100000:
        raise ValueError('provide a nonempty goal of at most 100000 characters')
    try:
        queue.conn.execute('BEGIN IMMEDIATE')
        job = _read_for_update(queue, job_id, expected_revision)
        if job['status'] not in {'queued', 'paused'} or job['kind'] != 'prompt' or job['source'] == 'review-workflow':
            raise ValueError('pause an ordinary prompt task before editing; review inputs are immutable')
        payload = dict(job['payload'])
        previous = payload['prompt']
        payload['prompt'] = prompt.strip()
        queue.conn.execute('UPDATE jobs SET payload_json=?,revision=revision+1,updated_at=? WHERE id=?',
                           (json.dumps(payload), int(time.time()), job_id))
        queue._event(job_id, 'goal_revised', {'previous_goal': previous, 'goal': payload['prompt'],
                                             'revision': job['revision'] + 1})
        queue.conn.commit()
        return queue.get(job_id)
    except Exception:
        queue.conn.rollback()
        raise
