"""Bounded, explicitly requested conversation analysis on the durable queue."""
from __future__ import annotations

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .db import Store


def validate_insight_scope(store: Store, project_names=None) -> list[str]:
    if project_names is None:
        project_names = [row['name'] for row in store.list_projects() if row['name'] != 'mycodex']
    if (not isinstance(project_names, list) or not project_names or len(project_names) > 20
            or any(not isinstance(name, str) or not name.strip() for name in project_names)):
        raise ValueError('select between 1 and 20 registered project names')
    names = sorted(set(project_names), key=str.casefold)
    for name in names:
        if store.get_project(name) is None:
            raise ValueError(f'unknown project: {name}')
    return names


def submit_insight_refresh(store: Store, project_names=None, *, use_model=True, force=False,
                           timeout=360, max_total_tokens=120000, source='web') -> dict:
    from .jobs import JobQueue
    names = validate_insight_scope(store, project_names)
    if type(use_model) is not bool or type(force) is not bool:
        raise ValueError('use_model and force must be booleans')
    if type(timeout) is not int or not 1 <= timeout <= 86400:
        raise ValueError('timeout must be between 1 and 86400 seconds')
    if type(max_total_tokens) is not int or max_total_tokens <= 0:
        raise ValueError('max_total_tokens must be a positive integer')
    # A repeated button click reuses the same pending request, never another
    # background model call. Completed refreshes can be requested again and
    # the analysis layer then compares source fingerprints.
    for row in store.conn.execute("SELECT id FROM jobs WHERE kind='insight_refresh' AND status IN ('queued','leased','running') ORDER BY id"):
        prior = JobQueue(store).get(row['id'])
        if (prior['payload'].get('project_names') == names
                and prior['payload'].get('use_model', True) == use_model
                and prior['payload'].get('force', False) == force
                and prior['payload'].get('timeout') == timeout
                and prior['payload'].get('max_total_tokens') == max_total_tokens):
            return prior
    return JobQueue(store).submit(names[0], 'insight_refresh',
        {'project_names': names, 'use_model': use_model, 'force': force,
         'timeout': timeout, 'max_total_tokens': max_total_tokens}, source=source)


def execute_insight_refresh(store: Store, job: dict, cancelled, usage_limit) -> dict:
    from .insights import Insights
    if cancelled():
        return {'status': 'cancelled', 'error': 'cancelled before conversation analysis'}
    payload = job['payload']
    names = validate_insight_scope(store, payload['project_names'])
    index_updates = []
    if payload.get('use_model', True):
        from .service import sync_sessions
        for name in names:
            if cancelled():
                return {'status': 'cancelled', 'error': 'cancelled while updating the conversation index'}
            try:
                threads = sync_sessions(store, name, limit=1000)
                index_updates.append({'project_name': name, 'status': 'completed', 'sessions': len(threads)})
            except Exception as exc:
                # A failed discovery must not be presented as an up-to-date
                # reading of every project. Retain prior reports and bill no
                # new inference until their input scope can be established.
                return {'status': 'failed', 'error': f'conversation index update failed for {name}: {type(exc).__name__}',
                        'index_updates': index_updates, 'usage': {'model_calls': 0, 'observed_tokens': 0}}
    result = Insights(store).refresh(names, use_model=payload.get('use_model', True),
        force=payload.get('force', False), timeout=payload.get('timeout', 360),
        max_total_tokens=payload.get('max_total_tokens', 120000),
        cancellation_callback=cancelled, usage_callback=usage_limit)
    errors = result.get('errors') or []
    state = 'cancelled' if cancelled() else 'failed' if errors else 'completed'
    return {**result, 'status': state, 'error': '; '.join(str(item) for item in errors)[:2000],
            'index_updates': index_updates}
