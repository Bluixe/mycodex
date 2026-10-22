"""Explicit refresh of public-domain official materials; never run by benchmarks."""
from __future__ import annotations
import argparse
import hashlib
import json
import re
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path


class Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
    def handle_data(self, data):
        self.parts.append(data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive', required=True)
    args = parser.parse_args()
    archive = Path(args.archive)
    archive.mkdir(parents=True, exist_ok=True)
    definitions = [
        {'id': 'threading', 'title': 'Using SQLite In Multi-Threaded Applications',
         'url': 'https://www.sqlite.org/threadsafe.html', 'section': '2. Compile-time selection of threading mode',
         'sentences': ['The default mode is serialized.',
            'If single-thread mode is selected at compile-time, then critical mutexing logic is omitted from the build and it is impossible to enable either multi-thread or serialized modes at start-time or run-time.'],
         'evidence_marker': 'critical mutexing logic is omitted'},
        {'id': 'wal', 'title': 'Write-Ahead Logging', 'url': 'https://www.sqlite.org/wal.html',
         'section': '2.2. Concurrency', 'sentences': [
            'Writers merely append new content to the end of the WAL file.',
            'Because writers do nothing that would interfere with the actions of readers, writers and readers can run at the same time.',
            'However, since there is only one WAL file, there can only be one writer at a time.'],
         'evidence_marker': 'there can only be one writer at a time'},
        {'id': 'backup', 'title': 'SQLite Backup API', 'url': 'https://www.sqlite.org/backup.html',
         'section': '1. Using the SQLite Online Backup API', 'sentences': [
            'The copy operation may be done incrementally, in which case the source database does not need to be locked for the duration of the copy, only for the brief periods of time when it is actually being read from.',
            'The effect of completing the backup call sequence is to make the destination a bit-wise identical copy of the source database as it was when the copying commenced.'],
         'evidence_marker': 'bit-wise identical copy of the source database'}]
    accessed = datetime.now(timezone.utc).isoformat()
    for definition in definitions:
        payload = urllib.request.urlopen(urllib.request.Request(definition['url'],
            headers={'User-Agent': 'mycodex-benchmark-source-verification/1'}), timeout=30).read()
        (archive / (definition['id'] + '.html')).write_bytes(payload)
        parsed = Text()
        parsed.feed(payload.decode('utf-8'))
        normalized = re.sub(r'\s+', ' ', ' '.join(parsed.parts))
        for sentence in definition['sentences']:
            if sentence not in normalized:
                raise ValueError('official source no longer contains the pinned sentence: ' + sentence)
        definition['text'] = '\n'.join(definition.pop('sentences'))
        definition['accessed_at'] = accessed
        definition['raw_sha256'] = hashlib.sha256(payload).hexdigest()
        definition['excerpt_sha256'] = hashlib.sha256(definition['text'].encode()).hexdigest()
    output = {'schema_version': 1, 'license': 'SQLite public-domain documentation',
        'license_url': 'https://www.sqlite.org/copyright.html', 'accessed_at': accessed,
        'research_question': 'SQLite SQLITE_THREADSAFE serialized mutexing compile-time modes; WAL writer concurrency; online backup snapshot copying',
        'sources': definitions, 'questions': [
            {'id': 'Q1', 'claim': 'WAL permits more than one writer at a time because readers and writers run concurrently.',
             'gold_status': 'contradicted', 'gold_source_id': 'wal', 'gold_answer': 'WAL can overlap readers with a writer, but still allows only one writer at a time.'},
            {'id': 'Q2', 'claim': 'A build whose single-thread mode was selected at compile time can later enable serialized mode at run time.',
             'gold_status': 'contradicted', 'gold_source_id': 'threading', 'gold_answer': 'Compile-time single-thread mode omits critical mutexing code and cannot be upgraded at startup or run time.'},
            {'id': 'Q3', 'claim': 'Completing the online backup API sequence produces a destination snapshot, while incremental copying only briefly locks the source when reading it.',
             'gold_status': 'supported', 'gold_source_id': 'backup', 'gold_answer': 'The documentation specifies an identical snapshot and brief source locks during incremental reads.'},
            {'id': 'Q4', 'claim': 'The supplied sources prove that WAL is faster for every possible workload.',
             'gold_status': 'unknown', 'gold_source_id': None, 'gold_answer': 'No supplied excerpt establishes a universal workload performance guarantee.'}]}
    target = Path(__file__).resolve().parent / 'materials' / 'sqlite.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    print(target)


if __name__ == '__main__':
    main()
