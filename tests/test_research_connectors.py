from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sqlite3
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from mycodex.research_connectors import (
    ProviderHTTPError, ResearchSearchResult, _extract_pdf_with_pdftotext,
    _zotero_snapshot, ingest_text_source, search_openalex, search_public_sources,
)


class ResearchConnectorTests(unittest.TestCase):
    def test_failed_provider_is_not_empty_search(self):
        with patch('mycodex.research_connectors.search_arxiv', side_effect=TimeoutError('offline')):
            batch = search_public_sources('agents', providers=['arxiv'], retries=0)
        self.assertIsInstance(batch, list)
        self.assertEqual(batch, [])
        self.assertEqual(batch.coverage, 'unavailable')
        self.assertEqual(batch.receipts[0]['status'], 'error')
        with patch('mycodex.research_connectors.search_arxiv', return_value=[]):
            empty = search_public_sources('agents', providers=['arxiv'])
        self.assertEqual(empty.coverage, 'complete_requested')
        self.assertEqual(empty.receipts[0]['status'], 'empty')
        self.assertFalse(empty.receipts[0]['exhaustive'])

    def test_partial_provider_coverage_survives(self):
        source = ResearchSearchResult('Agent paper', 'https://example.test/paper', provider='arxiv')
        with patch('mycodex.research_connectors.search_arxiv', return_value=[source]), \
             patch('mycodex.research_connectors.search_openalex', side_effect=RuntimeError('rate limited')):
            batch = search_public_sources('agents', providers=['arxiv', 'openalex'], retries=0)
        self.assertEqual(batch, [source])
        self.assertEqual(batch.coverage, 'partial')
        self.assertEqual([item['status'] for item in batch.receipts], ['success', 'error'])

    def test_search_cache_replays_bounded_provider_results(self):
        source = ResearchSearchResult('Agent paper', 'https://example.test/paper', authors=('Ada',), provider='arxiv')
        with tempfile.TemporaryDirectory() as tmp, \
             patch('mycodex.research_connectors.search_arxiv', return_value=[source]) as request:
            fresh = search_public_sources('agents', providers=['arxiv'], cache_dir=tmp)
            cached = search_public_sources('agents', providers=['arxiv'], cache_dir=tmp)
            self.assertEqual(request.call_count, 1)
            self.assertFalse(fresh.receipts[0]['from_cache'])
            self.assertTrue(cached.receipts[0]['from_cache'])
            self.assertEqual(cached[0], source)
            self.assertEqual(cached.receipts[0]['attempts'], 0)
            self.assertEqual(fresh.receipts[0]['fetched_at'], cached.receipts[0]['fetched_at'])

    def test_transient_provider_retry_is_bounded(self):
        with patch('mycodex.research_connectors.search_arxiv', side_effect=[ProviderHTTPError(429, 'https://example.test'), []]) as request, \
             patch('mycodex.research_connectors.time.sleep'):
            batch = search_public_sources('agents', providers=['arxiv'], retries=1)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(batch.receipts[0]['attempts'], 2)
        self.assertEqual(batch.receipts[0]['status'], 'empty')

    def test_corrupt_cache_is_refetched(self):
        source = ResearchSearchResult('Agent paper', 'https://example.test/paper', provider='arxiv')
        with tempfile.TemporaryDirectory() as tmp, \
             patch('mycodex.research_connectors.search_arxiv', return_value=[source]) as request:
            search_public_sources('agents', providers=['arxiv'], cache_dir=tmp)
            cache = next(Path(tmp).glob('*.json'))
            payload = json.loads(cache.read_text(encoding='utf-8'))
            payload['results'][0]['title'] = 'Tampered result'
            cache.write_text(json.dumps(payload), encoding='utf-8')
            batch = search_public_sources('agents', providers=['arxiv'], cache_dir=tmp)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(batch[0].title, 'Agent paper')
        self.assertFalse(batch.receipts[0]['from_cache'])

    def test_permanent_auth_failure_not_retried_and_secret_redacted(self):
        with patch.dict(os.environ, {'OPENALEX_API_KEY': 'secret-one'}), \
             patch('mycodex.research_connectors.search_openalex', side_effect=ProviderHTTPError(401, 'https://example.test?api_key=secret-one')) as request:
            batch = search_public_sources('agents', providers=['openalex'])
        self.assertEqual(request.call_count, 1)
        self.assertNotIn('secret-one', json.dumps(batch.receipts))
        self.assertIn('[redacted]', batch.receipts[0]['error'])

    def test_openalex_uses_environment_key(self):
        with patch.dict(os.environ, {'OPENALEX_API_KEY': 'secret-two'}), \
             patch('mycodex.research_connectors._read_json', return_value={'results': []}) as request:
            self.assertEqual(search_openalex('agents'), [])
        self.assertIn('api_key=secret-two', request.call_args.args[0])

    def test_missing_zotero_library_is_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {'ZOTERO_DATA_DIR': tmp}):
            batch = search_public_sources('agents', providers=['zotero'])
        self.assertEqual(batch.receipts[0]['status'], 'unavailable')

    def test_zotero_snapshot_includes_committed_wal_data_without_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'zotero.sqlite'
            conn = sqlite3.connect(path)
            try:
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('CREATE TABLE items (title TEXT)')
                conn.commit()
                conn.execute("INSERT INTO items VALUES ('committed in WAL')")
                conn.commit()
                before = path.read_bytes()
                with _zotero_snapshot(path) as snapshot:
                    reader = sqlite3.connect(snapshot)
                    try:
                        self.assertEqual(reader.execute('SELECT title FROM items').fetchone()[0], 'committed in WAL')
                    finally:
                        reader.close()
                self.assertEqual(path.read_bytes(), before)
            finally:
                conn.close()

    def test_unknown_provider_is_visible(self):
        batch = search_public_sources('agents', providers=['unknown'])
        self.assertEqual(batch.coverage, 'unavailable')
        self.assertEqual(batch.receipts[0]['error_type'], 'ValueError')

    def test_pdf_file_is_readable_by_subprocess_and_cleaned(self):
        paths = []
        payload = b'%PDF-fake'
        def run(args, **kwargs):
            path = Path(args[2])
            paths.append(path)
            self.assertEqual(path.read_bytes(), payload)
            # Open with a second handle, matching pdftotext's Windows behavior.
            with path.open('rb') as reader:
                self.assertEqual(reader.read(), payload)
            return subprocess.CompletedProcess(args, 0, stdout='page one\fpage two', stderr='')
        with patch('mycodex.research_connectors.subprocess.run', side_effect=run):
            text = _extract_pdf_with_pdftotext(payload, max_chars=100)
        self.assertIn('\f', text)
        self.assertFalse(paths[0].exists())

    def test_ingestion_receipt_hashes_original_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'source.txt'
            path.write_bytes(b'agent adaptation evidence')
            receipt = ingest_text_source(str(path), max_chars=5)
        self.assertEqual(receipt['content_hash'], hashlib.sha256(b'agent adaptation evidence').hexdigest())
        self.assertEqual(receipt['text'], 'agent')
        self.assertTrue(receipt['truncated'])
        self.assertEqual(receipt['extraction_status'], 'available')


if __name__ == '__main__':
    unittest.main()
