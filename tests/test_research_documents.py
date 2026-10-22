from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from mycodex.db import Store
from mycodex.research_connectors import ingest_text_source
from mycodex.research_documents import extract_pdf_document, extract_pdf_document_bounded, fetch_grobid_structure
from mycodex.research_evidence import EvidenceLedger, _prepare_passages


def pdf_fixture(contents=None):
    """Small real PDF fixture with independent physical text coordinates."""
    if contents is None:
        contents = [br'''BT /F2 14 Tf 60 730 Td (1 Introduction) Tj ET
BT /F1 11 Tf 60 710 Td (Agents use feedback.) Tj ET
BT /F1 11 Tf 60 690 Td (1 A regular numbered list is not a heading.) Tj ET
BT /F1 11 Tf 60 640 Td (Figure 1: Architecture.) Tj ET
BT /F1 11 Tf 60 580 Td (Table 1: Heldout scores.) Tj ET
60 570 m 550 570 l S
60 530 m 550 530 l S
BT /F1 11 Tf 70 550 Td (Model  Score) Tj ET
BT /F1 11 Tf 160 480 Td (f(x) = x + 1) Tj ET
BT /F1 11 Tf 510 480 Td (\(1\)) Tj ET
BT /F1 11 Tf 60 420 Td (Inline a = b has no numbered equation region.) Tj ET''',
                    b'BT /F2 14 Tf 60 730 Td (2 Results) Tj ET\nBT /F1 11 Tf 60 710 Td (Feedback improves adaptation.) Tj ET']
    objects = [b'<< /Type /Catalog /Pages 2 0 R >>', b'',
               b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>',
               b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>']
    page_ids = []
    for content in contents:
        page_id = len(objects) + 1; content_id = page_id + 1; page_ids.append(page_id)
        objects.append(f'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> /Contents {content_id} 0 R >>'.encode())
        objects.append(f'<< /Length {len(content)} >>\nstream\n'.encode() + content + b'\nendstream')
    objects[1] = f'<< /Type /Pages /Count {len(page_ids)} /Kids [ {" ".join(f"{i} 0 R" for i in page_ids)} ] >>'.encode()
    result = b'%PDF-1.4\n'; offsets = [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(result)); result += f'{index} 0 obj\n'.encode() + obj + b'\nendobj\n'
    xref = len(result)
    result += f'xref\n0 {len(offsets)}\n0000000000 65535 f \n'.encode()
    result += b''.join(f'{offset:010d} 00000 n \n'.encode() for offset in offsets[1:])
    return result + f'trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF'.encode()


@unittest.skipUnless(importlib.util.find_spec('pdfplumber'), 'Optional documents dependency required')
class ResearchDocumentExtractionTests(unittest.TestCase):
    def test_real_pdf_has_physical_sections_captions_tables_and_numbered_equation(self):
        payload = pdf_fixture(); result = extract_pdf_document(payload)
        structure = result['document_structure']
        self.assertEqual(structure['page_count'], 2)
        self.assertEqual(structure['coverage']['pages_processed'], 2)
        self.assertFalse(structure['semantic_verified'])
        by_kind = {}
        for element in structure['elements']:
            by_kind.setdefault(element['kind'], []).append(element)
        self.assertEqual([h['label'] for h in by_kind['section_heading']], ['1 Introduction', '2 Results'])
        self.assertEqual(len(by_kind['figure_caption']), 1)
        self.assertEqual(len(by_kind['table_region']), 1)
        self.assertEqual(len(by_kind['numbered_equation']), 1)
        self.assertIsNone(by_kind['numbered_equation'][0]['latex'])
        prepared = _prepare_passages(result['text'], result['passages'], source_hash=result['content_hash'], structure=structure)
        self.assertTrue(all(p['locator']['source_sha256'] == hashlib.sha256(payload).hexdigest() for p in prepared))
        results = [p for p in prepared if p['text'] == 'Feedback improves adaptation.']
        self.assertEqual(results[0]['locator']['page'], 2)
        self.assertEqual(results[0]['locator']['section'], '2 Results')
        self.assertAlmostEqual(results[0]['locator']['bbox'][0], 60, delta=.02)
        self.assertEqual(structure['grobid']['status'], 'not_configured')

    def test_truncation_cannot_emit_passages_outside_indexed_text(self):
        result = extract_pdf_document(pdf_fixture(), max_chars=20)
        self.assertTrue(result['truncated'])
        self.assertLessEqual(len(result['text']), 20)
        self.assertFalse(result['document_structure']['coverage']['complete_text_extraction'])
        _prepare_passages(result['text'], result['passages'], source_hash=result['content_hash'], structure=result['document_structure'])

    def test_blank_page_does_not_mean_complete_reading(self):
        result = extract_pdf_document(pdf_fixture([b'BT /F1 11 Tf 60 700 Td (Text.) Tj ET', b'']))
        self.assertEqual(result['document_structure']['coverage']['pages_processed'], 2)
        self.assertEqual(result['document_structure']['coverage']['pages_with_text'], 1)
        self.assertFalse(result['document_structure']['coverage']['complete_text_extraction'])

    def test_ingestion_indexes_real_bounded_geometry_and_compact_summary(self):
        from mycodex.research_ops import run_research_task
        with tempfile.TemporaryDirectory() as tmp, Store(Path(tmp) / 'db.sqlite') as store:
            path = Path(tmp) / 'source.pdf'; path.write_bytes(pdf_fixture())
            project = store.add_project('demo', tmp)
            research = store.add_research_thread(project['id'], name='agents', thread_id='thread')
            source = store.upsert_research_source(research['id'], title='Fixture', url=str(path))
            task = store.add_research_task(research['id'], task_type='deep_read', source_id=source)
            result = run_research_task(store, 'demo', 'agents', task_id=task)
            self.assertEqual(result['document_summary']['coverage']['page_count'], 2)
            found = EvidenceLedger(store).search_evidence(research['id'], 'Feedback improves adaptation')
            self.assertTrue(any(p['locator']['page'] == 2 and p['locator'].get('bbox') for p in found))
            metadata = json.loads(store.get_research_source(research['id'], source)['metadata_json'])
            self.assertNotIn('document_structure', metadata['ingestion'])
            self.assertEqual(metadata['ingestion']['document_summary']['coverage']['page_count'], 2)


class ResearchDocumentBoundaryTests(unittest.TestCase):
    def test_optional_parser_missing_retains_plaintext_pdf_fallback(self):
        with patch('mycodex.research_documents._load_pdfplumber', side_effect=ImportError('not installed')):
            missing = extract_pdf_document(pdf_fixture())
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'source.pdf'; path.write_bytes(pdf_fixture())
            with patch('mycodex.research_connectors.extract_pdf_document_bounded', return_value=missing), \
                    patch('mycodex.research_connectors._extract_pdf_with_pdftotext', return_value='first\fsecond'):
                result = ingest_text_source(str(path))
        self.assertEqual(result['method'], 'pdftotext')
        self.assertEqual(result['text'], 'first\fsecond')
        self.assertIsNone(result['passages'])
        self.assertEqual(result['document_structure']['coordinate_system'], 'extracted_text')

    def test_bounded_parser_timeout_never_promotes_missing_text(self):
        with patch('mycodex.research_documents.subprocess.run', side_effect=subprocess.TimeoutExpired('parser', 1)):
            result = extract_pdf_document_bounded(pdf_fixture(), timeout=1)
        self.assertEqual(result['extraction_status'], 'unavailable')
        self.assertEqual(result['text'], '')
        self.assertIn('TimeoutExpired', result['extraction_error'])

    def test_source_input_limit_is_enforced_before_parsing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'source.txt'; path.write_bytes(b'x' * 33)
            with patch('mycodex.research_connectors.MAX_SOURCE_BYTES', 32), self.assertRaisesRegex(ValueError, 'input limit'):
                ingest_text_source(str(path))

    def test_false_character_offsets_and_pdf_page_geometry_are_rejected(self):
        digest = 'a' * 64
        structure = {'source_sha256': digest, 'pages': [{'page': 1, 'page_bbox': [0, 0, 612, 792]}]}
        locator = {'page': 1, 'bbox': [0, 0, 100, 20], 'source_sha256': digest,
                   'coordinate_system': 'pdfplumber_page_points_top_left'}
        for changed in ({**locator, 'page': 2}, {**locator, 'bbox': [0, 0, 700, 20]},
                        {**locator, 'source_sha256': 'b' * 64},
                        {**locator, 'char_start': 1, 'char_end': 5}):
            with self.subTest(locator=changed), self.assertRaises(ValueError):
                _prepare_passages('Text', [{'text': 'Text', 'locator': changed}], source_hash=digest, structure=structure)

    def test_grobid_unavailability_is_an_explicit_coverage_gap(self):
        with patch('urllib.request.urlopen', side_effect=urllib.error.URLError('offline')):
            result = fetch_grobid_structure(pdf_fixture(), 'http://localhost:8070')
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['elements'], [])

    def test_grobid_preserves_provider_coordinates_without_asserting_validation(self):
        from unittest.mock import MagicMock
        response = MagicMock(); response.__enter__.return_value.read.return_value = b'<TEI><text><head coords="2,10,20,30,40">2 Results</head><formula coords="2,40,80,90,20">x=1</formula></text></TEI>'
        with patch('urllib.request.urlopen', return_value=response):
            result = fetch_grobid_structure(pdf_fixture(), 'http://localhost:8070')
        self.assertEqual(result['status'], 'available')
        self.assertEqual(result['elements'][0]['coordinates'], [{'page': 2, 'bbox': [10, 20, 40, 60]}])
        self.assertFalse(result['independently_verified'])


class ExperimentModuleBoundaryTests(unittest.TestCase):
    def test_policy_and_protocol_import_without_loading_execution_module(self):
        code = "import sys; import mycodex.experiment_policy; import mycodex.experiment_protocol; assert 'mycodex.experiment_ops' not in sys.modules"
        result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_existing_imports_resolve_to_refactored_policy_and_protocol(self):
        from mycodex import experiment_ops, experiment_policy, experiment_protocol
        self.assertIs(experiment_ops.extract_execution_contract, experiment_policy.extract_execution_contract)
        self.assertIs(experiment_ops.record_execution_contract_task, experiment_protocol.record_execution_contract_task)
        self.assertIs(experiment_ops.recover_stale_experiment_tasks, experiment_protocol.recover_stale_experiment_tasks)


if __name__ == '__main__':
    unittest.main()
