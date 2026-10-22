from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from mycodex.db import Store
from mycodex.research_connectors import SearchBatch, ResearchSearchResult
from mycodex.research_evidence import EvidenceLedger
from mycodex.research_ops import apply_research_contract, build_research_control_context, run_research_task
from mycodex.research_quality import synthesize_ledgers


class ResearchEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'db.sqlite')
        self.project = self.store.add_project('demo', self.tmp.name)
        self.research = self.store.add_research_thread(self.project['id'], name='agents',
            thread_id='thread-one', question='language agent adaptation evidence')
        self.rid = self.research['id']
        self.sid = self.store.upsert_research_source(self.rid, title='Agent adaptation',
            url='https://example.test/agent', depth='abstract', status='candidate')
        self.ledger = EvidenceLedger(self.store)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _index(self, text='Agents adapt using feedback. The baseline does not adapt.', **kwargs):
        return self.ledger.index_source(self.rid, self.sid, text=text, **kwargs)

    def test_versioning_is_immutable_and_idempotent(self):
        first = self._index(metadata={'version': 1})
        replay = self._index(metadata={'version': 2}, passages=[{'text': 'Agents adapt', 'locator': {'page': 99}}])
        second = self._index('Agents adapt faster with memory.')
        self.assertEqual(first['id'], replay['id'])
        self.assertEqual(replay['metadata'], {'version': 1})
        self.assertEqual(first['passage_ids'], replay['passage_ids'])
        self.assertNotEqual(first['id'], second['id'])
        self.assertEqual(len(self.ledger.get_source_versions(self.rid, self.sid)), 2)
        self.assertIsNone(self.ledger.search_evidence(self.rid, 'memory')[0]['locator']['page'])

    def test_empty_and_fabricated_passages_rejected(self):
        with self.assertRaises(ValueError):
            self._index(' \n ')
        with self.assertRaises(ValueError):
            self._index(passages=[{'text': 'fabricated excerpt', 'locator': {'page': 1}}])
        self.assertEqual(self.ledger.get_source_versions(self.rid, self.sid), [])

    def test_reobserved_prior_version_becomes_current_without_mutation(self):
        old = self._index('Original feedback evidence.')
        self._index('Replacement adaptation result.')
        replay = self._index('Original feedback evidence.', metadata={'new_observation': True})
        self.assertEqual(old['id'], replay['id'])
        self.assertEqual(self.ledger.search_evidence(self.rid, 'feedback')[0]['version_id'], old['id'])
        self.assertEqual(self.ledger.get_source_versions(self.rid, self.sid)[0]['is_current'], 1)
        self.assertEqual(replay['metadata'], {})

    def test_pdf_page_locators_follow_actual_form_feed_boundaries(self):
        self._index('First page agents.\fSecond page feedback.')
        result = self.ledger.search_evidence(self.rid, 'feedback')[0]
        self.assertEqual(result['locator']['page'], 2)
        self.assertEqual(result['locator']['coordinate_system'], 'extracted_text')

    def test_chinese_passage_retrieval_uses_character_bigrams(self):
        self._index('语言智能体根据反馈调整策略，完成跨回合适应。')
        results = self.ledger.search_evidence(self.rid, '反馈适应')
        self.assertEqual(results[0]['source_id'], self.sid)

    def test_verified_support_conflict_and_retraction(self):
        version = self._index()
        passage_id = version['passage_ids'][0]
        claim = self.store.add_research_claim(self.rid, claim='Agents adapt using feedback.')
        self.ledger.link_claim_evidence(self.rid, claim, passage_id)
        self.assertEqual(self.store.list_research_claims(self.rid)[0]['status'], 'uncertain')
        with self.assertRaises(ValueError):
            self.ledger.link_claim_evidence(self.rid, claim, passage_id, verification_status='verified')
        self.ledger.link_claim_evidence(self.rid, claim, passage_id, verification_status='verified',
            verifier='reviewer', notes='Passage states the mechanism directly.')
        self.assertEqual(self.store.list_research_claims(self.rid)[0]['status'], 'supported')
        self.ledger.link_claim_evidence(self.rid, claim, passage_id, relation='contradicts',
            verification_status='verified', verifier='reviewer', notes='A competing interpretation remains.')
        self.assertEqual(self.store.list_research_claims(self.rid)[0]['status'], 'uncertain')
        self.ledger.link_claim_evidence(self.rid, claim, passage_id, relation='contradicts',
            verification_status='rejected', verifier='reviewer', notes='Interpretation ruled out on rereading.')
        self.assertEqual(self.store.list_research_claims(self.rid)[0]['status'], 'supported')
        self.assertEqual(len(self.ledger.list_evidence(self.rid, claim_id=claim)), 4)

    def test_cross_research_link_rejected(self):
        version = self._index()
        other = self.store.add_research_thread(self.project['id'], name='other', thread_id='other')
        claim = self.store.add_research_claim(other['id'], claim='Other claim')
        with self.assertRaises(ValueError):
            self.ledger.link_claim_evidence(other['id'], claim, version['passage_ids'][0])

    def test_retrieval_uses_full_index_and_latest_source_version(self):
        self._index('Unique agent feedback retrieval evidence target.')
        for i in range(20):
            self.store.upsert_research_source(self.rid, title=f'Unrelated {i}', url=f'https://example.test/{i}')
        context = json.loads(build_research_control_context(self.store, 'demo', 'agents', source_limit=1))
        self.assertEqual(len(context['sources']), 1)
        self.assertEqual(context['evidence_passages'][0]['source_id'], self.sid)
        self.assertEqual(context['evidence_passages'][0]['verification_status'], 'unverified')
        self._index('Completely changed text.')
        self.assertEqual(self.ledger.search_evidence(self.rid, 'feedback'), [])

    def test_model_claim_requires_real_passage_and_verification(self):
        contract = {'claim_ledger_delta': [{'claim': 'Unsupported claim', 'status': 'supported',
            'support': 'An unrelated title'}]}
        apply_research_contract(self.store, 'demo', 'agents', contract)
        claim = self.store.list_research_claims(self.rid)[0]
        self.assertEqual(claim['status'], 'uncertain')
        version = self._index()
        contract['claim_ledger_delta'] = [{'claim': 'Agents adapt using feedback.', 'status': 'supported',
            'evidence_links': [{'passage_id': version['passage_ids'][0], 'verification_status': 'verified',
                                'notes': 'The passage explicitly states feedback adaptation.'}]}]
        apply_research_contract(self.store, 'demo', 'agents', contract, run_id=None)
        claims = self.store.list_research_claims(self.rid)
        self.assertTrue(any(row['status'] == 'supported' for row in claims))

    def test_local_synthesis_never_claims_support(self):
        for sources in ([], [{'title': 'Irrelevant title', 'status': 'kept', 'relevance': 5}]):
            synthesis = synthesize_ledgers(question='LLM reasoning', sources=sources)
            for claim in synthesis['claims']:
                self.assertEqual(claim['status'], 'uncertain')
                self.assertEqual(claim['support'], '')
                self.assertEqual(claim['confidence'], 0)

    def test_empty_pdf_does_not_promote_reading_or_evidence(self):
        task = self.store.add_research_task(self.rid, task_type='deep_read', source_id=self.sid)
        def ingest(*args, **kwargs):
            return {'text': ' \n', 'method': 'pdf-unavailable', 'extraction_status': 'unavailable'}
        with self.assertRaisesRegex(RuntimeError, 'unavailable'):
            run_research_task(self.store, 'demo', 'agents', task_id=task, ingester=ingest)
        source = self.store.get_research_source(self.rid, self.sid)
        self.assertEqual(source['depth'], 'abstract')
        self.assertEqual(source['status'], 'candidate')
        self.assertEqual(source['evidence_quality'], 0)
        self.assertEqual(self.ledger.get_source_versions(self.rid, self.sid), [])
        self.assertEqual(self.store.get_research_task(self.rid, task)['status'], 'failed')

    def test_partial_coverage_receipts_persist_without_claiming_complete(self):
        task = self.store.add_research_task(self.rid, task_type='search', query='agents',
            payload={'expand_queries': False})
        def search(*args, **kwargs):
            return SearchBatch([ResearchSearchResult('Agent paper', 'https://example.test/result')],
                receipts=[{'provider': 'arxiv', 'status': 'success', 'result_count': 1},
                          {'provider': 'openalex', 'status': 'error', 'error': 'offline'}])
        result = run_research_task(self.store, 'demo', 'agents', task_id=task, searcher=search)
        self.assertEqual(result['coverage']['search_status'], 'partial')
        receipts = self.ledger.list_search_receipts(self.rid)
        self.assertEqual(receipts[0]['coverage'], 'partial')
        events = self.store.list_research_events(self.rid)
        self.assertTrue(any(event['event_type'] == 'search_coverage' and 'offline' in event['payload_json'] for event in events))

    def test_all_providers_unavailable_fails_and_records_receipts(self):
        task = self.store.add_research_task(self.rid, task_type='search', query='agents',
            payload={'expand_queries': False})
        def search(*args, **kwargs):
            return SearchBatch([], receipts=[{'provider': 'arxiv', 'status': 'error', 'error': 'offline'}])
        with self.assertRaisesRegex(RuntimeError, 'providers unavailable'):
            run_research_task(self.store, 'demo', 'agents', task_id=task, searcher=search)
        self.assertEqual(self.ledger.list_search_receipts(self.rid)[0]['coverage'], 'unavailable')
        self.assertEqual(self.store.get_research_task(self.rid, task)['status'], 'failed')

    def test_plain_list_searcher_is_compatible_and_coverage_unknown(self):
        task = self.store.add_research_task(self.rid, task_type='search', query='agents',
            payload={'expand_queries': False})
        result = run_research_task(self.store, 'demo', 'agents', task_id=task,
            searcher=lambda *args, **kwargs: [])
        self.assertEqual(result['status'], 'done')
        self.assertEqual(result['coverage']['search_status'], 'unknown')

    def test_invalid_extracted_passage_does_not_promote_source(self):
        task = self.store.add_research_task(self.rid, task_type='deep_read', source_id=self.sid)
        def ingest(*args, **kwargs):
            return {'text': 'Real extracted text', 'method': 'fake',
                    'passages': [{'text': 'fabricated passage'}]}
        with self.assertRaisesRegex(ValueError, 'exact nonempty excerpt'):
            run_research_task(self.store, 'demo', 'agents', task_id=task, ingester=ingest)
        source = self.store.get_research_source(self.rid, self.sid)
        self.assertEqual(source['status'], 'candidate')
        self.assertEqual(source['depth'], 'abstract')

    def test_empty_top_read_fails_without_promoting_evidence(self):
        task = self.store.add_research_task(self.rid, task_type='deep_read_top')
        with self.assertRaisesRegex(RuntimeError, 'could not extract'):
            run_research_task(self.store, 'demo', 'agents', task_id=task,
                ingester=lambda *args, **kwargs: {'text': '', 'method': 'pdf-unavailable'})
        self.assertEqual(self.ledger.get_source_versions(self.rid, self.sid), [])
        self.assertEqual(self.store.get_research_source(self.rid, self.sid)['status'], 'candidate')

    def test_repeat_search_does_not_replace_successful_full_read(self):
        task = self.store.add_research_task(self.rid, task_type='deep_read', source_id=self.sid)
        run_research_task(self.store, 'demo', 'agents', task_id=task,
            ingester=lambda *args, **kwargs: {'text': 'Agent feedback adaptation full text evidence.', 'method': 'text'})
        self.store.add_research_task(self.rid, task_type='search', query='agents', payload={'expand_queries': False})
        run_research_task(self.store, 'demo', 'agents',
            searcher=lambda *args, **kwargs: [{'title': 'Agent adaptation', 'url': 'https://example.test/agent',
                                              'abstract': 'Short provider abstract.'}])
        source = self.store.get_research_source(self.rid, self.sid)
        self.assertEqual(source['depth'], 'skim')
        self.assertEqual(self.ledger.search_evidence(self.rid, 'feedback')[0]['extraction_method'], 'text')

    def test_unavailable_threat_search_does_not_claim_novelty(self):
        idea = self.store.add_research_idea(self.rid, title='Feedback policy memory',
            hypothesis='Memory improves adaptation', minimal_test='Compare on heldout tasks', kill_criteria='No improvement')
        task = self.store.add_research_task(self.rid, task_type='threat_search', payload={'idea_ids': [idea]})
        def search(*args, **kwargs):
            return SearchBatch([], receipts=[{'provider': 'arxiv', 'status': 'error', 'error': 'offline'}])
        with self.assertRaisesRegex(RuntimeError, 'novelty remains unverified'):
            run_research_task(self.store, 'demo', 'agents', task_id=task, searcher=search)
        critique = self.store.add_research_task(self.rid, task_type='idea_critique', payload={'idea_ids': [idea]})
        result = run_research_task(self.store, 'demo', 'agents', task_id=critique)
        updated = self.store.get_research_idea(self.rid, idea)
        self.assertEqual(updated['status'], 'candidate')
        self.assertEqual(result['critiques'][0]['score']['novelty'], 0)
        self.assertIn('unknown_coverage_incomplete', updated['metadata_json'])


if __name__ == '__main__':
    unittest.main()
