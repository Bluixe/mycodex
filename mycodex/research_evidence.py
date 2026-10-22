"""Versioned research evidence, independent of mutable source/claim summaries.

Indexing records availability, not scientific support. Only an explicit verified
link can promote a claim; lexical retrieval never performs that verification.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import time
from typing import Any


def init_evidence_schema(conn) -> None:
    expected = {'research_source_versions', 'research_passages', 'research_evidence_heads',
                'research_claim_evidence', 'research_search_receipts'}
    existing = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    if expected <= existing:
        return
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS research_source_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            research_id INTEGER NOT NULL REFERENCES research_threads(id) ON DELETE CASCADE,
            source_id INTEGER NOT NULL REFERENCES research_sources(id) ON DELETE CASCADE,
            content_hash TEXT NOT NULL,
            text_hash TEXT NOT NULL,
            text TEXT NOT NULL,
            location TEXT NOT NULL DEFAULT '',
            extraction_method TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at INTEGER NOT NULL,
            UNIQUE(source_id, content_hash, text_hash)
        );
        CREATE TABLE IF NOT EXISTS research_passages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL REFERENCES research_source_versions(id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            text TEXT NOT NULL,
            locator_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(version_id, ordinal)
        );
        CREATE TABLE IF NOT EXISTS research_evidence_heads (
            source_id INTEGER PRIMARY KEY REFERENCES research_sources(id) ON DELETE CASCADE,
            version_id INTEGER NOT NULL REFERENCES research_source_versions(id) ON DELETE CASCADE,
            observed_at INTEGER NOT NULL
        );
        INSERT OR IGNORE INTO research_evidence_heads (source_id, version_id, observed_at)
            SELECT source_id, MAX(id), MAX(created_at) FROM research_source_versions GROUP BY source_id;
        CREATE TABLE IF NOT EXISTS research_claim_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            research_id INTEGER NOT NULL REFERENCES research_threads(id) ON DELETE CASCADE,
            claim_id INTEGER NOT NULL REFERENCES research_claims(id) ON DELETE CASCADE,
            passage_id INTEGER NOT NULL REFERENCES research_passages(id) ON DELETE CASCADE,
            relation TEXT NOT NULL CHECK(relation IN ('supports', 'contradicts')),
            verification_status TEXT NOT NULL CHECK(verification_status IN ('unverified','verified','rejected')),
            verifier TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            UNIQUE(claim_id, passage_id, relation, verification_status, verifier, notes)
        );
        CREATE TABLE IF NOT EXISTS research_search_receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            research_id INTEGER NOT NULL REFERENCES research_threads(id) ON DELETE CASCADE,
            task_id INTEGER REFERENCES research_tasks(id) ON DELETE SET NULL,
            query TEXT NOT NULL,
            coverage TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_evidence_research ON research_source_versions(research_id, source_id);
        CREATE INDEX IF NOT EXISTS idx_claim_evidence_claim ON research_claim_evidence(research_id, claim_id);
    """)


class EvidenceLedger:
    def __init__(self, store) -> None:
        self.store = store
        self.conn = store.conn
        init_evidence_schema(self.conn)

    def index_source(self, research_id: int, source_id: int, *, text: str,
                     location: str = '', extraction_method: str = '',
                     metadata: dict[str, Any] | None = None,
                     passages: list[dict[str, Any]] | None = None,
                     content_hash: str | None = None) -> dict[str, Any]:
        if not text or not text.strip():
            raise ValueError('cannot index empty or unavailable extracted text')
        source = self.store.get_research_source(research_id, source_id)
        if source is None:
            raise ValueError('source does not belong to this research')
        text_hash = hashlib.sha256(text.encode('utf-8')).hexdigest()
        digest = content_hash or text_hash
        if not re.fullmatch(r'[0-9a-f]{64}', digest):
            raise ValueError('content_hash must be a SHA-256 hex digest')
        prepared = _prepare_passages(text, passages, source_hash=digest,
                                     structure=(metadata or {}).get('document_structure'))
        with self.conn:
            inserted = self.conn.execute("""
                INSERT OR IGNORE INTO research_source_versions
                    (research_id, source_id, content_hash, text_hash, text, location,
                     extraction_method, metadata_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (research_id, source_id, digest, text_hash, text, location,
                  extraction_method, json.dumps(metadata or {}, ensure_ascii=False), int(time.time())))
            row = self.conn.execute("""SELECT * FROM research_source_versions
                WHERE source_id = ? AND content_hash = ? AND text_hash = ?""",
                (source_id, digest, text_hash)).fetchone()
            for ordinal, passage in enumerate(prepared if inserted.rowcount == 1 else []):
                self.conn.execute("""INSERT OR IGNORE INTO research_passages
                    (version_id, ordinal, text, locator_json) VALUES (?, ?, ?, ?)""",
                    (row['id'], ordinal, passage['text'], json.dumps(passage['locator'], ensure_ascii=False)))
            self.conn.execute("""INSERT INTO research_evidence_heads (source_id, version_id, observed_at)
                VALUES (?, ?, ?) ON CONFLICT(source_id) DO UPDATE
                SET version_id = excluded.version_id, observed_at = excluded.observed_at""",
                (source_id, row['id'], int(time.time())))
        version = _decode(row)
        version['passage_ids'] = [item['id'] for item in self.conn.execute(
            'SELECT id FROM research_passages WHERE version_id = ? ORDER BY ordinal', (row['id'],))]
        return version

    def get_source_versions(self, research_id: int, source_id: int, *, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute("""SELECT v.id, v.research_id, v.source_id, v.content_hash, v.text_hash,
            v.location, v.extraction_method, v.metadata_json, v.created_at, length(v.text) AS chars,
            v.id = h.version_id AS is_current, h.observed_at
            FROM research_source_versions v LEFT JOIN research_evidence_heads h ON h.source_id = v.source_id
            WHERE v.research_id = ? AND v.source_id = ?
            ORDER BY is_current DESC, v.id DESC LIMIT ?""", (research_id, source_id, _limit(limit)))
        return [_decode(row) for row in rows]

    def search_evidence(self, research_id: int, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        terms = _terms(query)
        if not terms:
            return []
        # All versions remain retrievable for audit. Retrieval uses only each
        # source's latest version to avoid treating old text as current evidence.
        rows = self.conn.execute("""SELECT p.id AS passage_id, p.text, p.locator_json,
            v.id AS version_id, v.source_id, v.content_hash, v.text_hash,
            v.location, v.extraction_method, s.title AS source_title, s.url AS source_url
            FROM research_passages p
            JOIN research_source_versions v ON v.id = p.version_id
            JOIN research_sources s ON s.id = v.source_id
            JOIN research_evidence_heads h ON h.source_id = v.source_id AND h.version_id = v.id
            WHERE v.research_id = ?
        """, (research_id,))
        ranked = []
        for row in rows:
            body_terms = _terms(row['text'])
            matched = terms & body_terms
            if not matched:
                continue
            item = _decode(row)
            item['retrieval_score'] = round(len(matched) / len(terms), 4)
            item['verification_status'] = 'unverified'
            ranked.append(item)
        ranked.sort(key=lambda item: (-item['retrieval_score'], -item['version_id'], item['passage_id']))
        return ranked[:_limit(limit)]

    def list_evidence(self, research_id: int, *, claim_id: int | None = None,
                      limit: int = 50) -> list[dict[str, Any]]:
        sql = """SELECT e.*, p.text AS passage_text, p.locator_json,
            v.id AS version_id, v.source_id, v.content_hash, v.text_hash, v.location,
            s.title AS source_title, c.claim
            FROM research_claim_evidence e JOIN research_passages p ON p.id = e.passage_id
            JOIN research_source_versions v ON v.id = p.version_id
            JOIN research_sources s ON s.id = v.source_id
            JOIN research_claims c ON c.id = e.claim_id WHERE e.research_id = ?"""
        params: list[Any] = [research_id]
        if claim_id is not None:
            sql += ' AND e.claim_id = ?'
            params.append(claim_id)
        sql += ' ORDER BY e.id DESC LIMIT ?'
        params.append(_limit(limit))
        return [_decode(row) for row in self.conn.execute(sql, params)]

    def link_claim_evidence(self, research_id: int, claim_id: int, passage_id: int, *,
                            relation: str = 'supports', verification_status: str = 'unverified',
                            verifier: str = '', notes: str = '') -> dict[str, Any]:
        if relation not in {'supports', 'contradicts'}:
            raise ValueError('relation must be supports or contradicts')
        if verification_status not in {'unverified', 'verified', 'rejected'}:
            raise ValueError('invalid verification status')
        if verification_status == 'verified' and (not verifier.strip() or not notes.strip()):
            raise ValueError('verified evidence requires an explicit verifier and verification notes')
        claim = self.conn.execute('SELECT * FROM research_claims WHERE id = ? AND research_id = ?',
                                  (claim_id, research_id)).fetchone()
        passage = self.conn.execute("""SELECT p.*, v.research_id FROM research_passages p
            JOIN research_source_versions v ON v.id = p.version_id WHERE p.id = ?""", (passage_id,)).fetchone()
        if claim is None or passage is None or passage['research_id'] != research_id:
            raise ValueError('claim and passage must belong to the same research')
        with self.conn:
            self.conn.execute("""INSERT OR IGNORE INTO research_claim_evidence
                (research_id, claim_id, passage_id, relation, verification_status, verifier, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (research_id, claim_id, passage_id, relation, verification_status, verifier.strip(), notes.strip(), int(time.time())))
            row = self.conn.execute("""SELECT * FROM research_claim_evidence WHERE claim_id = ?
                AND passage_id = ? AND relation = ? AND verification_status = ? AND verifier = ? AND notes = ?""",
                (claim_id, passage_id, relation, verification_status, verifier.strip(), notes.strip())).fetchone()
            # Append-only review history; the latest assessment of each link is
            # authoritative. Opposing verified evidence keeps the claim uncertain.
            verified = self.conn.execute("""SELECT e.relation FROM research_claim_evidence e
                WHERE e.claim_id = ? AND e.verification_status = 'verified'
                AND e.id = (SELECT MAX(e2.id) FROM research_claim_evidence e2
                    WHERE e2.claim_id = e.claim_id AND e2.passage_id = e.passage_id
                    AND e2.relation = e.relation)""", (claim_id,)).fetchall()
            relations = {item['relation'] for item in verified}
            state = 'supported' if relations == {'supports'} else 'contradicted' if relations == {'contradicts'} else 'uncertain'
            metadata = json.loads(claim['metadata_json'])
            metadata['verification_status'] = 'passage_verified' if relations else 'unverified'
            if len(relations) > 1:
                metadata['verification_status'] = 'conflicting_verified_evidence'
            self.conn.execute('UPDATE research_claims SET status = ?, metadata_json = ?, updated_at = ? WHERE id = ?',
                              (state, json.dumps(metadata, ensure_ascii=False), int(time.time()), claim_id))
        return dict(row)

    def record_search(self, research_id: int, query: str, *, receipts: list[dict[str, Any]],
                       coverage: str, task_id: int | None = None) -> int:
        with self.conn:
            cur = self.conn.execute("""INSERT INTO research_search_receipts
                (research_id, task_id, query, coverage, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)""",
                (research_id, task_id, query, coverage, json.dumps(receipts, ensure_ascii=False), int(time.time())))
        return int(cur.lastrowid)

    def list_search_receipts(self, research_id: int, *, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute('SELECT * FROM research_search_receipts WHERE research_id = ? ORDER BY id DESC LIMIT ?',
                                 (research_id, _limit(limit)))
        return [_decode(row) for row in rows]


def _prepare_passages(text: str, passages: list[dict[str, Any]] | None, *,
                      source_hash: str | None = None, structure: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    prepared = []
    if passages is not None:
        for item in passages:
            excerpt = str(item.get('text') or '')
            if not excerpt.strip() or excerpt not in text:
                raise ValueError('passage must be an exact nonempty excerpt of indexed text')
            locator = {'page': None, 'section': None, **dict(item.get('locator') or {})}
            start, end = locator.get('char_start'), locator.get('char_end')
            if start is not None or end is not None:
                if (type(start) is not int or type(end) is not int or not 0 <= start < end <= len(text)
                        or text[start:end] != excerpt):
                    raise ValueError('passage character locator does not match its exact excerpt')
            if locator['page'] is not None and (type(locator['page']) is not int or locator['page'] < 1):
                raise ValueError('passage page must be a positive physical page number or unknown')
            if locator.get('coordinate_system') == 'pdfplumber_page_points_top_left':
                _validate_pdf_locator(locator, structure, source_hash)
                locator['locator_verification'] = 'source_hash_page_and_geometry_checked'
            else:
                locator.setdefault('locator_verification', 'caller_supplied_unverified')
            prepared.append({'text': excerpt, 'locator': locator})
        if not prepared:
            raise ValueError('at least one passage is required')
        return prepared
    # Preserve actual PDF form-feed page boundaries, otherwise report only
    # extraction-text character offsets. These are never called PDF coordinates.
    offset = 0
    paginated = '\f' in text
    for page_no, page in enumerate(text.split('\f'), 1):
        for start in range(0, len(page), 1800):
            excerpt = page[start:start + 1800]
            if excerpt.strip():
                locator = {'char_start': offset + start, 'char_end': offset + start + len(excerpt),
                           'coordinate_system': 'extracted_text', 'page': page_no if paginated else None,
                           'section': None}
                prepared.append({'text': excerpt, 'locator': locator})
        offset += len(page) + 1
    return prepared


def _validate_pdf_locator(locator, structure, source_hash):
    if (not isinstance(structure, dict) or structure.get('source_sha256') != source_hash
            or locator.get('source_sha256') != source_hash):
        raise ValueError('PDF locator source hash does not match indexed source bytes')
    page = next((p for p in structure.get('pages', []) if p.get('page') == locator.get('page')), None)
    if page is None:
        raise ValueError('PDF locator page is absent from the extraction receipt')
    box = locator.get('bbox')
    bounds = page.get('page_bbox')
    if (not isinstance(box, list) or len(box) != 4 or not isinstance(bounds, list) or len(bounds) != 4
            or not all(type(value) in (int, float) and math.isfinite(value) for value in box + bounds)
            or not bounds[0] - .02 <= box[0] <= box[2] <= bounds[2] + .02
            or not bounds[1] - .02 <= box[1] <= box[3] <= bounds[3] + .02):
        raise ValueError('PDF locator bbox is invalid or outside the observed physical page')


def _decode(row) -> dict[str, Any]:
    value = dict(row)
    for key in ('metadata_json', 'locator_json', 'payload_json'):
        if key in value:
            value[key.removesuffix('_json')] = json.loads(value.pop(key))
    return value


def _terms(text: str) -> set[str]:
    stop = {'the', 'and', 'for', 'with', 'from', 'that', 'this', 'what', 'does', 'how', 'can', 'are'}
    terms = {item for item in re.findall(r'\w+', text.casefold()) if len(item) > 2 and item not in stop}
    for phrase in re.findall(r'[\u3400-\u9fff]+', text):
        terms.update(phrase[index:index + 2] for index in range(len(phrase) - 1))
    return terms


def _limit(value: int) -> int:
    return max(1, min(int(value), 500))
