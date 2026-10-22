from __future__ import annotations

import json
import hashlib
import os
import re
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable

from .research_documents import extract_pdf_document_bounded


DEFAULT_SEARCH_PROVIDERS = ("zotero", "arxiv", "openalex", "semanticscholar")
USER_AGENT = "mycodex/0.1 (+https://github.com/openai/codex)"
MAX_SOURCE_BYTES = 50_000_000


class SearchBatch(list):
    """List-compatible results plus bounded provider coverage and provenance."""

    def __init__(self, results=(), *, receipts=None):
        super().__init__(results)
        self.receipts = list(receipts or [])
        states = {receipt['status'] for receipt in self.receipts}
        self.coverage = ('unknown' if not states else 'unavailable' if states <= {'error', 'unavailable'}
                         else 'partial' if states & {'error', 'unavailable'} else 'complete_requested')


class ProviderHTTPError(RuntimeError):
    def __init__(self, code: int, url: str):
        self.code = code
        super().__init__(f'HTTP {code} from {_redact_sensitive(url)}')


@dataclass(frozen=True)
class ResearchSearchResult:
    title: str
    url: str
    source_type: str = "paper"
    role: str = "candidate"
    abstract: str = ""
    year: int | None = None
    authors: tuple[str, ...] = ()
    venue: str = ""
    provider: str = ""
    provider_id: str = ""
    doi: str = ""
    pdf_url: str = ""
    citation_count: int | None = None

    def as_source_kwargs(self) -> dict[str, Any]:
        metadata = {
            "provider": self.provider,
            "provider_id": self.provider_id,
            "year": self.year,
            "authors": list(self.authors),
            "venue": self.venue,
            "doi": self.doi,
            "pdf_url": self.pdf_url,
            "citation_count": self.citation_count,
            "abstract": self.abstract,
        }
        return {
            "title": self.title,
            "url": self.url,
            "source_type": self.source_type,
            "role": self.role,
            "depth": "abstract" if self.abstract else "title",
            "status": "candidate",
            "relevance": 0,
            "evidence_quality": 0,
            "notes": _compact_notes(self.abstract),
            "metadata": {k: v for k, v in metadata.items() if v not in (None, "", [], ())},
        }


def search_public_sources(
    query: str,
    *,
    providers: Iterable[str] | None = None,
    limit: int = 5,
    timeout: int = 30,
    semantic_scholar_api_key: str | None = None,
    cache_dir: str | Path | None = None,
    cache_ttl: int = 3600,
    retries: int = 2,
) -> SearchBatch:
    query = query.strip()
    if not query:
        return SearchBatch()
    selected = tuple(providers or DEFAULT_SEARCH_PROVIDERS)
    per_provider = max(1, int(limit or 5))
    results: list[ResearchSearchResult] = []
    receipts = []
    for provider in selected:
        provider_key = provider.strip().lower()
        started_at = int(time.time())
        receipt = {'provider': provider_key, 'query': query, 'limit': per_provider,
                   'requested_at': started_at, 'from_cache': False, 'attempts': 0,
                   'exhaustive': False}
        cache_root = Path(cache_dir) if cache_dir else None
        cache_key = hashlib.sha256(json.dumps([provider_key, query, per_provider]).encode()).hexdigest()
        cache_path = cache_root / f'{cache_key}.json' if cache_root and provider_key != 'zotero' else None
        try:
            cached = _read_search_cache(cache_path, ttl=cache_ttl)
            if cached is not None:
                provider_results = [ResearchSearchResult(**{**item, 'authors': tuple(item.get('authors') or ())})
                                    for item in cached['results']]
                receipt.update(from_cache=True, fetched_at=cached['fetched_at'])
            else:
                for attempt in range(max(0, int(retries)) + 1):
                    receipt['attempts'] = attempt + 1
                    try:
                        if provider_key == "arxiv":
                            provider_results = search_arxiv(query, limit=per_provider, timeout=timeout)
                        elif provider_key == "zotero":
                            provider_results = search_zotero(query, limit=per_provider)
                        elif provider_key == "openalex":
                            provider_results = search_openalex(query, limit=per_provider, timeout=timeout)
                        elif provider_key in {"semantic_scholar", "semanticscholar", "s2"}:
                            provider_results = search_semantic_scholar(query, limit=per_provider, timeout=timeout,
                                                                      api_key=semantic_scholar_api_key)
                        else:
                            raise ValueError(f'unsupported research provider: {provider_key}')
                        break
                    except Exception as exc:
                        if attempt >= max(0, int(retries)) or not _retryable(exc):
                            raise
                        time.sleep(min(2.0, 0.25 * (2 ** attempt)))
                receipt['fetched_at'] = int(time.time())
                if cache_path:
                    _write_search_cache(cache_path, provider_results, receipt['fetched_at'])
            results.extend(provider_results)
            receipt.update(status='success' if provider_results else 'empty', result_count=len(provider_results),
                           result_hash=_search_result_hash(provider_results))
        except Exception as exc:
            receipt.update(status='unavailable' if isinstance(exc, FileNotFoundError) else 'error',
                           result_count=0, error_type=type(exc).__name__, error=_redact_sensitive(str(exc)))
        receipts.append(receipt)
    deduped = _dedupe_results(results)
    return SearchBatch(deduped[:per_provider * max(1, len(selected))], receipts=receipts)


def search_zotero(
    query: str,
    *,
    limit: int = 5,
    zotero_dir: str | Path | None = None,
) -> list[ResearchSearchResult]:
    data_dir = _zotero_data_dir(zotero_dir)
    db_path = data_dir / "zotero.sqlite"
    if not db_path.exists():
        raise FileNotFoundError(f'Zotero library unavailable: {db_path}')
    with _zotero_snapshot(db_path) as snapshot:
        conn = sqlite3.connect(snapshot)
        conn.row_factory = sqlite3.Row
        try:
            rows = _zotero_item_rows(conn, data_dir=data_dir)
        finally:
            conn.close()
    terms = [term.casefold() for term in re.findall(r"\w+", query) if term.strip()]
    scored: list[tuple[int, sqlite3.Row, dict[str, Any]]] = []
    for row in rows:
        metadata = json.loads(row["metadata_json"])
        haystack = " ".join(
            str(value)
            for value in (
                row["title"],
                row["abstract"],
                row["publication_title"],
                row["doi"],
                row["url"],
                row["creators"],
                row["tags"],
                row["collections"],
                row["item_type"],
            )
            if value
        ).casefold()
        score = sum(1 for term in terms if term in haystack)
        if terms and score == 0:
            continue
        if row["title"] and query.casefold() in str(row["title"]).casefold():
            score += 3
        if row["abstract"] and query.casefold() in str(row["abstract"]).casefold():
            score += 2
        if row["date_modified"]:
            score += 1
        scored.append((score, row, metadata))
    scored.sort(key=lambda item: (item[0], item[1]["date_modified"] or ""), reverse=True)
    return [_zotero_result(row, metadata) for _score, row, metadata in scored[: max(1, int(limit or 5))]]


def _zotero_item_rows(conn: sqlite3.Connection, *, data_dir: Path) -> list[sqlite3.Row]:
    sql = """
    WITH item_fields AS (
        SELECT
            itemData.itemID,
            MAX(CASE fields.fieldName WHEN 'title' THEN itemDataValues.value END) AS title,
            MAX(CASE fields.fieldName WHEN 'abstractNote' THEN itemDataValues.value END) AS abstract,
            MAX(CASE fields.fieldName WHEN 'date' THEN itemDataValues.value END) AS date,
            MAX(CASE fields.fieldName WHEN 'url' THEN itemDataValues.value END) AS url,
            MAX(CASE fields.fieldName WHEN 'DOI' THEN itemDataValues.value END) AS doi,
            MAX(CASE fields.fieldName WHEN 'publicationTitle' THEN itemDataValues.value END) AS publication_title
        FROM itemData
        JOIN fields ON fields.fieldID = itemData.fieldID
        JOIN itemDataValues ON itemDataValues.valueID = itemData.valueID
        GROUP BY itemData.itemID
    ),
    creator_rows AS (
        SELECT
            itemCreators.itemID,
            GROUP_CONCAT(
                TRIM(COALESCE(creators.firstName, '') || ' ' || COALESCE(creators.lastName, '')),
                '; '
            ) AS creators
        FROM itemCreators
        JOIN creators ON creators.creatorID = itemCreators.creatorID
        GROUP BY itemCreators.itemID
    ),
    tag_rows AS (
        SELECT itemTags.itemID, GROUP_CONCAT(tags.name, '; ') AS tags
        FROM itemTags
        JOIN tags ON tags.tagID = itemTags.tagID
        GROUP BY itemTags.itemID
    ),
    collection_rows AS (
        SELECT collectionItems.itemID, GROUP_CONCAT(collections.collectionName, '; ') AS collections
        FROM collectionItems
        JOIN collections ON collections.collectionID = collectionItems.collectionID
        GROUP BY collectionItems.itemID
    ),
    attachment_rows AS (
        SELECT
            itemAttachments.parentItemID AS itemID,
            attachment_items.key AS attachment_key,
            itemAttachments.path AS attachment_path,
            itemAttachments.contentType AS attachment_content_type
        FROM itemAttachments
        JOIN items AS attachment_items ON attachment_items.itemID = itemAttachments.itemID
        WHERE itemAttachments.parentItemID IS NOT NULL
        ORDER BY
            CASE
                WHEN itemAttachments.contentType = 'application/pdf' THEN 0
                ELSE 1
            END,
            itemAttachments.itemID ASC
    )
    SELECT
        items.itemID,
        items.key,
        items.dateModified AS date_modified,
        itemTypes.typeName AS item_type,
        item_fields.title,
        item_fields.abstract,
        item_fields.date,
        item_fields.url,
        item_fields.doi,
        item_fields.publication_title,
        creator_rows.creators,
        tag_rows.tags,
        collection_rows.collections,
        attachment_rows.attachment_key,
        attachment_rows.attachment_path,
        attachment_rows.attachment_content_type
    FROM items
    JOIN itemTypes ON itemTypes.itemTypeID = items.itemTypeID
    LEFT JOIN item_fields ON item_fields.itemID = items.itemID
    LEFT JOIN creator_rows ON creator_rows.itemID = items.itemID
    LEFT JOIN tag_rows ON tag_rows.itemID = items.itemID
    LEFT JOIN collection_rows ON collection_rows.itemID = items.itemID
    LEFT JOIN attachment_rows ON attachment_rows.itemID = items.itemID
    WHERE itemTypes.typeName NOT IN ('attachment', 'note', 'annotation')
    GROUP BY items.itemID
    """
    rows: list[sqlite3.Row] = []
    for row in conn.execute(sql):
        local_attachment_path = _zotero_attachment_path(
            data_dir,
            row["attachment_key"],
            row["attachment_path"],
        )
        metadata = {
            "zotero_item_id": row["itemID"],
            "zotero_key": row["key"],
            "item_type": row["item_type"],
            "external_url": row["url"],
            "tags": _split_multi(row["tags"]),
            "collections": _split_multi(row["collections"]),
            "local_attachment_path": local_attachment_path,
            "attachment_content_type": row["attachment_content_type"],
            "date_modified": row["date_modified"],
        }
        rows.append(
            _dict_row(
                {
                    **dict(row),
                    "metadata_json": json.dumps(metadata, ensure_ascii=False),
                }
            )
        )
    return rows


def _zotero_result(row: sqlite3.Row, metadata: dict[str, Any]) -> ResearchSearchResult:
    authors = tuple(_split_multi(row["creators"]))
    title = str(row["title"] or f"Zotero item {row['key']}")
    return ResearchSearchResult(
        title=title,
        url=f"zotero://select/library/items/{row['key']}",
        source_type="zotero",
        role="personal_library",
        abstract=str(row["abstract"] or ""),
        year=_year_from_text(str(row["date"] or "")),
        authors=authors,
        venue=str(row["publication_title"] or row["item_type"] or ""),
        provider="zotero",
        provider_id=str(row["key"]),
        doi=str(row["doi"] or ""),
        pdf_url=str(metadata.get("local_attachment_path") or ""),
    )


def search_arxiv(query: str, *, limit: int = 5, timeout: int = 30) -> list[ResearchSearchResult]:
    params = urllib.parse.urlencode(
        {
            "search_query": f"all:{query}",
            "start": 0,
            "max_results": max(1, int(limit or 5)),
            "sortBy": "relevance",
            "sortOrder": "descending",
        }
    )
    root = ET.fromstring(_read_url(f"https://export.arxiv.org/api/query?{params}", timeout=timeout))
    ns = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
    results: list[ResearchSearchResult] = []
    for entry in root.findall("atom:entry", ns):
        title = _clean_ws(_xml_text(entry.find("atom:title", ns)))
        url = _clean_ws(_xml_text(entry.find("atom:id", ns)))
        abstract = _clean_ws(_xml_text(entry.find("atom:summary", ns)))
        year = _year_from_text(_xml_text(entry.find("atom:published", ns)))
        authors = tuple(
            _clean_ws(_xml_text(author.find("atom:name", ns)))
            for author in entry.findall("atom:author", ns)
            if _xml_text(author.find("atom:name", ns)).strip()
        )
        pdf_url = ""
        for link in entry.findall("atom:link", ns):
            if link.attrib.get("title") == "pdf" or link.attrib.get("type") == "application/pdf":
                pdf_url = link.attrib.get("href", "")
                break
        primary_category = entry.find("arxiv:primary_category", ns)
        venue = primary_category.attrib.get("term", "") if primary_category is not None else ""
        if title and url:
            results.append(
                ResearchSearchResult(
                    title=title,
                    url=url,
                    abstract=abstract,
                    year=year,
                    authors=authors,
                    venue=venue,
                    provider="arxiv",
                    provider_id=url.rsplit("/", 1)[-1],
                    pdf_url=pdf_url,
                )
            )
    return results


def search_openalex(query: str, *, limit: int = 5, timeout: int = 30) -> list[ResearchSearchResult]:
    params = {"search": query, "per-page": max(1, int(limit or 5))}
    mailto = os.environ.get("OPENALEX_MAILTO")
    if mailto:
        params["mailto"] = mailto
    if os.environ.get('OPENALEX_API_KEY'):
        params['api_key'] = os.environ['OPENALEX_API_KEY']
    data = _read_json(
        "https://api.openalex.org/works?" + urllib.parse.urlencode(params),
        timeout=timeout,
    )
    results: list[ResearchSearchResult] = []
    for item in data.get("results") or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("display_name") or "").strip()
        url = _openalex_landing_url(item)
        if not title or not url:
            continue
        authors = tuple(
            str(authorship.get("author", {}).get("display_name") or "").strip()
            for authorship in item.get("authorships") or []
            if isinstance(authorship, dict)
            and str(authorship.get("author", {}).get("display_name") or "").strip()
        )
        open_access = item.get("open_access") if isinstance(item.get("open_access"), dict) else {}
        results.append(
            ResearchSearchResult(
                title=title,
                url=url,
                abstract=_abstract_from_openalex(item.get("abstract_inverted_index")),
                year=_int_or_none(item.get("publication_year")),
                authors=authors,
                venue=str(((item.get("primary_location") or {}).get("source") or {}).get("display_name") or ""),
                provider="openalex",
                provider_id=str(item.get("id") or ""),
                doi=str(item.get("doi") or ""),
                pdf_url=str(open_access.get("oa_url") or ""),
                citation_count=_int_or_none(item.get("cited_by_count")),
            )
        )
    return results


def search_semantic_scholar(
    query: str,
    *,
    limit: int = 5,
    timeout: int = 30,
    api_key: str | None = None,
) -> list[ResearchSearchResult]:
    params = urllib.parse.urlencode(
        {
            "query": query,
            "limit": max(1, min(int(limit or 5), 100)),
            "fields": (
                "paperId,title,url,abstract,year,authors,venue,citationCount,"
                "openAccessPdf,externalIds"
            ),
        }
    )
    headers = {}
    key = api_key or os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    if key:
        headers["x-api-key"] = key
    data = _read_json(
        f"https://api.semanticscholar.org/graph/v1/paper/search?{params}",
        timeout=timeout,
        headers=headers,
    )
    results: list[ResearchSearchResult] = []
    for item in data.get("data") or []:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        if not title or not url:
            continue
        open_pdf = item.get("openAccessPdf") if isinstance(item.get("openAccessPdf"), dict) else {}
        external = item.get("externalIds") if isinstance(item.get("externalIds"), dict) else {}
        authors = tuple(
            str(author.get("name") or "").strip()
            for author in item.get("authors") or []
            if isinstance(author, dict) and str(author.get("name") or "").strip()
        )
        results.append(
            ResearchSearchResult(
                title=title,
                url=url,
                abstract=str(item.get("abstract") or ""),
                year=_int_or_none(item.get("year")),
                authors=authors,
                venue=str(item.get("venue") or ""),
                provider="semanticscholar",
                provider_id=str(item.get("paperId") or ""),
                doi=str(external.get("DOI") or ""),
                pdf_url=str(open_pdf.get("url") or ""),
                citation_count=_int_or_none(item.get("citationCount")),
            )
        )
    return results


def ingest_text_source(
    location: str,
    *,
    max_chars: int = 12000,
    timeout: int = 60,
    grobid_url: str | None = None,
) -> dict[str, Any]:
    location = location.strip()
    if not location:
        raise ValueError("source URL or path is required")
    if type(max_chars) is not int or not 1 <= max_chars <= 2_000_000:
        raise ValueError("max_chars must be an integer between 1 and 2000000")
    if _looks_like_url(location):
        payload, content_type, final_url = _read_url_bytes(location, timeout=timeout)
        suffix = _suffix_for_content(final_url, content_type)
        extraction = _extract_source_bytes(payload, suffix=suffix, max_chars=max_chars, grobid_url=grobid_url, timeout=timeout)
        text, method = extraction["text"], extraction["method"]
        return {
            **extraction,
            "location": location,
            "resolved_location": final_url,
            "content_type": content_type,
            "bytes": len(payload),
            "text": text[:max_chars],
            "method": method,
            "content_hash": hashlib.sha256(payload).hexdigest(),
            "extraction_status": 'available' if text.strip() else 'unavailable',
            "truncated": extraction.get("truncated", len(text) >= max_chars),
            "fetched_at": int(time.time()),
        }
    parsed = urllib.parse.urlparse(location)
    if parsed.scheme == "file":
        path = Path(urllib.request.url2pathname(('//' + parsed.netloc if parsed.netloc else '') + parsed.path)).expanduser().resolve()
    else:
        path = Path(location).expanduser().resolve()
    if not path.is_file():
        raise ValueError("research source must be a regular readable file")
    with path.open('rb') as reader:
        payload = _bounded_source_bytes(reader)
    extraction = _extract_source_bytes(payload, suffix=path.suffix.lower(), max_chars=max_chars, grobid_url=grobid_url, timeout=timeout)
    text, method = extraction["text"], extraction["method"]
    return {
        **extraction,
        "location": location,
        "resolved_location": str(path),
        "content_type": _content_type_for_suffix(path.suffix.lower()),
        "bytes": len(payload),
        "text": text[:max_chars],
        "method": method,
        "content_hash": hashlib.sha256(payload).hexdigest(),
        "extraction_status": 'available' if text.strip() else 'unavailable',
        "truncated": extraction.get("truncated", len(text) >= max_chars),
        "fetched_at": int(time.time()),
    }


class _zotero_snapshot:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.tmpdir: tempfile.TemporaryDirectory[str] | None = None
        self.snapshot: Path | None = None

    def __enter__(self) -> Path:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.snapshot = Path(self.tmpdir.name) / "zotero.sqlite"
        source = destination = None
        try:
            # SQLite's backup API includes committed WAL data and produces one
            # consistent snapshot while leaving the user's library read-only.
            source = sqlite3.connect(self.db_path.as_uri() + '?mode=ro', uri=True, timeout=10)
            destination = sqlite3.connect(self.snapshot)
            source.backup(destination)
        except Exception:
            if destination is not None:
                destination.close()
                destination = None
            self.tmpdir.cleanup()
            raise
        finally:
            if source is not None:
                source.close()
            if destination is not None:
                destination.close()
        return self.snapshot

    def __exit__(self, *_exc: object) -> None:
        if self.tmpdir is not None:
            self.tmpdir.cleanup()


class _dict_row(dict):
    def __getitem__(self, key: str) -> Any:
        return self.get(key)


def _read_url(url: str, *, timeout: int, headers: dict[str, str] | None = None) -> str:
    payload, _content_type, _final_url = _read_url_bytes(url, timeout=timeout, headers=headers)
    return payload.decode("utf-8", errors="replace")


def _read_json(url: str, *, timeout: int, headers: dict[str, str] | None = None) -> dict[str, Any]:
    return json.loads(_read_url(url, timeout=timeout, headers=headers))


def _redact_sensitive(value: str) -> str:
    value = re.sub(r'(?i)(api[_-]?key|token|password|secret)=([^&\s]+)', r'\1=[redacted]', value)
    for name in ('OPENALEX_API_KEY', 'SEMANTIC_SCHOLAR_API_KEY'):
        secret = os.environ.get(name)
        if secret:
            value = value.replace(secret, '[redacted]').replace(urllib.parse.quote(secret), '[redacted]')
    return value[:1200]


def _retryable(exc: Exception) -> bool:
    code = getattr(exc, 'code', None)
    return code == 429 or (isinstance(code, int) and code >= 500) or isinstance(exc, (urllib.error.URLError, TimeoutError))


def _read_search_cache(path: Path | None, *, ttl: int) -> dict[str, Any] | None:
    if path is None or ttl <= 0:
        return None
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        if value.get('schema_version') != 1 or not isinstance(value.get('results'), list):
            return None
        if not 0 <= time.time() - float(value['fetched_at']) <= ttl:
            return None
        items = [ResearchSearchResult(**item) for item in value['results']]
        if value.get('result_hash') != _search_result_hash(items):
            return None
        return value
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _write_search_cache(path: Path, results: list[ResearchSearchResult], fetched_at: int) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Atomic replacement also tolerates concurrent cache users. Cache writes
        # are optional; their failure must not discard a successful provider result.
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            json.dump({'schema_version': 1, 'fetched_at': fetched_at,
                       'result_hash': _search_result_hash(results),
                       'results': [asdict(item) for item in results]}, tmp, ensure_ascii=False)
        try:
            tmp_path.replace(path)
        finally:
            tmp_path.unlink(missing_ok=True)
    except OSError:
        return


def _search_result_hash(results: list[ResearchSearchResult]) -> str:
    return hashlib.sha256(json.dumps([asdict(item) for item in results], sort_keys=True,
                                     ensure_ascii=False).encode('utf-8')).hexdigest()


def _read_url_bytes(
    url: str,
    *,
    timeout: int,
    headers: dict[str, str] | None = None,
) -> tuple[bytes, str, str]:
    req_headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    req_headers.update(headers or {})
    request = urllib.request.Request(url, headers=req_headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return (
                _bounded_source_bytes(response),
                response.headers.get("content-type", ""),
                response.geturl(),
            )
    except urllib.error.HTTPError as exc:
        raise ProviderHTTPError(exc.code, url) from exc


def _bounded_source_bytes(stream) -> bytes:
    payload = stream.read(MAX_SOURCE_BYTES + 1)
    if len(payload) > MAX_SOURCE_BYTES:
        raise ValueError('research source exceeds the 50 MB input limit')
    return payload


def _extract_source_bytes(payload: bytes, *, suffix: str, max_chars: int,
                          grobid_url: str | None = None, timeout: int = 30) -> dict[str, Any]:
    if suffix.lower() == ".pdf" or payload[:5] == b"%PDF-":
        started = time.monotonic()
        result = extract_pdf_document_bounded(payload, max_chars=max_chars, grobid_url=grobid_url, timeout=timeout)
        if result["text"].strip():
            return result
        remaining = timeout - (time.monotonic() - started)
        fallback = _extract_pdf_with_pdftotext(payload, max_chars=max_chars + 1, timeout=min(30, remaining)) if remaining > 0 else ""
        if fallback.strip():
            return {**result, "text": fallback[:max_chars], "passages": None, "method": "pdftotext",
                    "extraction_status": "available", "truncated": len(fallback) > max_chars,
                    "document_structure": {"schema_version": 1, "source_sha256": hashlib.sha256(payload).hexdigest(),
                        "coordinate_system": "extracted_text", "pages": [], "elements": [],
                        "fallback_reason": result.get("extraction_error", "structured text unavailable"),
                        "coverage": {"page_count": None, "complete_text_extraction": False},
                        "semantic_verified": False}}
        return {**result, "method": "pdf-unavailable"}
    text, method = _extract_bytes(payload, suffix=suffix, max_chars=max_chars)
    return {"text": text, "method": method, "truncated": len(text) >= max_chars}


def _extract_bytes(payload: bytes, *, suffix: str, max_chars: int) -> tuple[str, str]:
    suffix = suffix.lower()
    if suffix == ".pdf" or payload[:5] == b"%PDF-":
        text = _extract_pdf_with_pdftotext(payload, max_chars=max_chars)
        if text:
            return text, "pdftotext"
        return "", "pdf-unavailable"
    raw = payload.decode("utf-8", errors="replace")
    if "<html" in raw[:2000].lower() or suffix in {".html", ".htm"}:
        return _html_to_text(raw)[:max_chars], "html"
    return raw[:max_chars], "text"


def _extract_pdf_with_pdftotext(payload: bytes, *, max_chars: int, timeout: float = 30) -> str:
    # Windows does not allow another process to open NamedTemporaryFile while
    # Python still holds its default exclusive handle.
    with tempfile.TemporaryDirectory(prefix='mycodex-pdf-') as tmpdir:
        pdf_path = Path(tmpdir) / 'source.pdf'
        pdf_path.write_bytes(payload)
        try:
            result = subprocess.run(
                ["pdftotext", "-layout", str(pdf_path), "-"],
                check=False,
                capture_output=True,
                timeout=timeout,
                text=True,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return ""
    if result.returncode != 0:
        return ""
    return result.stdout[:max_chars]


class _TextHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style", "noscript"}:
            self._skip += 1
        elif tag in {"p", "br", "div", "section", "article", "li", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript"} and self._skip:
            self._skip -= 1
        elif tag in {"p", "div", "section", "article", "li"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip and data.strip():
            self.parts.append(data)


def _html_to_text(html: str) -> str:
    parser = _TextHTMLParser()
    parser.feed(html)
    return _clean_ws("\n".join(parser.parts))


def _dedupe_results(results: list[ResearchSearchResult]) -> list[ResearchSearchResult]:
    seen: set[str] = set()
    deduped: list[ResearchSearchResult] = []
    for result in results:
        key = _source_key(result)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped


def _source_key(result: ResearchSearchResult) -> str:
    if result.doi:
        return "doi:" + result.doi.lower().removeprefix("https://doi.org/")
    title_key = _clean_ws(result.title).lower()
    if len(title_key) > 12:
        return "title:" + title_key
    if result.url:
        return "url:" + result.url.lower().rstrip("/")
    return "title:" + title_key


def _openalex_landing_url(item: dict[str, Any]) -> str:
    for path in (
        ("primary_location", "landing_page_url"),
        ("primary_location", "pdf_url"),
        ("doi",),
        ("id",),
    ):
        value: Any = item
        for part in path:
            value = value.get(part) if isinstance(value, dict) else None
        if value:
            return str(value)
    return ""


def _abstract_from_openalex(index: Any) -> str:
    if not isinstance(index, dict):
        return ""
    words: list[tuple[int, str]] = []
    for word, positions in index.items():
        if not isinstance(positions, list):
            continue
        for position in positions:
            pos = _int_or_none(position)
            if pos is not None:
                words.append((pos, str(word)))
    return " ".join(word for _pos, word in sorted(words))


def _compact_notes(text: str, *, max_chars: int = 1200) -> str:
    return _clean_ws(text)[:max_chars]


def _clean_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _xml_text(node: ET.Element | None) -> str:
    if node is None or node.text is None:
        return ""
    return node.text


def _year_from_text(text: str) -> int | None:
    match = re.search(r"\b(19|20)\d{2}\b", text or "")
    return int(match.group(0)) if match else None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _looks_like_url(value: str) -> bool:
    return urllib.parse.urlparse(value).scheme in {"http", "https"}


def _suffix_for_content(url: str, content_type: str) -> str:
    parsed_suffix = Path(urllib.parse.urlparse(url).path).suffix.lower()
    if parsed_suffix:
        return parsed_suffix
    if "pdf" in content_type.lower():
        return ".pdf"
    if "html" in content_type.lower():
        return ".html"
    return ".txt"


def _content_type_for_suffix(suffix: str) -> str:
    return {
        ".pdf": "application/pdf",
        ".html": "text/html",
        ".htm": "text/html",
        ".txt": "text/plain",
        ".md": "text/markdown",
    }.get(suffix.lower(), "application/octet-stream")


def _zotero_data_dir(value: str | Path | None = None) -> Path:
    raw = value or os.environ.get("ZOTERO_DATA_DIR") or "~/Zotero"
    return Path(raw).expanduser().resolve()


def _zotero_attachment_path(
    data_dir: Path,
    attachment_key: str | None,
    attachment_path: str | None,
) -> str:
    if not attachment_key or not attachment_path:
        return ""
    path = str(attachment_path)
    if path.startswith("storage:"):
        resolved = data_dir / "storage" / attachment_key / path.removeprefix("storage:")
    else:
        resolved = Path(path).expanduser()
        if not resolved.is_absolute():
            resolved = data_dir / resolved
    return str(resolved) if resolved.exists() else ""


def _split_multi(value: Any) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(";") if part.strip()]
