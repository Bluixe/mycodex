"""PDF text and physical locators, with explicit limits on inferred structure.

Extraction locates visible evidence. It does not read charts, reconstruct LaTeX,
verify equations, or establish scientific support. pdfplumber is optional.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
import statistics
import subprocess
import sys
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path
from typing import Any

_CAPTION = re.compile(r"^(Figure|Fig\.?|Table)\s*(\d+[A-Za-z]?)\s*[:.]", re.I)
_HEADING = re.compile(r"^(\d{1,2}(?:\.\d{1,2})*)\s+([A-Za-z].{2,110})$")
_EQUATION_NUMBER = re.compile(r"\((\d{1,3}[a-z]?)\)\s*$")
_BOLD = re.compile(r"bold|medi|demi|semibold", re.I)
_MATH_FONT = re.compile(r"cmmi|cmsy|symbol|math|stix", re.I)


def document_summary(result: dict[str, Any]) -> dict[str, Any] | None:
    structure = result.get("document_structure")
    if not isinstance(structure, dict):
        return None
    return {"source_sha256": structure.get("source_sha256"), "coordinate_system": structure.get("coordinate_system"),
            "coverage": structure.get("coverage", {}), "text_integrity": structure.get("text_integrity", {}),
            "element_counts": dict(Counter(item["kind"] for item in structure.get("elements", []))),
            "semantic_verified": False, "grobid_status": structure.get("grobid", {}).get("status", "not_configured"),
            "limits": structure.get("limits", [])}


def _load_pdfplumber():
    import pdfplumber
    return pdfplumber


def extract_pdf_document_bounded(payload: bytes, *, max_chars: int = 12000,
                                 grobid_url: str | None = None, timeout: float = 30) -> dict[str, Any]:
    """Isolate optional PDF parsing so a corrupt input cannot hang a worker turn."""
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("PDF extraction timeout must be positive finite seconds")
    options = {"creationflags": subprocess.CREATE_NO_WINDOW} if sys.platform == "win32" else {}
    try:
        command = [sys.executable, str(Path(__file__).resolve()), "--extract-json", str(max_chars)]
        if grobid_url:
            command.append(grobid_url)
        process = subprocess.run(command, input=payload, capture_output=True, timeout=timeout, check=False, **options)
        if process.returncode != 0:
            raise ValueError("PDF parser child failed")
        result = json.loads(process.stdout)
        if (not isinstance(result, dict) or result.get('content_hash') != hashlib.sha256(payload).hexdigest()
                or not isinstance(result.get('text'), str)):
            raise ValueError("PDF parser child returned an invalid source receipt")
        return result
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        return {"text": "", "passages": None, "method": "pdfplumber-unavailable",
                "content_hash": hashlib.sha256(payload).hexdigest(), "extraction_status": "unavailable",
                "truncated": True, "extraction_error": type(exc).__name__ + ": bounded PDF parser unavailable",
                "document_structure": {"schema_version": 1, "source_sha256": hashlib.sha256(payload).hexdigest(),
                    "pages": [], "elements": [], "coverage": {"pages_processed": 0, "page_count": None,
                    "complete_text_extraction": False}, "semantic_verified": False}}


def _bbox(items: list[dict[str, Any]]) -> list[float]:
    return [round(min(x["x0"] for x in items), 3), round(min(x["top"] for x in items), 3),
            round(max(x["x1"] for x in items), 3), round(max(x["bottom"] for x in items), 3)]


def _line_size(line: dict[str, Any]) -> float:
    sizes = [float(c.get("size", 0)) for c in line.get("chars", [])]
    return statistics.median(sizes) if sizes else 10


def _heading(line: dict[str, Any], body_size: float) -> str | None:
    text = line["text"].strip()
    bold = any(_BOLD.search(str(c.get("fontname", ""))) for c in line.get("chars", []))
    if _HEADING.match(text) and (bold or _line_size(line) >= body_size * 1.08):
        return text
    if text.casefold() in {"abstract", "references", "acknowledgments", "acknowledgements", "appendix"} and bold:
        return text
    return None


def extract_pdf_document(payload: bytes, *, max_chars: int = 12000, max_pages: int = 300,
                         grobid_url: str | None = None, timeout: int = 30) -> dict[str, Any]:
    if type(max_chars) is not int or not 1 <= max_chars <= 2_000_000:
        raise ValueError("max_chars must be an integer between 1 and 2000000")
    if type(max_pages) is not int or not 1 <= max_pages <= 1000:
        raise ValueError("max_pages must be an integer between 1 and 1000")
    if not payload.startswith(b"%PDF-"):
        raise ValueError("PDF extraction requires a PDF byte stream")
    if len(payload) > 50_000_000:
        raise ValueError("PDF exceeds the 50 MB extraction limit")
    digest = hashlib.sha256(payload).hexdigest()
    result: dict[str, Any] = {"text": "", "passages": [], "method": "pdfplumber",
        "content_hash": digest, "truncated": False, "extraction_status": "unavailable",
        "document_structure": {"schema_version": 1, "source_sha256": digest, "pages": [], "elements": [],
            "coordinate_system": "pdfplumber_page_points_top_left", "semantic_verified": False,
            "limits": ["Reading order and section boundaries are heuristic, especially in multiple columns.",
                       "Figure/table captions are text anchors; image contents and table values are not interpreted.",
                       "Only explicitly numbered display equations are candidates; no LaTeX or mathematical validation.",
                       "No OCR is performed; scanned or damaged glyph text may be unavailable."]}}
    structure = result["document_structure"]
    try:
        parser = _load_pdfplumber()
    except ImportError:
        result["method"] = "pdfplumber-unavailable"
        result["extraction_error"] = "optional documents dependency is not installed"
        return result
    structure["parser_version"] = parser.__version__
    text_parts: list[str] = []
    offset = 0
    current_section: str | None = None
    try:
        with parser.open(io.BytesIO(payload)) as document:
            total_pages = len(document.pages)
            structure["page_count"] = total_pages
            for page_no, page in enumerate(document.pages[:max_pages], 1):
                if offset >= max_chars:
                    result["truncated"] = True
                    break
                lines = page.extract_text_lines(layout=False, strip=True, return_chars=True)
                if len(lines) > 10000:
                    raise ValueError("PDF page exceeds the bounded line extraction limit")
                sizes = [round(float(c["size"]), 1) for c in page.chars if c.get("text", "").strip()]
                body_size = Counter(sizes).most_common(1)[0][0] if sizes else 10
                page_record = {"page": page_no, "page_number_basis": "physical_pdf_page_1_based",
                    "width": float(page.width), "height": float(page.height), "page_bbox": list(page.bbox),
                    "rotation": int(page.rotation), "char_start": offset, "char_end": offset,
                    "text_available": False, "complete": True}
                accepted = []
                for raw in lines:
                    if len(result["passages"]) >= 20000:
                        page_record["complete"] = False; result["truncated"] = True; break
                    remaining = max_chars - offset
                    if remaining <= 0:
                        page_record["complete"] = False; result["truncated"] = True; break
                    value = raw["text"]
                    if not value.strip():
                        continue
                    text = value[:remaining]
                    section = _heading(raw, body_size)
                    if section:
                        current_section = section
                    locator = {"page": page_no, "page_number_basis": "physical_pdf_page_1_based",
                        "page_bbox": list(page.bbox), "bbox": _bbox([raw]),
                        "coordinate_system": structure["coordinate_system"],
                        "char_start": offset, "char_end": offset + len(text), "section": current_section,
                        "section_basis": "observed_heading_heuristic" if current_section else None,
                        "source_sha256": digest, "kind": "text_line"}
                    line = {**raw, "text": text, "locator": locator, "heading": section}
                    accepted.append(line)
                    result["passages"].append({"text": text, "locator": locator})
                    text_parts.append(text); offset += len(text)
                    if len(text) < len(value):
                        page_record["complete"] = False; result["truncated"] = True
                        break
                    if offset < max_chars:
                        text_parts.append("\n"); offset += 1
                page_record.update(char_end=offset, text_available=bool(accepted))
                structure["pages"].append(page_record)
                _page_elements(page, accepted, structure, result["passages"], digest)
                if page_no < total_pages and offset < max_chars:
                    text_parts.append("\f"); offset += 1
                if result["truncated"]:
                    break
            if len(structure["pages"]) < total_pages:
                result["truncated"] = True
    except Exception as exc:
        result["extraction_error"] = f"{type(exc).__name__}: {str(exc)[:400]}"
        result["truncated"] = True
    result["text"] = "".join(text_parts)
    result["extraction_status"] = "available" if result["text"].strip() else "unavailable"
    structure["text_integrity"] = {"replacement_characters": result["text"].count("\ufffd"),
        "cid_placeholders": len(re.findall(r"\(cid:\d+\)", result["text"])),
        "reading_order": "heuristic", "character_fidelity_verified": False}
    structure["coverage"] = {"pages_processed": len(structure["pages"]),
        "pages_with_text": sum(bool(p["text_available"]) for p in structure["pages"]),
        "page_count": structure.get("page_count"), "truncated": result["truncated"],
        "all_pages_processed": len(structure["pages"]) == structure.get("page_count"),
        "complete_text_extraction": bool(structure["pages"]) and not result["truncated"]
            and all(p["text_available"] and p["complete"] for p in structure["pages"])
            and not structure["text_integrity"]["replacement_characters"]
            and not structure["text_integrity"]["cid_placeholders"]}
    structure["grobid"] = (fetch_grobid_structure(payload, grobid_url, timeout=timeout)
                            if grobid_url else {"status": "not_configured"})
    return result


def _page_elements(page, lines, structure, passages, digest):
    def add(kind, selected, *, label=None, bbox=None, **extra):
        first, last = selected[0], selected[-1]
        selected = [line for line in lines if first["locator"]["char_start"] <= line["locator"]["char_start"]
                    <= last["locator"]["char_start"]]
        text = "\n".join(line["text"] for line in selected)
        locator = {**first["locator"], "char_end": last["locator"]["char_end"],
                   "kind": kind, "bbox": bbox or _bbox(selected)}
        element = {"kind": kind, "label": label, "text": text, "locator": locator,
                   "semantic_verified": False, **extra}
        structure["elements"].append(element)
        passages.append({"text": text, "locator": locator})
        return element

    for index, line in enumerate(lines):
        if line["heading"]:
            add("section_heading", [line], label=line["heading"], detection="numbered_or_named_heading_heuristic")
        caption = _CAPTION.match(line["text"])
        if caption:
            selected = [line]
            for following in lines[index + 1:index + 5]:
                gap = following["top"] - selected[-1]["bottom"]
                if (gap > _line_size(line) * 0.9 or abs(following["x0"] - line["x0"]) > 12
                        or following["heading"] or _CAPTION.match(following["text"])):
                    break
                selected.append(following)
            kind = "table_caption" if caption.group(1).casefold() == "table" else "figure_caption"
            element = add(kind, selected, label=caption.group(0).rstrip(":. "), detection="explicit_caption_prefix")
            if kind == "table_caption":
                _table_region(page, lines, index, selected, element, add)
        marker = _EQUATION_NUMBER.search(line["text"])
        math_font = any(_MATH_FONT.search(str(c.get("fontname", ""))) for c in line.get("chars", []))
        if (marker and line["x1"] > page.width * 0.72 and len(line["text"]) < 180
                and re.search(r"=|∑|∏|softmax|log", line["text"])
                and (math_font or line["x0"] > page.width * 0.25)):
            selected = [candidate for candidate in lines[max(0, index - 2):index + 3]
                        if candidate["top"] >= line["top"] - 18 and candidate["bottom"] <= line["bottom"] + 18
                        and candidate["x0"] >= page.width * 0.25 and not candidate["heading"]]
            add("numbered_equation", selected or [line], label=marker.group(1),
                detection="right_aligned_number_and_display_math_heuristic", latex=None,
                mathematical_validity="unverified", text_quality="glyph_extraction_only")
    for image in page.images[:100]:
        structure["elements"].append({"kind": "embedded_image", "text": None, "semantic_verified": False,
            "locator": {"page": page.page_number, "page_number_basis": "physical_pdf_page_1_based",
                "bbox": _bbox([image]), "page_bbox": list(page.bbox),
                "coordinate_system": structure["coordinate_system"], "source_sha256": digest},
            "association_to_caption": "unverified"})


def _table_region(page, lines, index, caption_lines, caption, add):
    top = caption_lines[-1]["bottom"]
    boundary = min((line["top"] for line in lines[index + len(caption_lines):]
                    if line["heading"] or _CAPTION.match(line["text"])), default=page.bbox[3])
    rules = [edge for edge in page.lines if edge["x1"] - edge["x0"] >= page.width * 0.4
             and top <= edge["top"] < min(boundary, top + 360)
             and abs(edge["bottom"] - edge["top"]) < 1]
    if len(rules) < 2:
        caption["table_region_status"] = "not_detected"
        return
    # Horizontal rules establish a physical region, not row/column semantics.
    region = _bbox(rules)
    selected = [line for line in lines if region[1] <= (line["top"] + line["bottom"]) / 2 <= region[3]]
    if not selected:
        return
    element = add("table_region", selected, label=caption["label"], bbox=region,
                  detection="horizontal_rule_envelope_heuristic", cells=None, cell_values_verified=False)
    caption["table_region_status"] = "candidate"
    caption["table_region_bbox"] = element["locator"]["bbox"]


def fetch_grobid_structure(payload: bytes, endpoint: str, *, timeout: int = 30) -> dict[str, Any]:
    """Explicit opt-in only: send PDF bytes to a caller-configured GROBID service."""
    parsed = urllib.parse.urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query:
        raise ValueError("GROBID endpoint must be an HTTP(S) URL without credentials or query")
    url = endpoint.rstrip("/")
    if not url.endswith("processFulltextDocument"):
        url += "/api/processFulltextDocument"
    boundary = "mycodex-" + uuid.uuid4().hex
    parts = []
    for name, value in (("consolidateHeader", "0"), ("teiCoordinates", "head"),
                        ("teiCoordinates", "figure"), ("teiCoordinates", "formula"), ("teiCoordinates", "s")):
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
    parts.extend([f'--{boundary}\r\nContent-Disposition: form-data; name="input"; filename="source.pdf"\r\nContent-Type: application/pdf\r\n\r\n'.encode(),
                  payload, f"\r\n--{boundary}--\r\n".encode()])
    request = urllib.request.Request(url, data=b"".join(parts), headers={"Content-Type": "multipart/form-data; boundary=" + boundary})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            tei = response.read(10_000_001)
        if len(tei) > 10_000_000:
            raise ValueError("GROBID response exceeds 10 MB")
        root = ET.fromstring(tei)
        elements = []
        for node in root.iter():
            kind = node.tag.rsplit("}", 1)[-1]
            if kind not in {"head", "figure", "formula"}:
                continue
            coordinates = []
            for raw in (node.get("coords") or "").split(";"):
                if not raw:
                    continue
                values = [float(value) for value in raw.split(",")]
                if (len(values) != 5 or not all(math.isfinite(x) for x in values)
                        or values[0] < 1 or not values[0].is_integer() or min(values[3:]) < 0):
                    raise ValueError("invalid GROBID coordinates")
                page, x, y, width, height = values
                coordinates.append({"page": int(page), "bbox": [x, y, x + width, y + height]})
            elements.append({"kind": kind, "text": " ".join("".join(node.itertext()).split()),
                             "coordinates": coordinates, "semantic_verified": False})
        return {"status": "available", "source_sha256": hashlib.sha256(payload).hexdigest(),
                "tei_sha256": hashlib.sha256(tei).hexdigest(), "elements": elements,
                "coordinate_system": "grobid_pdf_points_top_left", "independently_verified": False}
    except Exception as exc:
        return {"status": "unavailable", "error_type": type(exc).__name__,
                "error": "GROBID request or TEI parsing failed; no structure inferred", "elements": []}


if __name__ == "__main__" and len(sys.argv) >= 3 and sys.argv[1] == "--extract-json":
    value = extract_pdf_document(sys.stdin.buffer.read(50_000_001), max_chars=int(sys.argv[2]),
                                 grobid_url=sys.argv[3] if len(sys.argv) > 3 else None)
    sys.stdout.buffer.write(json.dumps(value, ensure_ascii=False).encode("utf-8"))
