"""Bounded, read-only imports of registered Codex conversation rollouts.

The SQLite cache is disposable source material, never a store of inferred user
facts. Only ``role == 'user'`` can be cited as something the human said.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .db import Store

_CHUNK_BYTES = 64 * 1024
_MAX_LINE_BYTES = 1024 * 1024
_MAX_TEXT_CHARS = 32000
CONTEXT_SEPARATOR = "\n…\n"
_INJECTED_TAGS = (
    "environment_context", "INSTRUCTIONS", "app-context", "memory",
    "in-app-browser-context", "external_codex_apps_open_page",
    "codex_internal_context", "skills_instructions", "permissions",
    "collaboration_mode", "tools", "recommended_plugins", "ambient_context",
    "context", "image", "turn_aborted", "user_instructions", "memory_summary",
    "untrusted_text", "subagent_notification",
)
_INJECTED = re.compile(
    r"<(" + "|".join(map(re.escape, _INJECTED_TAGS)) + r")(?:\s[^>]*)?>[\s\S]*?</\1\s*>",
    re.IGNORECASE,
)
_UNCLOSED_INJECTED = re.compile(
    r"<(?:" + "|".join(map(re.escape, _INJECTED_TAGS)) + r")(?:\s[^>]*)?>[\s\S]*$",
    re.IGNORECASE,
)
_SECRETS = (
    (re.compile(r"\b(?:sk-(?:proj-)?|gh[pousr]_|github_pat_)[A-Za-z0-9_-]{12,}"), "[REDACTED_TOKEN]"),
    (re.compile(r"(?i)\b(?:api[_ -]?key|access[_ -]?token|password|passwd|secret|bot[_ -]?token)\s*[:=]\s*['\"]?[^\s'\"<>,;]{4,}"), "[REDACTED_CREDENTIAL]"),
    (re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]{12,}=*"), "Bearer [REDACTED_TOKEN]"),
    (re.compile(r"\b\d{7,12}:[A-Za-z0-9_-]{25,}\b"), "[REDACTED_BOT_TOKEN]"),
    (re.compile(r"(?i)(https?://)[^\s/@:]+:[^\s/@]+@"), r"\1[REDACTED_CREDENTIAL]@"),
    (re.compile(r"-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?(?:-----END [^-]*PRIVATE KEY-----|$)"), "[REDACTED_PRIVATE_KEY]"),
)


def _path_key(value: str | Path) -> str:
    text = str(value)
    if text.startswith("\\\\?\\UNC\\"):
        text = "\\\\" + text[8:]
    elif text.startswith("\\\\?\\"):
        text = text[4:]
    return os.path.normcase(str(Path(text).expanduser().resolve()))


def _timestamp(value: Any) -> str | None:
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            moment = datetime.fromtimestamp(value, timezone.utc)
        elif isinstance(value, str):
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if moment.tzinfo is None:
                return None
        else:
            return None
        return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except (ValueError, OverflowError, OSError):
        return None


def _clean_text(text: str, *, role: str) -> str:
    # Remove automatically supplied parts even when a genuine request follows.
    if role == "user":
        text = re.sub(r"(?im)^# AGENTS\.md instructions[^\n]*\n?", "", text)
        text = _INJECTED.sub("", text)
        text = _UNCLOSED_INJECTED.sub("", text)
        text = re.sub(r"(?i)</?(?:image|external_codex_apps_open_page)\b[^>]*>", "", text)
        def human_answers(match: re.Match[str]) -> str:
            try:
                replies = json.loads(match.group(1))
            except (ValueError, TypeError):
                return ""
            if not isinstance(replies, list):
                return ""
            def literal_answer(value: Any) -> str:
                if isinstance(value, str):
                    return value
                if isinstance(value, list):
                    return "\n".join(filter(None, (literal_answer(item) for item in value)))
                if isinstance(value, dict):
                    for field in ("label", "text", "value"):
                        if isinstance(value.get(field), str):
                            return value[field]
                return ""
            # The question is assistant-authored context, not a human assertion.
            return "\n".join(filter(None, (literal_answer(reply.get("answer")) for reply in replies
                                         if isinstance(reply, dict))))
        text = re.sub(r"<send_user_message_question_reply>\s*([\s\S]*?)\s*</send_user_message_question_reply>",
                      human_answers, text, flags=re.IGNORECASE)
        if text.lstrip().startswith(("## Memory", "# Memory", "========= MEMORY_SUMMARY BEGINS", "<user_instructions>")):
            return ""
    for pattern, replacement in _SECRETS:
        text = pattern.sub(replacement, text)
    return text.strip()


class ConversationArchive:
    def __init__(self, store: Store, *, source_roots: Iterable[str | Path] | None = None) -> None:
        self.store = store
        home = Path(os.environ.get("CODEX_HOME", "~/.codex")).expanduser()
        self.source_roots = tuple(Path(_path_key(p)) for p in (
            source_roots if source_roots is not None else (home / "sessions", home / "archived_sessions")
        ))
        store.conn.executescript("""
            CREATE TABLE IF NOT EXISTS conversation_sources (
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                thread_id TEXT NOT NULL REFERENCES sessions(thread_id) ON DELETE CASCADE,
                source_path TEXT NOT NULL, receipt_json TEXT NOT NULL,
                PRIMARY KEY(project_id, thread_id)
            );
            CREATE TABLE IF NOT EXISTS conversation_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                thread_id TEXT NOT NULL REFERENCES sessions(thread_id) ON DELETE CASCADE,
                message_key TEXT NOT NULL, turn_id TEXT, role TEXT NOT NULL CHECK(role IN ('user','assistant')),
                text TEXT NOT NULL, recorded_at TEXT, source_path TEXT NOT NULL,
                source_kind TEXT NOT NULL, content_hash TEXT NOT NULL,
                locator_json TEXT NOT NULL, truncated INTEGER NOT NULL DEFAULT 0,
                UNIQUE(project_id, thread_id, message_key)
            );
            CREATE INDEX IF NOT EXISTS idx_conversation_project_time
                ON conversation_messages(project_id, recorded_at DESC, id DESC);
        """)

    def _project(self, name: str) -> Any:
        project = self.store.get_project(name)
        if project is None:
            raise ValueError(f"Unknown registered project: {name}")
        return project

    def _source_path(self, session: Any, project: Any) -> Path:
        if not session["cwd"] or _path_key(session["cwd"]) != _path_key(project["path"]):
            raise ValueError("indexed_cwd_mismatch")
        raw = Path(session["path"] or "")
        if not raw.is_absolute() or ".." in raw.parts or raw.suffix.lower() != ".jsonl":
            raise ValueError("invalid_source_path")
        path = Path(_path_key(raw))
        if not any(path.is_relative_to(root) for root in self.source_roots):
            raise ValueError("source_outside_codex_sessions")
        return path

    def _scan(self, path: Path, session: Any, project: Any, budget: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        stats: dict[str, Any] = {"bytes_scanned": 0, "invalid_lines": 0, "oversized_lines": 0,
                                 "leading_fragment": False, "trailing_fragment": False,
                                 "cwd_mismatch_records": 0}
        records: list[dict[str, Any]] = []
        with path.open("rb") as source:
            before = os.fstat(source.fileno())
            size = before.st_size
            stats.update(source_bytes=size, mtime_ns=before.st_mtime_ns)
            # A verified identity header is mandatory, including for a tail scan.
            header = bytearray()
            while len(header) < min(budget, 256 * 1024):
                part = source.readline(min(_CHUNK_BYTES, min(budget, 256 * 1024) - len(header)))
                header.extend(part)
                if not part or part.endswith(b"\n"):
                    break
            stats["bytes_scanned"] += len(header)
            try:
                meta = json.loads(header)
                identity = meta["payload"]
                valid = (meta["type"] == "session_meta" and isinstance(identity, dict)
                         and bool(identity.get("id", identity.get("session_id")))
                         and identity.get("id", identity.get("session_id")) in {session["thread_id"], session["session_id"]}
                         and isinstance(identity.get("cwd"), str) and bool(identity["cwd"]))
            except (ValueError, KeyError, TypeError):
                valid = False
            if not valid:
                raise ValueError("unverified_rollout_identity")
            header_matches = _path_key(identity["cwd"]) == _path_key(project["path"])
            stats.update(source_header_cwd=identity["cwd"], current_cwd=project["path"],
                         migrated_context=not header_matches, verified_turn_contexts=0)
            header_end = len(header)
            stats["header_bytes_scanned"] = header_end
            remaining = budget - header_end
            start = max(header_end, size - remaining)
            boundary = b"\n"
            if start > header_end and remaining > 0:
                # Reserve one byte of the same budget to distinguish a partial
                # UTF-8/JSON line from an exact JSONL boundary.
                start += 1
                remaining -= 1
                source.seek(start - 1)
                boundary = source.read(1)
                stats["bytes_scanned"] += len(boundary)
            stats["scan_start_byte"] = start
            stats["leading_fragment"] = start > header_end
            stats["discarded_leading_fragment"] = start > header_end and boundary != b"\n"
            source.seek(start)
            buffer = bytearray()
            offset = start
            cursor = start
            discarding = stats["discarded_leading_fragment"]
            oversized = False
            turn_id: str | None = None
            # A tail can start inside an unrelated historical cwd. Wait for an
            # explicit turn_context rather than trusting injected user text.
            cwd_matches = header_matches and start == header_end
            recent: dict[tuple[str, str], dict[str, Any]] = {}

            def accept(line: bytes, byte_offset: int) -> None:
                nonlocal turn_id, cwd_matches
                if not line.strip():
                    return
                try:
                    obj = json.loads(line)
                except (ValueError, UnicodeError, RecursionError):
                    stats["invalid_lines"] += 1
                    return
                if not isinstance(obj, dict) or not isinstance(obj.get("payload"), dict):
                    return
                payload = obj["payload"]
                kind = obj.get("type")
                if kind == "turn_context" or (kind == "event_msg" and payload.get("type") == "task_started"):
                    turn_id = payload.get("turn_id") or None
                    if kind == "turn_context":
                        cwd_matches = False
                    if isinstance(payload.get("cwd"), str) and payload["cwd"]:
                        cwd_matches = _path_key(payload["cwd"]) == _path_key(project["path"])
                        if cwd_matches:
                            stats["verified_turn_contexts"] += 1
                role = None
                text = ""
                if kind == "response_item" and payload.get("type") == "message":
                    role = payload.get("role")
                    if role not in {"user", "assistant"} or payload.get("channel", payload.get("phase")) in {"analysis", "summary"}:
                        return
                    if payload.get("recipient") not in (None, "all"):
                        return
                    content = payload.get("content", [])
                    if isinstance(content, list):
                        # Clean each part: injected memory must not swallow a later human part.
                        text = "\n".join(_clean_text(item["text"], role=role) for item in content
                                         if isinstance(item, dict) and item.get("type") in {"input_text", "output_text", "text"}
                                         and isinstance(item.get("text"), str))
                elif kind == "event_msg" and payload.get("type") in {"user_message", "agent_message"}:
                    role = "user" if payload["type"] == "user_message" else "assistant"
                    if payload.get("channel", payload.get("phase")) in {"analysis", "summary"}:
                        return
                    text = payload.get("message", "")
                if role is None or not isinstance(text, str):
                    return
                if not cwd_matches:
                    stats["cwd_mismatch_records"] += 1
                    return
                text = _clean_text(text, role=role)
                if not text:
                    return
                metadata = payload.get("internal_chat_message_metadata_passthrough") or {}
                if not isinstance(metadata, dict):
                    metadata = {}
                message_turn = payload.get("turn_id") or metadata.get("turn_id") or turn_id
                if not isinstance(message_turn, str):
                    message_turn = None
                stamp = _timestamp(obj.get("timestamp")) or _timestamp(metadata.get("create_time"))
                digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
                record = {"turn_id": message_turn, "role": role, "text": text[:_MAX_TEXT_CHARS],
                          "recorded_at": stamp, "source_kind": kind, "content_hash": digest,
                          "locator": {"byte_offset": byte_offset}, "truncated": len(text) > _MAX_TEXT_CHARS}
                previous = recent.get((role, digest))
                # Cross-format duplicates only; repeated real user requests remain distinct.
                if previous and previous["source_kind"] != kind:
                    same_turn = message_turn is not None and message_turn == previous["turn_id"]
                    nearby = byte_offset - previous["locator"]["byte_offset"] < 64 * 1024
                    near_time = (stamp == previous["recorded_at"] or not stamp or not previous["recorded_at"])
                    if same_turn or (nearby and near_time and (not message_turn or not previous["turn_id"])):
                        if kind == "response_item":
                            previous.update(record)
                        return
                records.append(record)
                recent[(role, digest)] = record

            while remaining > 0:
                block = source.read(min(_CHUNK_BYTES, remaining))
                if not block:
                    break
                remaining -= len(block)
                stats["bytes_scanned"] += len(block)
                pieces = block.split(b"\n")
                for index, piece in enumerate(pieces):
                    ends_line = index < len(pieces) - 1
                    segment = piece + (b"\n" if ends_line else b"")
                    cursor += len(segment)
                    if not discarding:
                        buffer.extend(segment)
                        if len(buffer) > _MAX_LINE_BYTES:
                            buffer.clear()
                            discarding = True
                            oversized = True
                            stats["oversized_lines"] += 1
                    if ends_line:
                        if not discarding:
                            accept(bytes(buffer), offset)
                        offset = cursor
                        buffer.clear()
                        discarding = False
                        oversized = False
            if buffer:
                stats["trailing_fragment"] = True
                accept(bytes(buffer), offset)
            elif discarding or oversized:
                stats["trailing_fragment"] = True
            after = os.fstat(source.fileno())
            stats["changed_during_scan"] = (after.st_size, after.st_mtime_ns) != (size, before.st_mtime_ns)
            stats["full_scan"] = start == header_end
            stats["scan_end_byte"] = cursor
            stats["unread_source_bytes"] = max(0, size - stats["bytes_scanned"])
        return records, stats

    def sync(self, project_name: str, max_messages_per_session: int = 120,
             max_scan_bytes: int = 16 * 1024 * 1024) -> dict[str, Any]:
        if isinstance(max_messages_per_session, bool) or not isinstance(max_messages_per_session, int) or not 1 <= max_messages_per_session <= 10000:
            raise ValueError("max_messages_per_session must be between 1 and 10000")
        if isinstance(max_scan_bytes, bool) or not isinstance(max_scan_bytes, int) or not 1 <= max_scan_bytes <= 256 * 1024 * 1024:
            raise ValueError("max_scan_bytes must be between 1 and 256 MiB")
        project = self._project(project_name)
        for session in self.store.list_sessions(project["id"]):
            records: list[dict[str, Any]] = []
            receipt: dict[str, Any] = {"thread_id": session["thread_id"], "status": "unavailable", "complete": False}
            path_text = session["path"] or ""
            try:
                path = self._source_path(session, project)
                path_text = str(path)
                records, stats = self._scan(path, session, project, max_scan_bytes)
                receipt.update(stats)
                receipt["message_limit_truncated"] = len(records) > max_messages_per_session
                records = records[-max_messages_per_session:]
                known_times = [r["recorded_at"] for r in records if r["recorded_at"]]
                receipt.update(cached_messages=len(records),
                    oldest_recorded_at=min(known_times) if known_times else None,
                    latest_recorded_at=max(known_times) if known_times else None,
                    unknown_time_messages=sum(r["recorded_at"] is None for r in records),
                    truncated_messages=sum(r["truncated"] for r in records))
                receipt["complete"] = (stats["full_scan"] and not receipt["message_limit_truncated"]
                    and not any(stats[k] for k in ("invalid_lines", "oversized_lines", "trailing_fragment", "changed_during_scan", "cwd_mismatch_records"))
                    and not any(record["truncated"] for record in records))
                receipt["status"] = "complete" if receipt["complete"] else "partial"
            except (OSError, ValueError) as exc:
                receipt["error"] = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            receipt["synced_at"] = _timestamp(datetime.now(timezone.utc).timestamp())
            keys = []
            for record in records:
                key = hashlib.sha256(f"{record['role']}:{record['locator']['byte_offset']}:{record['content_hash']}".encode()).hexdigest()
                keys.append(key)
                self.store.conn.execute("""INSERT INTO conversation_messages
                    (project_id,thread_id,message_key,turn_id,role,text,recorded_at,source_path,source_kind,content_hash,locator_json,truncated)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(project_id,thread_id,message_key) DO UPDATE SET
                    turn_id=excluded.turn_id,text=excluded.text,recorded_at=excluded.recorded_at,source_path=excluded.source_path,
                    source_kind=excluded.source_kind,locator_json=excluded.locator_json,truncated=excluded.truncated""",
                    (project["id"],session["thread_id"],key,record["turn_id"],record["role"],record["text"],record["recorded_at"],path_text,
                     record["source_kind"],record["content_hash"],json.dumps(record["locator"]),int(record["truncated"])))
            # Avoid unbounded SQL placeholder lists for large explicit cache limits.
            keep = set(keys)
            old = self.store.conn.execute("SELECT id,message_key FROM conversation_messages WHERE project_id=? AND thread_id=?",
                                          (project["id"],session["thread_id"])).fetchall()
            self.store.conn.executemany("DELETE FROM conversation_messages WHERE id=?", [(r["id"],) for r in old if r["message_key"] not in keep])
            self.store.conn.execute("""INSERT INTO conversation_sources VALUES (?,?,?,?)
                ON CONFLICT(project_id,thread_id) DO UPDATE SET source_path=excluded.source_path,receipt_json=excluded.receipt_json""",
                (project["id"],session["thread_id"],path_text,json.dumps(receipt)))
            # Do not hold the SQLite writer across the next large source scan.
            self.store.conn.commit()
        self.store.conn.commit()
        return self.coverage(project_name)

    def _visible(self, row: Any) -> bool:
        return (_path_key(row["session_cwd"]) == _path_key(row["project_path"])
                and bool(row["session_path"]) and _path_key(row["session_path"]) == _path_key(row["source_path"]))

    @staticmethod
    def _message(row: Any) -> dict[str, Any]:
        return {key: row[key] for key in ("id", "project_name", "thread_id", "turn_id", "role", "text", "recorded_at", "source_path", "source_kind", "content_hash")} | {
            "locator": json.loads(row["locator_json"]), "truncated": bool(row["truncated"])}

    def _rows(self, project_name: str | None):
        return self.store.conn.execute("""SELECT m.*,p.name AS project_name,p.path AS project_path,
            s.cwd AS session_cwd,s.path AS session_path FROM conversation_messages m
            JOIN projects p ON p.id=m.project_id JOIN sessions s ON s.thread_id=m.thread_id AND s.project_id=m.project_id
            WHERE (? IS NULL OR p.name=?) ORDER BY m.recorded_at IS NULL,m.recorded_at DESC,m.id DESC""", (project_name,project_name))

    def messages(self, project_name: str | None = None, limit: int = 120,
                 roles: Iterable[str] | None = None) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 0 <= limit <= 10000:
            raise ValueError("limit must be between 0 and 10000")
        if project_name is not None:
            self._project(project_name)
        roles = tuple(roles) if roles is not None else ("user", "assistant")
        if any(role not in {"user", "assistant"} for role in roles):
            raise ValueError("roles must contain user or assistant")
        if not roles or not limit:
            return []
        result = []
        for row in self._rows(project_name):
            if row["role"] in roles and self._visible(row):
                result.append(self._message(row))
                if len(result) == limit:
                    break
        return result

    def get_message(self, message_id: int) -> dict[str, Any] | None:
        if isinstance(message_id, bool) or not isinstance(message_id, int):
            return None
        row = self.store.conn.execute("""SELECT m.*,p.name AS project_name,p.path AS project_path,
            s.cwd AS session_cwd,s.path AS session_path FROM conversation_messages m
            JOIN projects p ON p.id=m.project_id JOIN sessions s ON s.thread_id=m.thread_id AND s.project_id=m.project_id
            WHERE m.id=?""", (message_id,)).fetchone()
        return self._message(row) if row is not None and self._visible(row) else None

    def coverage(self, project_name: str) -> dict[str, Any]:
        project = self._project(project_name)
        sessions = self.store.list_sessions(project["id"])
        receipts = {r["thread_id"]: json.loads(r["receipt_json"]) for r in self.store.conn.execute(
            "SELECT * FROM conversation_sources WHERE project_id=?", (project["id"],))}
        sources = []
        for session in sessions:
            receipt = receipts.get(session["thread_id"], {"thread_id": session["thread_id"], "status": "not_synced", "complete": False}).copy()
            try:
                path = self._source_path(session, project)
                current = path.stat()
                if "source_bytes" in receipt and (current.st_size,current.st_mtime_ns) != (receipt["source_bytes"],receipt["mtime_ns"]):
                    receipt.update(status="stale",complete=False)
            except (OSError, ValueError):
                receipt.update(status="unavailable",complete=False)
            sources.append(receipt)
        messages = [row for row in self._rows(project_name) if self._visible(row)]
        times = [r["recorded_at"] for r in messages if r["recorded_at"]]
        return {"project_name": project_name, "indexed_sessions": len(sessions),
                "synced_sessions": sum(r["status"] not in {"not_synced", "unavailable"} for r in sources),
                "complete": bool(sources) and all(r["complete"] for r in sources),
                "complete_sessions": sum(bool(r["complete"]) for r in sources),
                "partial_sessions": sum(r["status"] in {"partial", "stale"} for r in sources),
                "cached_messages": len(messages), "user_messages": sum(r["role"] == "user" for r in messages),
                "assistant_messages": sum(r["role"] == "assistant" for r in messages),
                "oldest_recorded_at": min(times) if times else None, "latest_recorded_at": max(times) if times else None,
                "unknown_time_messages": sum(r["recorded_at"] is None for r in messages), "sessions": sources}

    def project_context(self, project_name: str, max_chars: int = 24000,
                        recent_days: int = 30) -> dict[str, Any]:
        if isinstance(max_chars, bool) or not isinstance(max_chars, int) or max_chars < 0:
            raise ValueError("max_chars must be a nonnegative integer")
        if isinstance(recent_days, bool) or not isinstance(recent_days, int) or recent_days < 0:
            raise ValueError("recent_days must be a nonnegative integer")
        cutoff = _timestamp((datetime.now(timezone.utc) - timedelta(days=recent_days)).timestamp())
        candidates = self.messages(project_name, limit=10000)
        selected = []
        used = 0
        omitted = 0
        truncated = 0
        per_message_cap = min(6000, max(1, max_chars // 2))
        for record in candidates:
            if record["recorded_at"] is None or record["recorded_at"] < cutoff:
                continue
            available = min(per_message_cap, max_chars - used)
            if available <= 0:
                omitted += 1
                continue
            item = dict(record)
            item["context_truncated"] = len(item["text"]) > available
            if item["context_truncated"]:
                original_text = item["text"]
                item["context_separator"] = CONTEXT_SEPARATOR
                if available <= len(CONTEXT_SEPARATOR) + 1:
                    item["context_spans"] = [{"start": 0, "end": available}]
                else:
                    content_budget = available - len(CONTEXT_SEPARATOR)
                    prefix = (content_budget + 1) // 2
                    tail = content_budget // 2
                    item["context_spans"] = [{"start": 0, "end": prefix},
                        {"start": len(original_text) - tail, "end": len(original_text)}]
                item["text"] = CONTEXT_SEPARATOR.join(original_text[span["start"]:span["end"]]
                                                      for span in item["context_spans"])
                truncated += 1
            selected.append(item)
            used += len(item["text"])
        coverage = self.coverage(project_name) | {"context_chars": used, "context_message_count": len(selected),
            "context_omitted_for_size": omitted, "context_recent_days": recent_days,
            "context_truncated_messages": truncated,
            "latest_selected_at": max((r["recorded_at"] for r in selected), default=None),
            "context_excluded_unknown_time": sum(r["recorded_at"] is None for r in candidates)}
        coverage["context_candidates_limited"] = len(candidates) < coverage["cached_messages"]
        return {"project_name": project_name, "messages": list(reversed(selected)), "coverage": coverage}
