#!/usr/bin/env python3
"""MindOS bridge — native Hermes SQLite session adapter.

Read-only adapter over the live Hermes session store (HERMES_HOME/state.db,
tables `sessions` and `messages`) that produces the exact same normalized
message list and secret-guard semantics as the JSONL fixture path in
`mindos_bridge.py`. The live store is opened with `mode=ro` and
`immutable=0` (WAL readers) — never mutated, never locked exclusively.

Normalization contract (mirrors autopilot._parse_session_file):
- only user/assistant conversational rows are cached; tool calls/results,
  system/session_meta rows, compacted and inactive rows are counted as
  skipped, never stored;
- assistant `content` is preferred over `reasoning_content` (thinking text
  is agent-internal, not conversation evidence);
- consecutive identical role/content pairs collapse (retry artifacts);
- timestamps are epoch floats in Hermes; they are emitted as UTC ISO 8601.

Provenance: every row carries source="hermes-sqlite", the Hermes profile
name, the originating session_id, and a stable content-addressed row id
(sha256 of source + db identity + hermes session id), so JSONL rows and
SQLite rows for the same conversation can never collide or double-index.
Idempotence: a sha256 over the full normalized message stream is stored as
the row file_hash — unchanged sessions are skipped on re-ingest exactly
like unchanged files. Secret handling: refuse / redact / allow flags are
enforced before any cache or export write, values never logged.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import autopilot as ap

ADAPTER_SOURCE = "hermes-sqlite"
SESSION_ROLES = ap.SESSION_ROLES  # ("user", "assistant")


class SqliteStoreRef:
    """Identity of one read-only Hermes state.db binding."""

    def __init__(self, db_path: Path):
        self.path = Path(db_path).expanduser().resolve()
        # Stable per-machine identity: inode is enough (same file re-opened),
        # but fall back to the resolved path when stat is unavailable.
        try:
            st = self.path.stat()
            identity = f"ino:{st.st_dev}:{st.st_ino}"
        except OSError:
            identity = f"path:{self.path}"
        self.identity = identity


def resolve_state_db(explicit: str = "") -> Path:
    """Hermes state.db path: explicit arg > HERMES_HOME > default ~/.hermes.

    Never hardcodes a write target; this is a read-only source location.
    """
    cand = (explicit or "").strip() or \
        __import__("os").environ.get("HERMES_HOME", "").strip()
    p = Path(cand).expanduser() / "state.db" if cand else Path.home() / ".hermes" / "state.db"
    return p


def _iso(ts: float) -> str:
    return (datetime.fromtimestamp(float(ts), timezone.utc)
            .replace(microsecond=0).isoformat())


def parse_sqlite_session(rows: list, meta: dict) -> dict:
    """Normalize one Hermes session's message rows to the shared shape.

    Same output contract as autopilot._parse_session_file: status, messages,
    tool_results_skipped, malformed_lines (= non-conversational unparseable
    rows), duplicates_collapsed, size_bytes, first_at, last_at.
    """
    out = {"status": "indexed", "messages": [], "tool_results_skipped": 0,
           "malformed_lines": 0, "duplicates_collapsed": 0,
           "size_bytes": int(meta.get("n_bytes") or 0)}
    prev_key = ""
    prev_hash = ""
    for r in rows:
        role = str(r["role"] or "").lower()
        if role not in SESSION_ROLES or int(r["active"] or 0) != 1 \
                or int(r["compacted"] or 0) == 1:
            out["tool_results_skipped"] += 1
            continue
        content = r["content"]
        if not isinstance(content, str) or not content.strip():
            # Assistant turns that carry only tool_calls/reasoning have no
            # conversational text; count honestly instead of storing None.
            out["tool_results_skipped"] += 1
            continue
        key = f"{role}\x00{content[:4096]}"
        if key == prev_key:
            out["duplicates_collapsed"] += 1
            continue
        prev_key = key
        ch = hashlib.sha256((role + "\x00" + content).encode()).hexdigest()
        at = _iso(r["timestamp"]) if r["timestamp"] is not None else ""
        out["messages"].append({"role": role, "content": content, "at": at})
    if not out["messages"]:
        out["status"] = "unsupported"
    stamps = sorted(m["at"] for m in out["messages"] if m["at"])
    out["first_at"] = stamps[0] if stamps else ""
    out["last_at"] = stamps[-1] if stamps else ""
    return out


def fetch_session(store: SqliteStoreRef, session_id: str,
                  max_messages: int) -> tuple[dict, dict]:
    """Read-only snapshot of one session from the live Hermes store."""
    uri = f"file:{store.path}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA query_only=ON")
        meta = con.execute(
            "SELECT id, source, profile_name, started_at, ended_at FROM sessions "
            "WHERE id=?", (session_id,)).fetchone()
        if meta is None:
            raise LookupError(f"session_id not found in {store.path}: {session_id!r}")
        rows = con.execute(
            "SELECT role, content, timestamp, active, compacted "
            "FROM messages WHERE session_id=? AND active=1 AND compacted=0 "
            "ORDER BY id ASC LIMIT ?",
            (session_id, max_messages)).fetchall()
    finally:
        con.close()
    parsed = parse_sqlite_session([dict(r) for r in rows], {"n_bytes": len(rows)})
    provenance = {
        "hermes_source": meta["source"],
        "profile_name": meta["profile_name"] or "",
        "started_at": _iso(meta["started_at"]) if meta["started_at"] else "",
        "ended_at": _iso(meta["ended_at"]) if meta["ended_at"] else "",
    }
    return parsed, provenance


def sqlite_row_id(store: SqliteStoreRef, session_id: str) -> str:
    """Stable cache-row id, namespaced away from the JSONL row-id space."""
    return hashlib.sha256(
        f"{ADAPTER_SOURCE}\x00{store.identity}\x00{session_id}".encode()).hexdigest()


def stream_hash(messages: list) -> str:
    """Content hash over the normalized message stream (idempotence key)."""
    h = hashlib.sha256()
    for m in messages:
        h.update(m["role"].encode())
        h.update(b"\x00")
        h.update(m["content"].encode())
        h.update(b"\x01")
    return h.hexdigest()


def ingest_session(args) -> dict:
    """Ingest one live Hermes session into the MindOS cache.

    Honors the same guard ladder as the JSONL path: refuse by default,
    --redact stores [REDACTED:<kind>] copies, --allow-secret audited override.
    Re-ingesting an unchanged stream is a no-op (idempotent).
    """
    store = SqliteStoreRef(Path(resolve_state_db(getattr(args, "state_db", ""))))
    if not store.path.is_file():
        raise SystemExit(f"--state-db not found: {store.path}")
    session_id = (getattr(args, "sqlite_session_id", "") or "").strip()
    if not session_id:
        raise SystemExit("--sqlite-session-id is required")
    max_messages = max(1, int(getattr(args, "max_messages", 5000)))

    parsed, prov = fetch_session(store, session_id, max_messages)
    result = {
        "adapter": ADAPTER_SOURCE, "session_id": session_id,
        "state_db": str(store.path), "status": parsed["status"],
        "messages": len(parsed["messages"]),
        "tool_results_skipped": parsed["tool_results_skipped"],
        "duplicates_collapsed": parsed["duplicates_collapsed"],
    }
    if parsed["status"] != "indexed":
        return result  # nothing conversational; honest no-op

    msgs = parsed["messages"]
    kinds = sorted({f["kind"] for m in msgs for f in ap._secret_findings(m["content"])})
    redact = bool(getattr(args, "redact", False))
    allow = bool(getattr(args, "allow_secret", False))
    result["secret_kinds"] = kinds
    if kinds and not (redact or allow):
        raise SystemExit(
            f"refusing to ingest credential-shaped session content ({', '.join(kinds)}); "
            "re-run with --redact or --allow-secret")
    if redact:
        msgs = [{**m, "content": ap._redact_secrets(m["content"])} for m in msgs]

    row_id = sqlite_row_id(store, session_id)
    fh = stream_hash(msgs)
    t = ap.now()
    applied = False
    with ap.conn() as db:
        from mindos_bridge import _ensure_bridge, _message_key
        _ensure_bridge(db)
        row = db.execute("SELECT file_hash FROM sessions WHERE id=?", (row_id,)).fetchone()
        if row and row["file_hash"] == fh:
            result.update(unchanged=True, row_id=row_id)
            return result
        old = {r["seq"]: r["content_hash"] for r in db.execute(
            "SELECT seq,content_hash FROM bridge_hindsight_ledger WHERE session_row=?",
            (row_id,)).fetchall()}
        db.execute("DELETE FROM session_messages WHERE session_row=?", (row_id,))
        db.execute(
            "INSERT INTO sessions(id,source,profile,project,session_id,path,file_hash,"
            "size_bytes,message_count,tool_results_skipped,first_at,last_at,ingested_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET source=excluded.source,profile=excluded.profile,"
            "project=excluded.project,session_id=excluded.session_id,path=excluded.path,"
            "file_hash=excluded.file_hash,size_bytes=excluded.size_bytes,"
            "message_count=excluded.message_count,"
            "tool_results_skipped=excluded.tool_results_skipped,"
            "first_at=excluded.first_at,last_at=excluded.last_at,"
            "ingested_at=excluded.ingested_at",
            (row_id, ADAPTER_SOURCE,
             getattr(args, "profile", "") or prov["profile_name"],
             getattr(args, "project", ""), session_id,
             f"sqlite:{store.path}", fh, len(msgs), len(msgs),
             parsed["tool_results_skipped"], parsed["first_at"], parsed["last_at"], t))
        bank = getattr(args, "bank", "") or ""
        for seq, m in enumerate(msgs):
            ch = hashlib.sha256((m["role"] + "\x00" + m["content"]).encode()).hexdigest()
            db.execute(
                "INSERT INTO session_messages(session_row,seq,role,content,content_hash,at) "
                "VALUES(?,?,?,?,?,?)",
                (row_id, seq, m["role"], m["content"], ch, m["at"]))
            if old.get(seq) != ch:
                db.execute(
                    "INSERT OR REPLACE INTO bridge_hindsight_ledger(message_key,"
                    "session_row,seq,role,at,content_hash,bank,state,attempts,"
                    "last_error_kind,exported_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,'pending',0,'','','')",
                    (_message_key(row_id, seq), row_id, seq, m["role"], m["at"],
                     ch, bank))
        ap.audit(db, "session", row_id, "session_ingested_sqlite",
                 {"adapter": ADAPTER_SOURCE, "session_id": session_id,
                  "state_db_identity": store.identity, "messages": len(msgs),
                  "tool_results_skipped": parsed["tool_results_skipped"],
                  "duplicates_collapsed": parsed["duplicates_collapsed"],
                  "redacted": redact, "secret_kinds": kinds,
                  "hermes_source": prov["hermes_source"],
                  "stream_sha256": fh})
        applied = True
    result.update(unchanged=False, row_id=row_id, applied=applied,
                  first_at=parsed["first_at"], last_at=parsed["last_at"])
    return result
