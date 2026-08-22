#!/usr/bin/env python3
"""IdeatorX Autopilot Control Plane v1.

Durable task registry, leases, heartbeats, receipts, and a compact dashboard.
This tool intentionally does not deploy, merge, send messages, or execute work.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import uuid
import hashlib
import tempfile
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(os.environ.get("HERMES_AUTOPILOT_HOME", Path.home() / ".hermes" / "autopilot"))
DB = ROOT / "state.db"
RECEIPTS = ROOT / "receipts"
POLICIES = ROOT / "policies"
SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  project TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  owner TEXT NOT NULL DEFAULT 'hermes',
  status TEXT NOT NULL DEFAULT 'queued',
  priority TEXT NOT NULL DEFAULT 'P2',
  next_action TEXT NOT NULL DEFAULT '',
  blocked_reason TEXT NOT NULL DEFAULT '',
  worktree TEXT NOT NULL DEFAULT '',
  branch TEXT NOT NULL DEFAULT '',
  pr_url TEXT NOT NULL DEFAULT '',
   lease_owner TEXT NOT NULL DEFAULT '',
   lease_expires_at TEXT NOT NULL DEFAULT '',
   lease_epoch INTEGER NOT NULL DEFAULT 0,
   retry_count INTEGER NOT NULL DEFAULT 0,
   recover_after TEXT NOT NULL DEFAULT '',
   due_at TEXT NOT NULL DEFAULT '',
   not_before TEXT NOT NULL DEFAULT '',
   tags TEXT NOT NULL DEFAULT '[]',
   requires_receipts TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_receipt TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project);
CREATE TABLE IF NOT EXISTS heartbeats (
  task_id TEXT PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
  owner TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'alive',
  at TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS receipts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  file_hash TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  action TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  prev_hash TEXT NOT NULL DEFAULT '',
  hash TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_events(entity_type, entity_id, created_at);
CREATE TABLE IF NOT EXISTS task_deps (
  task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  depends_on TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  PRIMARY KEY (task_id, depends_on)
);
CREATE INDEX IF NOT EXISTS idx_task_deps_dep ON task_deps(depends_on);
CREATE TABLE IF NOT EXISTS notes (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  kind TEXT NOT NULL DEFAULT 'fact',
  content TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT '',
  content_hash TEXT NOT NULL,
   created_at TEXT NOT NULL,
   superseded_by TEXT NOT NULL DEFAULT '',
   pinned INTEGER NOT NULL DEFAULT 0,
   expires_at TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_notes_task ON notes(task_id);
CREATE INDEX IF NOT EXISTS idx_notes_hash ON notes(task_id, content_hash);
CREATE TABLE IF NOT EXISTS handoffs (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  from_agent TEXT NOT NULL DEFAULT '',
  to_agent TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  objective TEXT NOT NULL DEFAULT '',
  evidence TEXT NOT NULL DEFAULT '[]',
  constraints TEXT NOT NULL DEFAULT '[]',
  decisions TEXT NOT NULL DEFAULT '[]',
  files TEXT NOT NULL DEFAULT '[]',
  commit_ref TEXT NOT NULL DEFAULT '',
  next_actions TEXT NOT NULL DEFAULT '[]',
  risks TEXT NOT NULL DEFAULT '[]',
  content_hash TEXT NOT NULL,
  recall_digest TEXT NOT NULL DEFAULT '',
  acked_by TEXT NOT NULL DEFAULT '',
  acked_at TEXT NOT NULL DEFAULT '',
  ack_recall_digest TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  superseded_by TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_handoffs_task ON handoffs(task_id);
-- Session-ingestion cache (read-only adapter over external agent session
-- stores). Derived data only: the source stores are never mutated and raw
-- conversations are never execution truth; rows are rebuildable from source.
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL DEFAULT '',
  profile TEXT NOT NULL DEFAULT '',
  project TEXT NOT NULL DEFAULT '',
  session_id TEXT NOT NULL,
  path TEXT NOT NULL,
  file_hash TEXT NOT NULL DEFAULT '',
  size_bytes INTEGER NOT NULL DEFAULT 0,
  message_count INTEGER NOT NULL DEFAULT 0,
  tool_results_skipped INTEGER NOT NULL DEFAULT 0,
  first_at TEXT NOT NULL DEFAULT '',
  last_at TEXT NOT NULL DEFAULT '',
  ingested_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_project ON sessions(project);
CREATE TABLE IF NOT EXISTS session_messages (
  session_row TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  seq INTEGER NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  at TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (session_row, seq)
);
-- Temporal fact graph (the sidecar): fleet-level evolving facts and
-- relationships with validity windows. Subject/predicate/object are
-- restricted tokens (the tag charset), so credential-shaped values cannot
-- enter by construction. task_id is a deliberate soft reference: facts are
-- fleet knowledge and must survive task archival (archive detaches the ref);
-- doctor flags any dangling ref as evidence of out-of-band surgery.
CREATE TABLE IF NOT EXISTS facts (
  id TEXT PRIMARY KEY,
  subject TEXT NOT NULL,
  predicate TEXT NOT NULL,
  object TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT '',
  task_id TEXT NOT NULL DEFAULT '',
  valid_from TEXT NOT NULL,
  valid_until TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_facts_subject ON facts(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_facts_object ON facts(object);
"""
# Full-text retrieval (FTS5, stdlib sqlite3): external-content indexes over
# notes/tasks kept in sync by triggers. Applied only when the SQLite build
# supports FTS5; every consumer must fall back to LIKE search otherwise.
FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
  content, kind, source, content='notes', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS notes_fts_ai AFTER INSERT ON notes BEGIN
  INSERT INTO notes_fts(rowid,content,kind,source) VALUES(new.rowid,new.content,new.kind,new.source);
END;
CREATE TRIGGER IF NOT EXISTS notes_fts_ad AFTER DELETE ON notes BEGIN
  INSERT INTO notes_fts(notes_fts,rowid,content,kind,source) VALUES('delete',old.rowid,old.content,old.kind,old.source);
END;
CREATE VIRTUAL TABLE IF NOT EXISTS tasks_fts USING fts5(
  title, description, next_action, blocked_reason, project,
  content='tasks', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS tasks_fts_ai AFTER INSERT ON tasks BEGIN
  INSERT INTO tasks_fts(rowid,title,description,next_action,blocked_reason,project)
  VALUES(new.rowid,new.title,new.description,new.next_action,new.blocked_reason,new.project);
END;
CREATE TRIGGER IF NOT EXISTS tasks_fts_au AFTER UPDATE ON tasks BEGIN
  INSERT INTO tasks_fts(tasks_fts,rowid,title,description,next_action,blocked_reason,project)
  VALUES('delete',old.rowid,old.title,old.description,old.next_action,old.blocked_reason,old.project);
  INSERT INTO tasks_fts(rowid,title,description,next_action,blocked_reason,project)
  VALUES(new.rowid,new.title,new.description,new.next_action,new.blocked_reason,new.project);
END;
CREATE TRIGGER IF NOT EXISTS tasks_fts_ad AFTER DELETE ON tasks BEGIN
  INSERT INTO tasks_fts(tasks_fts,rowid,title,description,next_action,blocked_reason,project)
  VALUES('delete',old.rowid,old.title,old.description,old.next_action,old.blocked_reason,old.project);
END;
-- Handoff indexed columns (objective/status/from_agent/to_agent) are immutable
-- after insert, so no UPDATE trigger is needed; supersede/ack state lives in
-- non-indexed columns. INSERT/DELETE triggers keep archive/restore in sync.
CREATE VIRTUAL TABLE IF NOT EXISTS handoffs_fts USING fts5(
  objective, status, from_agent, to_agent,
  content='handoffs', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS handoffs_fts_ai AFTER INSERT ON handoffs BEGIN
  INSERT INTO handoffs_fts(rowid,objective,status,from_agent,to_agent)
  VALUES(new.rowid,new.objective,new.status,new.from_agent,new.to_agent);
END;
CREATE TRIGGER IF NOT EXISTS handoffs_fts_ad AFTER DELETE ON handoffs BEGIN
  INSERT INTO handoffs_fts(handoffs_fts,rowid,objective,status,from_agent,to_agent)
  VALUES('delete',old.rowid,old.objective,old.status,old.from_agent,old.to_agent);
END;
-- Ingested session messages are immutable once written (a changed source file
-- is re-indexed as delete+insert), so no UPDATE trigger is needed.
CREATE VIRTUAL TABLE IF NOT EXISTS session_messages_fts USING fts5(
  content, role, content='session_messages', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS session_messages_fts_ai AFTER INSERT ON session_messages BEGIN
  INSERT INTO session_messages_fts(rowid,content,role) VALUES(new.rowid,new.content,new.role);
END;
CREATE TRIGGER IF NOT EXISTS session_messages_fts_ad AFTER DELETE ON session_messages BEGIN
  INSERT INTO session_messages_fts(session_messages_fts,rowid,content,role) VALUES('delete',old.rowid,old.content,old.role);
END;
-- Fact tokens are immutable once written (a changed world asserts a new fact
-- and retracts the old one), so no UPDATE trigger is needed.
CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5(
  subject, predicate, object, source, content='facts', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS facts_fts_ai AFTER INSERT ON facts BEGIN
  INSERT INTO facts_fts(rowid,subject,predicate,object,source) VALUES(new.rowid,new.subject,new.predicate,new.object,new.source);
END;
CREATE TRIGGER IF NOT EXISTS facts_fts_ad AFTER DELETE ON facts BEGIN
  INSERT INTO facts_fts(facts_fts,rowid,subject,predicate,object,source) VALUES('delete',old.rowid,old.subject,old.predicate,old.object,old.source);
END;
"""
STATUSES = {"queued", "claimed", "running", "waiting_for_agent", "waiting_for_user", "waiting_for_review", "ready_to_merge", "ready_to_deploy", "blocked", "completed", "failed", "cancelled"}
TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
PRIORITIES = {"P0", "P1", "P2", "P3"}
NOTE_KINDS = {"fact", "decision", "observation", "evidence", "constraint"}
TAG_RE = re.compile(r"^[a-z0-9][a-z0-9:_./-]{0,63}$")

def _valid_tag(tag: str) -> str:
    """Validate a task tag: a short lowercase capability/scope token.

    The restricted charset keeps tags safe inside the JSON-array LIKE filter
    (`%"tag"%`) and stable as CLI flags across every agent adapter.
    """
    t = (tag or "").strip()
    if not TAG_RE.fullmatch(t):
        raise SystemExit(f"invalid tag: {tag!r} (lowercase [a-z0-9] then [a-z0-9:_./-], max 64 chars)")
    return t

def _task_tags(row) -> list:
    try:
        v = json.loads(row["tags"] or "[]")
    except (json.JSONDecodeError, TypeError):
        return []
    return v if isinstance(v, list) else []

def _task_view(row) -> dict:
    """Serialize a task row for output, exposing tags/receipt requirements as JSON arrays."""
    d = dict(row)
    d["tags"] = _task_tags(row)
    d["requires_receipts"] = _task_requires(row)
    return d

def _valid_receipt_kind(kind: str) -> str:
    """Validate a required-receipt kind: the same token charset as tags.

    Receipt kinds are free-form elsewhere, but a *requirement* becomes CLI
    flags, dispatch data, and sweep output across every agent adapter, so it
    gets the restricted stable form. Normalized to lowercase.
    """
    k = (kind or "").strip().lower()
    if not TAG_RE.fullmatch(k):
        raise SystemExit(f"invalid receipt kind: {kind!r} (lowercase [a-z0-9] then [a-z0-9:_./-], max 64 chars)")
    return k

def _task_requires(row) -> list:
    try:
        v = json.loads(row["requires_receipts"] or "[]")
    except (json.JSONDecodeError, TypeError, KeyError):
        return []
    return v if isinstance(v, list) else []

def _missing_required_evidence(db, task_id: str, required: list) -> list:
    """Required receipt kinds with no receipt of that kind on this task yet."""
    if not required:
        return []
    have = {r["kind"] for r in db.execute(
        "SELECT DISTINCT kind FROM receipts WHERE task_id=?", (task_id,))}
    return [k for k in required if k not in have]

def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _normalize_iso(value: str, flag: str) -> str:
    """Normalize a user-supplied timestamp to UTC ISO 8601; '' stays ''."""
    v = (value or "").strip()
    if not v:
        return ""
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        raise SystemExit(f"invalid {flag} timestamp: {value!r} (use ISO 8601, e.g. 2026-08-21T17:00:00+00:00)")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def _normalize_due(value: str) -> str:
    """Normalize a user-supplied deadline to UTC ISO 8601; '' clears the deadline."""
    return _normalize_iso(value, "due-at")

def ensure() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    RECEIPTS.mkdir(exist_ok=True)
    POLICIES.mkdir(exist_ok=True)
    with sqlite3.connect(DB) as db:
        db.executescript(SCHEMA)
        _migrate(db)
        _ensure_fts(db)

def _fts_ready(db) -> bool:
    """True when both FTS5 indexes exist (graceful-unavailable path otherwise)."""
    names = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('notes_fts','tasks_fts')")}
    return {"notes_fts", "tasks_fts"} <= names

def _handoffs_fts_ready(db) -> bool:
    """True when the handoffs FTS index exists (independent degradation gate)."""
    names = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='handoffs_fts'")}
    return "handoffs_fts" in names

def _sessions_fts_ready(db) -> bool:
    """True when the ingested-session-message FTS index exists."""
    names = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='session_messages_fts'")}
    return "session_messages_fts" in names

def _facts_fts_ready(db) -> bool:
    """True when the temporal-fact FTS index exists."""
    names = {r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='facts_fts'")}
    return "facts_fts" in names

def _ensure_fts(db) -> None:
    """Create FTS indexes when supported; rebuild once on first creation so
    pre-existing rows become searchable. Silently skips on non-FTS5 builds."""
    try:
        complete = (_fts_ready(db) and _handoffs_fts_ready(db)
                    and _sessions_fts_ready(db) and _facts_fts_ready(db))
        db.executescript(FTS_SCHEMA)
        if not complete:
            db.execute("INSERT INTO notes_fts(notes_fts) VALUES('rebuild')")
            db.execute("INSERT INTO tasks_fts(tasks_fts) VALUES('rebuild')")
            db.execute("INSERT INTO handoffs_fts(handoffs_fts) VALUES('rebuild')")
            db.execute("INSERT INTO session_messages_fts(session_messages_fts) VALUES('rebuild')")
            db.execute("INSERT INTO facts_fts(facts_fts) VALUES('rebuild')")
    except sqlite3.OperationalError:
        pass  # FTS5 unavailable: search falls back to LIKE

def _fts_query(text: str) -> str:
    """Convert free text into a safe FTS5 query of quoted tokens ('' if none)."""
    toks = []
    for raw in text.split():
        tok = raw.replace('"', "")
        if tok and any(c.isalnum() for c in tok):
            toks.append('"%s"' % tok)
    return " ".join(toks)

def _migrate(db) -> None:
    """Add hash-chain columns to pre-existing audit_events tables and backfill."""
    cols = {r[1] for r in db.execute("PRAGMA table_info(audit_events)")}
    if "prev_hash" not in cols:
        db.execute("ALTER TABLE audit_events ADD COLUMN prev_hash TEXT NOT NULL DEFAULT ''")
    if "hash" not in cols:
        db.execute("ALTER TABLE audit_events ADD COLUMN hash TEXT NOT NULL DEFAULT ''")
    note_cols = {r[1] for r in db.execute("PRAGMA table_info(notes)")}
    if "pinned" not in note_cols:
        db.execute("ALTER TABLE notes ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0")
    if "expires_at" not in note_cols:
        db.execute("ALTER TABLE notes ADD COLUMN expires_at TEXT NOT NULL DEFAULT ''")
    task_cols = {r[1] for r in db.execute("PRAGMA table_info(tasks)")}
    if "lease_epoch" not in task_cols:
        db.execute("ALTER TABLE tasks ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0")
    if "due_at" not in task_cols:
        db.execute("ALTER TABLE tasks ADD COLUMN due_at TEXT NOT NULL DEFAULT ''")
    if "recover_after" not in task_cols:
        db.execute("ALTER TABLE tasks ADD COLUMN recover_after TEXT NOT NULL DEFAULT ''")
    if "not_before" not in task_cols:
        db.execute("ALTER TABLE tasks ADD COLUMN not_before TEXT NOT NULL DEFAULT ''")
    if "tags" not in task_cols:
        db.execute("ALTER TABLE tasks ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")
    if "requires_receipts" not in task_cols:
        db.execute("ALTER TABLE tasks ADD COLUMN requires_receipts TEXT NOT NULL DEFAULT '[]'")
    handoff_cols = {r[1] for r in db.execute("PRAGMA table_info(handoffs)")}
    if "recall_digest" not in handoff_cols:
        db.execute("ALTER TABLE handoffs ADD COLUMN recall_digest TEXT NOT NULL DEFAULT ''")
    for col in ("acked_by", "acked_at", "ack_recall_digest"):
        if col not in handoff_cols:
            db.execute(f"ALTER TABLE handoffs ADD COLUMN {col} TEXT NOT NULL DEFAULT ''")
    receipt_cols = {r[1] for r in db.execute("PRAGMA table_info(receipts)")}
    if "file_hash" not in receipt_cols:
        db.execute("ALTER TABLE receipts ADD COLUMN file_hash TEXT NOT NULL DEFAULT ''")
    prev = ""
    for row in db.execute("SELECT id,prev_hash,hash FROM audit_events ORDER BY id").fetchall():
        if row[2]:
            prev = row[2]
            continue
        full = db.execute("SELECT entity_type,entity_id,action,payload_json,created_at FROM audit_events WHERE id=?", (row[0],)).fetchone()
        h = _chain_hash(prev, *full)
        db.execute("UPDATE audit_events SET prev_hash=?,hash=? WHERE id=?", (prev, h, row[0]))
        prev = h

def _chain_hash(prev: str, entity_type: str, entity_id: str, action: str, payload_json: str, created_at: str) -> str:
    material = "|".join((prev, entity_type, entity_id, action, payload_json, created_at))
    return hashlib.sha256(material.encode()).hexdigest()

def audit_chain_problems(db) -> list:
    """Recompute the audit hash chain; returns a list of break descriptions."""
    problems = []
    prev = ""
    for r in db.execute(
        "SELECT id,entity_type,entity_id,action,payload_json,created_at,prev_hash,hash FROM audit_events ORDER BY id"
    ).fetchall():
        expected = _chain_hash(prev, r[1], r[2], r[3], r[4], r[5])
        if r[6] != prev:
            problems.append({"event_id": r[0], "kind": "broken_link", "expected_prev": prev, "actual_prev": r[6]})
        if r[7] != expected:
            problems.append({"event_id": r[0], "kind": "hash_mismatch", "expected": expected, "actual": r[7]})
        prev = r[7]
    return problems

CHECKPOINT_FORMAT = "autopilot-checkpoint-v1"

def _load_checkpoint(path: str) -> dict:
    """Read and integrity-check a checkpoint file; returns its body."""
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"checkpoint not found: {path}")
    try:
        doc = json.loads(p.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f"checkpoint is not valid JSON: {e}")
    body = doc.get("checkpoint")
    if not body or body.get("format") != CHECKPOINT_FORMAT:
        raise SystemExit("unrecognized checkpoint format")
    recomputed = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    if recomputed != doc.get("sha256"):
        raise SystemExit("checkpoint integrity check failed; refusing to use a tampered file")
    return body

def checkpoint_problems(db, cp: dict) -> list:
    """Compare a sealed chain checkpoint against the live audit ledger.

    A hash chain alone cannot detect tail truncation (deleting the newest
    events leaves every remaining link valid). A checkpoint pins the head at a
    point in time; divergence from it proves events were removed or rewritten.
    Growth past the checkpoint is normal and never flagged.
    """
    problems = []
    ev = db.execute("SELECT id,hash FROM audit_events WHERE id=?", (cp["last_event_id"],)).fetchone()
    if ev is None:
        problems.append({"kind": "chain_truncated", "missing_event_id": cp["last_event_id"]})
    elif ev["hash"] != cp["last_event_hash"]:
        problems.append({"kind": "checkpoint_head_mismatch", "event_id": cp["last_event_id"],
                         "expected": cp["last_event_hash"], "actual": ev["hash"]})
    total = db.execute("SELECT COUNT(*) n FROM audit_events").fetchone()["n"]
    if total < cp["total_events"]:
        problems.append({"kind": "events_removed_since_checkpoint",
                         "checkpointed_total": cp["total_events"], "current_total": total})
    return problems

def conn() -> sqlite3.Connection:
    ensure()
    c = sqlite3.connect(DB, timeout=10)
    c.row_factory = sqlite3.Row
    # Concurrent dispatchers/agents hit the same database; wait on locks instead
    # of failing with "database is locked", and actually enforce FK cascades.
    c.execute("PRAGMA busy_timeout=10000")
    c.execute("PRAGMA foreign_keys=ON")
    return c

def default_max_active() -> int:
    """Per-owner live-lease cap (0 = unlimited), overridable via env."""
    try:
        return max(0, int(os.environ.get("AUTOPILOT_MAX_ACTIVE_PER_OWNER", "0")))
    except ValueError:
        return 0

def resolve_max_active(args) -> int:
    """CLI --max-active wins; otherwise the env/default cap applies."""
    value = getattr(args, "max_active", None)
    return default_max_active() if value is None else max(0, value)

def json_out(value) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))

def audit(db, entity_type: str, entity_id: str, action: str, payload=None) -> None:
    """Append an immutable, hash-chained local audit event; never include credentials in payloads."""
    payload_json = json.dumps(payload or {}, sort_keys=True)
    created = now()
    prev = db.execute("SELECT hash FROM audit_events ORDER BY id DESC LIMIT 1").fetchone()
    prev_hash = prev[0] if prev else ""
    h = _chain_hash(prev_hash, entity_type, entity_id, action, payload_json, created)
    db.execute("INSERT INTO audit_events(entity_type,entity_id,action,payload_json,created_at,prev_hash,hash) VALUES(?,?,?,?,?,?,?)",
               (entity_type, entity_id, action, payload_json, created, prev_hash, h))

def task_row(db, task_id: str):
    row = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not row:
        raise SystemExit(f"task not found: {task_id}")
    return row

def unsatisfied_deps(db, task_id: str):
    """Dependencies of a task that are not yet completed (missing deps count as unsatisfied)."""
    return [dict(r) for r in db.execute(
        "SELECT d.depends_on AS id, COALESCE(t.status,'missing') AS status FROM task_deps d "
        "LEFT JOIN tasks t ON t.id=d.depends_on WHERE d.task_id=? AND COALESCE(t.status,'')!='completed'",
        (task_id,)).fetchall()]

def pending_dependents(db, task_id: str):
    """Direct dependents of a task that are still open (not completed/cancelled)."""
    return [dict(r) for r in db.execute(
        "SELECT d.task_id AS id, COALESCE(t.status,'missing') AS status FROM task_deps d "
        "LEFT JOIN tasks t ON t.id=d.task_id WHERE d.depends_on=? "
        "AND COALESCE(t.status,'') NOT IN ('completed','cancelled')",
        (task_id,)).fetchall()]

def unblock_count(db, task_id: str) -> int:
    """Queued direct dependents — work this task's completion frees for dispatch."""
    return db.execute(
        "SELECT COUNT(*) n FROM task_deps d JOIN tasks t ON t.id=d.task_id "
        "WHERE d.depends_on=? AND t.status='queued'", (task_id,)).fetchone()["n"]

def would_cycle(db, task_id: str, depends_on: str) -> bool:
    """True if adding edge task_id->depends_on creates a cycle (i.e. task_id is reachable from depends_on)."""
    row = db.execute(
        "WITH RECURSIVE up(x) AS (SELECT ? UNION SELECT d.depends_on FROM task_deps d JOIN up ON d.task_id=up.x) "
        "SELECT 1 FROM up WHERE x=? LIMIT 1", (depends_on, task_id)).fetchone()
    return row is not None

def add_dependency(db, task_id: str, depends_on: str) -> None:
    if task_id == depends_on:
        raise SystemExit("task cannot depend on itself")
    task_row(db, task_id)
    task_row(db, depends_on)
    if would_cycle(db, task_id, depends_on):
        raise SystemExit(f"dependency would create a cycle: {task_id} <-> {depends_on}")
    db.execute("INSERT INTO task_deps(task_id,depends_on,created_at) VALUES(?,?,?) ON CONFLICT DO NOTHING",
               (task_id, depends_on, now()))
    audit(db, "task", task_id, "dependency_added", {"depends_on": depends_on})

def _note_hash(task_id: str, content: str) -> str:
    """Content hash for exact-duplicate detection scoped to one task."""
    return hashlib.sha256(f"{task_id}|{content.strip()}".encode()).hexdigest()

def _near_dup_threshold() -> float:
    """Jaccard similarity above which a new note is flagged as a near-duplicate."""
    try:
        v = float(os.environ.get("AUTOPILOT_NEAR_DUP_THRESHOLD", "0.8"))
        return v if 0.0 < v <= 1.0 else 0.8
    except ValueError:
        return 0.8

def _tokens(text: str) -> set:
    return {t for t in re.findall(r"[a-z0-9]+", text.lower())}

def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)

_SECRET_PATTERNS = (
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("github_token", re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b")),
    ("openai_style_key", re.compile(r"\bsk-(?:proj-|ant-)?[A-Za-z0-9_-]{20,}\b")),
    ("slack_token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_\-]{35}\b")),
    ("private_key_block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("bearer_header", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-]{16,}")),
    ("credential_assignment", re.compile(
        r"(?i)\b(api[_-]?key|secret|password|passwd|access[_-]?token|auth[_-]?token|credential)[a-z0-9_-]*"
        r"\s*[:=]\s*['\"]?([^\s'\"]{8,})")),
)

def _secret_findings(text: str) -> list:
    """Credential-shaped spans in `text`, reported by kind only (never the value).

    The generic assignment pattern additionally requires a digit in the value
    so prose like 'token: superseded_by chain' does not false-positive while
    real secrets (which almost always mix digits in) still trip it.
    """
    findings = []
    for kind, pat in _SECRET_PATTERNS:
        for m in pat.finditer(text):
            if kind == "credential_assignment":
                value = m.group(2)
                if not any(c.isdigit() for c in value):
                    continue
                if value[0] in "$<{" or value.lower() in ("redacted", "placeholder", "none", "null"):
                    continue
            findings.append({"kind": kind})
            break
    return findings

def _redact_secrets(text: str) -> str:
    """Replace every credential-shaped span with a tagged placeholder."""
    out = text
    for kind, pat in _SECRET_PATTERNS:
        out = pat.sub(f"[REDACTED:{kind}]", out)
    return out

def _secret_guard(texts: dict, redact: bool, allow: bool, entity_id: str = ""):
    """Privacy boundary for shared-memory writes.

    Scans each field; on findings either blocks (audited `secret_blocked`),
    returns redacted copies (audited `secret_redacted`), or lets the caller
    proceed verbatim under an explicit `--allow-secret` override that is
    itself audited (`secret_allowed`) so fleet sweeps can still find it.
    Returns (findings, texts_after). Raises SystemExit on the block path.
    """
    findings = {field: _secret_findings(text) for field, text in texts.items()}
    findings = {f: ks for f, ks in findings.items() if ks}
    if not findings:
        return [], texts
    kinds = sorted({k["kind"] for ks in findings.values() for k in ks})
    with conn() as db:
        if allow:
            audit(db, "task", entity_id, "secret_allowed", {"fields": sorted(findings), "kinds": kinds})
            return kinds, texts
        if redact:
            audit(db, "task", entity_id, "secret_redacted", {"fields": sorted(findings), "kinds": kinds})
            return kinds, {f: (t if f not in findings else _redact_secrets(t)) for f, t in texts.items()}
        audit(db, "task", entity_id, "secret_blocked", {"fields": sorted(findings), "kinds": kinds})
    raise SystemExit(
        f"refusing to store credential-shaped content ({', '.join(kinds)}); "
        "re-run with --redact to store a redacted copy or --allow-secret to override")

def _near_duplicates(db, task_id: str, content: str) -> list:
    """Live notes on this task whose token overlap with `content` is high.

    Exact duplicates are already deduplicated by content_hash; this catches
    rephrased restatements so shared memory does not silently accumulate
    near-identical facts. Informational only: the note is still stored, and
    the flag travels in the output plus the audited event payload.
    """
    toks = _tokens(content)
    if not toks:
        return []
    threshold = _near_dup_threshold()
    similar = []
    for r in db.execute(
            "SELECT id,content FROM notes WHERE task_id=? AND superseded_by=''",
            (task_id,)).fetchall():
        sim = _jaccard(toks, _tokens(r["content"]))
        if sim >= threshold:
            similar.append({"note_id": r["id"], "similarity": round(sim, 3)})
    similar.sort(key=lambda s: (-s["similarity"], s["note_id"]))
    return similar

def _similar_open_tasks(db, project: str, text: str, exclude_id: str = "",
                        threshold: float | None = None) -> list:
    """Open tasks in one project whose title/description text overlaps heavily.

    Task-layer deduplication (the mirror of the note near-dup guard): two open
    tasks describing the same work split agent effort across two seams and
    both surface in dispatch. Only non-terminal tasks count — settled work is
    history, not a collision — and only within the same project, since the
    same title under a different project is a different checkout by the seam
    rule. Informational only: creation is never blocked. Deterministic:
    similarity descending, then id.
    """
    toks = _tokens(text)
    if not toks:
        return []
    if threshold is None:
        threshold = _near_dup_threshold()
    similar = []
    for r in db.execute(
            "SELECT id,title,description FROM tasks WHERE project=? "
            "AND status NOT IN ('completed','failed','cancelled') ORDER BY id",
            (project,)).fetchall():
        if r["id"] == exclude_id:
            continue
        other = _tokens(r["title"] + " " + r["description"])
        sim = _jaccard(toks, other)
        if sim >= threshold:
            similar.append({"task_id": r["id"], "title": r["title"],
                            "similarity": round(sim, 3)})
    similar.sort(key=lambda s: (-s["similarity"], s["task_id"]))
    return similar

def live_note(db, task_id: str, content_hash: str):
    return db.execute(
        "SELECT * FROM notes WHERE task_id=? AND content_hash=? AND superseded_by='' ORDER BY created_at DESC LIMIT 1",
        (task_id, content_hash)).fetchone()

def _ttl_hours(value) -> float:
    """Validate a --ttl-hours argument; None/absent means no expiry."""
    if value is None:
        return 0.0
    try:
        v = float(value)
    except (TypeError, ValueError):
        raise SystemExit("--ttl-hours must be a positive number")
    if v <= 0:
        raise SystemExit("--ttl-hours must be a positive number")
    return v

def _expires_at(hours: float) -> str:
    if hours <= 0:
        return ""
    dt = datetime.now(timezone.utc) + timedelta(hours=hours)
    return dt.replace(microsecond=0).isoformat()

def _ttl_past(r, t: str) -> bool:
    """Pure time check: the note's expiry instant has passed."""
    return bool(r["expires_at"]) and r["expires_at"] <= t

def _note_retired(r, t: str) -> bool:
    """Effective expiry: an unpinned note past its TTL retires from packs/retrieval.

    Pinned facts are immortal by design — they are critical constraints, and
    silently dropping one because time passed is exactly the failure mode TTL
    must not introduce. Retire a pinned fact explicitly via supersede-note.
    """
    return _ttl_past(r, t) and not r["pinned"]

def add_note(args):
    """Attach a provenance-tagged fact to a task; exact duplicates are deduplicated."""
    if args.kind not in NOTE_KINDS:
        raise SystemExit(f"invalid note kind: {args.kind} (choose from {sorted(NOTE_KINDS)})")
    content = args.content.strip()
    if not content:
        raise SystemExit("note content must not be empty")
    redact = getattr(args, "redact", False)
    allow = getattr(args, "allow_secret", False)
    secret_kinds, guarded = _secret_guard({"content": content}, redact, allow, args.task_id)
    content = guarded["content"]
    nid = uuid.uuid4().hex
    t = now()
    pinned = 1 if getattr(args, "pinned", False) else 0
    expires_at = _expires_at(_ttl_hours(getattr(args, "ttl_hours", None)))
    with conn() as db:
        task_row(db, args.task_id)
        h = _note_hash(args.task_id, content)
        existing = live_note(db, args.task_id, h)
        if existing:
            if pinned and not existing["pinned"]:
                # A duplicate add can promote an existing note to pinned.
                db.execute("UPDATE notes SET pinned=1 WHERE id=?", (existing["id"],))
                audit(db, "task", args.task_id, "note_pinned", {"note_id": existing["id"]})
            # TTL refresh/revival: a duplicate add restates the fact, so it also
            # restates the fact's lifetime. Without this, re-adding an expired
            # note would dedup straight into invisibility.
            revived = _note_retired(existing, t)
            if expires_at != (existing["expires_at"] or ""):
                db.execute("UPDATE notes SET expires_at=? WHERE id=?", (expires_at, existing["id"]))
                audit(db, "task", args.task_id, "note_ttl_refreshed",
                      {"note_id": existing["id"], "expires_at": expires_at,
                       "previous_expires_at": existing["expires_at"], "revived": revived})
            audit(db, "task", args.task_id, "note_deduplicated",
                  {"note_id": existing["id"], "kind": args.kind, **({"revived": True} if revived else {})})
            out = {"ok": True, "id": existing["id"], "task_id": args.task_id,
                   "deduplicated": True, "created_at": existing["created_at"]}
            if revived:
                out["revived"] = True
            if expires_at:
                out["expires_at"] = expires_at
            json_out(out)
            return
        similar = _near_duplicates(db, args.task_id, content)
        db.execute("INSERT INTO notes(id,task_id,kind,content,source,content_hash,created_at,pinned,expires_at) VALUES(?,?,?,?,?,?,?,?,?)",
                   (nid, args.task_id, args.kind, content, args.source, h, t, pinned, expires_at))
        payload = {"note_id": nid, "kind": args.kind, "source": args.source, "pinned": bool(pinned),
                   "similar_notes": [s["note_id"] for s in similar]}
        if secret_kinds:
            payload["secret_kinds"] = secret_kinds
        if expires_at:
            payload["expires_at"] = expires_at
        audit(db, "task", args.task_id, "note_added", payload)
    out = {"ok": True, "id": nid, "task_id": args.task_id, "deduplicated": False,
           "similar_to": similar, "created_at": t}
    if secret_kinds:
        out["secret_kinds"] = secret_kinds
    if expires_at:
        out["expires_at"] = expires_at
    json_out(out)

def list_notes(args):
    t = now()
    with conn() as db:
        task_row(db, args.task_id)
        q = ("SELECT n.id,n.kind,n.content,n.source,n.created_at,n.superseded_by,n.pinned,n.expires_at FROM notes n "
             "WHERE n.task_id=?")
        if not getattr(args, "all", False):
            q += " AND n.superseded_by=''"
        q += " ORDER BY n.rowid ASC"
        rows = []
        for r in db.execute(q, (args.task_id,)).fetchall():
            d = dict(r)
            if d["expires_at"]:
                # Pure time flag: a pinned note past its TTL still shows expired
                # so the operator can see it needs an explicit supersede.
                d["expired"] = _ttl_past(d, t)
            rows.append(d)
    json_out(rows)

def supersede_note(args):
    """Temporal facts: mark an old note superseded by a new one atomically."""
    if args.kind and args.kind not in NOTE_KINDS:
        raise SystemExit(f"invalid note kind: {args.kind}")
    new_content = args.content.strip()
    if not new_content:
        raise SystemExit("superseding content must not be empty")
    secret_kinds, guarded = _secret_guard(
        {"content": new_content}, getattr(args, "redact", False),
        getattr(args, "allow_secret", False), args.note_id)
    new_content = guarded["content"]
    old_id = args.note_id
    new_id = uuid.uuid4().hex
    t = now()
    with conn() as db:
        old = db.execute("SELECT * FROM notes WHERE id=?", (old_id,)).fetchone()
        if not old:
            raise SystemExit(f"note not found: {old_id}")
        if old["superseded_by"]:
            raise SystemExit(f"note already superseded by {old['superseded_by']}")
        task_row(db, old["task_id"])
        kind = args.kind or old["kind"]
        source = args.source or old["source"]
        h = _note_hash(old["task_id"], new_content)
        clash = live_note(db, old["task_id"], h)
        if clash:
            raise SystemExit(f"a live note with identical content already exists: {clash['id']}")
        # Single-statement guard so concurrent supersedes cannot both win.
        cur = db.execute("UPDATE notes SET superseded_by=? WHERE id=? AND superseded_by=''", (new_id, old_id))
        if cur.rowcount != 1:
            raise SystemExit("note was concurrently superseded; retry")
        # A superseding fact is fresh: without an explicit --ttl-hours it carries
        # no expiry, even when the retired predecessor did.
        new_expires = _expires_at(_ttl_hours(getattr(args, "ttl_hours", None)))
        db.execute("INSERT INTO notes(id,task_id,kind,content,source,content_hash,created_at,superseded_by,pinned,expires_at) VALUES(?,?,?,?,?,?,?,'',?,?)",
                   (new_id, old["task_id"], kind, new_content, source, h, t, old["pinned"], new_expires))
        audit(db, "task", old["task_id"], "note_superseded",
              {"old_note_id": old_id, "new_note_id": new_id, "kind": kind,
               **({"secret_kinds": secret_kinds} if secret_kinds else {}),
               **({"expires_at": new_expires} if new_expires else {})})
    out = {"ok": True, "old_note_id": old_id, "new_note_id": new_id, "task_id": old["task_id"]}
    if secret_kinds:
        out["secret_kinds"] = secret_kinds
    if new_expires:
        out["expires_at"] = new_expires
    json_out(out)

def note_history(args):
    """Walk a temporal fact chain: oldest predecessor → newest live successor."""
    with conn() as db:
        cur = db.execute("SELECT * FROM notes WHERE id=?", (args.note_id,)).fetchone()
        if not cur:
            raise SystemExit(f"note not found: {args.note_id}")
        chain = [dict(cur)]
        seen = {cur["id"]}
        node = cur
        while node["superseded_by"] and node["superseded_by"] not in seen:
            nxt = db.execute("SELECT * FROM notes WHERE id=?", (node["superseded_by"],)).fetchone()
            if not nxt:
                break
            chain.append(dict(nxt)); seen.add(nxt["id"]); node = nxt
        node = cur
        while True:
            prv = db.execute(
                "SELECT * FROM notes WHERE superseded_by=? LIMIT 1", (node["id"],)).fetchone()
            if not prv or prv["id"] in seen:
                break
            chain.insert(0, dict(prv)); seen.add(prv["id"]); node = prv
    json_out(chain)

def handoff_history(args):
    """Walk a handoff's supersession chain: oldest predecessor → newest live successor.

    The temporal mirror of note-history for the handoff protocol: given any
    handoff id, reconstruct how the resume point evolved — who handed to whom,
    what changed at each link — without manual `handoffs --all` archaeology.
    """
    with conn() as db:
        cur = db.execute("SELECT * FROM handoffs WHERE id=?", (args.handoff_id,)).fetchone()
        if not cur:
            raise SystemExit(f"handoff not found: {args.handoff_id}")
        chain = [dict(cur)]
        seen = {cur["id"]}
        node = cur
        while node["superseded_by"] and node["superseded_by"] not in seen:
            nxt = db.execute("SELECT * FROM handoffs WHERE id=?", (node["superseded_by"],)).fetchone()
            if not nxt:
                break
            chain.append(dict(nxt)); seen.add(nxt["id"]); node = nxt
        node = cur
        while True:
            prv = db.execute(
                "SELECT * FROM handoffs WHERE superseded_by=? LIMIT 1", (node["id"],)).fetchone()
            if not prv or prv["id"] in seen:
                break
            chain.insert(0, dict(prv)); seen.add(prv["id"]); node = prv
    json_out(chain)

HANDOFF_LIST_FIELDS = ("evidence", "constraints", "decisions", "files", "next_actions", "risks")
_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")

def _require_digest(value: str, flag: str) -> str:
    """Validate an optional recall digest (64-char lowercase hex sha256)."""
    v = (value or "").strip().lower()
    if v and not _DIGEST_RE.fullmatch(v):
        raise SystemExit(f"invalid {flag}: expected a 64-character hex sha256 digest")
    return v

def _handoff_payload(args) -> dict:
    """Canonical provider-neutral handoff fields from CLI args."""
    return {
        "from_agent": (args.from_agent or "").strip(),
        "to_agent": (getattr(args, "to_agent", "") or "").strip(),
        "status": (getattr(args, "status", "") or "").strip(),
        "objective": (getattr(args, "objective", "") or "").strip(),
        **{f: list(getattr(args, f, []) or []) for f in HANDOFF_LIST_FIELDS},
        "commit_ref": (getattr(args, "commit_ref", "") or "").strip(),
        "recall_digest": _require_digest(getattr(args, "recall_digest", "") or "", "--recall-digest"),
    }

def _handoff_hash(task_id: str, payload: dict) -> str:
    material = json.dumps({"task_id": task_id, **payload}, sort_keys=True)
    return hashlib.sha256(material.encode()).hexdigest()

def _live_handoff(db, task_id: str):
    return db.execute(
        "SELECT * FROM handoffs WHERE task_id=? AND superseded_by='' ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (task_id,)).fetchone()

def _handoff_parsed(row, include_meta: bool = False):
    out = {
        "id": row["id"], "task_id": row["task_id"],
        "from_agent": row["from_agent"], "to_agent": row["to_agent"],
        "status": row["status"], "objective": row["objective"],
        "commit_ref": row["commit_ref"], "recall_digest": row["recall_digest"],
        "acked_by": row["acked_by"], "acked_at": row["acked_at"],
        "ack_recall_digest": row["ack_recall_digest"],
        "created_at": row["created_at"],
    }
    for f in HANDOFF_LIST_FIELDS:
        try:
            out[f] = json.loads(row[f])
        except (json.JSONDecodeError, TypeError):
            out[f] = []
    if include_meta:
        out["superseded_by"] = row["superseded_by"]
        out["content_hash"] = row["content_hash"]
    return out

def _handoff_cost(h: dict) -> int:
    return sum(len(h.get(f, "")) for f in ("id", "from_agent", "to_agent", "status",
               "objective", "commit_ref", "created_at", "recall_digest")) \
        + sum(len(" ".join(h.get(f, []))) for f in HANDOFF_LIST_FIELDS) + 32

def add_handoff(args):
    """Record a durable agent-to-agent handoff for a task.

    The latest live handoff is the authoritative resume point: recording a new
    handoff atomically supersedes the previous one (temporal chain, queryable
    via `handoffs --all`). An identical live handoff is deduplicated instead of
    growing the store. Payloads must never carry credentials or private tokens.
    `--recall-digest` attaches proof of the context pack the handoff was
    written against (the digest from a prior `recall`), so downstream agents
    can detect when the handoff predates newer context.
    """
    payload = _handoff_payload(args)
    texts = {"objective": payload["objective"]}
    for f in HANDOFF_LIST_FIELDS:
        for i, item in enumerate(payload[f]):
            texts[f"{f}[{i}]"] = item
    secret_kinds, guarded = _secret_guard(
        texts, getattr(args, "redact", False), getattr(args, "allow_secret", False), args.task_id)
    if secret_kinds:
        payload["objective"] = guarded["objective"]
        for f in HANDOFF_LIST_FIELDS:
            payload[f] = [guarded.get(f"{f}[{i}]", item) for i, item in enumerate(payload[f])]
    hid = uuid.uuid4().hex
    t = now()
    with conn() as db:
        task_row(db, args.task_id)
        h = _handoff_hash(args.task_id, payload)
        existing = db.execute(
            "SELECT * FROM handoffs WHERE task_id=? AND content_hash=? AND superseded_by='' "
            "ORDER BY created_at DESC LIMIT 1", (args.task_id, h)).fetchone()
        if existing:
            audit(db, "task", args.task_id, "handoff_deduplicated", {"handoff_id": existing["id"]})
            json_out({"ok": True, "id": existing["id"], "task_id": args.task_id,
                      "deduplicated": True, "created_at": existing["created_at"]})
            return
        prev = _live_handoff(db, args.task_id)
        cur = db.execute("UPDATE handoffs SET superseded_by=? WHERE task_id=? AND superseded_by=''",
                         (hid, args.task_id))
        db.execute(
            "INSERT INTO handoffs(id,task_id,from_agent,to_agent,status,objective,evidence,"
            "constraints,decisions,files,commit_ref,next_actions,risks,content_hash,recall_digest,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (hid, args.task_id, payload["from_agent"], payload["to_agent"], payload["status"],
             payload["objective"], json.dumps(payload["evidence"]), json.dumps(payload["constraints"]),
             json.dumps(payload["decisions"]), json.dumps(payload["files"]), payload["commit_ref"],
             json.dumps(payload["next_actions"]), json.dumps(payload["risks"]), h,
             payload["recall_digest"], t))
        audit(db, "task", args.task_id, "handoff_recorded",
              {"handoff_id": hid, "from_agent": payload["from_agent"],
               "to_agent": payload["to_agent"], "superseded": prev["id"] if prev and cur.rowcount else None,
               "recall_digest": payload["recall_digest"] or None,
               **({"secret_kinds": secret_kinds} if secret_kinds else {})})
    out = {"ok": True, "id": hid, "task_id": args.task_id, "deduplicated": False,
           "superseded": prev["id"] if prev and cur.rowcount else None, "created_at": t}
    if secret_kinds:
        out["secret_kinds"] = secret_kinds
    json_out(out)

def list_handoffs(args):
    with conn() as db:
        task_row(db, args.task_id)
        q = "SELECT * FROM handoffs WHERE task_id=?"
        if not getattr(args, "all", False):
            q += " AND superseded_by=''"
        q += " ORDER BY created_at DESC, rowid DESC"
        rows = [_handoff_parsed(r, include_meta=True) for r in db.execute(q, (args.task_id,)).fetchall()]
    json_out(rows)

def current_handoff(args):
    """Latest durable handoff — the recovery point for a killed session."""
    with conn() as db:
        task_row(db, args.task_id)
        row = _live_handoff(db, args.task_id)
    json_out(_handoff_parsed(row, include_meta=True) if row else None)

def ack_handoff(args):
    """Acknowledge the live handoff addressed to you (provider-neutral protocol).

    The inbox surfaces inbound work; `ack` records that the recipient has
    *accepted* it, closing the loop between "handed to" and "picked up by".
    Only the addressed agent may ack, only the live handoff is acked (a new
    handoff resets acceptance), re-acking is idempotent, and an optional
    --recall-digest ties the acceptance to proof of recalled context. The
    transition is audited as `handoff_acknowledged` in the hash chain.
    """
    agent = (args.agent or "").strip()
    if not agent:
        raise SystemExit("--agent is required")
    digest = _require_digest(getattr(args, "recall_digest", "") or "", "--recall-digest")
    t = now()
    with conn() as db:
        task_row(db, args.task_id)
        row = _live_handoff(db, args.task_id)
        if row is None:
            raise SystemExit("no live handoff to acknowledge; record one with `handoff` first")
        if row["to_agent"] != agent:
            raise SystemExit(f"live handoff is addressed to '{row['to_agent'] or 'nobody'}', not '{agent}'")
        if row["acked_by"]:
            json_out({"ok": True, "task_id": args.task_id, "handoff_id": row["id"],
                      "already_acked": True, "acked_by": row["acked_by"],
                      "acked_at": row["acked_at"], "ack_recall_digest": row["ack_recall_digest"]})
            return
        db.execute("UPDATE handoffs SET acked_by=?,acked_at=?,ack_recall_digest=? WHERE id=?",
                   (agent, t, digest, row["id"]))
        audit(db, "task", args.task_id, "handoff_acknowledged",
              {"handoff_id": row["id"], "agent": agent, "recall_digest": digest or None})
    json_out({"ok": True, "task_id": args.task_id, "handoff_id": row["id"],
              "already_acked": False, "acked_by": agent, "acked_at": t,
              "ack_recall_digest": digest})

def handoff_inbox(args):
    """Fleet-wide inbound view: every live handoff addressed to an agent.

    The per-task commands (`handoffs`, `handoff-current`) answer "what is the
    state of this task"; the inbox answers the complementary question an
    incoming agent actually starts from: "what work was handed to me across
    the whole fleet?" Only *live* (non-superseded) handoffs whose to_agent
    matches are listed — when a handoff is superseded by one addressed to
    someone else, the task leaves the previous recipient's inbox automatically.
    Each item joins the task's live state (title, status, priority, lease) so
    the agent can triage without a follow-up `show` per task, and carries the
    handoff's acknowledgment state (`acked` / `acked_at`); `--unacked-only`
    restricts the view to work not yet picked up.
    """
    agent = (args.agent or "").strip()
    if not agent:
        raise SystemExit("--agent is required")
    clauses = ["h.superseded_by=''", "h.to_agent=?"]
    vals = [agent]
    if getattr(args, "unacked_only", False):
        clauses.append("h.acked_by=''")
    if getattr(args, "project", ""):
        clauses.append("t.project=?")
        vals.append(args.project)
    limit = max(0, args.limit)
    t = now()
    with conn() as db:
        rows = db.execute(
            "SELECT h.id,h.task_id,h.from_agent,h.status,h.objective,h.commit_ref,"
            "h.recall_digest,h.acked_by,h.acked_at,h.created_at,t.project,t.title AS task_title,"
            "t.status AS task_status,t.priority,t.lease_owner,t.lease_expires_at "
            "FROM handoffs h JOIN tasks t ON t.id=h.task_id "
            "WHERE " + " AND ".join(clauses) +
            " ORDER BY h.created_at DESC, h.rowid DESC LIMIT ?", (*vals, limit)).fetchall()
    items = []
    for r in rows:
        items.append({
            "handoff_id": r["id"], "task_id": r["task_id"], "project": r["project"],
            "task_title": r["task_title"], "task_status": r["task_status"],
            "priority": r["priority"],
            "lease": {"owner": r["lease_owner"],
                      "live": bool(r["lease_owner"]) and r["lease_expires_at"] > t},
            "from_agent": r["from_agent"], "status": r["status"],
            "objective": r["objective"], "commit_ref": r["commit_ref"],
            "recall_digest": r["recall_digest"],
            "acked": bool(r["acked_by"]), "acked_at": r["acked_at"] or None,
            "created_at": r["created_at"],
        })
    json_out({"ok": True, "agent": agent, "generated_at": t,
              "count": len(items), "items": items})

def _rerank_notes(rows: list, recency_half_life_hours: float, pinned_boost: float) -> list:
    """Hybrid temporal rerank over retrieved note rows (pure function).

    Pure BM25 is blind to time: a perfectly-matched note from months ago
    outranks a fresh one, and stale facts are exactly what agents must not
    pack first. The hybrid score blends a normalized lexical match (best bm25
    → 1.0; rows without a score — the LIKE fallback — count as 1.0 since that
    path already returns newest-first) with an exponential recency decay
    (half-life = recency_half_life_hours) plus a flat pinned_boost bonus.
    Each row gains `rank_score`; output sorts by it descending, ties broken
    newest-first. Deterministic: same inputs, same order — note ages are
    floored to whole hours before the decay is applied, so a bundle's
    rank_scores (and therefore its recall digest) stay stable within the
    hour instead of drifting with every recomputation.
    """
    if not rows:
        return rows
    scored = [dict(r) for r in rows]
    bm = [r["score"] for r in scored if r.get("score") is not None]
    best = min(bm) if bm else None  # bm25(): lower (more negative) = better match
    now_dt = datetime.now(timezone.utc)
    half_life = max(float(recency_half_life_hours), 1e-9)
    for r in scored:
        s = r.get("score")
        if s is None or best is None or s == 0:
            norm = 1.0
        else:
            norm = max(0.0, min(1.0, best / s))
        try:
            age_h = max(0.0, (now_dt - datetime.fromisoformat(r["created_at"])).total_seconds() / 3600.0)
        except ValueError:
            age_h = 0.0
        r["rank_score"] = round(norm * (0.5 ** (float(int(age_h)) / half_life))
                                + (float(pinned_boost) if r.get("pinned") else 0.0), 6)
    scored.sort(key=lambda r: r["created_at"], reverse=True)   # tie-break: newest first
    scored.sort(key=lambda r: -r["rank_score"])                # stable: rank dominates
    return scored

def _related_note_candidates(db, task_id: str, text: str, limit: int, scope: str,
                             rerank: bool = False,
                             recency_half_life_hours: float = 168.0,
                             pinned_boost: float = 0.5) -> list:
    """Cross-task retrieval: live notes on *other* tasks matching this task's text.

    FTS5 path OR-combines the tokens (recall-oriented) and ranks by BM25 so the
    best matches pack first; on non-FTS builds it degrades to any-token LIKE
    matching with identical output shape minus `score`. Scope 'project'
    restricts candidates to the task's own project; 'global' searches all.
    With rerank=True, candidates are re-scored by the temporal hybrid
    (`_rerank_notes`) so fresh and pinned matches surface before stale ones;
    each row then also carries `rank_score`.
    """
    if limit <= 0 or not text.strip():
        return []
    toks = []
    for raw in text.split():
        tok = raw.replace('"', "")
        if tok and any(c.isalnum() for c in tok):
            toks.append(tok)
    if not toks:
        return []
    scope_sql, scope_vals = "", []
    if scope != "global":
        scope_sql = " AND t.project=(SELECT project FROM tasks WHERE id=?)"
        scope_vals = [task_id]
    # Retired (expired unpinned) notes are stale facts — exactly what cross-task
    # RAG must not surface; pinned candidates are exempt like everywhere else.
    live_sql = " AND (n.expires_at='' OR n.expires_at>? OR n.pinned=1)"
    live_val = [now()]
    cols = ("SELECT n.id,n.kind,n.content,n.source,n.created_at,n.pinned,n.task_id,"
            "t.title AS via_task_title")
    if _fts_ready(db):
        match = " OR ".join('"%s"' % t for t in toks)
        sql = (cols + ",bm25(notes_fts) AS score "
               "FROM notes_fts f JOIN notes n ON n.rowid=f.rowid JOIN tasks t ON t.id=n.task_id "
               "WHERE notes_fts MATCH ? AND n.superseded_by='' AND n.task_id!=?" + scope_sql + live_sql +
               " ORDER BY score LIMIT ?")
        vals = [match, task_id, *scope_vals, *live_val, limit]
    else:
        likes = " OR ".join("n.content LIKE ?" for _ in toks)
        sql = (cols + " FROM notes n JOIN tasks t ON t.id=n.task_id "
               "WHERE (" + likes + ") AND n.superseded_by='' AND n.task_id!=?" + scope_sql + live_sql +
               " ORDER BY n.created_at DESC LIMIT ?")
        vals = [*( "%" + t + "%" for t in toks), task_id, *scope_vals, *live_val, limit]
    rows = [dict(r) for r in db.execute(sql, vals).fetchall()]
    if rerank:
        rows = _rerank_notes(rows, recency_half_life_hours, pinned_boost)
    return rows

def _related_handoff_candidates(db, task_id: str, text: str, limit: int, scope: str) -> list:
    """Cross-task retrieval: live handoffs on *other* tasks matching this task's text.

    The handoff is the authoritative resume point of the protocol; this surfaces
    *other* tasks' resume points relevant to the current work so an agent
    inherits neighboring decisions instead of rediscovering them. FTS5 path
    OR-combines tokens over objective/status/from_agent/to_agent for matching;
    non-FTS builds degrade to any-token LIKE over those fields. Both paths
    order deterministically by created_at DESC, rowid — recall bundles are
    digest-sealed, and BM25 ranking/scores drift whenever *any* handoff joins
    the index (document-frequency shift), which would make every prior core
    digest stale without any real context change. Only live (non-superseded)
    handoffs are candidates — superseded ones are history, not resume points.
    Scope 'project' restricts to the task's own project; 'global' searches all.
    """
    if limit <= 0 or not text.strip():
        return []
    toks = []
    for raw in text.split():
        tok = raw.replace('"', "")
        if tok and any(c.isalnum() for c in tok):
            toks.append(tok)
    if not toks:
        return []
    scope_sql, scope_vals = "", []
    if scope != "global":
        scope_sql = " AND t.project=(SELECT project FROM tasks WHERE id=?)"
        scope_vals = [task_id]
    cols = ("SELECT h.id,h.task_id,h.from_agent,h.to_agent,h.status,h.objective,"
            "h.commit_ref,h.created_at,t.title AS via_task_title")
    if _handoffs_fts_ready(db):
        match = " OR ".join('"%s"' % t for t in toks)
        sql = (cols + " FROM handoffs_fts f JOIN handoffs h ON h.rowid=f.rowid "
               "JOIN tasks t ON t.id=h.task_id "
               "WHERE handoffs_fts MATCH ? AND h.superseded_by='' AND h.task_id!=?" + scope_sql +
               " ORDER BY h.created_at DESC, h.rowid DESC LIMIT ?")
        vals = [match, task_id, *scope_vals, limit]
    else:
        likes = " OR ".join(
            "(h.objective LIKE ? OR h.status LIKE ? OR h.from_agent LIKE ? OR h.to_agent LIKE ?)"
            for _ in toks)
        sql = (cols + " FROM handoffs h JOIN tasks t ON t.id=h.task_id "
               "WHERE (" + likes + ") AND h.superseded_by='' AND h.task_id!=?" + scope_sql +
               " ORDER BY h.created_at DESC, h.rowid DESC LIMIT ?")
        pats = []
        for t in toks:
            pats.extend(["%" + t + "%"] * 4)
        vals = [*pats, task_id, *scope_vals, limit]
    return [dict(r) for r in db.execute(sql, vals).fetchall()]

def _dep_context_candidates(db, task_id: str, limit: int) -> list:
    """Verified evidence of completed direct prerequisites.

    Dependency-aware orchestration: when a prerequisite completes, its live
    handoff and latest sealed receipt ARE the verified evidence the
    dependent's agent must inherit — yet only *unsatisfied* dependencies
    surface in packs today, so downstream work starts blind to what upstream
    proved. Returns, per completed direct dependency in deterministic edge
    order (task_deps.rowid), the dep's id/title plus its live handoff (the
    resume point) and latest receipt (sealed evidence with payload).
    """
    if limit <= 0:
        return []
    deps = db.execute(
        "SELECT d.depends_on AS id, t.title AS title FROM task_deps d "
        "JOIN tasks t ON t.id=d.depends_on WHERE d.task_id=? AND t.status='completed' "
        "ORDER BY d.rowid ASC LIMIT ?", (task_id, limit)).fetchall()
    out = []
    for d in deps:
        hrow = _live_handoff(db, d["id"])
        rrow = db.execute(
            "SELECT id,kind,payload_json,created_at FROM receipts WHERE task_id=? "
            "ORDER BY created_at DESC, id DESC LIMIT 1", (d["id"],)).fetchone()
        out.append({"id": d["id"], "title": d["title"],
                    "handoff": _handoff_parsed(hrow) if hrow else None,
                    "receipt": ({**{k: rrow[k] for k in ("id", "kind", "created_at")},
                                 "payload": json.loads(rrow["payload_json"])} if rrow else None)})
    return out

def _dep_context_cost(entry: dict) -> int:
    c = len(entry["id"]) + len(entry["title"]) + 16
    if entry["handoff"]:
        c += _handoff_cost(entry["handoff"])
    if entry["receipt"]:
        r = entry["receipt"]
        c += len(json.dumps(r["payload"], sort_keys=True)) \
            + len(r["id"]) + len(r["kind"]) + len(r["created_at"]) + 8
    return c

@dataclass
class _PackSection:
    """One flag-gated context-pack section in the pack spec.

    `collect(db, ctx)` returns the candidate rows (possibly empty); a section
    is active only when its flag count is > 0, so packs built without the
    flag keep the legacy byte-identical shape. `keys`/`prefix` name the
    output keys (`<prefix>_requested/_matched/_packed` + `keys`), `cost`
    prices one row against the remaining budget, and `decorate` may tag a
    row before packing (e.g. related=True).
    """
    prefix: str
    keys: str
    collect: object          # (db, ctx, limit) -> [rows]
    cost: object             # (row) -> int
    decorate: object = None  # (row) -> row

def _pack_section_rows(section, db, ctx, limit):
    return section.collect(db, ctx, max(0, limit)) if limit > 0 else []

def _pack_fit(rows, used, budget, truncated, section):
    """Greedy in-order fit of candidate rows into the remaining budget.

    Shared by every section (and by notes): rows that do not fit mark the
    pack truncated and are skipped, not fatal.
    """
    packed = []
    for r in rows:
        if section.decorate is not None:
            r = section.decorate(r)
        c = section.cost(r)
        if used + c > budget:
            truncated = True
            continue
        packed.append(r)
        used += c
    return packed, used, truncated

def _pack_spec():
    """The context-pack specification: every flag-gated cross-task section.

    Adding a flag-gated section is one registration here rather than
    threading kwargs through five signatures; ordering below is the pack
    order (related notes -> handoffs -> dep context -> sessions -> facts).
    """
    rel_note_cost = lambda r: (len(r["content"]) + len(r["kind"]) + len(r["source"])
                               + len(r["created_at"]) + len(r["task_id"])
                               + len(r["via_task_title"]) + 16)
    return [
        _PackSection("related", "notes",
            lambda db, ctx, n: _related_note_candidates(
                db, ctx["task_id"], ctx["pack_text"], n, ctx["rel_scope"],
                rerank=ctx["rerank"],
                recency_half_life_hours=ctx["recency_half_life_hours"],
                pinned_boost=ctx["pinned_boost"]),
            rel_note_cost,
            decorate=lambda r: {**r, "related": True}),
        _PackSection("related_handoffs", "related_handoffs",
            lambda db, ctx, n: _related_handoff_candidates(db, ctx["task_id"], ctx["pack_text"], n, ctx["rel_scope"]),
            lambda h: (len(h["objective"]) + len(h["status"]) + len(h["from_agent"])
                       + len(h["to_agent"]) + len(h["commit_ref"]) + len(h["created_at"])
                       + len(h["task_id"]) + len(h["via_task_title"]) + 16)),
        _PackSection("dep_context", "dep_context",
            lambda db, ctx, n: _dep_context_candidates(db, ctx["task_id"], n),
            _dep_context_cost),
        _PackSection("related_sessions", "related_sessions",
            lambda db, ctx, n: _related_session_candidates(db, ctx["task_id"], ctx["pack_text"], n, ctx["rel_scope"]),
            lambda m: (len(m["content"]) + len(m["role"]) + len(m["at"])
                       + len(m["source"]) + len(m["session_id"]) + 16)),
        _PackSection("related_facts", "related_facts",
            lambda db, ctx, n: _related_fact_candidates(db, ctx["task_id"], ctx["pack_text"], n),
            lambda ft: (len(ft["id"]) + len(ft["subject"]) + len(ft["predicate"])
                        + len(ft["object"]) + len(ft["valid_from"]) + len(ft["valid_until"])
                        + len(ft["source"]) + 16)),
        _PackSection("related_semantic", "related_semantic",
            lambda db, ctx, n: _hindsight_candidates(db, ctx["task_id"], ctx["pack_text"],
                                                     n, ctx["rel_scope"]),
            lambda m: (len(m["id"]) + len(m["engine"]) + len(m["kind"]) + len(m["project"])
                       + len(m["at"]) + sum(len(t) for t in m["tags"]) + len(m["content"]) + 16),
            decorate=lambda r: {**r, "semantic": True}),
    ]

def _assemble_pack(db, task_id, budget, rel_limit, rel_scope, rerank=False,
                   recency_half_life_hours=168.0, pinned_boost=0.5,
                   section_limits=None):
    """Spec-driven pack assembly shared by `_build_pack` and `_build_recall_pack`.

    Sections come from `_pack_spec()`; each contributes at most
    `section_limits[prefix]` candidates (default 0 = inactive = legacy shape).
    """
    budget = max(0, budget)
    rel_limit = max(0, rel_limit)
    rel_scope = rel_scope or "project"
    section_limits = dict(section_limits or {})
    row = task_row(db, task_id)
    summary = {k: row[k] for k in ("id", "project", "title", "status", "priority", "next_action", "blocked_reason", "due_at")}
    pending = unsatisfied_deps(db, task_id)
    t = now()
    rows = [dict(r) for r in db.execute(
        "SELECT id,kind,content,source,created_at,pinned,expires_at FROM notes "
        "WHERE task_id=? AND superseded_by='' ORDER BY rowid ASC",
        (task_id,)).fetchall()]
    # Temporal facts: unpinned notes past their TTL retire from the pack; the
    # count is reported only when nonzero so packs without TTL notes stay
    # byte-identical (and digest-compatible) to the legacy shape.
    retired = [r for r in rows if _note_retired(r, t)]
    if retired:
        rows = [r for r in rows if not _note_retired(r, t)]
    pack_text = " ".join(filter(None, (row["title"], row["description"], row["next_action"])))
    hrow = _live_handoff(db, task_id)
    # Stable sort: pinned notes first, original (oldest→newest) order within groups.
    ordered = sorted(rows, key=lambda r: not r["pinned"])
    header_cost = 64 + len(summary["title"]) + len(summary["next_action"]) + len(summary["blocked_reason"])
    used, truncated = min(header_cost, budget), False
    # The live handoff is the authoritative resume point: it packs right after
    # the task header, before notes, so recovery context survives tight budgets.
    handoff = _handoff_parsed(hrow) if hrow else None
    handoff_packed = False
    if handoff is not None:
        cost = _handoff_cost(handoff)
        if used + cost <= budget:
            used += cost; handoff_packed = True
        else:
            truncated = True
    packed, pinned_packed = [], 0
    for r in ordered:
        cost = len(r["content"]) + len(r["kind"]) + len(r["source"]) + len(r["created_at"]) + 8
        if used + cost > budget:
            truncated = True
            continue
        if _ttl_past(r, t):
            # A pinned note past its TTL still packs (pinned facts are immortal)
            # but carries the flag so the agent knows it needs a fresh supersede.
            r = {**r, "expired": True}
        packed.append(r); used += cost
        if r["pinned"]:
            pinned_packed += 1
    ctx = {"task_id": task_id, "pack_text": pack_text, "rel_scope": rel_scope,
           "rerank": rerank, "recency_half_life_hours": recency_half_life_hours,
           "pinned_boost": pinned_boost}
    out = {"task_id": task_id, "budget": budget, "used_chars": used,
            "truncated": truncated, "task": summary,
            "unsatisfied_dependencies": pending,
            "handoff": handoff if handoff_packed else None,
            "handoff_packed": handoff_packed,
            "notes_total": len(rows), "notes_packed": len(packed),
            "notes_pinned_packed": pinned_packed,
            "related_requested": rel_limit, "related_matched": 0,
            "related_packed": 0,
            "notes": packed}
    for section in _pack_spec():
        limit = section_limits.get(section.prefix, 0)
        cand = _pack_section_rows(section, db, ctx, limit)
        got, used, truncated = _pack_fit(cand, used, budget, truncated, section)
        out["used_chars"] = used; out["truncated"] = truncated
        if limit <= 0:
            continue  # inactive section: legacy shape keeps no keys at all
        key = section.keys
        out[key] = got
        if key == "notes":
            # The first spec section shares the `notes` list with own notes;
            # report its counters under the historical related_* names and
            # fold its rows back into `out["notes"]`.
            pass
        out[f"{section.prefix}_requested"] = limit
        out[f"{section.prefix}_matched"] = len(cand)
        out[f"{section.prefix}_packed"] = len(got)
        if key != "notes":
            out[key] = got
        else:
            packed.extend(got)
            out["notes"] = packed
            out["related_matched"] = len(cand)
            out["related_packed"] = len(got)
            out["notes_packed"] = len(packed) - len(got)
    if retired:
        out["notes_expired_excluded"] = len(retired)
    if rerank:
        # Reported only when enabled so packs built without rerank stay
        # byte-identical to the legacy shape (and digest-compatible).
        out["rerank"] = {"recency_half_life_hours": recency_half_life_hours,
                         "pinned_boost": pinned_boost}
    return out

def _build_pack(db, task_id: str, budget: int, rel_limit: int, rel_scope: str,
                rerank: bool = False, recency_half_life_hours: float = 168.0,
                pinned_boost: float = 0.5, rel_handoffs: int = 0,
                dep_context: int = 0, rel_sessions: int = 0, rel_facts: int = 0,
                rel_semantic: int = 0) -> dict:
    """Assemble the prompt-ready context bundle for a task within a char budget.

    Thin wrapper over the spec-driven `_assemble_pack`: task summary header +
    unsatisfied dependencies, then the live handoff, then live notes
    pinned-first (oldest→newest within each group), then flag-gated sections
    from `_pack_spec()` (related notes, handoffs, dep evidence, sessions,
    facts). With rerank=True the related-note candidates are temporally
    re-scored (see `_rerank_notes`) and the pack reports the rerank parameters
    so recall digests stay exactly recomputable. Every flag-gated output key
    is present only when that flag is used, so packs built without them stay
    byte-identical (and digest-compatible) to the legacy shape.
    """
    return _assemble_pack(db, task_id, budget, rel_limit, rel_scope,
                          rerank=rerank,
                          recency_half_life_hours=recency_half_life_hours,
                          pinned_boost=pinned_boost,
                          section_limits={
                              "related": rel_limit,
                              "related_handoffs": rel_handoffs,
                              "dep_context": dep_context,
                              "related_sessions": rel_sessions,
                              "related_facts": rel_facts,
                              "related_semantic": rel_semantic})

def task_context(args):
    """Pack a prompt-ready context bundle within a character budget.

    Includes a task summary header and unsatisfied dependencies, then the live
    agent handoff (if any), then live notes packed pinned-first (oldest→newest within each group) so critical
    facts survive tight budgets. With --related N, up to N BM25-ranked live
    notes from other tasks (matching this task's title/description/next_action
    text) are appended afterwards within the same budget, each tagged with its
    source task for provenance. With --dep-context N, up to N completed direct
    prerequisites contribute their verified evidence (live handoff + latest
    sealed receipt) so downstream work inherits what upstream proved.
    """
    with conn() as db:
        json_out(_build_pack(db, args.task_id, args.budget,
                             getattr(args, "related", 0),
                             getattr(args, "related_scope", "project"),                             rerank=bool(getattr(args, "rerank", False)),
                             recency_half_life_hours=getattr(args, "recency_half_life_hours", 168.0),
                             pinned_boost=getattr(args, "pinned_boost", 0.5),
                             rel_handoffs=getattr(args, "related_handoffs", 0),
                             dep_context=getattr(args, "dep_context", 0),
                             rel_sessions=getattr(args, "related_sessions", 0),
                             rel_facts=getattr(args, "related_facts", 0),
                             rel_semantic=getattr(args, "related_semantic", 0)))

def _build_recall_bundle(db, task_id: str, agent: str, budget: int, rel_limit: int, rel_scope: str,
                         rerank: bool = False, recency_half_life_hours: float = 168.0,
                         pinned_boost: float = 0.5, rel_handoffs: int = 0,
                         dep_context: int = 0, rel_sessions: int = 0, rel_facts: int = 0,
                         rel_semantic: int = 0) -> dict:
    """Assemble the recall bundle and its deterministic digest (no audit write).

    Shared by `recall` (which audits the digest) and `recall-verify` (which
    recomputes it to test whether a previously recalled context is still fresh).
    """
    agent = (agent or "").strip()
    t = now()
    row = task_row(db, task_id)
    pack = _build_pack(db, task_id, budget, rel_limit, rel_scope or "project",
                       rerank=rerank, recency_half_life_hours=recency_half_life_hours,
                       pinned_boost=pinned_boost, rel_handoffs=rel_handoffs,
                       dep_context=dep_context, rel_sessions=rel_sessions,
                       rel_facts=rel_facts, rel_semantic=rel_semantic)
    lease_live = bool(row["lease_owner"]) and row["lease_expires_at"] > t
    bundle = {
        **pack,
        "agent": agent or None,
        "recalled_at": t,
        "lease": {
            "owner": row["lease_owner"],
            "expires_at": row["lease_expires_at"],
            "epoch": row["lease_epoch"],
            "live": lease_live,
            "held_by_caller": bool(agent) and lease_live and row["lease_owner"] == agent,
        },
        "latest_receipts": [
            {**{k: r[k] for k in ("id", "kind", "created_at")},
             "payload": json.loads(r["payload_json"])}
            for r in db.execute(
                "SELECT id,kind,payload_json,created_at FROM receipts "
                "WHERE task_id=? ORDER BY created_at DESC, id DESC LIMIT 3",
                (task_id,)).fetchall()],
    }
    # Digest covers durable context only — not the recall timestamp — so
    # identical state yields an identical, referenceable digest. The core
    # digest additionally excludes the live-handoff section: an agent recalls
    # first and records its handoff afterwards, so the handoff it writes must
    # not count as drift against its own citation (fleet sweeps compare cores;
    # note/receipt/lease drift still shows up in both).
    digest = hashlib.sha256(json.dumps(
        {k: v for k, v in bundle.items() if k != "recalled_at"},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    bundle["digest"] = digest
    core = hashlib.sha256(json.dumps(
        {k: v for k, v in bundle.items()
         if k not in ("recalled_at", "digest", "handoff", "handoff_packed",
                      "used_chars", "truncated")},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    bundle["core_digest"] = core
    return bundle

def _bundle_sections(bundle: dict) -> dict:
    """Compact per-section manifest of a recall bundle.

    Recorded alongside the digest in `context_recalled` / `session_resumed`
    audit payloads so `recall-diff` can name *which* sections moved since a
    cited recall (notes added/removed, handoff superseded, lease changed…)
    instead of only "something drifted". Derived purely from the bundle
    contents and never hashed into the digest itself, so digests stay
    byte-compatible with pre-manifest recalls.
    """
    notes = bundle.get("notes", [])
    h = bundle.get("handoff")
    sections = {
        "task": {k: bundle["task"].get(k)
                 for k in ("status", "priority", "due_at", "next_action", "blocked_reason")},
        "deps": [d["id"] for d in bundle.get("unsatisfied_dependencies", [])],
        "handoff": h["id"] if h else None,
        "notes": [{"id": n["id"], "pinned": bool(n["pinned"]), "expired": bool(n.get("expired"))}
                  for n in notes if not n.get("related")],
        "related_notes": [n["id"] for n in notes if n.get("related")],
        "lease": {k: bundle["lease"][k]
                  for k in ("owner", "epoch", "expires_at", "live")},
        "receipts": [r["id"] for r in bundle.get("latest_receipts", [])],
    }
    if "related_handoffs" in bundle:
        # Flag-gated section: manifests recorded without the flag stay
        # byte-identical to their pre-feature shape.
        sections["related_handoffs"] = [x["id"] for x in bundle["related_handoffs"]]
    if "dep_context" in bundle:
        sections["dep_context"] = [e["id"] for e in bundle["dep_context"]]
    if "related_sessions" in bundle:
        sections["related_sessions"] = [
            f"{x['session_id']}:{x['seq']}" for x in bundle["related_sessions"]]
    if "related_facts" in bundle:
        sections["related_facts"] = [x["id"] for x in bundle["related_facts"]]
    if "related_semantic" in bundle:
        sections["related_semantic"] = [x["id"] for x in bundle["related_semantic"]]
    return sections

def _diff_sections(old: dict, new: dict) -> dict:
    """Section-level diff between a recorded manifest and current state."""
    changes = {}
    tf = {k: {"from": old["task"].get(k), "to": new["task"].get(k)}
          for k in old["task"] if old["task"].get(k) != new["task"].get(k)}
    if tf:
        changes["task"] = tf
    od, nd = set(old["deps"]), set(new["deps"])
    if od != nd:
        changes["dependencies"] = {"satisfied": sorted(od - nd), "added": sorted(nd - od)}
    if old["handoff"] != new["handoff"]:
        changes["handoff"] = {"from": old["handoff"], "to": new["handoff"]}
    on = {n["id"]: n for n in old["notes"]}
    nn = {n["id"]: n for n in new["notes"]}
    nc = {}
    added = sorted(set(nn) - set(on))
    removed = sorted(set(on) - set(nn))
    if added:
        nc["added"] = added
    if removed:
        nc["removed"] = removed
    flags = {i: {f: [on[i][f], nn[i][f]]}
             for i in set(on) & set(nn)
             for f in ("pinned", "expired") if on[i][f] != nn[i][f]}
    if flags:
        nc["flags"] = flags
    if nc:
        changes["notes"] = nc
    orr, nrr = set(old["related_notes"]), set(new["related_notes"])
    if orr != nrr:
        changes["related_notes"] = {"added": sorted(nrr - orr), "removed": sorted(orr - nrr)}
    # Flag-gated section: absent on either side means that bundle was recalled
    # without --related-handoffs; .get keeps legacy manifests diffable.
    orh, nrh = set(old.get("related_handoffs") or []), set(new.get("related_handoffs") or [])
    if orh != nrh:
        changes["related_handoffs"] = {"added": sorted(nrh - orh), "removed": sorted(orh - nrh)}
    odc, ndc = set(old.get("dep_context") or []), set(new.get("dep_context") or [])
    if odc != ndc:
        changes["dep_context"] = {"added": sorted(ndc - odc), "removed": sorted(odc - ndc)}
    ors, nrs = set(old.get("related_sessions") or []), set(new.get("related_sessions") or [])
    if ors != nrs:
        changes["related_sessions"] = {"added": sorted(nrs - ors), "removed": sorted(ors - nrs)}
    orf, nrf = set(old.get("related_facts") or []), set(new.get("related_facts") or [])
    if orf != nrf:
        changes["related_facts"] = {"added": sorted(nrf - orf), "removed": sorted(orf - nrf)}
    osem, nsem = set(old.get("related_semantic") or []), set(new.get("related_semantic") or [])
    if osem != nsem:
        changes["related_semantic"] = {"added": sorted(nsem - osem), "removed": sorted(osem - nsem)}
    if old["lease"] != new["lease"]:
        changes["lease"] = {"from": old["lease"], "to": new["lease"]}
    orec, nrec = set(old["receipts"]), set(new["receipts"])
    if orec != nrec:
        changes["receipts"] = {"added": sorted(nrec - orec), "removed": sorted(orec - nrec)}
    return changes

def recall(args):
    """Session bootstrap: everything an agent needs before acting on a task.

    Bundles the context pack (task header, deps, live handoff, notes) with the
    lease state — including whether the calling agent holds it — and the latest
    receipts, then seals the bundle with a deterministic `digest`. The digest is
    audited (`context_recalled`) so an agent can prove exactly which context it
    recalled before acting; handoffs and completions can cite it downstream via
    `--recall-digest`, and `recall-verify` re-checks its freshness.
    Identical state yields an identical digest (stable across repeated calls).
    """
    with conn() as db:
        bundle = _build_recall_bundle(db, args.task_id,
                                      getattr(args, "agent", "") or "",
                                      args.budget, getattr(args, "related", 0),
                                      getattr(args, "related_scope", "project"),
                                      rerank=bool(getattr(args, "rerank", False)),
                                      recency_half_life_hours=getattr(args, "recency_half_life_hours", 168.0),
                                      pinned_boost=getattr(args, "pinned_boost", 0.5),
                                      rel_handoffs=getattr(args, "related_handoffs", 0),
                                      dep_context=getattr(args, "dep_context", 0),
                                      rel_sessions=getattr(args, "related_sessions", 0),
                                      rel_facts=getattr(args, "related_facts", 0),
                                      rel_semantic=getattr(args, "related_semantic", 0))
        # Record the bundle parameters alongside the digest so fleet sweeps
        # (ops.py recall-stale) can recompute the digest exactly as recalled.
        audit(db, "task", args.task_id, "context_recalled",
              {"agent": bundle["agent"], "digest": bundle["digest"],
               "core_digest": bundle["core_digest"],
               "budget": args.budget, "related": getattr(args, "related", 0),
               "related_scope": getattr(args, "related_scope", "project"),
               "rerank": bool(getattr(args, "rerank", False)),
               "recency_half_life_hours": getattr(args, "recency_half_life_hours", 168.0),
               "pinned_boost": getattr(args, "pinned_boost", 0.5),
               "related_handoffs": getattr(args, "related_handoffs", 0),
               "dep_context": getattr(args, "dep_context", 0),
                "related_sessions": getattr(args, "related_sessions", 0),
               "related_facts": getattr(args, "related_facts", 0),
               "related_semantic": getattr(args, "related_semantic", 0),
               "sections": _bundle_sections(bundle)})
    json_out(bundle)

def recall_verify(args):
    """Freshness check for a previously recalled context digest.

    Recomputes the current recall bundle for the task (same algorithm as
    `recall`, without auditing) and compares it to the caller's digest:
    `fresh` means nothing durable has changed since that recall — notes,
    handoffs, lease state, receipts, deps all match. A stale result carries
    the new `current_digest` so the agent can re-`recall` before acting.
    Exit code stays 0 either way; callers branch on the JSON.
    """
    digest = _require_digest(args.digest, "--digest")
    if not digest:
        raise SystemExit("--digest is required")
    with conn() as db:
        task_row(db, args.task_id)
        bundle = _build_recall_bundle(db, args.task_id,
                                      getattr(args, "agent", "") or "",
                                      args.budget, getattr(args, "related", 0),
                                      getattr(args, "related_scope", "project"),
                                      rerank=bool(getattr(args, "rerank", False)),
                                      recency_half_life_hours=getattr(args, "recency_half_life_hours", 168.0),
                                      pinned_boost=getattr(args, "pinned_boost", 0.5),
                                      rel_handoffs=getattr(args, "related_handoffs", 0),
                                      dep_context=getattr(args, "dep_context", 0),
                                      rel_sessions=getattr(args, "related_sessions", 0),
                                      rel_facts=getattr(args, "related_facts", 0),
                                      rel_semantic=getattr(args, "related_semantic", 0))
    json_out({"ok": True, "task_id": args.task_id,
              "fresh": bundle["digest"] == digest,
              "recalled_digest": digest, "current_digest": bundle["digest"]})

def recall_diff(args):
    """Explain *what* moved since a previously recalled context digest.

    `recall-verify` answers "is my context still fresh?" with a boolean;
    this answers the follow-up an agent actually acts on: "what changed?".
    It looks up the audited recall/resume event that produced the cited
    digest (the event stores a per-section manifest of the original bundle),
    recomputes the current bundle exactly as it was originally recalled
    (recorded parameters), and diffs section by section:

    - `task` — status/priority/due_at/next_action/blocked_reason field moves
    - `dependencies` — satisfied vs newly-added prerequisite ids
    - `handoff` — the live resume point was recorded/superseded
    - `notes` — added/removed note ids plus pinned/expired flag flips
    - `related_notes` — cross-task retrieval candidates that appeared/left
    - `lease` — owner/epoch/expiry/liveness changes
    - `receipts` — evidence receipts posted or rotated out of the top 3

    A digest with no audited provenance reports `unproven_recall_digest`;
    events recorded before section manifests existed degrade to the plain
    fresh/stale verdict (`legacy_event: true`) instead of guessing.
    Exit code stays 0 either way; callers branch on the JSON.
    """
    digest = _require_digest(args.digest, "--digest")
    if not digest:
        raise SystemExit("--digest is required")
    with conn() as db:
        task_row(db, args.task_id)
        ev = db.execute(
            "SELECT payload_json FROM audit_events WHERE entity_type='task' AND entity_id=? "
            "AND action IN ('context_recalled','session_resumed') "
            "AND payload_json LIKE ? ORDER BY id DESC LIMIT 1",
            (args.task_id, '%"digest": "' + digest + '"%')).fetchone()
        out = {"ok": True, "task_id": args.task_id, "recalled_digest": digest}
        if not ev:
            out["state"] = "unproven_recall_digest"
            json_out(out)
            return
        payload = json.loads(ev["payload_json"])
        params = {k: payload.get(k) for k in ("budget", "related", "related_scope")}
        # Legacy events predate --related-handoffs; absent means the original
        # bundle was built without it, which recomputes the digest exactly.
        rel_handoffs = payload.get("related_handoffs") or 0
        dep_ctx_n = payload.get("dep_context") or 0
        # Flag-gated sections added after the provenance loop; absent means
        # the original bundle was built without them, which recomputes the
        # digest exactly.
        rel_sess_n = payload.get("related_sessions") or 0
        rel_facts_n = payload.get("related_facts") or 0
        rel_sema_n = payload.get("related_semantic") or 0
        rerank = bool(payload.get("rerank"))
        half_life = payload.get("recency_half_life_hours")
        boost = payload.get("pinned_boost")
        if any(v is None for v in params.values()) or (rerank and (half_life is None or boost is None)):
            out["state"] = "unknown_recall_params"
            json_out(out)
            return
        bundle = _build_recall_bundle(db, args.task_id, payload.get("agent") or "",
                                      params["budget"], params["related"], params["related_scope"],
                                      rerank=rerank,
                                      recency_half_life_hours=168.0 if half_life is None else half_life,
                                      pinned_boost=0.5 if boost is None else boost,
                                      rel_handoffs=rel_handoffs,
                                      dep_context=dep_ctx_n,
                                      rel_sessions=rel_sess_n,
                                      rel_facts=rel_facts_n,
                                      rel_semantic=rel_sema_n)
        fresh = bundle["digest"] == digest
        out["current_digest"] = bundle["digest"]
        out["fresh"] = fresh
        sections = payload.get("sections")
        if sections is None:
            # Pre-manifest event: the digest math still works, but there is no
            # per-section record to diff against — report the verdict only.
            out["state"] = "fresh" if fresh else "stale"
            out["legacy_event"] = True
        else:
            changes = _diff_sections(sections, _bundle_sections(bundle))
            out["state"] = "fresh" if fresh else "stale"
            out["unchanged"] = not changes
            out["changes"] = changes
            out["sections_changed"] = sorted(changes.keys())
        json_out(out)

def resume(args):
    """Idempotent cross-agent recovery: recreate a killed session in one call.

    This is the recovery half of the handoff protocol. Given the latest
    durable handoff (via the recall bundle), an incoming agent calls `resume`
    instead of hand-orchestrating claim + recall:

    - live lease held by the caller → no mutation, bundle returned
      (`action: already_held`) — calling resume twice is safe;
    - expired or absent lease → claimed atomically for the caller
      (`action: claimed`), honoring per-owner caps and dep/blocked guards;
    - live lease held by someone else → rejected; ask them to `transfer`.

    The response embeds the full sealed recall bundle (digest, lease state,
    receipts) so the session restarts against exactly the context it will be
    held to, and every resume is audited as `session_resumed`.
    """
    agent = (args.agent or "").strip()
    if not agent:
        raise SystemExit("--agent is required")
    with conn() as db:
        row = task_row(db, args.task_id)
        if row["status"] in TERMINAL_STATUSES:
            raise SystemExit(f"cannot resume terminal task: {row['status']}")
        if row["status"] == "blocked":
            raise SystemExit("task is blocked: %s; unblock before resuming"
                             % (row["blocked_reason"] or "no reason recorded"))
        pending = unsatisfied_deps(db, args.task_id)
        if pending:
            raise SystemExit("unsatisfied dependencies: " + ", ".join(f"{d['id']}({d['status']})" for d in pending))
        t = now()
        live = bool(row["lease_owner"]) and row["lease_expires_at"] > t
        if live and row["lease_owner"] != agent:
            raise SystemExit(f"lease owned by {row['lease_owner']}; ask them to transfer it before resuming")
        if live:
            action = "already_held"
        else:
            cap = resolve_max_active(args)
            acquired, exp, epoch = _acquire(db, args.task_id, agent, args.minutes, cap)
            if not acquired:
                raise SystemExit(_explain_acquire_failure(db, args.task_id, agent, cap))
            db.execute("INSERT INTO heartbeats(task_id,owner,state,at,note) VALUES(?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET owner=excluded.owner,state=excluded.state,at=excluded.at,note=excluded.note",
                       (args.task_id, agent, "claimed", now(), "resumed session"))
            audit(db, "task", args.task_id, "claimed",
                  {"owner": agent, "lease_expires_at": exp, "lease_epoch": epoch, "via": "resume"})
            action = "claimed"
        bundle = _build_recall_bundle(db, args.task_id, agent, args.budget,
                                      getattr(args, "related", 0),
                                      getattr(args, "related_scope", "project"),
                                      rerank=bool(getattr(args, "rerank", False)),
                                      recency_half_life_hours=getattr(args, "recency_half_life_hours", 168.0),
                                      pinned_boost=getattr(args, "pinned_boost", 0.5),
                                      rel_handoffs=getattr(args, "related_handoffs", 0),
                                      dep_context=getattr(args, "dep_context", 0),
                                      rel_sessions=getattr(args, "related_sessions", 0),
                                      rel_facts=getattr(args, "related_facts", 0),
                                      rel_semantic=getattr(args, "related_semantic", 0))
        # session_resumed doubles as recall provenance: the digest is recorded
        # with its bundle parameters so a handoff citing a resume digest passes
        # the handoff-check lint and fleet sweeps can recompute it exactly.
        audit(db, "task", args.task_id, "session_resumed",
              {"agent": agent, "action": action, "digest": bundle["digest"],
               "core_digest": bundle["core_digest"],
               "budget": args.budget, "related": getattr(args, "related", 0),
               "related_scope": getattr(args, "related_scope", "project"),
               "rerank": bool(getattr(args, "rerank", False)),
               "recency_half_life_hours": getattr(args, "recency_half_life_hours", 168.0),
               "pinned_boost": getattr(args, "pinned_boost", 0.5),
               "related_handoffs": getattr(args, "related_handoffs", 0),
               "dep_context": getattr(args, "dep_context", 0),
                "related_sessions": getattr(args, "related_sessions", 0),
               "related_facts": getattr(args, "related_facts", 0),
               "related_semantic": getattr(args, "related_semantic", 0),
               "sections": _bundle_sections(bundle)})
    json_out({"ok": True, "task_id": args.task_id, "action": action, **bundle})

def search_notes(args):
    """Keyword retrieval over note content with kind/project/status filters via the task join.

    With --rank (and an FTS5-capable SQLite), results are BM25-ranked via the
    notes_fts index and carry a `score`; otherwise it falls back to substring
    LIKE matching with identical output shape minus the score.
    With --rerank, results are re-scored by the temporal hybrid
    (`_rerank_notes`): lexical match × recency decay + pinned bonus. Each row
    gains `rank_score` and ordering follows it — use this when stale facts
    outranking fresh ones would mislead the caller.
    """
    ranked = bool(getattr(args, "rank", False))
    do_rerank = bool(getattr(args, "rerank", False))
    half_life = getattr(args, "recency_half_life_hours", 168.0)
    boost = getattr(args, "pinned_boost", 0.5)
    include_expired = bool(getattr(args, "include_expired", False))
    t = now()
    with conn() as db:
        # Retired (expired unpinned) notes are hidden by default; pinned notes
        # are exempt everywhere per the TTL immutability rule.
        live_sql = "" if include_expired else " AND (n.expires_at='' OR n.expires_at>? OR n.pinned=1)"
        live_val = () if include_expired else (t,)
        if ranked and _fts_ready(db):
            match = _fts_query(args.query)
            if not match:
                json_out([])
                return
            clauses = ["notes_fts MATCH ?", "n.superseded_by=''"]
            vals = [match]
            if args.kind:
                if args.kind not in NOTE_KINDS:
                    raise SystemExit(f"invalid note kind: {args.kind} (choose from {sorted(NOTE_KINDS)})")
                clauses.append("n.kind=?"); vals.append(args.kind)
            if args.project:
                clauses.append("t.project=?"); vals.append(args.project)
            if args.status:
                clauses.append("t.status=?"); vals.append(args.status)
            rows = [dict(r) for r in db.execute(
                "SELECT n.id,n.task_id,t.project,n.kind,n.content,n.source,n.created_at,"
                "n.pinned,bm25(notes_fts) AS score FROM notes_fts f "
                "JOIN notes n ON n.rowid=f.rowid JOIN tasks t ON t.id=n.task_id "
                "WHERE " + " AND ".join(clauses) + live_sql +
                " ORDER BY score LIMIT ?", (*vals, *live_val, args.limit)).fetchall()]
            json_out(_rerank_notes(rows, half_life, boost) if do_rerank else rows)
            return
        pat = "%" + args.query + "%"
        clauses = ["n.content LIKE ?"]
        vals = [pat]
        if args.kind:
            if args.kind not in NOTE_KINDS:
                raise SystemExit(f"invalid note kind: {args.kind} (choose from {sorted(NOTE_KINDS)})")
            clauses.append("n.kind=?"); vals.append(args.kind)
        if args.project:
            clauses.append("t.project=?"); vals.append(args.project)
        if args.status:
            clauses.append("t.status=?"); vals.append(args.status)
        rows = [dict(r) for r in db.execute(
            "SELECT n.id,n.task_id,t.project,n.kind,n.content,n.source,n.created_at,n.pinned "
            "FROM notes n JOIN tasks t ON t.id=n.task_id WHERE n.superseded_by='' AND " +
            " AND ".join(clauses) + live_sql + " ORDER BY n.created_at DESC LIMIT ?",
            (*vals, *live_val, args.limit)).fetchall()]
    json_out(_rerank_notes(rows, half_life, boost) if do_rerank else rows)

def search_handoffs(args):
    """Fleet-wide keyword retrieval over the handoff protocol.

    The per-task commands (`handoffs`, `handoff-current`) answer "what is the
    resume point of this task"; this answers the cross-task question an agent
    or operator actually starts from: "what decided/did work like this before?"
    Live (non-superseded) handoffs are searched by default — superseded ones
    are history, not resume points; --all includes them (tagged with
    `superseded_by`). With --rank (and an FTS5-capable SQLite), results are
    BM25-ranked over objective/status/from_agent/to_agent via the handoffs_fts
    index and carry a `score`; otherwise substring LIKE matching with identical
    output shape minus the score. Each row joins its task's project/title so
    hits are triageable without a follow-up `show`.
    """
    include_superseded = bool(getattr(args, "all", False))
    clauses, vals = [], []
    if not include_superseded:
        clauses.append("h.superseded_by=''")
    for col in ("task", "from_agent", "to_agent"):
        v = getattr(args, col, "") or ""
        if v:
            clauses.append(f"h.{col}=?")
            vals.append(v)
    if getattr(args, "project", ""):
        clauses.append("t.project=?")
        vals.append(args.project)
    limit = max(0, args.limit)
    with conn() as db:
        if getattr(args, "rank", False) and _handoffs_fts_ready(db):
            match = _fts_query(args.query)
            if not match:
                json_out([])
                return
            where = " AND ".join(["handoffs_fts MATCH ?", *clauses])
            rows = [dict(r) for r in db.execute(
                "SELECT h.id,h.task_id,t.project,t.title AS task_title,h.from_agent,"
                "h.to_agent,h.status,h.objective,h.commit_ref,h.created_at,"
                "h.superseded_by,bm25(handoffs_fts) AS score "
                "FROM handoffs_fts f JOIN handoffs h ON h.rowid=f.rowid "
                "JOIN tasks t ON t.id=h.task_id WHERE " + where +
                " ORDER BY score LIMIT ?", [match, *vals, limit]).fetchall()]
            json_out(rows)
            return
        pat = "%" + args.query + "%"
        clauses.append("(h.objective LIKE ? OR h.status LIKE ? OR h.from_agent LIKE ? "
                       "OR h.to_agent LIKE ? OR h.commit_ref LIKE ?)")
        rows = [dict(r) for r in db.execute(
            "SELECT h.id,h.task_id,t.project,t.title AS task_title,h.from_agent,"
            "h.to_agent,h.status,h.objective,h.commit_ref,h.created_at,"
            "            h.superseded_by FROM handoffs h JOIN tasks t ON t.id=h.task_id WHERE " +
            " AND ".join(clauses) + " ORDER BY h.created_at DESC, h.rowid DESC LIMIT ?",
            [*vals, pat, pat, pat, pat, pat, limit]).fetchall()]
    json_out(rows)

# ---------------------------------------------------------------------------
# Temporal fact graph (the sidecar): fleet-level evolving facts and
# relationships with validity windows. Where notes are task-scoped prose, the
# fact graph is the machine-queryable layer: `subject predicate object`
# triples (e.g. `service-auth uses postgres-14`) that agents assert with
# provenance and retract or let expire as the world changes. Validity windows
# make time a first-class query dimension — retrieval packs only what is
# currently true, while history stays auditable.
# ---------------------------------------------------------------------------

def _valid_fact_token(value: str, flag: str) -> str:
    """Validate a subject/predicate/object token.

    The tag charset keeps facts safe inside CLI flags and LIKE filters across
    every agent adapter — and because credential shapes (mixed case, spaces,
    assignment syntax) cannot survive it, the secret guard is structural here
    rather than pattern-based.
    """
    return _valid_tag(value)

def _fact_live_sql(alias: str = "") -> str:
    c = f"{alias}." if alias else ""
    return f"({c}valid_until='' OR {c}valid_until>?)"

def fact_assert(args):
    """Assert a temporal fact: `subject predicate object` with provenance.

    An identical triple that is still within its validity window deduplicates
    to the existing row instead of growing the store; asserting after expiry
    records a fresh row (the old window stays as history). Every assertion is
    audited (`fact_asserted` / `fact_deduplicated`).
    """
    subject = _valid_fact_token(args.subject, "--subject")
    predicate = _valid_fact_token(args.predicate, "--predicate")
    obj = _valid_fact_token(args.object, "--object")
    source = (args.source or "").strip()
    task_id = getattr(args, "task", "") or ""
    valid_until = _expires_at(_ttl_hours(getattr(args, "valid_hours", None)))
    t = now()
    with conn() as db:
        if task_id:
            task_row(db, task_id)  # must exist: provenance points at real work
        existing = db.execute(
            "SELECT * FROM facts WHERE subject=? AND predicate=? AND object=? AND "
            + _fact_live_sql() + " ORDER BY created_at DESC LIMIT 1",
            (subject, predicate, obj, t)).fetchone()
        if existing:
            audit(db, "fact", existing["id"], "fact_deduplicated",
                  {"subject": subject, "predicate": predicate, "object": obj,
                   "source": source})
            json_out({**dict(existing), "deduplicated": True})
            return
        fid = uuid.uuid4().hex
        db.execute(
            "INSERT INTO facts(id,subject,predicate,object,source,task_id,valid_from,valid_until,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (fid, subject, predicate, obj, source, task_id, t, valid_until, t))
        audit(db, "fact", fid, "fact_asserted",
              {"subject": subject, "predicate": predicate, "object": obj,
               "source": source, "task_id": task_id, "valid_until": valid_until})
        row = dict(db.execute("SELECT * FROM facts WHERE id=?", (fid,)).fetchone())
    json_out({**row, "deduplicated": False})

def fact_retract(args):
    """Close a fact's validity window now (idempotent on already-closed rows)."""
    with conn() as db:
        row = db.execute("SELECT * FROM facts WHERE id=?", (args.fact_id,)).fetchone()
        if not row:
            raise SystemExit(f"unknown fact id: {args.fact_id}")
        t = now()
        if row["valid_until"] and row["valid_until"] <= t:
            json_out({**dict(row), "already_closed": True})
            return
        db.execute("UPDATE facts SET valid_until=? WHERE id=?", (t, args.fact_id))
        audit(db, "fact", args.fact_id, "fact_retracted",
              {"reason": args.reason or "", "subject": row["subject"],
               "predicate": row["predicate"], "object": row["object"]})
        out = dict(db.execute("SELECT * FROM facts WHERE id=?", (args.fact_id,)).fetchone())
    json_out({**out, "retracted": True})

def list_facts(args):
    """Query the fact graph. Default returns only currently-valid triples;
    --all includes closed windows tagged with their liveness."""
    t = now()
    clauses, vals = [], []
    for flag, col in (("subject", "subject"), ("predicate", "predicate"),
                      ("object", "object")):
        v = getattr(args, flag, "")
        if v:
            clauses.append(f"{col}=?")
            vals.append(_valid_fact_token(v, f"--{flag}"))
    if getattr(args, "task", ""):
        clauses.append("task_id=?")
        vals.append(args.task)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    live_sql = "" if getattr(args, "all", False) else \
        (" AND " if clauses else " WHERE ") + _fact_live_sql()
    live_val = [] if getattr(args, "all", False) else [t]
    with conn() as db:
        rows = [dict(r) for r in db.execute(
            "SELECT * FROM facts" + where + live_sql +
            " ORDER BY created_at DESC, id DESC LIMIT ?",
            [*vals, *live_val, max(0, args.limit)]).fetchall()]
    for r in rows:
        r["live"] = not (r["valid_until"] and r["valid_until"] <= t)
    json_out(rows)

def search_facts(args):
    """Keyword retrieval over the fact graph (BM25 via FTS5 with --rank,
    substring LIKE fallback otherwise). Live triples only unless --all."""
    limit = max(0, args.limit)
    live_only = not getattr(args, "all", False)
    t = now()
    live_sql = (" AND " + _fact_live_sql("f")) if live_only else ""
    live_val = [t] if live_only else []
    cols = ("SELECT f.id,f.subject,f.predicate,f.object,f.source,f.task_id,"
            "f.valid_from,f.valid_until,f.created_at")
    with conn() as db:
        if getattr(args, "rank", False) and _facts_fts_ready(db):
            match = _fts_query(args.query)
            if not match:
                json_out([])
                return
            rows = [dict(r) for r in db.execute(
                cols + ",bm25(facts_fts) AS score FROM facts_fts x "
                "JOIN facts f ON f.rowid=x.rowid WHERE facts_fts MATCH ?" + live_sql +
                " ORDER BY score LIMIT ?", [match, *live_val, limit]).fetchall()]
        else:
            pat = "%" + args.query + "%"
            rows = [dict(r) for r in db.execute(
                cols + " FROM facts f WHERE (f.subject LIKE ? OR f.predicate LIKE ? "
                "OR f.object LIKE ? OR f.source LIKE ?)" + live_sql +
                " ORDER BY f.created_at DESC, f.id DESC LIMIT ?",
                [pat, pat, pat, pat, *live_val, limit]).fetchall()]
    for r in rows:
        r["live"] = not (r["valid_until"] and r["valid_until"] <= t)
    json_out(rows)

def _related_fact_candidates(db, task_id: str, text: str, limit: int) -> list:
    """Fleet-level temporal facts whose tokens match this task's text.

    The sidecar's context-pack surface: facts asserted anywhere in the fleet
    that look relevant to this work pack after dep context and sessions.
    Only currently-valid triples are candidates — a pack must carry what is
    true *now*. Deterministic ordering (valid_from DESC, id) with no relevance
    score emitted, mirroring related handoffs: BM25 scores drift whenever any
    fact joins the index, which would falsely stale sealed digests.
    """
    if limit <= 0 or not text.strip():
        return []
    toks = []
    for raw in text.split():
        tok = raw.replace('"', "")
        if tok and any(c.isalnum() for c in tok):
            toks.append(tok)
    if not toks:
        return []
    t = now()
    cols = ("SELECT f.id,f.subject,f.predicate,f.object,f.valid_from,f.valid_until,f.source")
    live_sql = " AND " + _fact_live_sql("f")
    order = " ORDER BY f.valid_from DESC, f.id ASC LIMIT ?"
    vals_tail = [t, limit]
    if _facts_fts_ready(db):
        match = " OR ".join('"%s"' % tk for tk in toks)
        sql = (cols + " FROM facts_fts x JOIN facts f ON f.rowid=x.rowid "
               "WHERE facts_fts MATCH ?" + live_sql + order)
        vals = [match, *vals_tail]
    else:
        likes = " OR ".join("(f.subject LIKE ? OR f.predicate LIKE ? OR f.object LIKE ?)"
                            for _ in toks)
        sql = cols + " FROM facts f WHERE (" + likes + ")" + live_sql + order
        vals = [v for tk in toks for v in ("%"+tk+"%", "%"+tk+"%", "%"+tk+"%")] + vals_tail
    return [dict(r) for r in db.execute(sql, vals).fetchall()]

# ---------------------------------------------------------------------------
# Session ingestion: a read-only adapter over external agent session stores
# (Hermes/Claude Code-style JSONL transcripts). The ingested rows are a
# disposable, rebuildable cache — never execution truth — and the source
# stores are only ever opened for reading. Raw conversation becomes searchable
# shared context through the same FTS/context-pack protocol as notes and
# handoffs, with provenance (source/profile/project/session/role) on every
# message and the shared-memory secret guard applied before anything lands.
# ---------------------------------------------------------------------------

SESSION_ROLES = ("user", "assistant")
SESSION_SNIPPET_CHARS = 400
DEFAULT_SESSION_MAX_FILE_BYTES = 64 * 1024 * 1024

def _session_row_id(source: str, path: str) -> str:
    """Stable cache-row id for one source store file on this machine."""
    return hashlib.sha256(f"{source}\x00{path}".encode()).hexdigest()

def _session_discover(root: Path) -> list:
    """Candidate transcript files under an explicit root, deterministically.

    Symlinks are never followed (and symlinked files never indexed) so a
    planted link cannot smuggle unrelated private context into shared memory.
    """
    out = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dp = Path(dirpath)
        dirnames[:] = sorted(d for d in dirnames if not (dp / d).is_symlink())
        for name in sorted(filenames):
            p = dp / name
            if name.endswith(".jsonl") and not p.is_symlink() and p.is_file():
                out.append(p)
    return out

def _parse_session_file(path: Path, max_bytes: int) -> dict:
    """Read-only parse of one JSONL transcript into normalized messages.

    Recognizes the Claude Code shape (`{"type":"user"|"assistant",
    "message":{"role","content": str | [{"type":"text","text":...}]},
    "timestamp": iso}`) and the generic `{role, content, timestamp}` line;
    everything else (tool calls/results, system lines, unknown vendor shapes)
    is counted and skipped rather than guessed at, so an unstable format
    degrades into an honest report instead of silent garbage. Consecutive
    exact-duplicate lines (retry artifacts) collapse into one message.
    """
    st = path.stat()
    out = {"status": "indexed", "messages": [], "tool_results_skipped": 0,
           "malformed_lines": 0, "duplicates_collapsed": 0,
           "size_bytes": st.st_size}
    if st.st_size > max_bytes:
        out["status"] = "too_large"
        return out
    try:
        text = path.read_text(errors="replace")
    except OSError as e:
        out.update(status="error", error=str(e))
        return out
    prev_hash = ""
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            out["malformed_lines"] += 1
            continue
        at = ""
        for k in ("timestamp", "ts", "at", "created_at"):
            v = obj.get(k) if isinstance(obj, dict) else None
            if isinstance(v, str) and v:
                try:
                    at = _normalize_iso(v, "timestamp")
                except SystemExit:
                    at = ""
                break
        msg = obj.get("message") if isinstance(obj, dict) and isinstance(obj.get("message"), dict) else obj
        role = ""
        content = None
        if isinstance(msg, dict):
            role = str(msg.get("role") or obj.get("type") or "").lower()
            content = msg.get("content")
        non_text = 0
        if isinstance(content, list):
            texts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text" and isinstance(item.get("text"), str):
                    texts.append(item["text"])
                else:
                    non_text += 1
            content = "\n".join(t for t in texts if t.strip())
        if not isinstance(content, str):
            out["malformed_lines"] += 1
            continue
        if role not in SESSION_ROLES or not content.strip():
            # Tool calls/results, system lines, and unrecognized roles are
            # agent/tool output — recorded as a count, never stored.
            out["tool_results_skipped"] += 1 + non_text
            continue
        out["tool_results_skipped"] += non_text
        ch = hashlib.sha256((role + "\x00" + content).encode()).hexdigest()
        if ch == prev_hash:
            out["duplicates_collapsed"] += 1
            continue
        prev_hash = ch
        out["messages"].append({"role": role, "content": content, "at": at})
    if not out["messages"]:
        out["status"] = "unsupported"
    stamps = sorted(m["at"] for m in out["messages"] if m["at"])
    out["first_at"] = stamps[0] if stamps else ""
    out["last_at"] = stamps[-1] if stamps else ""
    return out

def _session_file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def _session_plan(args) -> dict:
    """Shared discovery/plan core for session-scan and session-ingest."""
    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"--root is not a directory: {args.root}")
    source = (getattr(args, "source", "") or "").strip()
    if getattr(args, "apply", False) and not source:
        raise SystemExit("--source is required")
    since = _normalize_iso(getattr(args, "since", ""), "--since")
    max_bytes = max(1, int(getattr(args, "max_file_bytes", DEFAULT_SESSION_MAX_FILE_BYTES)))
    profile = (getattr(args, "profile", "") or "").strip()
    project = (getattr(args, "project", "") or "").strip()
    files = []
    with conn() as db:
        for p in _session_discover(root):
            rel = str(p.relative_to(root))
            st = p.stat()
            entry = {"path": rel, "session_id": p.stem, "size_bytes": st.st_size}
            if since and datetime.fromtimestamp(st.st_mtime, timezone.utc).replace(microsecond=0).isoformat() < since:
                entry.update(status="skipped_older_than")
                files.append(entry)
                continue
            parsed = _parse_session_file(p, max_bytes)
            entry.update(status=parsed["status"],
                         message_count=len(parsed["messages"]),
                         tool_results_skipped=parsed["tool_results_skipped"],
                         malformed_lines=parsed["malformed_lines"],
                         duplicates_collapsed=parsed["duplicates_collapsed"],
                         first_at=parsed.get("first_at", ""), last_at=parsed.get("last_at", ""))
            if "error" in parsed:
                entry["error"] = parsed["error"]
            row_id = _session_row_id(source, str(p))
            row = db.execute("SELECT file_hash FROM sessions WHERE id=?", (row_id,)).fetchone()
            fh = _session_file_hash(p)
            entry["unchanged"] = bool(row) and row["file_hash"] == fh
            if parsed["status"] == "indexed":
                entry["secret_kinds"] = sorted({f["kind"] for m in parsed["messages"]
                                                for f in _secret_findings(m["content"])})
                files.append(entry)
                files[-1]["_parsed"] = parsed
                files[-1]["_row_id"] = row_id
                files[-1]["_file_hash"] = fh
            else:
                files.append(entry)
    plan = {"source": source, "profile": profile, "project": project, "root": str(root),
            "since": since, "files": [{k: v for k, v in f.items() if not k.startswith("_")} for f in files]}
    plan["totals"] = {
        "discovered": len(files),
        "indexable": sum(1 for f in files if f["status"] == "indexed"),
        "unchanged": sum(1 for f in files if f.get("unchanged")),
        "unsupported": sum(1 for f in files if f["status"] == "unsupported"),
        "too_large": sum(1 for f in files if f["status"] == "too_large"),
        "errored": sum(1 for f in files if f["status"] == "error"),
        "skipped_older_than": sum(1 for f in files if f["status"] == "skipped_older_than"),
        "messages": sum(f.get("message_count", 0) for f in files),
        "tool_results_skipped": sum(f.get("tool_results_skipped", 0) for f in files),
        "malformed_lines": sum(f.get("malformed_lines", 0) for f in files),
        "duplicates_collapsed": sum(f.get("duplicates_collapsed", 0) for f in files),
        "secret_kinds": sorted({k for f in files for k in f.get("secret_kinds", [])}),
    }
    return plan, files

def session_scan(args):
    """Redacted dry-run inventory of discoverable session transcripts.

    Read-only by construction: nothing is written to the control plane and
    message content never leaves this process — the report carries counts and
    kind-only secret findings so an operator can decide what may be ingested
    before any bytes move.
    """
    plan, _ = _session_plan(args)
    plan["dry_run"] = True
    json_out(plan)

def session_ingest(args):
    """Incrementally index external agent sessions into the disposable cache.

    Without --apply this is exactly the redacted inventory (`session-scan`)
    plus what would change. With --apply, each new or changed transcript is
    re-indexed atomically (delete+insert keyed by the file's sha256, so
    interrupted runs resume by simply re-running and unchanged files cost one
    hash read). The shared-memory secret guard applies to every message before
    anything lands: refuse by default, --redact stores [REDACTED:<kind>]
    copies, --allow-secret is audited. Source stores are opened read-only and
    never mutated; raw conversation stays derived cache, never execution truth.
    """
    apply_mode = bool(getattr(args, "apply", False))
    plan, files = _session_plan(args)
    indexable = [f for f in files if f["status"] == "indexed" and not f.get("unchanged")]
    # The guard gates what THIS run would write: unchanged files were already
    # settled at their original ingest (possibly redacted there), so flagging
    # them forever would make every later incremental run refuse without any
    # new bytes moving. Fresh/changed transcripts are always scanned.
    kinds = sorted({k for f in indexable for k in f.get("secret_kinds", [])})
    redact, allow = bool(getattr(args, "redact", False)), bool(getattr(args, "allow_secret", False))
    if apply_mode:
        # The guard verdict is audited on its own connection so a refusal
        # survives its own SystemExit (the apply transaction rolls back).
        if kinds:
            with conn() as db:
                audit(db, "session", plan["source"],
                      "secret_blocked" if not (redact or allow) else
                      ("secret_redacted" if redact else "secret_allowed"),
                      {"files": len(indexable), "kinds": kinds})
            if not (redact or allow):
                raise SystemExit(
                    f"refusing to ingest credential-shaped session content ({', '.join(kinds)}); "
                    "re-run with --redact to store redacted copies or --allow-secret to override")
        with conn() as db:
            applied = 0
            t = now()
            for f in indexable:
                parsed = f["_parsed"]
                msgs = parsed["messages"]
                if redact:
                    msgs = [{**m, "content": _redact_secrets(m["content"])} for m in msgs]
                row_id = f["_row_id"]
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
                    "first_at=excluded.first_at,last_at=excluded.last_at,ingested_at=excluded.ingested_at",
                    (row_id, plan["source"], plan["profile"], plan["project"], f["session_id"],
                     str(Path(plan["root"]) / f["path"]), f["_file_hash"], f["size_bytes"],
                     len(msgs), f["tool_results_skipped"], f["first_at"], f["last_at"], t))
                for seq, m in enumerate(msgs):
                    db.execute(
                        "INSERT INTO session_messages(session_row,seq,role,content,content_hash,at) "
                        "VALUES(?,?,?,?,?,?)",
                        (row_id, seq, m["role"], m["content"],
                         hashlib.sha256((m["role"] + "\x00" + m["content"]).encode()).hexdigest(),
                         m["at"]))
                audit(db, "session", row_id, "session_ingested",
                      {"source": plan["source"], "path": f["path"],
                       "messages": len(msgs), "tool_results_skipped": f["tool_results_skipped"],
                       "malformed_lines": f["malformed_lines"],
                       "duplicates_collapsed": f["duplicates_collapsed"],
                       "redacted": redact})
                applied += 1
            plan["applied_files"] = applied
    plan["dry_run"] = not apply_mode
    json_out(plan)

def search_sessions(args):
    """Keyword retrieval over ingested session messages with full provenance.

    The same retrieval contract as notes/handoffs: BM25-ranked FTS5 when the
    index exists (with --rank), substring LIKE fallback otherwise, every hit
    carrying its source/profile/project/session/role provenance so raw
    conversation is always traceable back to its transcript — and always
    readable as cache, never as execution truth.
    """
    role = getattr(args, "role", "") or ""
    if role and role not in SESSION_ROLES:
        raise SystemExit(f"--role must be one of: {', '.join(SESSION_ROLES)}")
    clauses, vals = [], []
    if getattr(args, "source", ""):
        clauses.append("s.source=?"); vals.append(args.source)
    if getattr(args, "project", ""):
        clauses.append("s.project=?"); vals.append(args.project)
    if role:
        clauses.append("sm.role=?"); vals.append(role)
    limit = max(0, args.limit)
    where = " AND ".join(["session_messages_fts MATCH ?", *clauses])
    with conn() as db:
        if getattr(args, "rank", False) and _sessions_fts_ready(db):
            match = _fts_query(args.query)
            if not match:
                json_out([])
                return
            rows = [dict(r) for r in db.execute(
                "SELECT sm.session_row,s.session_id,s.source,s.profile,s.project,"
                "sm.seq,sm.role,sm.at,sm.content,bm25(session_messages_fts) AS score "
                "FROM session_messages_fts f JOIN session_messages sm ON sm.rowid=f.rowid "
                "JOIN sessions s ON s.id=sm.session_row WHERE " + where +
                " ORDER BY score LIMIT ?",
                [match, *vals, limit]).fetchall()]
            json_out(rows)
            return
        pat = "%" + args.query + "%"
        lvals = [pat, pat] + vals
        rows = [dict(r) for r in db.execute(
            "SELECT sm.session_row,s.session_id,s.source,s.profile,s.project,"
            "sm.seq,sm.role,sm.at,sm.content "
            "FROM session_messages sm JOIN sessions s ON s.id=sm.session_row WHERE " +
            " AND ".join(["(sm.content LIKE ? OR sm.role LIKE ?)", *clauses]) +
            " ORDER BY sm.at DESC, sm.session_row ASC, sm.seq ASC LIMIT ?",
            [*lvals, limit]).fetchall()]
    json_out(rows)

def _retention_cutoff(value: str) -> str:
    """Validate --older-than: a relative age (Nd/Nh/Nm) or an absolute ISO timestamp."""
    v = (value or "").strip()
    if not v:
        raise SystemExit("--older-than is required to bound any prune (an unbounded wipe is refused)")
    m = re.fullmatch(r"(\d+)([dhm])", v.lower())
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if n <= 0:
            raise SystemExit("--older-than duration must be positive")
        delta = {"d": timedelta(days=n), "h": timedelta(hours=n), "m": timedelta(minutes=n)}[unit]
        return (datetime.now(timezone.utc) - delta).replace(microsecond=0).isoformat()
    return _normalize_iso(v, "--older-than")

def sessions_prune(args):
    """Retention pass over the disposable session cache.

    Ingested transcripts are derived data — rebuildable from their source
    stores by re-running session-ingest — but until now they grew without
    bound. Pruning stays honest by construction: an --older-than bound is
    mandatory so no filter combination can ever express "delete everything",
    the default run is a read-only dry-run plan, and --apply deletes only
    cache rows inside one transaction (the FTS triggers keep the search index
    in sync; source transcript files are external and never touched). Each
    affected source gets one audited event with exact counts, and because the
    rows are rebuildable, recovery after an over-eager prune is simply a
    re-ingest. A zero-candidate apply audits nothing: empty runs leave no
    ledger noise.
    """
    cutoff = _retention_cutoff(getattr(args, "older_than", ""))
    filters = {}
    for flag in ("source", "profile", "project"):
        v = (getattr(args, flag, "") or "").strip()
        if v:
            filters[flag] = v
    where = ["COALESCE(NULLIF(s.last_at,''), s.ingested_at) <= ?"] + \
            [f"s.{col}=?" for col in filters]
    base_vals = [cutoff, *filters.values()]
    with conn() as db:
        cands = [dict(r) for r in db.execute(
            "SELECT s.id,s.source,s.session_id,s.path,s.size_bytes,"
            "COALESCE(NULLIF(s.last_at,''), s.ingested_at) AS effective_at,"
            "(SELECT COUNT(*) FROM session_messages sm WHERE sm.session_row=s.id) AS messages "
            "FROM sessions s WHERE " + " AND ".join(where) +
            " ORDER BY s.source, s.session_id", base_vals).fetchall()]
        pre_total = db.execute("SELECT COUNT(*) n FROM sessions").fetchone()["n"]
    totals = {
        "sessions": len(cands),
        "messages": sum(c["messages"] for c in cands),
        "bytes": sum(c["size_bytes"] for c in cands),
    }
    by_source = {}
    for c in cands:
        b = by_source.setdefault(c["source"], {"sessions": 0, "messages": 0, "bytes": 0})
        b["sessions"] += 1; b["messages"] += c["messages"]; b["bytes"] += c["size_bytes"]
    out = {"dry_run": not bool(getattr(args, "apply", False)), "cutoff": cutoff,
           "filters": filters, "totals": totals, "by_source": by_source,
           "candidates": [{k: c[k] for k in ("id", "source", "session_id", "path",
                                             "size_bytes", "effective_at", "messages")}
                          for c in cands]}
    if getattr(args, "apply", False):
        with conn() as db:
            for c in cands:
                db.execute("DELETE FROM sessions WHERE id=?", (c["id"],))
            for src, b in sorted(by_source.items()):
                audit(db, "session", src, "session_pruned",
                      {"sessions": b["sessions"], "messages": b["messages"],
                       "bytes": b["bytes"], "cutoff": cutoff, **filters})
            remaining = db.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(message_count),0) m FROM sessions").fetchone()
        out["pruned"] = totals
        out["remaining"] = {"sessions": remaining["n"], "messages": remaining["m"]}
    else:
        out["remaining_if_applied"] = {"sessions": pre_total - len(cands)}
    json_out(out)

def _related_session_candidates(db, task_id: str, text: str, limit: int, scope: str) -> list:
    """Cross-task retrieval: ingested session messages matching this task's text.

    Session transcripts are the freshest record of what agents actually said
    and tried — context packs that only see distilled notes miss the reasoning.
    Deterministic ordering (at DESC, then session/seq) keeps recall digests
    stable; snippets are bounded so one long transcript cannot eat the budget;
    scope 'project' restricts to the task's own project, 'global' searches all.
    """
    if limit <= 0 or not text.strip():
        return []
    toks = []
    for raw in text.split():
        tok = raw.replace('"', "")
        if tok and any(c.isalnum() for c in tok):
            toks.append(tok)
    if not toks:
        return []
    scope_sql, scope_vals = "", []
    if scope != "global":
        scope_sql = " AND s.project=(SELECT project FROM tasks WHERE id=?)"
        scope_vals = [task_id]
    cols = ("SELECT sm.session_row,s.session_id,s.source,s.profile,s.project,"
            "sm.seq,sm.role,sm.at,sm.content")
    def snippet(c: str) -> str:
        return c if len(c) <= SESSION_SNIPPET_CHARS else c[:SESSION_SNIPPET_CHARS - 1] + "…"
    if _sessions_fts_ready(db):
        match = " OR ".join('"%s"' % t for t in toks)
        sql = (cols + " FROM session_messages_fts f JOIN session_messages sm ON sm.rowid=f.rowid "
               "JOIN sessions s ON s.id=sm.session_row "
               "WHERE session_messages_fts MATCH ?" + scope_sql +
               " ORDER BY sm.at DESC, sm.session_row ASC, sm.seq ASC LIMIT ?")
        vals = [match, *scope_vals, limit]
    else:
        likes = " OR ".join("sm.content LIKE ?" for _ in toks)
        sql = (cols + " FROM session_messages sm JOIN sessions s ON s.id=sm.session_row "
               "WHERE (" + likes + ")" + scope_sql +
               " ORDER BY sm.at DESC, sm.session_row ASC, sm.seq ASC LIMIT ?")
        vals = [*( "%" + t + "%" for t in toks), *scope_vals, limit]
    rows = []
    for r in db.execute(sql, vals).fetchall():
        d = dict(r)
        d["content"] = snippet(d["content"])
        rows.append(d)
    return rows

# ---------------------------------------------------------------------------
# Hindsight semantic-memory adapter (read-only recall + guarded retain).
#
# Documented bank format ("bank v1"): a single JSONL file at
# $HERMES_HINDSIGHT_HOME/bank.jsonl (default ~/.hermes/hindsight/bank.jsonl),
# one memory per line: {"id","text","kind","project","created_at","tags"}.
# The runtime never mutates a live bank except through `hindsight-retain`,
# which appends after the same secret guard notes use. Recall is strictly
# read-only and deterministic (created_at DESC, then id) so pack digests are
# exactly recomputable; every packed row carries its own engine tag so
# staleness detection covers the semantic sections independently.
# ---------------------------------------------------------------------------
HINDSIGHT_ENGINE_TAG = "hindsight-bank-v1"

def _hindsight_home():
    return Path(os.environ.get("HERMES_HINDSIGHT_HOME",
                               Path.home() / ".hermes" / "hindsight"))

def _hindsight_bank_path():
    return _hindsight_home() / "bank.jsonl"

def _hindsight_available() -> bool:
    return _hindsight_bank_path().is_file()

def _hindsight_candidates(db, task_id: str, text: str, limit: int, scope: str) -> list:
    """Semantic recall from a Hindsight bank matching this task's text.

    Same shape discipline as the session/fact collectors: empty when the flag
    is unset, the bank is absent/unreadable, or the task text has no usable
    tokens (the graceful-unavailable path — never an error). Deterministic
    ordering keeps digests stable; snippets are bounded like sessions.
    """
    if limit <= 0 or not text.strip() or not _hindsight_available():
        return []
    toks = []
    for raw in text.split():
        tok = raw.replace('"', "")
        low = tok.lower()
        if tok and any(c.isalnum() for c in tok):
            toks.append(low)
    if not toks:
        return []
    mems = []
    try:
        with _hindsight_bank_path().open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    m = json.loads(line)
                except ValueError:
                    continue  # a torn/corrupt line degrades, never fails recall
                if not isinstance(m, dict) or not m.get("text"):
                    continue
                mems.append(m)
    except OSError:
        return []
    def matches(m):
        hay = " ".join(str(m.get(k) or "") for k in
                       ("text", "kind", "project")).lower() \
              + " " + " ".join(str(t).lower() for t in (m.get("tags") or []))
        return any(t in hay for t in toks)
    hits = [m for m in mems if matches(m)]
    if scope != "global":
        try:
            proj = db.execute("SELECT project FROM tasks WHERE id=?",
                              (task_id,)).fetchone()
        except sqlite3.Error:
            proj = None
        proj = proj["project"] if proj else ""
        scoped = [m for m in hits if (m.get("project") or "") in ("", proj)]
        if scoped:
            hits = scoped
    hits.sort(key=lambda m: (str(m.get("created_at") or ""), str(m.get("id") or "")),
              reverse=True)
    out = []
    for m in hits[:max(0, limit)]:
        txt = str(m.get("text") or "")
        if len(txt) > SESSION_SNIPPET_CHARS:
            txt = txt[:SESSION_SNIPPET_CHARS - 1] + "…"
        out.append({"id": str(m.get("id") or ""),
                    "engine": HINDSIGHT_ENGINE_TAG,
                    "kind": str(m.get("kind") or "memory"),
                    "project": str(m.get("project") or ""),
                    "at": str(m.get("created_at") or ""),
                    "tags": sorted(str(t) for t in (m.get("tags") or [])),
                    "content": txt})
    return out

def release(args):
    """Voluntarily give up a live lease without consuming retry budget."""
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in {"completed", "cancelled"}:
            raise SystemExit(f"cannot release terminal task: {row['status']}")
        _require_live_lease(row, args.owner, "releasing")
        _require_epoch(row, getattr(args, "epoch", None), "releasing")
        t = now()
        cur = db.execute(
            "UPDATE tasks SET status='queued',lease_owner='',lease_expires_at='',updated_at=? "
            "WHERE id=? AND lease_owner=? AND lease_expires_at>?", (t, args.id, args.owner, t))
        if cur.rowcount != 1:
            raise SystemExit("lease changed since check; reclaim before releasing")
        audit(db, "task", args.id, "lease_released", {"owner": args.owner})
        json_out(_task_view(task_row(db, args.id)))

def renew(args):
    """Extend a live lease from now without changing status or the fencing epoch.

    Unlike heartbeat (which forces status='running' and a fixed 15-minute
    window), renew keeps the task's current status and lets the holder pick the
    extension. The epoch is preserved so fencing tokens stay stable across a
    renewal; a superseded holder is still rejected by --epoch.
    """
    if args.minutes <= 0:
        raise SystemExit("--minutes must be positive")
    with conn() as db:
        row = task_row(db, args.id)
        _require_epoch(row, getattr(args, "epoch", None), "renewing")
        t = now()
        exp = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + args.minutes * 60, timezone.utc).replace(microsecond=0).isoformat()
        cur = db.execute(
            "UPDATE tasks SET lease_expires_at=?, updated_at=? "
            "WHERE id=? AND lease_owner=? AND lease_expires_at!='' AND lease_expires_at>?",
            (exp, t, args.id, args.owner, t))
        if cur.rowcount != 1:
            if row["lease_owner"] and row["lease_owner"] != args.owner:
                raise SystemExit(f"lease owned by {row['lease_owner']}")
            raise SystemExit("no live lease to renew; claim before renewing")
        audit(db, "task", args.id, "lease_renewed", {"owner": args.owner, "minutes": args.minutes, "lease_expires_at": exp})
        json_out({"ok": True, "task_id": args.id, "status": row["status"], "owner": args.owner,
                  "lease_expires_at": exp, "lease_epoch": row["lease_epoch"]})

def transfer(args):
    """Atomically reassign a live lease to another agent.

    The provider-neutral counterpart to a handoff: when work moves from one
    agent to another, ownership moves with it. Only the current holder of a
    live lease may transfer; the fencing epoch bumps on transfer so the old
    holder's token is invalidated immediately even though the owner-name check
    would otherwise still pass. Status is preserved and the new window starts
    from now.
    """
    if args.minutes <= 0:
        raise SystemExit("--minutes must be positive")
    if args.to_owner == args.from_owner:
        raise SystemExit("--to-owner must differ from --from-owner")
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in TERMINAL_STATUSES:
            raise SystemExit(f"cannot transfer terminal task: {row['status']}")
        _require_epoch(row, getattr(args, "epoch", None), "transferring")
        t = now()
        exp = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + args.minutes * 60, timezone.utc).replace(microsecond=0).isoformat()
        cur = db.execute(
            "UPDATE tasks SET lease_owner=?, lease_expires_at=?, lease_epoch=lease_epoch+1, updated_at=? "
            "WHERE id=? AND lease_owner=? AND lease_expires_at!='' AND lease_expires_at>?",
            (args.to_owner, exp, t, args.id, args.from_owner, t))
        if cur.rowcount != 1:
            if not row["lease_owner"]:
                raise SystemExit("no active lease; claim before transferring")
            if row["lease_owner"] != args.from_owner:
                raise SystemExit(f"lease owned by {row['lease_owner']}")
            raise SystemExit("lease expired; reclaim before transferring")
        new_epoch = db.execute("SELECT lease_epoch FROM tasks WHERE id=?", (args.id,)).fetchone()[0]
        audit(db, "task", args.id, "lease_transferred",
              {"from_owner": args.from_owner, "to_owner": args.to_owner,
               "minutes": args.minutes, "lease_expires_at": exp,
               "previous_epoch": row["lease_epoch"], "lease_epoch": new_epoch})
        json_out({"ok": True, "task_id": args.id, "status": row["status"],
                  "from_owner": args.from_owner, "to_owner": args.to_owner,
                  "lease_expires_at": exp, "lease_epoch": new_epoch})

def leases(args):
    """Fleet-wide lease observability: who holds what, until when, live or stale.

    Default lists only live leases sorted by soonest expiry so an operator sees
    what is about to free up first; --all includes expired-but-still-held
    leases (the recovery candidates), tagged with live=false.
    """
    t = now()
    with conn() as db:
        q = ("SELECT id, project, title, status, priority, lease_owner, lease_expires_at, lease_epoch "
             "FROM tasks WHERE lease_owner!='' AND lease_expires_at!=''")
        vals = []
        if not getattr(args, "all", False):
            q += " AND lease_expires_at>?"
            vals.append(t)
        if getattr(args, "owner", ""):
            q += " AND lease_owner=?"
            vals.append(args.owner)
        rows = db.execute(q + " ORDER BY lease_expires_at", vals).fetchall()
    now_ts = datetime.now(timezone.utc).timestamp()
    out = []
    for r in rows:
        exp = r["lease_expires_at"]
        try:
            remaining = int(datetime.fromisoformat(exp).timestamp() - now_ts)
        except ValueError:
            remaining = None
        out.append({
            "task_id": r["id"], "project": r["project"], "title": r["title"],
            "status": r["status"], "priority": r["priority"], "owner": r["lease_owner"],
            "lease_expires_at": exp, "lease_epoch": r["lease_epoch"],
            "live": exp > t,
            "seconds_remaining": max(remaining, 0) if remaining is not None else None,
        })
    json_out({"generated_at": t, "count": len(out),
              "live_count": sum(1 for l in out if l["live"]), "leases": out})

def _acquire(db, task_id: str, owner: str, minutes: int, max_active: int = 0):
    """Atomically acquire/renew a lease; returns (acquired, expires_at, epoch).

    The WHERE guard makes the check-and-set a single statement so concurrent
    claimers cannot both win. When max_active > 0 the same statement also
    enforces a per-owner cap on live leases, so one agent cannot hog dispatch.
    Each acquisition bumps lease_epoch — a monotonic fencing token so a stale
    holder whose lease expired and was reacquired cannot silently mutate state.
    """
    exp = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + minutes * 60, timezone.utc).replace(microsecond=0).isoformat()
    t = now()
    cur = db.execute(
        "UPDATE tasks SET status='claimed', lease_owner=?, lease_expires_at=?, lease_epoch=lease_epoch+1, recover_after='', updated_at=? "
        "WHERE id=? AND (lease_owner=? OR lease_owner='' OR lease_expires_at='' OR lease_expires_at<=?) "
        "AND (?<=0 OR (SELECT COUNT(*) FROM tasks o WHERE o.lease_owner=? AND o.id!=tasks.id "
        "AND o.lease_expires_at!='' AND o.lease_expires_at>?)<?)",
        (owner, exp, t, task_id, owner, t, max_active, owner, t, max_active))
    if cur.rowcount != 1:
        return False, "", -1
    epoch = db.execute("SELECT lease_epoch FROM tasks WHERE id=?", (task_id,)).fetchone()[0]
    return True, exp, epoch

def _require_epoch(row, epoch, verb: str) -> None:
    """Fencing guard: reject mutations from holders of a superseded lease."""
    if epoch is not None and epoch != row["lease_epoch"]:
        raise SystemExit(f"lease superseded (held epoch {epoch}, current {row['lease_epoch']}); reclaim before {verb}")

def _explain_acquire_failure(db, task_id: str, owner: str, max_active: int) -> str:
    """Human-readable reason for a failed _acquire, for operator ergonomics."""
    row = task_row(db, task_id)
    if row["lease_owner"] and row["lease_owner"] != owner:
        return f"lease owned by {row['lease_owner']}"
    if max_active > 0:
        active = db.execute(
            "SELECT COUNT(*) n FROM tasks WHERE lease_owner=? AND id!=? AND lease_expires_at!='' AND lease_expires_at>?",
            (owner, task_id, now())).fetchone()["n"]
        if active >= max_active:
            return f"owner '{owner}' at lease capacity ({active}/{max_active}); complete or release a lease first"
    return f"could not acquire lease for {task_id}"

def _seam_conflicts(db, task_id: str, worktree: str, branch: str, project: str) -> list:
    """Live leases held by OTHER tasks on the same seam as the given task.

    A seam is the shared filesystem/VCS resource two concurrent agents would
    physically collide on: an identical non-empty worktree path, or the same
    branch name within the same project (same branch across projects is a
    different repository checkout, so it is not a conflict). Empty values are
    never seams. Lease liveness uses the same rule as `leases`: expires_at in
    the future.
    """
    t = now()
    found = {}
    if worktree:
        for r in db.execute(
                "SELECT id,lease_owner,lease_expires_at,lease_epoch FROM tasks "
                "WHERE id!=? AND worktree=? AND lease_owner!='' AND lease_expires_at!='' AND lease_expires_at>?",
                (task_id, worktree, t)):
            found[r["id"]] = {"task_id": r["id"], "seam": "worktree", "value": worktree,
                              "owner": r["lease_owner"], "lease_epoch": r["lease_epoch"],
                              "lease_expires_at": r["lease_expires_at"]}
    if branch:
        for r in db.execute(
                "SELECT id,lease_owner,lease_expires_at,lease_epoch FROM tasks "
                "WHERE id!=? AND branch=? AND project=? AND lease_owner!='' AND lease_expires_at!='' AND lease_expires_at>?",
                (task_id, branch, project, t)):
            found.setdefault(r["id"], {"task_id": r["id"], "seam": "branch", "value": branch,
                                       "project": project, "owner": r["lease_owner"],
                                       "lease_epoch": r["lease_epoch"],
                                       "lease_expires_at": r["lease_expires_at"]})
    return sorted(found.values(), key=lambda c: c["task_id"])

def _seam_message(conflicts: list) -> str:
    detail = "; ".join(f"{c['task_id']} holds {c['seam']} {c['value']!r} (owner {c['owner']})"
                       for c in conflicts)
    return f"seam conflict: {detail}; complete/release the holder first or pass --force"

def _audit_claim_refusal(task_id: str, owner: str, action: str, detail: dict) -> None:
    """Record a claim refusal in the audit chain on its own connection.

    The caller's transaction is rolled back (the just-acquired lease must not
    survive), so the refusal is committed separately to keep the audit trail
    complete without resurrecting the lease.
    """
    with conn() as db:
        audit(db, "task", task_id, action, {"owner": owner, **detail})

def _audit_seam_refusal(task_id: str, owner: str, conflicts: list) -> None:
    _audit_claim_refusal(task_id, owner, "claim_refused_seam", {"conflicts": conflicts})

def create(args):
    task_id = args.id or f"{args.project.lower().replace(' ', '-')}-{uuid.uuid4().hex[:8]}"
    t = now()
    due_at = _normalize_due(getattr(args, "due_at", None) or "")
    not_before = _normalize_iso(getattr(args, "not_before", None) or "", "--not-before")
    tags = sorted({_valid_tag(x) for x in (getattr(args, "tag", None) or [])})
    requires = sorted({_valid_receipt_kind(x) for x in (getattr(args, "requires_receipt", None) or [])})
    with conn() as db:
        try:
            db.execute("INSERT INTO tasks(id,project,title,description,owner,status,priority,next_action,due_at,not_before,tags,requires_receipts,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (task_id,args.project,args.title,args.description,args.owner,"queued",args.priority,args.next_action,due_at,not_before,json.dumps(tags),json.dumps(requires),t,t))
        except sqlite3.IntegrityError:
            raise SystemExit(f"task id already exists: {task_id}")
        # Task-layer deduplication: flag open tasks in the same project whose
        # text restates this one. Informational — creation is never blocked.
        similar = _similar_open_tasks(db, args.project,
                                      f"{args.title} {args.description}", task_id)
        audit(db, "task", task_id, "created", {"project": args.project, "owner": args.owner, "priority": args.priority, "due_at": due_at, "not_before": not_before, **({"tags": tags} if tags else {}), **({"requires_receipts": requires} if requires else {}), **({"similar_open_tasks": [s["task_id"] for s in similar]} if similar else {})})
        for dep in getattr(args, "depends_on", []) or []:
            add_dependency(db, task_id, dep)
    out = {"ok": True, "id": task_id, "status": "queued"}
    if similar:
        out["similar_open_tasks"] = similar
    json_out(out)

def similar_tasks(args):
    """Triage view: open tasks in this task's project that restate its work.

    The read-side of create-time duplicate flagging: given any task, list the
    open (non-terminal) same-project tasks whose title/description text
    overlaps it at or above the near-duplicate threshold. Use it before
    claiming or merging duplicate work; `ops.py dup-tasks` sweeps the fleet.
    """
    threshold = getattr(args, "threshold", None)
    if threshold is not None:
        try:
            threshold = float(threshold)
        except (TypeError, ValueError):
            raise SystemExit("--threshold must be a number between 0 and 1")
        if not 0 < threshold <= 1:
            raise SystemExit("--threshold must be a number between 0 and 1")
    with conn() as db:
        row = task_row(db, args.id)
        similar = _similar_open_tasks(
            db, row["project"], f"{row['title']} {row['description']}", args.id,
            threshold=threshold)
    json_out({"ok": True, "task_id": args.id, "project": row["project"],
              "threshold": threshold if threshold is not None else _near_dup_threshold(),
              "count": len(similar), "similar": similar})

# Readiness statuses whose entry can be gated by a project policy file
# (policies/<project>.yaml, `<action>_requires_user: true` — the same
# convention ops.py policy reports on).
_GATED_STATUS_ACTIONS = {"ready_to_merge": "merge", "ready_to_deploy": "deploy"}

def _project_policy(project: str) -> dict:
    """Parse a project's policy file (policies/<project>.yaml) into a dict.

    The policy files are flat YAML ("key: value" lines), so a tiny
    dependency-free parser covers every key the runtime acts on; unknown
    keys are preserved verbatim for reporting. Booleans become bool,
    digit-only values int, everything else stays a string. A missing or
    unreadable file yields {} so policy-less fleets keep today's behavior.
    """
    try:
        text = (POLICIES / f"{project.lower()}.yaml").read_text()
    except OSError:
        return {}
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        k, _, v = line.partition(":")
        k, v = k.strip(), v.strip()
        if v.lower() in ("true", "false"):
            out[k] = v.lower() == "true"
        elif v.isdigit():
            out[k] = int(v)
        else:
            out[k] = v
    return out

def _policy_requires_user(project: str, action: str) -> bool:
    """True when the project's policy demands user approval for `action`.

    Mirrors ops.py policy semantics. A missing or unreadable policy file
    allows the transition so fleets without policies keep today's behavior.
    """
    return bool(_project_policy(project).get(f"{action}_requires_user"))

def _dispatch_required_tag(policy: dict) -> str:
    """The capability tag a project's policy requires on dispatchable work ('' = none)."""
    tag = policy.get("dispatch_requires_tag")
    return tag.strip() if isinstance(tag, str) else ""

def _wip_cap(policy: dict) -> int:
    """Per-owner live-lease cap inside one project (0 = uncapped)."""
    v = policy.get("max_wip_per_owner")
    return v if isinstance(v, int) and v > 0 else 0

def _owner_project_leases(db, owner: str, project: str, exclude_task: str = "") -> list:
    """Ids of the owner's live leases within one project (same liveness rule as `leases`)."""
    t = now()
    return [r["id"] for r in db.execute(
        "SELECT id FROM tasks WHERE lease_owner=? AND project=? AND id!=? "
        "AND lease_expires_at!='' AND lease_expires_at>?", (owner, project, exclude_task, t))]

def update(args):
    fields = {}
    for key in ("status", "next_action", "blocked_reason", "worktree", "branch", "pr_url", "owner",
                "due_at", "title", "description", "priority", "project", "not_before"):
        value = getattr(args, key, None)
        if value is not None:
            fields[key] = value
    if "status" in fields and fields["status"] not in STATUSES:
        raise SystemExit(f"invalid status: {fields['status']}")
    if "due_at" in fields:
        fields["due_at"] = _normalize_due(fields["due_at"])
    if "not_before" in fields:
        fields["not_before"] = _normalize_iso(fields["not_before"], "--not-before")
    if getattr(args, "requires_receipt", None) is not None:
        raw = list(args.requires_receipt)
        if raw == [""]:
            fields["requires_receipts"] = "[]"   # "" clears the definition of done
        else:
            fields["requires_receipts"] = json.dumps(
                sorted({_valid_receipt_kind(x) for x in raw}))
    if fields.get("status") in {"completed", "failed", "cancelled"}:
        # Terminal transitions release any held lease so the task cannot look active.
        fields["lease_owner"] = ""
        fields["lease_expires_at"] = ""
    if not fields:
        raise SystemExit("no updates supplied")
    fields["updated_at"] = now()
    with conn() as db:
        row = task_row(db, args.id)
        # Policy gate: entering a gated readiness state is a side-effectful
        # promise; when the project's policy demands a human, refuse until an
        # explicit --approved-by names who accepted it. Re-stating the current
        # status is not a transition and stays ungated.
        gate_action = _GATED_STATUS_ACTIONS.get(fields.get("status"))
        approval = None
        if gate_action and row["status"] != fields["status"] \
                and _policy_requires_user(row["project"], gate_action):
            approver = (getattr(args, "approved_by", "") or "").strip()
            if not approver:
                raise SystemExit(
                    f"project policy requires user approval for '{gate_action}' before "
                    f"entering {fields['status']}; re-run with --approved-by <name>")
            approval = {"policy_gate": gate_action, "approved_by": approver}
        db.execute(f"UPDATE tasks SET {', '.join(k+'=?' for k in fields)} WHERE id=?", (*fields.values(), args.id))
        audit_payload = {k: v for k, v in fields.items() if k != "updated_at"}
        if "requires_receipts" in audit_payload:
            audit_payload["requires_receipts"] = json.loads(audit_payload["requires_receipts"])
        audit(db, "task", args.id, "updated", {**audit_payload, **(approval or {})})
        json_out(_task_view(task_row(db, args.id)))

def _require_live_lease(row, owner: str, verb: str) -> None:
    """Strict guard: only the current holder of a live lease may proceed."""
    if not row["lease_owner"]:
        raise SystemExit("no active lease; claim before " + verb)
    if row["lease_owner"] != owner:
        raise SystemExit(f"lease owned by {row['lease_owner']}")
    if row["lease_expires_at"] and row["lease_expires_at"] <= now():
        raise SystemExit("lease expired; reclaim before " + verb)

def _require_not_foreign_lease(row, owner: str, verb: str) -> None:
    """Lenient guard for operator transitions: reject foreign or expired leases only."""
    if row["lease_owner"] and row["lease_owner"] != owner:
        raise SystemExit(f"lease owned by {row['lease_owner']}")
    if row["lease_owner"] and row["lease_expires_at"] and row["lease_expires_at"] <= now():
        raise SystemExit("lease expired; reclaim before " + verb)

def complete(args):
    """Mark a leased task completed, optionally citing sealed evidence receipts.

    A self-report is never execution truth without a receipt: `--receipt <id>`
    (repeatable) links the completion to integrity-sealed receipts that must
    already exist *on this task*, so the audited `completed` event carries
    verifiable provenance instead of a bare claim. Unknown ids and receipts
    belonging to other tasks are refused.
    """
    recall_digest = _require_digest(getattr(args, "recall_digest", "") or "", "--recall-digest")
    evidence = list(dict.fromkeys(getattr(args, "evidence_receipts", None) or []))
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in {"completed", "cancelled"}:
            raise SystemExit(f"cannot complete terminal task: {row['status']}")
        _require_live_lease(row, args.owner, "completing")
        _require_epoch(row, getattr(args, "epoch", None), "completing")
        for rid in evidence:
            hit = db.execute("SELECT id FROM receipts WHERE id=? AND task_id=?",
                             (rid, args.id)).fetchone()
            if not hit:
                raise SystemExit(f"evidence receipt not found on this task: {rid}")
        # Definition of done: a task's required receipt kinds are acceptance
        # criteria as data — completion refuses until at least one receipt of
        # every required kind exists on this task. The refusal is audited on
        # its own connection (the caller's transaction rolls back) so the gate
        # leaves provenance without resurrecting anything.
        required = _task_requires(row)
        missing_evidence = _missing_required_evidence(db, args.id, required)
        if missing_evidence:
            _audit_claim_refusal(args.id, args.owner, "completion_blocked_evidence",
                                 {"missing_receipt_kinds": missing_evidence,
                                  "required_receipts": required})
            raise SystemExit(
                "definition of done unmet: missing required receipt kind(s): "
                + ", ".join(missing_evidence))
        t = now()
        # Guarded mutation: the completion only lands while the lease is exactly
        # as checked above. A lease that expired and was re-acquired (or
        # transferred) between the row read and this write must not be clobbered
        # by a stale holder — the rowcount guard turns that race into a refusal.
        cur = db.execute(
            "UPDATE tasks SET status='completed',lease_owner='',lease_expires_at='',blocked_reason='',updated_at=? "
            "WHERE id=? AND lease_owner=? AND lease_expires_at>?", (t, args.id, args.owner, t))
        if cur.rowcount != 1:
            raise SystemExit("lease changed since check; reclaim before completing")
        # Downstream feedback: which queued dependents just became dispatchable.
        newly = sorted(d["id"] for d in pending_dependents(db, args.id)
                       if d["status"] == "queued" and not unsatisfied_deps(db, d["id"]))
        audit(db, "task", args.id, "completed", {"owner": args.owner, "note": args.note,
                                                 "recall_digest": recall_digest or None,
                                                 "newly_unblocked": newly,
                                                 **({"evidence_receipts": evidence} if evidence else {})})
        out = _task_view(task_row(db, args.id))
        out["newly_unblocked"] = newly
        if evidence:
            out["evidence_receipts"] = evidence
        if required:
            out["required_evidence_met"] = True
        json_out(out)

def cancel(args):
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in {"completed", "cancelled"}:
            raise SystemExit(f"cannot cancel terminal task: {row['status']}")
        _require_not_foreign_lease(row, args.owner, "cancelling")
        t = now()
        db.execute("UPDATE tasks SET status='cancelled',lease_owner='',lease_expires_at='',blocked_reason=?,updated_at=? WHERE id=?", (args.reason or 'cancelled by operator', t, args.id))
        audit(db, "task", args.id, "cancelled", {"owner": args.owner, "reason": args.reason})
        json_out(_task_view(task_row(db, args.id)))

def _backoff_deadline(retry_count: int, base: int, cap: int) -> str:
    """Deterministic exponential cooldown after the Nth failure: base * 2^(N-1), capped."""
    if base <= 0:
        return ""
    delay = min(base * (2 ** (retry_count - 1)), cap)
    dt = datetime.now(timezone.utc) + timedelta(seconds=delay)
    return dt.replace(microsecond=0).isoformat()

def fail(args):
    """Record an attempted-and-failed execution with a retry budget and backoff.

    The first-class counterpart of `complete`: an agent that attempted the work
    and could not finish reports it here instead of abusing generic
    `update --status failed`, which silently loses the attempt. Mirrors ops.py
    recover semantics so both failure paths share one budget:

    - The caller must hold the live lease (fenced by --epoch like complete).
    - Each failure consumes one unit of retry budget. While budget remains
      (`retry_count <= --max-retries`, default 3) the task returns to `queued`
      with an exponential cooldown `recover_after = now + backoff_base *
      2^(retry_count-1)` seconds (--backoff-base default 60, --backoff-cap
      default 3600; base 0 disables), so failing work cannot hot-loop through
      dispatch. `next` already skips cooling-down tasks; a direct claim stays
      allowed as a deliberate override and any acquisition clears the cooldown.
    - With the budget exhausted (or --no-retry) the task goes terminally
      `failed` with the reason preserved in blocked_reason, and direct queued
      dependents are reported as dependents_stranded so the operator sees what
      the permanent failure froze.
    - Audited as `task_failed` (retry scheduled) or `task_failed_terminal`;
      metrics reports failures_retried_total / failures_terminal_total.
    """
    if args.max_retries < 0:
        raise SystemExit("--max-retries must be >= 0")
    if args.backoff_cap < args.backoff_base:
        raise SystemExit("--backoff-cap must be >= --backoff-base")
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in TERMINAL_STATUSES:
            raise SystemExit(f"cannot fail terminal task: {row['status']}")
        _require_live_lease(row, args.owner, "failing")
        _require_epoch(row, getattr(args, "epoch", None), "failing")
        new_retry = row["retry_count"] + 1
        t = now()
        reason = args.reason or "failed by agent"
        if not args.no_retry and new_retry <= args.max_retries:
            ra = _backoff_deadline(new_retry, args.backoff_base, args.backoff_cap)
            cur = db.execute(
                "UPDATE tasks SET status='queued',lease_owner='',lease_expires_at='',"
                "blocked_reason=?,retry_count=?,recover_after=?,updated_at=? "
                "WHERE id=? AND lease_owner=? AND lease_expires_at>?",
                (reason, new_retry, ra, t, args.id, args.owner, t))
            if cur.rowcount != 1:
                raise SystemExit("lease changed since check; reclaim before failing")
            audit(db, "task", args.id, "task_failed",
                  {"owner": args.owner, "reason": reason, "retry_count": new_retry,
                   "recover_after": ra or None, "max_retries": args.max_retries})
            out = _task_view(task_row(db, args.id))
            out["outcome"] = "retry_scheduled"
            out["recover_after"] = ra
            out["retries_remaining"] = max(args.max_retries - new_retry, 0)
        else:
            stranded = sorted(d["id"] for d in pending_dependents(db, args.id)
                              if d["status"] not in TERMINAL_STATUSES)
            cur = db.execute(
                "UPDATE tasks SET status='failed',lease_owner='',lease_expires_at='',"
                "blocked_reason=?,retry_count=?,recover_after='',updated_at=? "
                "WHERE id=? AND lease_owner=? AND lease_expires_at>?",
                (reason, new_retry, t, args.id, args.owner, t))
            if cur.rowcount != 1:
                raise SystemExit("lease changed since check; reclaim before failing")
            audit(db, "task", args.id, "task_failed_terminal",
                  {"owner": args.owner, "reason": reason, "retry_count": new_retry,
                   "no_retry": bool(args.no_retry), "dependents_stranded": stranded})
            out = _task_view(task_row(db, args.id))
            out["outcome"] = "failed_terminal"
            out["dependents_stranded"] = stranded
        json_out(out)

def block(args):
    """Operator transition to blocked: park work with a reason without cancelling it.

    Never overrides a foreign or expired lease. Blocking a task the caller
    holds a live lease on releases that lease so blocked tasks cannot look
    active to recovery or dispatch.
    """
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in TERMINAL_STATUSES:
            raise SystemExit(f"cannot block terminal task: {row['status']}")
        if row["status"] == "blocked":
            raise SystemExit("task is already blocked")
        _require_not_foreign_lease(row, args.owner, "blocking")
        t = now()
        db.execute("UPDATE tasks SET status='blocked',blocked_reason=?,lease_owner='',lease_expires_at='',updated_at=? WHERE id=?",
                   (args.reason or 'blocked by operator', t, args.id))
        audit(db, "task", args.id, "blocked",
              {"owner": args.owner, "reason": args.reason, "previous_status": row["status"]})
        json_out(_task_view(task_row(db, args.id)))

def unblock(args):
    """Requeue a blocked task, clearing its blocked reason."""
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] != "blocked":
            raise SystemExit(f"task is not blocked (status: {row['status']})")
        t = now()
        db.execute("UPDATE tasks SET status='queued',blocked_reason='',updated_at=? WHERE id=?", (t, args.id))
        audit(db, "task", args.id, "unblocked", {"owner": args.owner})
        json_out(_task_view(task_row(db, args.id)))

def defer(args):
    """Park a task out of dispatch until a future instant (or clear the park).

    Sets not_before on a non-terminal task; `next` skips queued tasks whose
    not_before is still in the future (reason deferred_until under --explain),
    while an explicit claim stays allowed as a deliberate operator override —
    mirroring recovery backoff semantics. `--until ''` clears the deferral.
    Never overrides a foreign or expired lease.
    """
    until = _normalize_iso(args.until or "", "--until")
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in TERMINAL_STATUSES:
            raise SystemExit(f"cannot defer terminal task: {row['status']}")
        _require_not_foreign_lease(row, args.owner, "deferring")
        t = now()
        db.execute("UPDATE tasks SET not_before=?,updated_at=? WHERE id=?", (until, t, args.id))
        audit(db, "task", args.id, "deferred",
              {"owner": args.owner, "not_before": until,
               "previous_status": row["status"], "cleared": until == ""})
        json_out(_task_view(task_row(db, args.id)))

def tag_task(args):
    """Attach capability/scope tags to a task (idempotent, audited).

    Tags are the dispatch-policy vocabulary: an operator marks work
    `autopilot-safe` (or scopes it `client:trove`), and agents constrain their
    dispatch with `next --tag`. Adding an already-present tag is a no-op that
    still reports state, so retries are safe.
    """
    tags = [_valid_tag(x) for x in (args.tag or [])]
    if not tags:
        raise SystemExit("at least one --tag is required")
    with conn() as db:
        row = task_row(db, args.id)
        cur_tags = _task_tags(row)
        added = sorted(set(tags) - set(cur_tags))
        new_tags = sorted(set(cur_tags) | set(tags))
        if added:
            # Compare-and-swap on the observed tag set so concurrent tag/untag
            # calls cannot silently drop each other's writes.
            cur = db.execute("UPDATE tasks SET tags=?,updated_at=? WHERE id=? AND tags=?",
                             (json.dumps(new_tags), now(), args.id, row["tags"]))
            if cur.rowcount != 1:
                raise SystemExit("tags changed concurrently; retry")
            audit(db, "task", args.id, "task_tagged", {"tags": added})
        json_out({"ok": True, "task_id": args.id, "tags": new_tags,
                  "added": added, "already_tagged": [t for t in tags if t not in added]})

def untag_task(args):
    """Remove tags from a task (audited); removing an absent tag is rejected."""
    tag = _valid_tag(args.tag)
    with conn() as db:
        row = task_row(db, args.id)
        cur_tags = _task_tags(row)
        if tag not in cur_tags:
            raise SystemExit(f"task {args.id} does not carry tag '{tag}'")
        new_tags = [t for t in cur_tags if t != tag]
        cur = db.execute("UPDATE tasks SET tags=?,updated_at=? WHERE id=? AND tags=?",
                         (json.dumps(new_tags), now(), args.id, row["tags"]))
        if cur.rowcount != 1:
            raise SystemExit("tags changed concurrently; retry")
        audit(db, "task", args.id, "task_untagged", {"tag": tag})
    json_out({"ok": True, "task_id": args.id, "tags": new_tags, "removed": tag})

def blocked_by(args):
    """Transitive blockers: walk the dependency DAG upward from a task.

    Returns every prerequisite reachable through task_deps with its depth
    (direct deps at depth 1), live status, and satisfaction flag. The graph is
    acyclic by construction (would_cycle guards every edge insert), so the
    recursive walk always terminates.
    """
    with conn() as db:
        task_row(db, args.id)
        rows = [dict(r) for r in db.execute(
            "WITH RECURSIVE up(id,depth) AS ("
            " SELECT ?,0 UNION"
            " SELECT d.depends_on,up.depth+1 FROM task_deps d JOIN up ON d.task_id=up.id"
            ") "
            "SELECT up.id,up.depth,COALESCE(t.status,'missing') AS status,"
            "COALESCE(t.title,'') AS title,(COALESCE(t.status,'')='completed') AS satisfied "
            "FROM up LEFT JOIN tasks t ON t.id=up.id WHERE up.depth>0 "
            "ORDER BY up.depth,up.id", (args.id,)).fetchall()]
    json_out({"ok": True, "task_id": args.id,
              "blocked": any(not r["satisfied"] for r in rows),
              "blockers": rows})

def impact(args):
    """Blast radius: walk the dependency DAG downward from a task.

    The mirror of `blocked-by`: every transitive dependent with its depth
    (direct dependents at depth 1), live status, and a settled flag
    (completed/cancelled work no longer cares). The summary answers the
    operator question "what happens if I block, defer, or cancel this?" —
    `open` is the number of downstream tasks still waiting somewhere on this
    one. The graph is acyclic by construction (would_cycle guards every edge
    insert), so the recursive walk always terminates.
    """
    with conn() as db:
        task_row(db, args.id)
        rows = [dict(r) for r in db.execute(
            "WITH RECURSIVE down(id,depth) AS ("
            " SELECT ?,0 UNION"
            " SELECT d.task_id,down.depth+1 FROM task_deps d JOIN down ON d.depends_on=down.id"
            ") "
            "SELECT down.id,down.depth,COALESCE(t.status,'missing') AS status,"
            "COALESCE(t.title,'') AS title,"
            "(COALESCE(t.status,'') IN ('completed','cancelled')) AS settled "
            "FROM down LEFT JOIN tasks t ON t.id=down.id WHERE down.depth>0 "
            "ORDER BY down.depth,down.id", (args.id,)).fetchall()]
    by_status = {}
    for r in rows:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    json_out({"ok": True, "task_id": args.id,
              "impacted": len(rows),
              "open": sum(1 for r in rows if not r["settled"]),
              "by_status": by_status,
              "dependents": rows})

def _critical_path(db, project: str = ""):
    """Longest chain of unfinished dependency work, root prerequisite first.

    Nodes are non-terminal tasks (plus 'missing' prerequisites referenced by
    live edges — they block dispatch exactly like real tasks); edges are
    dependency edges whose prerequisite is not completed. Depth of a node is
    1 + max(depth of its unfinished prerequisites), so the maximum depth is
    the minimum number of sequential dispatch waves needed to drain the open
    graph. Ties break on the lexicographically smallest id at every step, so
    identical state yields an identical path. The graph is acyclic by
    construction (would_cycle guards every edge insert), so the memoized walk
    always terminates.
    """
    q = "SELECT id,title,status,priority FROM tasks WHERE status NOT IN ('completed','failed','cancelled')"
    vals = []
    if project:
        q += " AND project=?"; vals.append(project)
    nodes = {}
    for r in db.execute(q, vals).fetchall():
        nodes[r["id"]] = dict(r)
    prereqs = {}
    for r in db.execute(
            "SELECT d.task_id,d.depends_on,COALESCE(t.status,'missing') AS ds "
            "FROM task_deps d LEFT JOIN tasks t ON t.id=d.depends_on").fetchall():
        if r["ds"] == "completed" or r["task_id"] not in nodes:
            continue  # satisfied prerequisites and settled dependents leave the graph
        prereqs.setdefault(r["task_id"], set()).add(r["depends_on"])
        if r["depends_on"] not in nodes:
            nodes[r["depends_on"]] = {"id": r["depends_on"], "title": "",
                                      "status": "missing", "priority": ""}
    depth = {}
    def _depth(n):
        if n not in depth:
            depth[n] = 0  # cycle guard; unreachable for cycle-checked edges
            depth[n] = 1 + max((_depth(p) for p in prereqs.get(n, ())), default=0)
        return depth[n]
    for n in nodes:
        _depth(n)
    if not depth:
        return {"length": 0, "path": [], "open_tasks": 0, "by_status": {}}
    tip = min((n for n in depth if depth[n] == max(depth.values())),
              key=lambda n: (depth[n], n))
    path = []
    cur = tip
    while True:
        path.append({"id": cur, **{k: nodes[cur][k] for k in ("title", "status", "priority")}})
        nxt = [p for p in prereqs.get(cur, ()) if depth[p] == depth[cur] - 1]
        if not nxt:
            break
        cur = min(nxt)
    path.reverse()
    by_status = {}
    for n in nodes.values():
        by_status[n["status"]] = by_status.get(n["status"], 0) + 1
    return {"length": len(path), "path": path,
            "open_tasks": len(nodes), "by_status": by_status}

def critical_path(args):
    """Critical path across the open dependency DAG.

    `blocked-by` answers what holds one task and `impact` what waits on one;
    this answers the fleet-level question: what is the longest chain of still-
    unfinished prerequisite work? Its length is the minimum number of serial
    dispatch waves to drain the queue, and its members are the bottleneck
    chain — slipping on any of them slips everything behind it.
    """
    with conn() as db:
        out = _critical_path(db, (args.project or "").strip())
    json_out({"ok": True, **({"project": args.project} if getattr(args, "project", "") else {}),
              **out})

def _dispatch_plan(db, project: str = "", tag: str = ""):
    """Deterministic parallel dispatch-wave schedule over the open DAG.

    `critical-path` names how many serial waves the open graph needs and which
    chain defines that bound; this computes the actual waves — wave 1 is every
    task whose prerequisites are all completed (including in-flight work, shown
    with its live status), then each task joins the first wave after all of its
    prerequisites' waves. Within a wave, tasks order by effective dispatch
    preference (priority rank, then earliest deadline, then oldest-created,
    then id) so identical state yields an identical schedule and two runs of
    the same fleet diff to nothing.

    Prerequisites outside the plan's scope (a missing id, or a live task in
    another project when --project is set) can never be scheduled here, so
    everything downstream of them is reported under `unschedulable` with the
    blocking ids and their statuses instead of being silently dropped or
    falsely promised. The plan is read-only simulation: runtime guards that
    depend on live state (seam conflicts, recovery backoff, deferral windows)
    still apply at claim time and are deliberately not folded in.
    """
    q = ("SELECT id,title,status,priority,due_at,created_at FROM tasks "
         "WHERE status NOT IN ('completed','failed','cancelled')")
    vals = []
    if project:
        q += " AND project=?"; vals.append(project)
    if tag:
        q += " AND tags LIKE ?"; vals.append('%"' + _valid_tag(tag) + '"%')
    nodes = {}
    for r in db.execute(q, vals).fetchall():
        nodes[r["id"]] = dict(r)
    prereqs = {}
    for r in db.execute(
            "SELECT d.task_id,d.depends_on,COALESCE(t.status,'missing') AS ds "
            "FROM task_deps d LEFT JOIN tasks t ON t.id=d.depends_on").fetchall():
        if r["ds"] == "completed" or r["task_id"] not in nodes:
            continue  # satisfied prerequisites and settled dependents leave the graph
        prereqs.setdefault(r["task_id"], {})[r["depends_on"]] = r["ds"]
    wave_ids = []
    done = set()
    pending = set(nodes)
    while True:
        ready = sorted(
            (n for n in pending if all(p in done for p in prereqs.get(n, ()))),
            key=lambda n: (_prio_rank(nodes[n]["priority"]), nodes[n]["due_at"] == "",
                           nodes[n]["due_at"], nodes[n]["created_at"], n))
        if not ready:
            break
        wave_ids.append(ready)
        done.update(ready)
        pending.difference_update(ready)
    return {
        "waves": [{"wave": i + 1,
                   "tasks": [{"id": n, **{k: nodes[n][k] for k in ("title", "status", "priority")}}
                             for n in ids]}
                  for i, ids in enumerate(wave_ids)],
        "unschedulable": [{"id": n,
                           **{k: nodes[n][k] for k in ("title", "status", "priority")},
                           "blocked_by": [{"id": p, "status": s}
                                          for p, s in sorted(prereqs[n].items()) if p not in done]}
                          for n in sorted(pending)],
        "waves_total": len(wave_ids),
        "scheduled_tasks": len(done),
        "open_tasks": len(nodes),
    }

def plan(args):
    """Parallel dispatch-wave schedule for the open dependency DAG.

    `critical-path` answers how many waves and which chain bounds them; `plan`
    answers which tasks run in each wave. Wave 1 is every ready task (in-flight
    included), each later wave is what the previous waves unblock, and anything
    that can never start inside the requested scope lands in `unschedulable`
    with its blockers. Read-only and deterministic: same fleet state, same
    schedule. Use it to brief operators, size parallel capacity per wave, or
    sanity-check that dispatch is actually draining the graph in wave order.
    """
    project = (args.project or "").strip()
    tag = (getattr(args, "tag", "") or "").strip()
    with conn() as db:
        out = _dispatch_plan(db, project, tag)
    json_out({"ok": True, **({"project": project} if project else {}),
              **({"tag": tag} if tag else {}), **out})

def verify_chain(args):
    """Recompute the audit hash chain; optionally pin-check against a sealed
    checkpoint file to also detect tail truncation or history rewriting."""
    with conn() as db:
        problems = audit_chain_problems(db)
        total = db.execute("SELECT COUNT(*) n FROM audit_events").fetchone()["n"]
        out = {"ok": not problems, "events": total, "problems": problems}
        cp_path = (getattr(args, "checkpoint", "") or "").strip()
        if cp_path:
            cp = _load_checkpoint(cp_path)
            cprobs = checkpoint_problems(db, cp)
            out["problems"] = problems + cprobs
            out["ok"] = not out["problems"]
            out["checkpoint"] = {"path": cp_path, "ok": not cprobs,
                                 "last_event_id": cp["last_event_id"],
                                 "last_event_hash": cp["last_event_hash"],
                                 "total_events": cp["total_events"],
                                 "created_at": cp["created_at"]}
    json_out(out)

def events(args):
    """Query the global audit event stream with filters; newest first.

    Supports entity/action filters, an ISO 8601 --since/--until window
    (normalized to UTC), a limit, and optional inline chain verification so
    operators can confirm the ledger is intact in the same call.
    """
    clauses, vals = [], []
    for col in ("entity_type", "entity_id", "action"):
        v = getattr(args, col, None)
        if v:
            clauses.append(col + "=?"); vals.append(v)
    since = _normalize_iso(getattr(args, "since", "") or "", "--since")
    until = _normalize_iso(getattr(args, "until", "") or "", "--until")
    if since:
        clauses.append("created_at>=?"); vals.append(since)
    if until:
        clauses.append("created_at<=?"); vals.append(until)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    limit = max(0, args.limit)
    with conn() as db:
        total = db.execute("SELECT COUNT(*) n FROM audit_events" + where, vals).fetchone()["n"]
        rows = [dict(r) for r in db.execute(
            "SELECT id,entity_type,entity_id,action,payload_json,created_at,prev_hash,hash "
            "FROM audit_events" + where + " ORDER BY id DESC LIMIT ?", (*vals, limit)).fetchall()]
        problems = audit_chain_problems(db) if args.verify else None
    for r in rows:
        r["payload"] = json.loads(r.pop("payload_json"))
    out = {"ok": True, "count": len(rows), "total_matching": total,
           "truncated": total > len(rows), "events": rows}
    if problems is not None:
        out["chain"] = {"ok": not problems, "problems": problems}
    json_out(out)

def claim(args):
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in {"completed", "cancelled"}:
            raise SystemExit(f"cannot claim terminal task: {row['status']}")
        if row["status"] == "blocked":
            raise SystemExit("task is blocked: %s; unblock before claiming"
                             % (row["blocked_reason"] or "no reason recorded"))
        pending = unsatisfied_deps(db, args.id)
        if pending:
            raise SystemExit("unsatisfied dependencies: " + ", ".join(f"{d['id']}({d['status']})" for d in pending))
        # Seam guard: refuse to claim a task whose worktree/branch is held by
        # another live lease — two agents on the same checkout collide no
        # matter what the task graph says. --force is a deliberate override.
        if not getattr(args, "force", False):
            conflicts = _seam_conflicts(db, args.id, row["worktree"], row["branch"], row["project"])
            if conflicts:
                _audit_seam_refusal(args.id, args.owner, conflicts)
                raise SystemExit(_seam_message(conflicts))
        # Dispatch policy gates (policies/<project>.yaml): a project may
        # require a capability tag on its work and cap how many live leases
        # one owner holds inside it. --force is the deliberate override; the
        # override is recorded in the claimed event so provenance survives.
        policy = _project_policy(row["project"])
        overrides = []
        required_tag = _dispatch_required_tag(policy)
        if required_tag and required_tag not in _task_tags(row):
            if getattr(args, "force", False):
                overrides.append({"gate": "dispatch_requires_tag", "required_tag": required_tag})
            else:
                _audit_claim_refusal(args.id, args.owner, "claim_refused_policy",
                                     {"gate": "dispatch_requires_tag", "required_tag": required_tag})
                raise SystemExit(
                    f"project policy requires tag {required_tag!r} before dispatch; "
                    f"tag the task or pass --force")
        cap = _wip_cap(policy)
        if cap:
            held = _owner_project_leases(db, args.owner, row["project"], args.id)
            if len(held) >= cap:
                if getattr(args, "force", False):
                    overrides.append({"gate": "max_wip_per_owner", "cap": cap, "held": held})
                else:
                    _audit_claim_refusal(args.id, args.owner, "claim_refused_policy",
                                         {"gate": "max_wip_per_owner", "cap": cap, "held": held})
                    raise SystemExit(
                        f"project policy caps owner '{args.owner}' at {cap} live leases in "
                        f"{row['project']} (held: {', '.join(held)}); complete/release first or pass --force")
        # Atomic acquire: the WHERE guard makes the lease check-and-set a single
        # statement so concurrent claimers cannot both win the same lease.
        acquired, exp, epoch = _acquire(db, args.id, args.owner, args.minutes, resolve_max_active(args))
        if not acquired:
            raise SystemExit(_explain_acquire_failure(db, args.id, args.owner, resolve_max_active(args)))
        db.execute("INSERT INTO heartbeats(task_id,owner,state,at,note) VALUES(?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET owner=excluded.owner,state=excluded.state,at=excluded.at,note=excluded.note", (args.id,args.owner,"claimed",now(),"lease claimed"))
        audit(db, "task", args.id, "claimed", {"owner": args.owner, "lease_expires_at": exp, "lease_epoch": epoch,
                                               **({"policy_overrides": overrides} if overrides else {})})
        out = _task_view(task_row(db, args.id))
        json_out(out)

def add_dep(args):
    with conn() as db:
        add_dependency(db, args.id, args.depends_on)
    json_out({"ok": True, "task_id": args.id, "depends_on": args.depends_on})

def remove_dep(args):
    """Remove a dependency edge, correcting mistaken `dep` / create --depends-on calls."""
    with conn() as db:
        task_row(db, args.id)
        task_row(db, args.depends_on)
        cur = db.execute("DELETE FROM task_deps WHERE task_id=? AND depends_on=?",
                         (args.id, args.depends_on))
        if cur.rowcount != 1:
            raise SystemExit(f"no such dependency: {args.id} does not depend on {args.depends_on}")
        audit(db, "task", args.id, "dependency_removed", {"depends_on": args.depends_on})
    json_out({"ok": True, "task_id": args.id, "removed": args.depends_on})

def _dispatch_candidates(db, args, t_now):
    """Stage 1 — candidates: queued tasks in scope (project/tag), due-ordered."""
    q = "SELECT * FROM tasks WHERE status='queued'"
    vals = []
    if args.project:
        q += " AND project=?"; vals.append(args.project)
    if getattr(args, "tag", ""):
        # Tag-scoped dispatch: an agent constrained to a capability (e.g.
        # --tag autopilot-safe) only ever sees work marked for it. The
        # LIKE filter is exact because tags are validated against a
        # charset that cannot contain quotes.
        q += " AND tags LIKE ?"; vals.append('%"' + _valid_tag(args.tag) + '"%')
    q += _due_order()
    return db.execute(q, vals).fetchall()

def _filter_candidate(db, r, args, t_now, explain, skipped, pol_cache, inherited,
                      t_dt=None):
    t_dt = t_dt or datetime.now(timezone.utc)
    """Stage 2 — filter: one candidate's admissibility gates, in order.

    Returns (eligible_tuple | None). Refusals append an explain record to
    `skipped` when `explain` is set: deferred_until, unsatisfied_dependencies,
    recovery_backoff, policy_missing_tag, policy_wip_cap, seam_conflict.
    """
    if r["not_before"] and r["not_before"] > t_now:
        if explain:
            skipped.append({"task_id": r["id"], "reason": "deferred_until",
                            "not_before": r["not_before"]})
        return None
    pending = unsatisfied_deps(db, r["id"])
    if pending:
        if explain:
            skipped.append({"task_id": r["id"], "reason": "unsatisfied_dependencies",
                            "blocked_by": [d["id"] for d in pending]})
        return None
    if r["recover_after"] and r["recover_after"] > t_now:
        if explain:
            skipped.append({"task_id": r["id"], "reason": "recovery_backoff",
                            "recover_after": r["recover_after"]})
        return None
    # Project dispatch policy: cache per project so a fleet-wide
    # sweep reads each policy file once.
    pol = pol_cache.get(r["project"])
    if pol is None:
        pol = pol_cache[r["project"]] = _project_policy(r["project"])
    required_tag = _dispatch_required_tag(pol)
    if required_tag and required_tag not in _task_tags(r):
        if explain:
            skipped.append({"task_id": r["id"], "reason": "policy_missing_tag",
                            "required_tag": required_tag})
        return None
    if args.claim:
        cap = _wip_cap(pol)
        if cap:
            held = _owner_project_leases(db, args.owner, r["project"], r["id"])
            if len(held) >= cap:
                if explain:
                    skipped.append({"task_id": r["id"], "reason": "policy_wip_cap",
                                    "cap": cap, "held": held})
                return None
    if args.claim:
        # Dispatch must not hand out work whose seam (worktree/branch)
        # is already held by another live lease — the claim would
        # collide physically. Skip rather than fail after picking.
        conflicts = _seam_conflicts(db, r["id"], r["worktree"], r["branch"], r["project"])
        if conflicts:
            if explain:
                skipped.append({"task_id": r["id"], "reason": "seam_conflict",
                                "conflicts": conflicts})
            return None
    eff, boost = _effective_priority(r, t_dt, getattr(args, "aging_minutes", 360),
                                     getattr(args, "aging_boost", 2))
    inh = inherited.get(r["id"])
    via = None
    if inh is not None and _prio_rank(inh[0]) < _prio_rank(eff):
        eff, via = inh[0], inh[1]
    return (eff, boost, r, via, unblock_count(db, r["id"]))

def _rank_candidates(eligible, prefer_unblocking):
    """Stage 3 — rank: order the eligible candidates and return the pick.

    Effective priority first, then earliest deadline (undated last),
    then oldest-created first: within one effective tier the
    longest-waiting task wins, which is what makes aging fair.
    With --prefer-unblocking, the count of queued direct dependents
    breaks ties before age: between equally urgent, equally due
    candidates, finishing the hub frees more of the graph than
    finishing a leaf (critical-path scheduling as a tie-break — it
    never overrides priority or deadlines).
    """
    if not eligible:
        return None
    eligible = sorted(eligible, key=lambda e: (
        _prio_rank(e[0]), e[2]["due_at"] == "", e[2]["due_at"],
        (-e[4] if prefer_unblocking else 0), e[2]["created_at"]))
    return eligible[0]

def _claim_pick(db, picked, args):
    """Stage 4 — claim: acquire the lease for the picked candidate.

    Mirrors `claim` exactly: fenced acquire, heartbeat row, audited
    claimed event with via=next. Returns (expires_at, epoch).
    """
    cap = resolve_max_active(args)
    acquired, exp, epoch = _acquire(db, picked["id"], args.owner, args.minutes, cap)
    if not acquired:
        raise SystemExit(_explain_acquire_failure(db, picked["id"], args.owner, cap))
    db.execute("INSERT INTO heartbeats(task_id,owner,state,at,note) VALUES(?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET owner=excluded.owner,state=excluded.state,at=excluded.at,note=excluded.note", (picked["id"],args.owner,"claimed",now(),"claimed via next"))
    audit(db, "task", picked["id"], "claimed", {"owner": args.owner, "lease_expires_at": exp, "lease_epoch": epoch, "via": "next"})
    return exp, epoch

def next_task(args):
    """Dispatch: highest-priority queued task whose dependencies are all completed.

    Tasks in recovery backoff (recover_after in the future, set by ops.py
    recover after a stale lease or by `fail` after a reported execution
    failure) are skipped so failing work cannot hot-loop
    through dispatch; an explicit claim remains allowed as a deliberate
    operator override. Deferred tasks (not_before in the future, set via
    defer) are skipped the same way until their scheduled instant arrives.

    Dispatch aging guards against starvation: a queued task that has waited
    --aging-minutes (default 360) per level, up to --aging-boost levels
    (default 2), is dispatched at a virtually promoted priority so fresh P0/P1
    work cannot crowd out old P3 work forever. Aging is computed from
    created_at at dispatch time and never mutates stored priority; pass
    --aging-minutes 0 to restore strict static ordering.

    With --explain, the result also reports how many queued candidates were
    considered and why each skipped candidate was not picked
    (unsatisfied_dependencies with the blocking ids, recovery_backoff with its
    cooldown deadline, deferred_until with its not_before, seam_conflict
    when another live lease holds the candidate's worktree/branch,
    policy_missing_tag / policy_wip_cap when the candidate's project policy
    gates it out of dispatch), plus the effective priority of the pick when
    aging boosted it.

    Project dispatch policy (policies/<project>.yaml) is enforced here the
    same way it is on direct claim: `dispatch_requires_tag: <tag>` keeps
    untagged work in the project undispatchable (direct claim refuses unless
    --force), and `max_wip_per_owner: N` makes dispatch skip candidates whose
    project is at the owner's live-lease cap instead of failing after the
    pick — so a multi-project dispatcher is steered toward work it may
    actually take.

    --tag scopes dispatch to tasks carrying that capability/scope tag
    (see `tag`/`untag`): an agent constrained to `--tag autopilot-safe` never
    receives untagged or differently-tagged work, so policy lives in the task
    graph instead of per-agent prompts.

    --prefer-unblocking adds a critical-path tie-break: within one effective
    priority tier and deadline class, candidates are ordered by descending
    count of queued direct dependents (`unblocks`), so finishing a hub frees
    more of the graph than finishing a leaf. It never overrides priority,
    deadlines, or aging fairness; without the flag, ordering is unchanged.

    With --claim --recall, the dispatch response embeds the full sealed recall
    bundle (digest, lease state, receipts) for the claimed task and audits it
    as `context_recalled` — one call takes work AND proves which context it
    was taken against, instead of a follow-up `recall` round trip. The agent
    defaults to the claiming --owner; --budget/--related/--related-scope tune
    the bundle exactly like `recall`.

    The pipeline is staged for independent testability: `_dispatch_candidates`
    selects, `_filter_candidate` gates each row, `_rank_candidates` orders and
    picks, and `_claim_pick` acquires the lease.
    """
    explain = bool(getattr(args, "explain", False))
    if getattr(args, "recall", False) and not args.claim:
        raise SystemExit("--recall requires --claim: the bundle seals the context of the task you claimed")
    t_now = now()
    t_dt = datetime.now(timezone.utc)
    with conn() as db:
        inherited = _inherit_priorities(db)
        rows = _dispatch_candidates(db, args, t_now)
        eligible = []
        skipped = []
        pol_cache = {}
        for r in rows:
            got = _filter_candidate(db, r, args, t_now, explain, skipped, pol_cache, inherited)
            if got is not None:
                eligible.append(got)
        prefer_unblocking = bool(getattr(args, "prefer_unblocking", False))
        pick = _rank_candidates(eligible, prefer_unblocking)
        picked = pick[2] if pick else None
        out = {"ok": True, "task": _task_view(picked) if picked else None}
        if picked is not None:
            out["unblocks"] = pick[4]
        if explain:
            out["considered"] = len(rows)
            out["skipped"] = skipped
            if picked is not None:
                out["effective_priority"] = pick[0]
                out["priority_boost"] = pick[1]
                if pick[3]:
                    out["inherited_via"] = pick[3]
            if prefer_unblocking:
                out["unblock_scheduling"] = True
        if picked is None:
            json_out(out)
            return
        if args.claim:
            exp, epoch = _claim_pick(db, picked, args)
            out["claimed"] = True
            out["lease_expires_at"] = exp
            out["lease_epoch"] = epoch
            if getattr(args, "recall", False):
                agent = (getattr(args, "agent", "") or "").strip() or args.owner
                bundle = _build_recall_bundle(db, picked["id"], agent, args.recall_budget,
                                              getattr(args, "related", 0),
                                              getattr(args, "related_scope", "project"),
                                              rerank=bool(getattr(args, "rerank", False)),
                                              recency_half_life_hours=getattr(args, "recency_half_life_hours", 168.0),
                                              pinned_boost=getattr(args, "pinned_boost", 0.5),
                                              rel_handoffs=getattr(args, "related_handoffs", 0),
                                      dep_context=getattr(args, "dep_context", 0),
                                      rel_sessions=getattr(args, "related_sessions", 0),
                                      rel_facts=getattr(args, "related_facts", 0),
                                      rel_semantic=getattr(args, "related_semantic", 0))
                # Same provenance contract as `recall`: the digest is recorded
                # with its bundle parameters so handoffs/completions can cite
                # it and fleet sweeps can recompute it exactly.
                audit(db, "task", picked["id"], "context_recalled",
                      {"agent": bundle["agent"], "digest": bundle["digest"],
                       "core_digest": bundle["core_digest"],
                       "budget": args.recall_budget, "related": getattr(args, "related", 0),
                       "related_scope": getattr(args, "related_scope", "project"),
                       "rerank": bool(getattr(args, "rerank", False)),
                       "recency_half_life_hours": getattr(args, "recency_half_life_hours", 168.0),
                       "pinned_boost": getattr(args, "pinned_boost", 0.5),
                       "related_handoffs": getattr(args, "related_handoffs", 0),
                       "dep_context": getattr(args, "dep_context", 0),
                "related_sessions": getattr(args, "related_sessions", 0),
                       "related_facts": getattr(args, "related_facts", 0),
                       "related_semantic": getattr(args, "related_semantic", 0),
                       "sections": _bundle_sections(bundle),
                       "via": "next"})
                out["recall"] = bundle
                out["recall_digest"] = bundle["digest"]
        json_out(out)



def _handoff_parser_actions():
    """Introspect the handoff subparser's argument spec (single source of truth)."""
    import argparse as _argparse
    parser = _argparse.ArgumentParser(prog="autopilot")
    sub = parser.add_subparsers(dest="cmd")
    # Rebuild just the handoff subparser exactly as main() registers it.
    p = sub.add_parser("handoff")
    p.add_argument("task_id")
    p.add_argument("--from-agent", required=True)
    p.add_argument("--to-agent", default="")
    p.add_argument("--status", default="")
    p.add_argument("--objective", default="")
    p.add_argument("--evidence", action="append", default=[])
    p.add_argument("--constraint", dest="constraints", action="append", default=[])
    p.add_argument("--decision", dest="decisions", action="append", default=[])
    p.add_argument("--file", dest="files", action="append", default=[])
    p.add_argument("--commit", dest="commit_ref", default="")
    p.add_argument("--next-action", dest="next_actions", action="append", default=[])
    p.add_argument("--risk", dest="risks", action="append", default=[])
    p.add_argument("--recall-digest", dest="recall_digest", default="")
    return p._actions

_PACK_SECTION_FLAGS = {
    "related": "--related",
    "related_handoffs": "--related-handoffs",
    "dep_context": "--dep-context",
    "related_sessions": "--related-sessions",
    "related_facts": "--related-facts",
    "related_semantic": "--related-semantic",
}

def _protocol_doc() -> dict:
    """Build the machine-readable handoff-protocol self-description.

    Generated from the live code wherever possible (the argparse registry,
    status/priority/kind vocabularies, dispatch skip reasons) so it cannot
    silently drift from behavior. Sealed with the house digest format:
    sha256 over the sorted-key JSON body with `created_at` outside.
    """
    # Handoff field contract, generated from the parser spec so new flags
    # appear automatically.
    handoff_fields = {}
    for a in _handoff_parser_actions():
        if a.dest in ('help', 'fn'):
            continue
        handoff_fields[a.dest] = {
            'flags': sorted(a.option_strings),
            'required': a.required,
            'repeatable': a.nargs == 0 or 'append' in str(getattr(a, 'action', '')),
            'default': None if a.default is None else (a.default if isinstance(a.default, (str, int, float, bool, list)) else str(a.default)),
        }
    doc = {
        'format': 'autopilot-protocol-v1',
        'handoff_field_contract': {
            'command': 'autopilot.py handoff <task_id> [fields]',
            'fields': handoff_fields,
            'supersession': 'writing a new handoff atomically supersedes the previous live one; history stays queryable via handoffs --all and handoff-history',
            'deduplication': 'an identical payload deduplicates onto the live handoff instead of adding a row',
        },
        'recall_ack_receipt_loop': {
            'recall': {
                'commands': ['recall', 'resume', 'next --claim --recall'],
                'proves': 'context_recalled audit event carrying digest + core_digest + bundle parameters',
                'digest': 'sha256 over the sealed bundle (recalled_at excluded); core_digest additionally excludes the handoff section and used/truncated counters',
            },
            'acknowledge': {
                'command': 'ack <task_id> --agent <recipient>',
                'proves': 'handoff_acknowledged audit event; idempotent re-ack; supersession resets acceptance',
            },
            'receipt': {
                'command': 'receipt <task_id> --kind <kind> --payload <json>',
                'proves': 'sealed receipt file whose sha256 is recorded in SQLite; completions may cite evidence receipts; definition-of-done gates on required kinds',
            },
            'freshness_sweeps': ['recall-verify', 'recall-diff', 'ops.py recall-stale'],
            'lint': 'ops.py handoff-check enforces objective/addressing/evidence/provenance/SLA',
        },
        'status_machine': {
            'statuses': sorted(STATUSES),
            'terminal_statuses': sorted(TERMINAL_STATUSES),
            'transitions': {
                'queued': ['claimed', 'blocked', 'cancelled'],
                'claimed': ['running', 'queued', 'completed', 'failed', 'blocked', 'cancelled'],
                'running': ['completed', 'failed', 'queued', 'blocked', 'cancelled'],
                'waiting_for_agent': ['queued', 'cancelled'],
                'waiting_for_user': ['queued', 'cancelled'],
                'waiting_for_review': ['queued', 'ready_to_merge', 'cancelled'],
                'ready_to_merge': ['ready_to_deploy', 'cancelled'],
                'ready_to_deploy': ['completed', 'cancelled'],
                'blocked': ['queued', 'cancelled'],
                'completed': [], 'failed': [], 'cancelled': [],
            },
            'lease_rules': {
                'exclusivity': 'one live lease per task; second owner refused',
                'fencing': 'each acquisition bumps lease_epoch; stale epochs are refused',
                'expiry': 'an expired lease requeues the task and consumes retry budget',
            },
        },
        'refusal_vocabulary': {
            'handoff_lint_reasons': sorted({
                'unaddressed', 'missing_objective', 'sparse_no_evidence_or_next_actions',
                'unproven_recall_digest', 'older_than_latest_recall',
                'terminal_task_handoff', 'stale_unacknowledged'}),
            'recall_states': sorted({'fresh', 'stale', 'unproven_recall_digest'}),
            'dispatch_skip_reasons': sorted({
                'unsatisfied_dependencies', 'recovery_backoff', 'deferred_until',
                'seam_conflict', 'policy_missing_tag', 'policy_wip_cap'}),
            'secret_guard_kinds_note': 'credential-shaped content is refused by note/handoff/fact/export paths unless --redact or --allow-secret',
        },
        'flag_gated_pack_sections': [
            {'prefix': s.prefix, 'keys': s.keys,
             'limit_flag': _PACK_SECTION_FLAGS.get(s.prefix)}
            for s in _pack_spec()
        ],
    }
    return doc

def protocol(args):
    """Emit the sealed protocol document (stable across calls)."""
    doc = _protocol_doc()
    doc['created_at'] = now()
    doc['sha256'] = hashlib.sha256(json.dumps(
        {k: v for k, v in doc.items() if k not in ('sha256', 'created_at')},
        sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    json_out(doc)

def hindsight_retain(args):
    """Guarded retain of facts/decisions into the Hindsight bank.

    Write path behind the same secret guard as notes: credential-shaped
    content is refused unless --redact or --allow-secret. Appends one JSONL
    line to bank.jsonl (creating the bank directory only when explicitly
    asked via --create); refuses when no bank is configured so a typo'd
    environment never forks an accidental new memory store. The append is
    audited in the Autopilot ledger with the memory id.
    """
    text = (args.text or "").strip()
    if not text:
        raise SystemExit("--text is required")
    with conn() as db:
        kinds, guarded = _secret_guard(
            {"content": text}, getattr(args, "redact", False),
            getattr(args, "allow_secret", False), args.task_id or "hindsight")
        t = now()
        bank = _hindsight_bank_path()
        if not _hindsight_available():
            if not args.create:
                raise SystemExit(
                    f"no Hindsight bank configured at {bank}; "
                    "pass --create to initialize one")
            bank.parent.mkdir(parents=True, exist_ok=True)
        mid = "hs-" + hashlib.sha256(
            f"{t}|{guarded['content']}".encode()).hexdigest()[:16]
        mem = {"id": mid, "text": guarded["content"], "kind": args.kind,
               "project": args.project or "", "created_at": t,
               "tags": [x for x in (args.tag or []) if x],
               **({"secret_kinds": kinds} if kinds else {})}
        with bank.open("a", encoding="utf-8") as f:
            f.write(json.dumps(mem, sort_keys=True) + "\n")
        audit(db, "task", args.task_id or "", "hindsight_retained",
              {"memory_id": mid, "kind": args.kind,
               "project": mem["project"],
               **({"secret_kinds": kinds} if kinds else {})})
        json_out({"ok": True, "memory_id": mid, "bank": str(bank),
                  **({"secret_kinds": kinds} if kinds else {})})

def heartbeat(args):
    with conn() as db:
        row = task_row(db, args.id)
        _require_epoch(row, getattr(args, "epoch", None), "heartbeat")
        t = now()
        exp = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + 15 * 60, timezone.utc).replace(microsecond=0).isoformat()
        # Atomic renewal: only the current lease holder with a live lease can renew.
        cur = db.execute(
            "UPDATE tasks SET status='running', lease_owner=?, lease_expires_at=?, updated_at=? "
            "WHERE id=? AND (lease_owner=? OR lease_owner='') AND (lease_expires_at='' OR lease_expires_at>?)",
            (args.owner, exp, t, args.id, args.owner, t))
        if cur.rowcount != 1:
            if row["lease_owner"] and row["lease_owner"] != args.owner:
                raise SystemExit(f"lease owned by {row['lease_owner']}")
            raise SystemExit("lease expired; reclaim before heartbeat")
        db.execute("INSERT INTO heartbeats(task_id,owner,state,at,note) VALUES(?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET owner=excluded.owner,state=excluded.state,at=excluded.at,note=excluded.note", (args.id,args.owner,"alive",now(),args.note))
        audit(db, "task", args.id, "heartbeat", {"owner": args.owner, "lease_expires_at": exp})
        json_out({"ok": True, "task_id": args.id, "status": "running", "lease_expires_at": exp,
                  "lease_epoch": row["lease_epoch"], "heartbeat_at": now()})

def receipt(args):
    payload = json.loads(args.payload) if args.payload else {}
    rid = uuid.uuid4().hex
    created = now()
    data = json.dumps({"id":rid,"task_id":args.task_id,"kind":args.kind,"created_at":created,"payload":payload}, indent=2, sort_keys=True)+"\n"
    # Seal the receipt file: the sha256 recorded in SQLite must match the bytes
    # on disk, so doctor can detect silent corruption or tampering later.
    file_hash = hashlib.sha256(data.encode()).hexdigest()
    with conn() as db:
        task_row(db, args.task_id)
        db.execute("INSERT INTO receipts(id,task_id,kind,payload_json,created_at,file_hash) VALUES(?,?,?,?,?,?)", (rid,args.task_id,args.kind,json.dumps(payload,sort_keys=True),created,file_hash))
        db.execute("UPDATE tasks SET last_receipt=?,updated_at=? WHERE id=?", (rid,created,args.task_id))
        audit(db, "task", args.task_id, "receipt", {"receipt_id": rid, "kind": args.kind})
    target = RECEIPTS / f"{rid}.json"
    fd, tmp = tempfile.mkstemp(prefix=f".{rid}.", dir=RECEIPTS)
    try:
        with os.fdopen(fd, "w") as f: f.write(data); f.flush(); os.fsync(f.fileno())
        os.chmod(tmp, 0o600); os.replace(tmp, target)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    json_out({"ok": True, "receipt_id": rid, "task_id": args.task_id, "sha256": file_hash})

def show(args):
    with conn() as db:
        out = _task_view(task_row(db, args.id))
        receipts = [dict(r) for r in db.execute(
            "SELECT id,kind,payload_json,created_at FROM receipts WHERE task_id=? ORDER BY created_at DESC, id DESC LIMIT ?",
            (args.id, args.limit)).fetchall()]
        events = [dict(r) for r in db.execute(
            "SELECT action,payload_json,created_at FROM audit_events WHERE entity_type='task' AND entity_id=? ORDER BY id DESC LIMIT ?",
            (args.id, args.limit)).fetchall()]
        deps = [dict(r) for r in db.execute(
            "SELECT d.depends_on AS id, COALESCE(t.status,'missing') AS status, (COALESCE(t.status,'')='completed') AS satisfied "
            "FROM task_deps d LEFT JOIN tasks t ON t.id=d.depends_on WHERE d.task_id=? ORDER BY d.created_at",
            (args.id,)).fetchall()]
        dependents = [dict(r) for r in db.execute(
            "SELECT d.task_id AS id, COALESCE(t.status,'missing') AS status FROM task_deps d "
            "LEFT JOIN tasks t ON t.id=d.task_id WHERE d.depends_on=? ORDER BY d.created_at",
            (args.id,)).fetchall()]
        hrow = _live_handoff(db, args.id)
    for r in receipts:
        r["payload"] = json.loads(r.pop("payload_json"))
    for e in events:
        e["payload"] = json.loads(e.pop("payload_json"))
    out["receipts"] = receipts
    out["audit"] = events
    out["dependencies"] = deps
    out["dependents"] = dependents
    out["handoff"] = _handoff_parsed(hrow, include_meta=True) if hrow else None
    json_out(out)

def metrics(args):
    t = now()
    with conn() as db:
        by_status = {r["status"]: r["n"] for r in db.execute("SELECT status,COUNT(*) n FROM tasks GROUP BY status")}
        by_project = {r["project"]: r["n"] for r in db.execute("SELECT project,COUNT(*) n FROM tasks GROUP BY project")}
        stale = db.execute(
            "SELECT COUNT(*) n FROM tasks WHERE lease_expires_at!='' AND lease_expires_at<? AND status IN ('claimed','running','waiting_for_agent')",
            (t,)).fetchone()["n"]
        retries = db.execute("SELECT COALESCE(SUM(retry_count),0) s,COALESCE(MAX(retry_count),0) m FROM tasks").fetchone()
        blocked_by_deps = db.execute(
            "SELECT COUNT(*) n FROM tasks t WHERE t.status='queued' AND EXISTS("
            "SELECT 1 FROM task_deps d JOIN tasks dt ON dt.id=d.depends_on WHERE d.task_id=t.id AND dt.status!='completed')").fetchone()["n"]
        in_backoff = db.execute(
            "SELECT COUNT(*) n FROM tasks WHERE status='queued' AND recover_after!='' AND recover_after>?",
            (t,)).fetchone()["n"]
        deferred = db.execute(
            "SELECT COUNT(*) n FROM tasks WHERE status='queued' AND not_before!='' AND not_before>?",
            (t,)).fetchone()["n"]
        leases_by_owner = {r["lease_owner"]: r["n"] for r in db.execute(
            "SELECT lease_owner,COUNT(*) n FROM tasks WHERE lease_owner!='' AND lease_expires_at!='' AND lease_expires_at>? GROUP BY lease_owner",
            (t,))}
        receipts = db.execute("SELECT COUNT(*) n FROM receipts").fetchone()["n"]
        events = db.execute("SELECT COUNT(*) n FROM audit_events").fetchone()["n"]
        overdue = db.execute(
            "SELECT id FROM tasks WHERE due_at!='' AND due_at<=? AND status NOT IN ('completed','failed','cancelled') "
            "ORDER BY due_at", (t,)).fetchall()
        horizon = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + 24 * 3600, timezone.utc).replace(microsecond=0).isoformat()
        due_soon = db.execute(
            "SELECT COUNT(*) n FROM tasks WHERE due_at!='' AND due_at>? AND due_at<=? AND status NOT IN ('completed','failed','cancelled')",
            (t, horizon)).fetchone()["n"]
        notes = db.execute("SELECT COUNT(*) total, SUM(superseded_by!='') superseded, "
                           "SUM(CASE WHEN pinned!=0 AND superseded_by='' THEN 1 ELSE 0 END) pinned, "
                           "SUM(CASE WHEN superseded_by='' AND pinned=0 AND expires_at!='' AND expires_at<=? THEN 1 ELSE 0 END) expired "
                           "FROM notes", (t,)).fetchone()
        handoffs = db.execute("SELECT COUNT(*) total, SUM(superseded_by!='') superseded, "
                              "SUM(CASE WHEN recall_digest!='' THEN 1 ELSE 0 END) proven, "
                              "SUM(CASE WHEN acked_by!='' THEN 1 ELSE 0 END) acked FROM handoffs").fetchone()
        consolidated = db.execute(
            "SELECT COUNT(*) n FROM audit_events WHERE action='note_consolidated'").fetchone()["n"]
        secrets = {a: db.execute(
            "SELECT COUNT(*) n FROM audit_events WHERE action=?", (a,)).fetchone()["n"]
            for a in ("secret_blocked", "secret_redacted", "secret_allowed")}
        failures = {a: db.execute(
            "SELECT COUNT(*) n FROM audit_events WHERE action=?", (a,)).fetchone()["n"]
            for a in ("task_failed", "task_failed_terminal")}
        claims_refused_by_policy = db.execute(
            "SELECT COUNT(*) n FROM audit_events WHERE action='claim_refused_policy'").fetchone()["n"]
        cp_len = _critical_path(db)["length"]
        completed_no_receipt = db.execute(
            "SELECT COUNT(*) n FROM tasks t WHERE t.status='completed' AND NOT EXISTS("
            "SELECT 1 FROM receipts r WHERE r.task_id=t.id)").fetchone()["n"]
        # Definition-of-done observability: open work whose acceptance criteria
        # are not yet satisfiable, and completions the gate has refused.
        awaiting_evidence = []
        for r in db.execute(
                "SELECT id,requires_receipts FROM tasks WHERE "
                "requires_receipts!='' AND requires_receipts!='[]' AND "
                "status NOT IN ('completed','failed','cancelled')").fetchall():
            if _missing_required_evidence(db, r["id"], _task_requires(r)):
                awaiting_evidence.append(r["id"])
        completions_blocked = db.execute(
            "SELECT COUNT(*) n FROM audit_events WHERE action='completion_blocked_evidence'").fetchone()["n"]
        sessions = db.execute(
            "SELECT COUNT(*) n, COALESCE(SUM(message_count),0) m FROM sessions").fetchone()
        facts = db.execute(
            "SELECT COUNT(*) total, COALESCE(SUM(CASE WHEN valid_until='' OR valid_until>? "
            "THEN 1 ELSE 0 END),0) live FROM facts", (t,)).fetchone()
        pruned_total = db.execute(
            "SELECT COUNT(*) n FROM audit_events WHERE action='session_pruned'").fetchone()["n"]
    json_out({
        "generated_at": t,
        "tasks_total": sum(by_status.values()),
        "tasks_by_status": by_status,
        "tasks_by_project": by_project,
        "stale_leases": stale,
        "active_leases_by_owner": leases_by_owner,
        "queued_blocked_by_deps": blocked_by_deps,
        "critical_path_length": cp_len,
        "tasks_in_backoff": in_backoff,
        "queued_deferred": deferred,
        "overdue_tasks": [r["id"] for r in overdue],
        "due_within_24h": due_soon,
        "total_retries": retries["s"],
        "max_retries_seen": retries["m"],
        "receipts": receipts,
        "audit_events": events,
        "notes_total": notes["total"] or 0,
        "notes_superseded": notes["superseded"] or 0,
        "notes_pinned_live": notes["pinned"] or 0,
        "notes_expired_live": notes["expired"] or 0,
        "notes_consolidated_total": consolidated,
        "secrets_blocked_total": secrets["secret_blocked"],
        "secrets_redacted_total": secrets["secret_redacted"],
        "secrets_allowed_total": secrets["secret_allowed"],
        "failures_retried_total": failures["task_failed"],
        "failures_terminal_total": failures["task_failed_terminal"],
        "claims_refused_by_policy": claims_refused_by_policy,
        "handoffs_total": handoffs["total"] or 0,
        "handoffs_superseded": handoffs["superseded"] or 0,
        "handoffs_with_recall_proof": handoffs["proven"] or 0,
        "handoffs_acked_total": handoffs["acked"] or 0,
        "completions_without_receipt": completed_no_receipt,
        "tasks_missing_required_evidence": sorted(awaiting_evidence),
        "completions_blocked_by_evidence": completions_blocked,
        "sessions_indexed": sessions["n"],
        "session_messages_indexed": sessions["m"],
        "sessions_pruned_total": pruned_total,
        "facts_total": facts["total"],
        "facts_live": facts["live"],
        "facts_closed": facts["total"] - facts["live"],
    })

def list_tasks(args):
    with conn() as db:
        q = "SELECT * FROM tasks"
        vals = []
        clauses = []
        if args.status:
            clauses.append("status=?"); vals.append(args.status)
        if args.project:
            clauses.append("project=?"); vals.append(args.project)
        if getattr(args, "overdue", False):
            clauses.append("due_at!='' AND due_at<=? AND status NOT IN ('completed','failed','cancelled')")
            vals.append(now())
        if getattr(args, "tag", ""):
            clauses.append("tags LIKE ?")
            vals.append('%"' + _valid_tag(args.tag) + '"%')
        if clauses: q += " WHERE " + " AND ".join(clauses)
        q += _due_order(", created_at ASC")
        json_out([_task_view(r) for r in db.execute(q, vals).fetchall()])

def _due_order(final: str = ", updated_at DESC") -> str:
    """Priority first, then earliest deadline (undated last), then a caller tiebreak."""
    return (" ORDER BY CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END, "
            "(due_at=''), due_at" + final)

_PRIO_LEVELS = ("P0", "P1", "P2", "P3")

def _prio_rank(p: str) -> int:
    return _PRIO_LEVELS.index(p) if p in _PRIO_LEVELS else len(_PRIO_LEVELS)

def _inherit_priorities(db):
    """Map prerequisite task id -> (urgency priority, nearest dependent id).

    Walks reverse dependency edges to a fixpoint so a high-priority task makes
    its whole transitive prerequisite chain dispatch-urgent: if a P0 task
    depends on a P2 which depends on a P3, all three dispatch at P0 urgency.
    Terminal dependents confer nothing (their chain is already satisfied).
    Cycles are impossible (edges are cycle-checked), so the fixpoint terminates.
    """
    terminal = ("completed", "failed", "cancelled")
    rev = {}
    own = {}
    status = {}
    for r in db.execute(
            "SELECT d.depends_on AS dep, t.id AS id, t.status AS status, t.priority AS priority "
            "FROM task_deps d JOIN tasks t ON t.id=d.task_id"):
        rev.setdefault(r["dep"], []).append(r["id"])
        own[r["id"]] = r["priority"]
        status[r["id"]] = r["status"]
    inherited = {}
    changed = True
    while changed:
        changed = False
        for dep, dependents in rev.items():
            for did in dependents:
                if status.get(did) in terminal:
                    continue
                # The dependent's effective urgency is min(own, what it inherited).
                ip, _via = inherited.get(did, ("P3", None))
                cand = min(_prio_rank(own.get(did, "P3")), _prio_rank(ip))
                cur = inherited.get(dep)
                if cur is None or cand < _prio_rank(cur[0]):
                    inherited[dep] = (_PRIO_LEVELS[cand] if cand < len(_PRIO_LEVELS) else "P3", did)
                    changed = True
    return inherited

def _effective_priority(row, t_dt, aging_minutes: int, aging_boost: int):
    """Virtual dispatch priority after queue aging; returns (priority, boost).

    A task waits one promotion per full --aging-minutes since created_at,
    capped at --aging-boost levels. Stored priority is never mutated.
    """
    base = _prio_rank(row["priority"])
    if aging_minutes <= 0 or aging_boost <= 0 or base == 0 or not row["created_at"]:
        return row["priority"], 0
    try:
        created = datetime.fromisoformat(row["created_at"])
    except ValueError:
        return row["priority"], 0
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    waited_min = (t_dt - created).total_seconds() / 60.0
    boost = min(int(waited_min // aging_minutes), aging_boost)
    if boost <= 0:
        return row["priority"], 0
    return _PRIO_LEVELS[max(base - boost, 0)], boost

def search_tasks(args):
    """Operator search across task text fields with status/project/priority filters.

    With --rank (and an FTS5-capable SQLite), results are BM25-ranked via the
    tasks_fts index and carry a `score`; otherwise substring LIKE matching.
    """
    ranked = bool(getattr(args, "rank", False))
    with conn() as db:
        if ranked and _fts_ready(db):
            match = _fts_query(args.query)
            if not match:
                json_out([])
                return
            clauses = ["tasks_fts MATCH ?"]
            vals = [match]
            if args.status:
                clauses.append("t.status=?"); vals.append(args.status)
            if args.project:
                clauses.append("t.project=?"); vals.append(args.project)
            if args.priority:
                clauses.append("t.priority=?"); vals.append(args.priority)
            if getattr(args, "tag", ""):
                clauses.append("t.tags LIKE ?"); vals.append('%"' + _valid_tag(args.tag) + '"%')
            rows = [_task_view(r) for r in db.execute(
                "SELECT t.*,bm25(tasks_fts) AS score FROM tasks_fts f "
                "JOIN tasks t ON t.rowid=f.rowid WHERE " + " AND ".join(clauses) +
                " ORDER BY score LIMIT 50", vals).fetchall()]
            json_out(rows)
            return
        pat = "%" + args.query + "%"
        clauses = ["(id LIKE ? OR project LIKE ? OR title LIKE ? OR description LIKE ? "
                   "OR next_action LIKE ? OR blocked_reason LIKE ?)"]
        vals = [pat] * 6
        if args.status:
            clauses.append("status=?"); vals.append(args.status)
        if args.project:
            clauses.append("project=?"); vals.append(args.project)
        if args.priority:
            clauses.append("priority=?"); vals.append(args.priority)
        if getattr(args, "tag", ""):
            clauses.append("tags LIKE ?"); vals.append('%"' + _valid_tag(args.tag) + '"%')
        q = ("SELECT * FROM tasks WHERE " + " AND ".join(clauses) +
             " ORDER BY CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END, updated_at DESC")
        json_out([_task_view(r) for r in db.execute(q, vals).fetchall()])

def dashboard(args):
    with conn() as db:
        rows = [dict(r) for r in db.execute("SELECT * FROM tasks ORDER BY CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END, updated_at DESC").fetchall()]
    buckets = {
        "BLOCKED": [r for r in rows if r["status"] == "blocked"],
        "NEEDS LEO": [r for r in rows if r["status"] in ("waiting_for_user", "waiting_for_review")],
        "RUNNING": [r for r in rows if r["status"] in ("claimed", "running", "waiting_for_agent")],
        "CAN WAIT": [r for r in rows if r["status"] == "queued"],
    }
    active = sum(len(buckets[k]) for k in ("RUNNING",))
    waiting = len(buckets["NEEDS LEO"])
    blocked = len(buckets["BLOCKED"])
    print(f"AUTOPILOT | active:{active} needs_leo:{waiting} blocked:{blocked} can_wait:{len(buckets['CAN WAIT'])}")
    t = now()
    for heading, items in buckets.items():
        if not items:
            continue
        print(f"\n{heading}")
        for r in items[:20]:
            extra = r["next_action"] or r["blocked_reason"] or r["pr_url"] or ""
            if r["due_at"] and r["due_at"] <= t:
                extra = (extra + " " if extra else "") + f"[OVERDUE due {r['due_at']}]"
            print(f"- [{r['priority']}] {r['project']} · {r['title']}" + (f" — {extra}" if extra else ""))

def _add_secret_flags(p) -> None:
    """Attach the shared-memory privacy-boundary flags to write commands."""
    p.add_argument("--redact", action="store_true",
                   help="replace credential-shaped spans with [REDACTED:<kind>] before storing")
    p.add_argument("--allow-secret", dest="allow_secret", action="store_true",
                   help="store verbatim despite credential-shaped content (audited as secret_allowed)")

def _add_rerank_flags(p) -> None:
    """Attach the temporal-hybrid rerank flags shared by retrieval commands.

    Opt-in: without --rerank every command's output shape and ordering is
    byte-identical to the pre-rerank behavior (and recall digests stay
    comparable with ones cited before this feature existed).
    """
    p.add_argument("--rerank", action="store_true",
                   help="re-score results by lexical match x recency decay + pinned bonus")
    p.add_argument("--recency-half-life-hours", dest="recency_half_life_hours",
                   type=float, default=168.0,
                   help="recency decay half-life in hours for --rerank (default 168 = one week)")
    p.add_argument("--pinned-boost", dest="pinned_boost", type=float, default=0.5,
                   help="flat score bonus for pinned notes under --rerank (default 0.5)")

def main():
    ap = argparse.ArgumentParser(prog="autopilot")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p=sub.add_parser("init"); p.set_defaults(fn=lambda a: (ensure(), json_out({"ok":True,"db":str(DB)})))
    p=sub.add_parser("create"); p.add_argument("--project",required=True); p.add_argument("--title",required=True); p.add_argument("--description",default=""); p.add_argument("--owner",default="hermes"); p.add_argument("--priority",choices=sorted(PRIORITIES),default="P2"); p.add_argument("--next-action",default=""); p.add_argument("--due-at",default=""); p.add_argument("--not-before",dest="not_before",default=""); p.add_argument("--id"); p.add_argument("--depends-on",action="append",default=[]); p.add_argument("--tag",action="append",default=[],help="capability/scope tag (repeatable)"); p.add_argument("--requires-receipt",dest="requires_receipt",action="append",default=[],help="receipt kind required before completion — the definition of done (repeatable)"); p.set_defaults(fn=create)
    p=sub.add_parser("update"); p.add_argument("id"); p.add_argument("--status",choices=sorted(STATUSES)); p.add_argument("--next-action"); p.add_argument("--blocked-reason"); p.add_argument("--worktree"); p.add_argument("--branch"); p.add_argument("--pr-url"); p.add_argument("--owner"); p.add_argument("--due-at"); p.add_argument("--not-before",dest="not_before"); p.add_argument("--title"); p.add_argument("--description"); p.add_argument("--priority",choices=sorted(PRIORITIES)); p.add_argument("--project"); p.add_argument("--approved-by",dest="approved_by",default="",help="user approving a policy-gated readiness transition (recorded in the audit chain)"); p.add_argument("--requires-receipt",dest="requires_receipt",action="append",default=None,help="set required receipt kinds (repeatable); a single empty string clears them"); p.set_defaults(fn=update)
    p=sub.add_parser("complete"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--note",default=""); p.add_argument("--epoch",type=int,default=None); p.add_argument("--recall-digest",dest="recall_digest",default=""); p.add_argument("--receipt",dest="evidence_receipts",action="append",default=[],help="evidence receipt id on this task cited by the completion (repeatable)"); p.set_defaults(fn=complete)
    p=sub.add_parser("cancel"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--reason",default=""); p.set_defaults(fn=cancel)
    p=sub.add_parser("fail"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--reason",default=""); p.add_argument("--no-retry",dest="no_retry",action="store_true",help="skip the retry budget: fail terminally on the first attempt"); p.add_argument("--max-retries",dest="max_retries",type=int,default=3); p.add_argument("--backoff-base",dest="backoff_base",type=int,default=60); p.add_argument("--backoff-cap",dest="backoff_cap",type=int,default=3600); p.add_argument("--epoch",type=int,default=None); p.set_defaults(fn=fail)
    p=sub.add_parser("block"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--reason",default=""); p.set_defaults(fn=block)
    p=sub.add_parser("unblock"); p.add_argument("id"); p.add_argument("--owner",required=True); p.set_defaults(fn=unblock)
    p=sub.add_parser("defer"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--until",default=""); p.set_defaults(fn=defer)
    p=sub.add_parser("blocked-by"); p.add_argument("id"); p.set_defaults(fn=blocked_by)
    p=sub.add_parser("similar"); p.add_argument("id"); p.add_argument("--threshold",type=float,default=None,help="token-Jaccard cutoff (default: 0.8 / AUTOPILOT_NEAR_DUP_THRESHOLD)"); p.set_defaults(fn=similar_tasks)
    p=sub.add_parser("impact"); p.add_argument("id"); p.set_defaults(fn=impact)
    p=sub.add_parser("critical-path"); p.add_argument("--project",default=""); p.set_defaults(fn=critical_path)
    p=sub.add_parser("plan"); p.add_argument("--project",default=""); p.add_argument("--tag",default="",help="only schedule tasks carrying this tag"); p.set_defaults(fn=plan)
    p=sub.add_parser("verify-chain"); p.add_argument("--checkpoint",default=""); p.set_defaults(fn=verify_chain)
    p=sub.add_parser("events"); p.add_argument("--entity-type"); p.add_argument("--entity-id"); p.add_argument("--action"); p.add_argument("--since",default=""); p.add_argument("--until",default=""); p.add_argument("--limit",type=int,default=50); p.add_argument("--verify",action="store_true"); p.set_defaults(fn=events)
    p=sub.add_parser("claim"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--minutes",type=int,default=30); p.add_argument("--max-active",type=int,default=None); p.add_argument("--force",action="store_true",help="claim even if another live lease holds the same worktree/branch seam"); p.set_defaults(fn=claim)
    p=sub.add_parser("heartbeat"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--note",default=""); p.add_argument("--epoch",type=int,default=None); p.set_defaults(fn=heartbeat)
    p=sub.add_parser("receipt"); p.add_argument("task_id"); p.add_argument("--kind",required=True); p.add_argument("--payload",default="{}"); p.set_defaults(fn=receipt)
    p=sub.add_parser("list"); p.add_argument("--status"); p.add_argument("--project"); p.add_argument("--overdue",action="store_true"); p.add_argument("--tag",default=""); p.set_defaults(fn=list_tasks)
    p=sub.add_parser("show"); p.add_argument("id"); p.add_argument("--limit",type=int,default=20); p.set_defaults(fn=show)
    p=sub.add_parser("metrics"); p.set_defaults(fn=metrics)
    p=sub.add_parser("dashboard"); p.set_defaults(fn=dashboard)
    p=sub.add_parser("protocol"); p.set_defaults(fn=protocol)
    p=sub.add_parser("hindsight-retain"); p.add_argument("--text",required=True); p.add_argument("--kind",default="decision"); p.add_argument("--project",default=""); p.add_argument("--tag",action="append",default=[]); p.add_argument("--task-id",dest="task_id",default=""); p.add_argument("--redact",action="store_true"); p.add_argument("--allow-secret",action="store_true"); p.add_argument("--create",action="store_true"); p.set_defaults(fn=hindsight_retain)
    p=sub.add_parser("dep"); p.add_argument("id"); p.add_argument("depends_on"); p.set_defaults(fn=add_dep)
    p=sub.add_parser("tag"); p.add_argument("id"); p.add_argument("--tag",action="append",required=True,help="capability/scope tag (repeatable)"); p.set_defaults(fn=tag_task)
    p=sub.add_parser("untag"); p.add_argument("id"); p.add_argument("--tag",required=True); p.set_defaults(fn=untag_task)
    p=sub.add_parser("dep-remove"); p.add_argument("id"); p.add_argument("depends_on"); p.set_defaults(fn=remove_dep)
    p=sub.add_parser("next"); p.add_argument("--project"); p.add_argument("--claim",action="store_true"); p.add_argument("--owner",default="hermes"); p.add_argument("--minutes",type=int,default=30); p.add_argument("--max-active",type=int,default=None); p.add_argument("--explain",action="store_true"); p.add_argument("--aging-minutes",dest="aging_minutes",type=int,default=360); p.add_argument("--aging-boost",dest="aging_boost",type=int,default=2); p.add_argument("--recall",action="store_true"); p.add_argument("--agent",default=""); p.add_argument("--budget",dest="recall_budget",type=int,default=4000); p.add_argument("--related",type=int,default=0); p.add_argument("--related-handoffs",dest="related_handoffs",type=int,default=0); p.add_argument("--dep-context",dest="dep_context",type=int,default=0); p.add_argument("--related-sessions",dest="related_sessions",type=int,default=0,help="pack up to N ingested session-message snippets matching this task"); p.add_argument("--related-facts",dest="related_facts",type=int,default=0,help="pack up to N currently-valid temporal facts matching this task"); p.add_argument("--related-semantic",dest="related_semantic",type=int,default=0,help="pack up to N Hindsight semantic memories matching this task (no-op when no bank is configured)"); p.add_argument("--related-scope",dest="related_scope",choices=["project","global"],default="project"); p.add_argument("--tag",default="",help="only dispatch tasks carrying this tag"); p.add_argument("--prefer-unblocking",dest="prefer_unblocking",action="store_true",help="tie-break equal-priority candidates by queued dependents freed (critical-path scheduling)"); _add_rerank_flags(p); p.set_defaults(fn=next_task)
    p=sub.add_parser("search"); p.add_argument("query"); p.add_argument("--status"); p.add_argument("--project"); p.add_argument("--priority"); p.add_argument("--rank",action="store_true"); p.add_argument("--tag",default=""); p.set_defaults(fn=search_tasks)
    p=sub.add_parser("note"); p.add_argument("task_id"); p.add_argument("--kind",default="fact"); p.add_argument("--content",required=True); p.add_argument("--source",default=""); p.add_argument("--pinned",action="store_true"); p.add_argument("--ttl-hours",dest="ttl_hours",type=float,default=None); _add_secret_flags(p); p.set_defaults(fn=add_note)
    p=sub.add_parser("notes"); p.add_argument("task_id"); p.add_argument("--all",action="store_true"); p.set_defaults(fn=list_notes)
    p=sub.add_parser("supersede-note"); p.add_argument("note_id"); p.add_argument("--content",required=True); p.add_argument("--kind",default=None); p.add_argument("--source",default=""); p.add_argument("--ttl-hours",dest="ttl_hours",type=float,default=None); _add_secret_flags(p); p.set_defaults(fn=supersede_note)
    p=sub.add_parser("context"); p.add_argument("task_id"); p.add_argument("--budget",type=int,default=4000); p.add_argument("--related",type=int,default=0); p.add_argument("--related-handoffs",dest="related_handoffs",type=int,default=0); p.add_argument("--dep-context",dest="dep_context",type=int,default=0); p.add_argument("--related-sessions",dest="related_sessions",type=int,default=0,help="pack up to N ingested session-message snippets matching this task"); p.add_argument("--related-facts",dest="related_facts",type=int,default=0,help="pack up to N currently-valid temporal facts matching this task"); p.add_argument("--related-semantic",dest="related_semantic",type=int,default=0,help="pack up to N Hindsight semantic memories matching this task (no-op when no bank is configured)"); p.add_argument("--related-scope",choices=["project","global"],default="project"); _add_rerank_flags(p); p.set_defaults(fn=task_context)
    p=sub.add_parser("recall"); p.add_argument("task_id"); p.add_argument("--agent",default=""); p.add_argument("--budget",type=int,default=4000); p.add_argument("--related",type=int,default=0); p.add_argument("--related-handoffs",dest="related_handoffs",type=int,default=0); p.add_argument("--dep-context",dest="dep_context",type=int,default=0); p.add_argument("--related-sessions",dest="related_sessions",type=int,default=0,help="pack up to N ingested session-message snippets matching this task"); p.add_argument("--related-facts",dest="related_facts",type=int,default=0,help="pack up to N currently-valid temporal facts matching this task"); p.add_argument("--related-semantic",dest="related_semantic",type=int,default=0,help="pack up to N Hindsight semantic memories matching this task (no-op when no bank is configured)"); p.add_argument("--related-scope",choices=["project","global"],default="project"); _add_rerank_flags(p); p.set_defaults(fn=recall)
    p=sub.add_parser("recall-verify"); p.add_argument("task_id"); p.add_argument("--digest",required=True); p.add_argument("--agent",default=""); p.add_argument("--budget",type=int,default=4000); p.add_argument("--related",type=int,default=0); p.add_argument("--related-handoffs",dest="related_handoffs",type=int,default=0); p.add_argument("--dep-context",dest="dep_context",type=int,default=0); p.add_argument("--related-sessions",dest="related_sessions",type=int,default=0,help="pack up to N ingested session-message snippets matching this task"); p.add_argument("--related-facts",dest="related_facts",type=int,default=0,help="pack up to N currently-valid temporal facts matching this task"); p.add_argument("--related-semantic",dest="related_semantic",type=int,default=0,help="pack up to N Hindsight semantic memories matching this task (no-op when no bank is configured)"); p.add_argument("--related-scope",choices=["project","global"],default="project"); _add_rerank_flags(p); p.set_defaults(fn=recall_verify)
    p=sub.add_parser("recall-diff"); p.add_argument("task_id"); p.add_argument("--digest",required=True); _add_rerank_flags(p); p.set_defaults(fn=recall_diff)
    p=sub.add_parser("search-notes"); p.add_argument("query"); p.add_argument("--kind"); p.add_argument("--project"); p.add_argument("--status"); p.add_argument("--limit",type=int,default=50); p.add_argument("--rank",action="store_true"); p.add_argument("--include-expired",dest="include_expired",action="store_true"); _add_rerank_flags(p); p.set_defaults(fn=search_notes)
    p=sub.add_parser("search-handoffs"); p.add_argument("query"); p.add_argument("--task",default=""); p.add_argument("--from-agent",dest="from_agent",default=""); p.add_argument("--to-agent",dest="to_agent",default=""); p.add_argument("--project",default=""); p.add_argument("--limit",type=int,default=50); p.add_argument("--rank",action="store_true"); p.add_argument("--all",action="store_true"); p.set_defaults(fn=search_handoffs)
    p=sub.add_parser("session-scan"); p.add_argument("--root",required=True,help="directory tree of session transcripts to inventory (read-only)"); p.add_argument("--profile",default=""); p.add_argument("--project",default=""); p.add_argument("--since",default="",help="only files modified at/after this ISO timestamp"); p.add_argument("--max-file-bytes",dest="max_file_bytes",type=int,default=DEFAULT_SESSION_MAX_FILE_BYTES); p.set_defaults(fn=session_scan)
    p=sub.add_parser("session-ingest"); p.add_argument("--source",required=True,help="provenance label for the session store (e.g. claude-code)"); p.add_argument("--root",required=True); p.add_argument("--profile",default=""); p.add_argument("--project",default=""); p.add_argument("--since",default=""); p.add_argument("--max-file-bytes",dest="max_file_bytes",type=int,default=DEFAULT_SESSION_MAX_FILE_BYTES); p.add_argument("--apply",action="store_true",help="without this flag the command is a read-only dry-run plan"); p.add_argument("--redact",action="store_true"); p.add_argument("--allow-secret",dest="allow_secret",action="store_true"); p.set_defaults(fn=session_ingest)
    p=sub.add_parser("search-sessions"); p.add_argument("query"); p.add_argument("--source",default=""); p.add_argument("--project",default=""); p.add_argument("--role",default="",help="user or assistant"); p.add_argument("--limit",type=int,default=50); p.add_argument("--rank",action="store_true"); p.set_defaults(fn=search_sessions)
    p=sub.add_parser("sessions-prune"); p.add_argument("--older-than",dest="older_than",required=True,help="prune sessions whose last message predates this age (Nd/Nh/Nm) or ISO timestamp (required: an unbounded wipe is refused)"); p.add_argument("--source",default=""); p.add_argument("--profile",default=""); p.add_argument("--project",default=""); p.add_argument("--apply",action="store_true",help="without this flag the command is a read-only dry-run plan"); p.set_defaults(fn=sessions_prune)
    p=sub.add_parser("fact-assert"); p.add_argument("--subject",required=True,help="entity token (lowercase tag charset)"); p.add_argument("--predicate",required=True,help="relationship token (lowercase tag charset)"); p.add_argument("--object",required=True,help="entity token (lowercase tag charset)"); p.add_argument("--source",default="",help="agent/operator asserting the fact"); p.add_argument("--task",default="",help="task providing provenance (must exist)"); p.add_argument("--valid-hours",dest="valid_hours",type=float,default=None,help="validity window in hours (omit = valid until retracted)"); p.set_defaults(fn=fact_assert)
    p=sub.add_parser("fact-retract"); p.add_argument("fact_id"); p.add_argument("--reason",default=""); p.set_defaults(fn=fact_retract)
    p=sub.add_parser("facts"); p.add_argument("--subject",default=""); p.add_argument("--predicate",default=""); p.add_argument("--object",default=""); p.add_argument("--task",default=""); p.add_argument("--all",action="store_true",help="include closed validity windows (tagged with live flag)"); p.add_argument("--limit",type=int,default=100); p.set_defaults(fn=list_facts)
    p=sub.add_parser("search-facts"); p.add_argument("query"); p.add_argument("--limit",type=int,default=50); p.add_argument("--rank",action="store_true"); p.add_argument("--all",action="store_true",help="include closed validity windows"); p.set_defaults(fn=search_facts)
    p=sub.add_parser("note-history"); p.add_argument("note_id"); p.set_defaults(fn=note_history)
    p=sub.add_parser("handoff-history"); p.add_argument("handoff_id"); p.set_defaults(fn=handoff_history)
    p=sub.add_parser("handoff"); p.add_argument("task_id"); p.add_argument("--from-agent",required=True); p.add_argument("--to-agent",default=""); p.add_argument("--status",default=""); p.add_argument("--objective",default=""); p.add_argument("--evidence",action="append",default=[]); p.add_argument("--constraint",dest="constraints",action="append",default=[]); p.add_argument("--decision",dest="decisions",action="append",default=[]); p.add_argument("--file",dest="files",action="append",default=[]); p.add_argument("--commit",dest="commit_ref",default=""); p.add_argument("--next-action",dest="next_actions",action="append",default=[]); p.add_argument("--risk",dest="risks",action="append",default=[]); p.add_argument("--recall-digest",dest="recall_digest",default=""); _add_secret_flags(p); p.set_defaults(fn=add_handoff)
    p=sub.add_parser("handoffs"); p.add_argument("task_id"); p.add_argument("--all",action="store_true"); p.set_defaults(fn=list_handoffs)
    p=sub.add_parser("handoff-current"); p.add_argument("task_id"); p.set_defaults(fn=current_handoff)
    p=sub.add_parser("handoff-inbox"); p.add_argument("--agent",required=True); p.add_argument("--project"); p.add_argument("--limit",type=int,default=50); p.add_argument("--unacked-only",dest="unacked_only",action="store_true"); p.set_defaults(fn=handoff_inbox)
    p=sub.add_parser("ack"); p.add_argument("task_id"); p.add_argument("--agent",required=True); p.add_argument("--recall-digest",dest="recall_digest",default=""); p.set_defaults(fn=ack_handoff)
    p=sub.add_parser("release"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--epoch",type=int,default=None); p.set_defaults(fn=release)
    p=sub.add_parser("renew"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--minutes",type=int,default=30); p.add_argument("--epoch",type=int,default=None); p.set_defaults(fn=renew)
    p=sub.add_parser("transfer"); p.add_argument("id"); p.add_argument("--from-owner",required=True,dest="from_owner"); p.add_argument("--to-owner",required=True,dest="to_owner"); p.add_argument("--minutes",type=int,default=30); p.add_argument("--epoch",type=int,default=None); p.set_defaults(fn=transfer)
    p=sub.add_parser("resume"); p.add_argument("task_id"); p.add_argument("--agent",required=True); p.add_argument("--minutes",type=int,default=30); p.add_argument("--budget",type=int,default=4000); p.add_argument("--related",type=int,default=0); p.add_argument("--related-handoffs",dest="related_handoffs",type=int,default=0); p.add_argument("--dep-context",dest="dep_context",type=int,default=0); p.add_argument("--related-sessions",dest="related_sessions",type=int,default=0,help="pack up to N ingested session-message snippets matching this task"); p.add_argument("--related-facts",dest="related_facts",type=int,default=0,help="pack up to N currently-valid temporal facts matching this task"); p.add_argument("--related-semantic",dest="related_semantic",type=int,default=0,help="pack up to N Hindsight semantic memories matching this task (no-op when no bank is configured)"); p.add_argument("--related-scope",choices=["project","global"],default="project"); p.add_argument("--max-active",type=int,default=None); p.set_defaults(fn=resume)
    p=sub.add_parser("leases"); p.add_argument("--owner"); p.add_argument("--all",action="store_true"); p.set_defaults(fn=leases)
    args=ap.parse_args(); args.fn(args)

if __name__ == "__main__": main()
