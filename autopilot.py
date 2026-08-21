#!/usr/bin/env python3
"""IdeatorX Autopilot Control Plane v1.

Durable task registry, leases, heartbeats, receipts, and a compact dashboard.
This tool intentionally does not deploy, merge, send messages, or execute work.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import uuid
import hashlib
import tempfile
from datetime import datetime, timezone
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
   due_at TEXT NOT NULL DEFAULT '',
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
  created_at TEXT NOT NULL
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
   pinned INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_notes_task ON notes(task_id);
CREATE INDEX IF NOT EXISTS idx_notes_hash ON notes(task_id, content_hash);
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
"""
STATUSES = {"queued", "claimed", "running", "waiting_for_agent", "waiting_for_user", "waiting_for_review", "ready_to_merge", "ready_to_deploy", "blocked", "completed", "failed", "cancelled"}
TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
PRIORITIES = {"P0", "P1", "P2", "P3"}
NOTE_KINDS = {"fact", "decision", "observation", "evidence", "constraint"}

def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _normalize_due(value: str) -> str:
    """Normalize a user-supplied deadline to UTC ISO 8601; '' clears the deadline."""
    v = (value or "").strip()
    if not v:
        return ""
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        raise SystemExit(f"invalid due-at timestamp: {value!r} (use ISO 8601, e.g. 2026-08-21T17:00:00+00:00)")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

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

def _ensure_fts(db) -> None:
    """Create FTS indexes when supported; rebuild once on first creation so
    pre-existing rows become searchable. Silently skips on non-FTS5 builds."""
    try:
        existed = _fts_ready(db)
        db.executescript(FTS_SCHEMA)
        if not existed:
            db.execute("INSERT INTO notes_fts(notes_fts) VALUES('rebuild')")
            db.execute("INSERT INTO tasks_fts(tasks_fts) VALUES('rebuild')")
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
    task_cols = {r[1] for r in db.execute("PRAGMA table_info(tasks)")}
    if "lease_epoch" not in task_cols:
        db.execute("ALTER TABLE tasks ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0")
    if "due_at" not in task_cols:
        db.execute("ALTER TABLE tasks ADD COLUMN due_at TEXT NOT NULL DEFAULT ''")
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

def live_note(db, task_id: str, content_hash: str):
    return db.execute(
        "SELECT * FROM notes WHERE task_id=? AND content_hash=? AND superseded_by='' ORDER BY created_at DESC LIMIT 1",
        (task_id, content_hash)).fetchone()

def add_note(args):
    """Attach a provenance-tagged fact to a task; exact duplicates are deduplicated."""
    if args.kind not in NOTE_KINDS:
        raise SystemExit(f"invalid note kind: {args.kind} (choose from {sorted(NOTE_KINDS)})")
    content = args.content.strip()
    if not content:
        raise SystemExit("note content must not be empty")
    nid = uuid.uuid4().hex
    t = now()
    pinned = 1 if getattr(args, "pinned", False) else 0
    with conn() as db:
        task_row(db, args.task_id)
        h = _note_hash(args.task_id, content)
        existing = live_note(db, args.task_id, h)
        if existing:
            if pinned and not existing["pinned"]:
                # A duplicate add can promote an existing note to pinned.
                db.execute("UPDATE notes SET pinned=1 WHERE id=?", (existing["id"],))
                audit(db, "task", args.task_id, "note_pinned", {"note_id": existing["id"]})
            audit(db, "task", args.task_id, "note_deduplicated", {"note_id": existing["id"], "kind": args.kind})
            json_out({"ok": True, "id": existing["id"], "task_id": args.task_id,
                      "deduplicated": True, "created_at": existing["created_at"]})
            return
        db.execute("INSERT INTO notes(id,task_id,kind,content,source,content_hash,created_at,pinned) VALUES(?,?,?,?,?,?,?,?)",
                   (nid, args.task_id, args.kind, content, args.source, h, t, pinned))
        audit(db, "task", args.task_id, "note_added", {"note_id": nid, "kind": args.kind, "source": args.source, "pinned": bool(pinned)})
    json_out({"ok": True, "id": nid, "task_id": args.task_id, "deduplicated": False, "created_at": t})

def list_notes(args):
    with conn() as db:
        task_row(db, args.task_id)
        q = ("SELECT n.id,n.kind,n.content,n.source,n.created_at,n.superseded_by,n.pinned FROM notes n "
             "WHERE n.task_id=?")
        if not getattr(args, "all", False):
            q += " AND n.superseded_by=''"
        q += " ORDER BY n.rowid ASC"
        rows = [dict(r) for r in db.execute(q, (args.task_id,)).fetchall()]
    json_out(rows)

def supersede_note(args):
    """Temporal facts: mark an old note superseded by a new one atomically."""
    if args.kind and args.kind not in NOTE_KINDS:
        raise SystemExit(f"invalid note kind: {args.kind}")
    new_content = args.content.strip()
    if not new_content:
        raise SystemExit("superseding content must not be empty")
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
        db.execute("INSERT INTO notes(id,task_id,kind,content,source,content_hash,created_at,superseded_by,pinned) VALUES(?,?,?,?,?,?,?,'',?)",
                   (new_id, old["task_id"], kind, new_content, source, h, t, old["pinned"]))
        audit(db, "task", old["task_id"], "note_superseded",
              {"old_note_id": old_id, "new_note_id": new_id, "kind": kind})
    json_out({"ok": True, "old_note_id": old_id, "new_note_id": new_id, "task_id": old["task_id"]})

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

def _related_note_candidates(db, task_id: str, text: str, limit: int, scope: str) -> list:
    """Cross-task retrieval: live notes on *other* tasks matching this task's text.

    FTS5 path OR-combines the tokens (recall-oriented) and ranks by BM25 so the
    best matches pack first; on non-FTS builds it degrades to any-token LIKE
    matching with identical output shape minus `score`. Scope 'project'
    restricts candidates to the task's own project; 'global' searches all.
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
    cols = ("SELECT n.id,n.kind,n.content,n.source,n.created_at,n.pinned,n.task_id,"
            "t.title AS via_task_title")
    if _fts_ready(db):
        match = " OR ".join('"%s"' % t for t in toks)
        sql = (cols + ",bm25(notes_fts) AS score "
               "FROM notes_fts f JOIN notes n ON n.rowid=f.rowid JOIN tasks t ON t.id=n.task_id "
               "WHERE notes_fts MATCH ? AND n.superseded_by='' AND n.task_id!=?" + scope_sql +
               " ORDER BY score LIMIT ?")
        vals = [match, task_id, *scope_vals, limit]
    else:
        likes = " OR ".join("n.content LIKE ?" for _ in toks)
        sql = (cols + " FROM notes n JOIN tasks t ON t.id=n.task_id "
               "WHERE (" + likes + ") AND n.superseded_by='' AND n.task_id!=?" + scope_sql +
               " ORDER BY n.created_at DESC LIMIT ?")
        vals = [*( "%" + t + "%" for t in toks), task_id, *scope_vals, limit]
    return [dict(r) for r in db.execute(sql, vals).fetchall()]

def task_context(args):
    """Pack a prompt-ready context bundle within a character budget.

    Includes a task summary header and unsatisfied dependencies, then live
    notes packed pinned-first (oldest→newest within each group) so critical
    facts survive tight budgets. With --related N, up to N BM25-ranked live
    notes from other tasks (matching this task's title/description/next_action
    text) are appended afterwards within the same budget, each tagged with its
    source task for provenance.
    """
    budget = max(0, args.budget)
    rel_limit = max(0, getattr(args, "related", 0))
    rel_scope = getattr(args, "related_scope", "project") or "project"
    with conn() as db:
        row = task_row(db, args.task_id)
        summary = {k: row[k] for k in ("id", "project", "title", "status", "priority", "next_action", "blocked_reason", "due_at")}
        pending = unsatisfied_deps(db, args.task_id)
        rows = [dict(r) for r in db.execute(
            "SELECT id,kind,content,source,created_at,pinned FROM notes "
            "WHERE task_id=? AND superseded_by='' ORDER BY rowid ASC",
            (args.task_id,)).fetchall()]
        related = _related_note_candidates(
            db, args.task_id,
            " ".join(filter(None, (row["title"], row["description"], row["next_action"]))),
            rel_limit, rel_scope)
    # Stable sort: pinned notes first, original (oldest→newest) order within groups.
    ordered = sorted(rows, key=lambda r: not r["pinned"])
    header_cost = 64 + len(summary["title"]) + len(summary["next_action"]) + len(summary["blocked_reason"])
    used, truncated = min(header_cost, budget), False
    packed, pinned_packed = [], 0
    for r in ordered:
        cost = len(r["content"]) + len(r["kind"]) + len(r["source"]) + len(r["created_at"]) + 8
        if used + cost > budget:
            truncated = True
            continue
        packed.append(r); used += cost
        if r["pinned"]:
            pinned_packed += 1
    related_packed = 0
    for r in related:
        r["related"] = True
        cost = (len(r["content"]) + len(r["kind"]) + len(r["source"]) + len(r["created_at"])
                + len(r["task_id"]) + len(r["via_task_title"]) + 16)
        if used + cost > budget:
            truncated = True
            continue
        packed.append(r); used += cost; related_packed += 1
    json_out({"task_id": args.task_id, "budget": budget, "used_chars": used,
              "truncated": truncated, "task": summary,
              "unsatisfied_dependencies": pending,
              "notes_total": len(rows), "notes_packed": len(packed) - related_packed,
              "notes_pinned_packed": pinned_packed,
              "related_requested": rel_limit, "related_matched": len(related),
              "related_packed": related_packed,
              "notes": packed})

def search_notes(args):
    """Keyword retrieval over note content with kind/project/status filters via the task join.

    With --rank (and an FTS5-capable SQLite), results are BM25-ranked via the
    notes_fts index and carry a `score`; otherwise it falls back to substring
    LIKE matching with identical output shape minus the score.
    """
    ranked = bool(getattr(args, "rank", False))
    with conn() as db:
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
                "bm25(notes_fts) AS score FROM notes_fts f "
                "JOIN notes n ON n.rowid=f.rowid JOIN tasks t ON t.id=n.task_id "
                "WHERE " + " AND ".join(clauses) +
                " ORDER BY score LIMIT ?", (*vals, args.limit)).fetchall()]
            json_out(rows)
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
            "SELECT n.id,n.task_id,t.project,n.kind,n.content,n.source,n.created_at "
            "FROM notes n JOIN tasks t ON t.id=n.task_id WHERE n.superseded_by='' AND " +
            " AND ".join(clauses) + " ORDER BY n.created_at DESC LIMIT ?", (*vals, args.limit)).fetchall()]
    json_out(rows)

def release(args):
    """Voluntarily give up a live lease without consuming retry budget."""
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in {"completed", "cancelled"}:
            raise SystemExit(f"cannot release terminal task: {row['status']}")
        _require_live_lease(row, args.owner, "releasing")
        _require_epoch(row, getattr(args, "epoch", None), "releasing")
        t = now()
        db.execute("UPDATE tasks SET status='queued',lease_owner='',lease_expires_at='',updated_at=? WHERE id=?", (t, args.id))
        audit(db, "task", args.id, "lease_released", {"owner": args.owner})
        json_out(dict(task_row(db, args.id)))

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
        "UPDATE tasks SET status='claimed', lease_owner=?, lease_expires_at=?, lease_epoch=lease_epoch+1, updated_at=? "
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

def create(args):
    task_id = args.id or f"{args.project.lower().replace(' ', '-')}-{uuid.uuid4().hex[:8]}"
    t = now()
    due_at = _normalize_due(getattr(args, "due_at", None) or "")
    with conn() as db:
        db.execute("INSERT INTO tasks(id,project,title,description,owner,status,priority,next_action,due_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (task_id,args.project,args.title,args.description,args.owner,"queued",args.priority,args.next_action,due_at,t,t))
        audit(db, "task", task_id, "created", {"project": args.project, "owner": args.owner, "priority": args.priority, "due_at": due_at})
        for dep in getattr(args, "depends_on", []) or []:
            add_dependency(db, task_id, dep)
    json_out({"ok": True, "id": task_id, "status": "queued"})

def update(args):
    fields = {}
    for key in ("status", "next_action", "blocked_reason", "worktree", "branch", "pr_url", "owner", "due_at"):
        value = getattr(args, key, None)
        if value is not None:
            fields[key] = value
    if "status" in fields and fields["status"] not in STATUSES:
        raise SystemExit(f"invalid status: {fields['status']}")
    if "due_at" in fields:
        fields["due_at"] = _normalize_due(fields["due_at"])
    if fields.get("status") in {"completed", "failed", "cancelled"}:
        # Terminal transitions release any held lease so the task cannot look active.
        fields["lease_owner"] = ""
        fields["lease_expires_at"] = ""
    if not fields:
        raise SystemExit("no updates supplied")
    fields["updated_at"] = now()
    with conn() as db:
        task_row(db, args.id)
        db.execute(f"UPDATE tasks SET {', '.join(k+'=?' for k in fields)} WHERE id=?", (*fields.values(), args.id))
        audit(db, "task", args.id, "updated", {k: v for k, v in fields.items() if k != "updated_at"})
        json_out(dict(task_row(db, args.id)))

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
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in {"completed", "cancelled"}:
            raise SystemExit(f"cannot complete terminal task: {row['status']}")
        _require_live_lease(row, args.owner, "completing")
        _require_epoch(row, getattr(args, "epoch", None), "completing")
        t = now()
        db.execute("UPDATE tasks SET status='completed',lease_owner='',lease_expires_at='',blocked_reason='',updated_at=? WHERE id=?", (t, args.id))
        audit(db, "task", args.id, "completed", {"owner": args.owner, "note": args.note})
        json_out(dict(task_row(db, args.id)))

def cancel(args):
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in {"completed", "cancelled"}:
            raise SystemExit(f"cannot cancel terminal task: {row['status']}")
        _require_not_foreign_lease(row, args.owner, "cancelling")
        t = now()
        db.execute("UPDATE tasks SET status='cancelled',lease_owner='',lease_expires_at='',blocked_reason=?,updated_at=? WHERE id=?", (args.reason or 'cancelled by operator', t, args.id))
        audit(db, "task", args.id, "cancelled", {"owner": args.owner, "reason": args.reason})
        json_out(dict(task_row(db, args.id)))

def verify_chain(args):
    with conn() as db:
        problems = audit_chain_problems(db)
        total = db.execute("SELECT COUNT(*) n FROM audit_events").fetchone()["n"]
    json_out({"ok": not problems, "events": total, "problems": problems})

def claim(args):
    with conn() as db:
        row = task_row(db, args.id)
        if row["status"] in {"completed", "cancelled"}:
            raise SystemExit(f"cannot claim terminal task: {row['status']}")
        pending = unsatisfied_deps(db, args.id)
        if pending:
            raise SystemExit("unsatisfied dependencies: " + ", ".join(f"{d['id']}({d['status']})" for d in pending))
        # Atomic acquire: the WHERE guard makes the lease check-and-set a single
        # statement so concurrent claimers cannot both win the same lease.
        acquired, exp, epoch = _acquire(db, args.id, args.owner, args.minutes, resolve_max_active(args))
        if not acquired:
            raise SystemExit(_explain_acquire_failure(db, args.id, args.owner, resolve_max_active(args)))
        db.execute("INSERT INTO heartbeats(task_id,owner,state,at,note) VALUES(?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET owner=excluded.owner,state=excluded.state,at=excluded.at,note=excluded.note", (args.id,args.owner,"claimed",now(),"lease claimed"))
        audit(db, "task", args.id, "claimed", {"owner": args.owner, "lease_expires_at": exp, "lease_epoch": epoch})
        out = dict(task_row(db, args.id))
        json_out(out)

def add_dep(args):
    with conn() as db:
        add_dependency(db, args.id, args.depends_on)
    json_out({"ok": True, "task_id": args.id, "depends_on": args.depends_on})

def next_task(args):
    """Dispatch: highest-priority queued task whose dependencies are all completed."""
    with conn() as db:
        q = "SELECT * FROM tasks WHERE status='queued'"
        vals = []
        if args.project:
            q += " AND project=?"; vals.append(args.project)
        q += _due_order()
        picked = None
        for r in db.execute(q, vals).fetchall():
            if not unsatisfied_deps(db, r["id"]):
                picked = r
                break
        out = {"ok": True, "task": dict(picked) if picked else None}
        if picked is None:
            json_out(out)
            return
        if args.claim:
            cap = resolve_max_active(args)
            acquired, exp, epoch = _acquire(db, picked["id"], args.owner, args.minutes, cap)
            if not acquired:
                raise SystemExit(_explain_acquire_failure(db, picked["id"], args.owner, cap))
            db.execute("INSERT INTO heartbeats(task_id,owner,state,at,note) VALUES(?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET owner=excluded.owner,state=excluded.state,at=excluded.at,note=excluded.note", (picked["id"],args.owner,"claimed",now(),"claimed via next"))
            audit(db, "task", picked["id"], "claimed", {"owner": args.owner, "lease_expires_at": exp, "lease_epoch": epoch, "via": "next"})
            out["claimed"] = True
            out["lease_expires_at"] = exp
            out["lease_epoch"] = epoch
        json_out(out)

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
    with conn() as db:
        task_row(db, args.task_id)
        db.execute("INSERT INTO receipts(id,task_id,kind,payload_json,created_at) VALUES(?,?,?,?,?)", (rid,args.task_id,args.kind,json.dumps(payload,sort_keys=True),created))
        db.execute("UPDATE tasks SET last_receipt=?,updated_at=? WHERE id=?", (rid,created,args.task_id))
        audit(db, "task", args.task_id, "receipt", {"receipt_id": rid, "kind": args.kind})
    data = json.dumps({"id":rid,"task_id":args.task_id,"kind":args.kind,"created_at":created,"payload":payload}, indent=2, sort_keys=True)+"\n"
    target = RECEIPTS / f"{rid}.json"
    fd, tmp = tempfile.mkstemp(prefix=f".{rid}.", dir=RECEIPTS)
    try:
        with os.fdopen(fd, "w") as f: f.write(data); f.flush(); os.fsync(f.fileno())
        os.chmod(tmp, 0o600); os.replace(tmp, target)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    json_out({"ok": True, "receipt_id": rid, "task_id": args.task_id, "sha256": hashlib.sha256(data.encode()).hexdigest()})

def show(args):
    with conn() as db:
        out = dict(task_row(db, args.id))
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
    for r in receipts:
        r["payload"] = json.loads(r.pop("payload_json"))
    for e in events:
        e["payload"] = json.loads(e.pop("payload_json"))
    out["receipts"] = receipts
    out["audit"] = events
    out["dependencies"] = deps
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
                           "SUM(CASE WHEN pinned!=0 AND superseded_by='' THEN 1 ELSE 0 END) pinned FROM notes").fetchone()
    json_out({
        "generated_at": t,
        "tasks_total": sum(by_status.values()),
        "tasks_by_status": by_status,
        "tasks_by_project": by_project,
        "stale_leases": stale,
        "active_leases_by_owner": leases_by_owner,
        "queued_blocked_by_deps": blocked_by_deps,
        "overdue_tasks": [r["id"] for r in overdue],
        "due_within_24h": due_soon,
        "total_retries": retries["s"],
        "max_retries_seen": retries["m"],
        "receipts": receipts,
        "audit_events": events,
        "notes_total": notes["total"] or 0,
        "notes_superseded": notes["superseded"] or 0,
        "notes_pinned_live": notes["pinned"] or 0,
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
        if clauses: q += " WHERE " + " AND ".join(clauses)
        q += _due_order(", created_at ASC")
        json_out([dict(r) for r in db.execute(q, vals).fetchall()])

def _due_order(final: str = ", updated_at DESC") -> str:
    """Priority first, then earliest deadline (undated last), then a caller tiebreak."""
    return (" ORDER BY CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END, "
            "(due_at=''), due_at" + final)

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
            rows = [dict(r) for r in db.execute(
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
        q = ("SELECT * FROM tasks WHERE " + " AND ".join(clauses) +
             " ORDER BY CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END, updated_at DESC")
        json_out([dict(r) for r in db.execute(q, vals).fetchall()])

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

def main():
    ap = argparse.ArgumentParser(prog="autopilot")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p=sub.add_parser("init"); p.set_defaults(fn=lambda a: (ensure(), json_out({"ok":True,"db":str(DB)})))
    p=sub.add_parser("create"); p.add_argument("--project",required=True); p.add_argument("--title",required=True); p.add_argument("--description",default=""); p.add_argument("--owner",default="hermes"); p.add_argument("--priority",choices=sorted(PRIORITIES),default="P2"); p.add_argument("--next-action",default=""); p.add_argument("--due-at",default=""); p.add_argument("--id"); p.add_argument("--depends-on",action="append",default=[]); p.set_defaults(fn=create)
    p=sub.add_parser("update"); p.add_argument("id"); p.add_argument("--status",choices=sorted(STATUSES)); p.add_argument("--next-action"); p.add_argument("--blocked-reason"); p.add_argument("--worktree"); p.add_argument("--branch"); p.add_argument("--pr-url"); p.add_argument("--owner"); p.add_argument("--due-at"); p.set_defaults(fn=update)
    p=sub.add_parser("complete"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--note",default=""); p.add_argument("--epoch",type=int,default=None); p.set_defaults(fn=complete)
    p=sub.add_parser("cancel"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--reason",default=""); p.set_defaults(fn=cancel)
    p=sub.add_parser("verify-chain"); p.set_defaults(fn=verify_chain)
    p=sub.add_parser("claim"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--minutes",type=int,default=30); p.add_argument("--max-active",type=int,default=None); p.set_defaults(fn=claim)
    p=sub.add_parser("heartbeat"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--note",default=""); p.add_argument("--epoch",type=int,default=None); p.set_defaults(fn=heartbeat)
    p=sub.add_parser("receipt"); p.add_argument("task_id"); p.add_argument("--kind",required=True); p.add_argument("--payload",default="{}"); p.set_defaults(fn=receipt)
    p=sub.add_parser("list"); p.add_argument("--status"); p.add_argument("--project"); p.add_argument("--overdue",action="store_true"); p.set_defaults(fn=list_tasks)
    p=sub.add_parser("show"); p.add_argument("id"); p.add_argument("--limit",type=int,default=20); p.set_defaults(fn=show)
    p=sub.add_parser("metrics"); p.set_defaults(fn=metrics)
    p=sub.add_parser("dashboard"); p.set_defaults(fn=dashboard)
    p=sub.add_parser("dep"); p.add_argument("id"); p.add_argument("depends_on"); p.set_defaults(fn=add_dep)
    p=sub.add_parser("next"); p.add_argument("--project"); p.add_argument("--claim",action="store_true"); p.add_argument("--owner",default="hermes"); p.add_argument("--minutes",type=int,default=30); p.add_argument("--max-active",type=int,default=None); p.set_defaults(fn=next_task)
    p=sub.add_parser("search"); p.add_argument("query"); p.add_argument("--status"); p.add_argument("--project"); p.add_argument("--priority"); p.add_argument("--rank",action="store_true"); p.set_defaults(fn=search_tasks)
    p=sub.add_parser("note"); p.add_argument("task_id"); p.add_argument("--kind",default="fact"); p.add_argument("--content",required=True); p.add_argument("--source",default=""); p.add_argument("--pinned",action="store_true"); p.set_defaults(fn=add_note)
    p=sub.add_parser("notes"); p.add_argument("task_id"); p.add_argument("--all",action="store_true"); p.set_defaults(fn=list_notes)
    p=sub.add_parser("supersede-note"); p.add_argument("note_id"); p.add_argument("--content",required=True); p.add_argument("--kind",default=None); p.add_argument("--source",default=""); p.set_defaults(fn=supersede_note)
    p=sub.add_parser("context"); p.add_argument("task_id"); p.add_argument("--budget",type=int,default=4000); p.add_argument("--related",type=int,default=0); p.add_argument("--related-scope",choices=["project","global"],default="project"); p.set_defaults(fn=task_context)
    p=sub.add_parser("search-notes"); p.add_argument("query"); p.add_argument("--kind"); p.add_argument("--project"); p.add_argument("--status"); p.add_argument("--limit",type=int,default=50); p.add_argument("--rank",action="store_true"); p.set_defaults(fn=search_notes)
    p=sub.add_parser("note-history"); p.add_argument("note_id"); p.set_defaults(fn=note_history)
    p=sub.add_parser("release"); p.add_argument("id"); p.add_argument("--owner",required=True); p.add_argument("--epoch",type=int,default=None); p.set_defaults(fn=release)
    args=ap.parse_args(); args.fn(args)

if __name__ == "__main__": main()
