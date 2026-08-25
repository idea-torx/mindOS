#!/usr/bin/env python3
"""MindOS live Hermes conversation bridge.

Bridges important live Hermes conversations into the MindOS shared brain:

1. Read-only source adapter over Hermes-style JSONL session stores (the same
   normalized parse and secret guard as `autopilot.py session-ingest`); source
   files are never mutated.
2. Incremental durable cache in sessions/session_messages keyed by
   source/profile/path identity + file sha256; unchanged input is skipped and
   changed transcripts are re-indexed atomically — re-runs are idempotent.
3. Provenance-complete export manifests for downstream consumers. The bridge
   calls no external memory service: semantic memory is local and in-database
   (`autopilot.py memory-retain`), and `export` simply emits a deterministic,
   provenance-complete JSONL manifest of not-yet-exported messages. Export
   state lives in the bridge_export_ledger table and is reported as
   pending/exported/failed — never silently "synced".
4. Secret/PII guard before any cache or export write: refuse by default,
   --redact stores [REDACTED:<kind>] copies, audited --allow-secret override.
   Secret values never appear in receipts, audit payloads, exports, or errors.
5. Promotion hooks: explicit `promote` turns chosen messages into task notes,
   temporal facts, or new tasks with provenance citations — raw session cache
   stays evidence, never execution truth.
6. Near-real-time operation: `watch` polls a store root on an interval and
   ingests changes; the existing scheduled batch remains the fallback.

This tool intentionally does not deploy, merge, send messages, or touch live
state; run it against an explicit MindOS home (HERMES_AUTOPILOT_HOME).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import autopilot as ap  # noqa: E402  (shared home resolution, schema, guards)
from mindos_sqlite_adapter import ingest_session, resolve_state_db  # noqa: E402

# The export ledger. Renamed from bridge_hindsight_ledger when the external
# Hindsight service was retired: the table never had anything to do with that
# provider's semantics, it tracks which cached messages have been emitted into
# an export manifest. `channel` replaces the former `bank` for the same reason.
LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS bridge_export_ledger (
  message_key TEXT PRIMARY KEY,          -- row_id:seq of the cached message
  session_row TEXT NOT NULL,
  seq INTEGER NOT NULL,
  role TEXT NOT NULL,
  at TEXT NOT NULL DEFAULT '',
  content_hash TEXT NOT NULL,
  channel TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL DEFAULT 'pending', -- pending|exported|failed|skipped
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error_kind TEXT NOT NULL DEFAULT '',
  exported_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_bel_channel_state ON bridge_export_ledger(channel, state);
CREATE INDEX IF NOT EXISTS idx_bel_session_row ON bridge_export_ledger(session_row, seq);
"""

# Every ledger row carries an explicit channel. The retired schema allowed an
# empty bank and then treated it as a wildcard at export time, so an export
# with no --bank drained and marked-exported the pending rows of *every* bank.
# A single named default removes the wildcard entirely.
DEFAULT_EXPORT_CHANNEL = "shared-context"

BRIDGE_EXPORT_FORMAT = "mindos-bridge-export-v1"


def _ensure_bridge(db) -> None:
    """Create the export ledger, migrating the retired Hindsight-named table.

    Rows carry over rather than being dropped, and legacy rows with an empty
    bank adopt the default channel so they stay exportable under the stricter
    (non-wildcard) channel filter.
    """
    def _has(name):
        return bool(db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,)).fetchone())
    legacy, current = _has("bridge_hindsight_ledger"), _has("bridge_export_ledger")
    db.executescript(LEDGER_SCHEMA)
    if legacy and not current:
        db.execute(
            "INSERT OR IGNORE INTO bridge_export_ledger(message_key,session_row,seq,"
            "role,at,content_hash,channel,state,attempts,last_error_kind,"
            "exported_at,updated_at) SELECT message_key,session_row,seq,role,at,"
            "content_hash,CASE WHEN bank='' THEN ? ELSE bank END,state,attempts,"
            "last_error_kind,exported_at,updated_at FROM bridge_hindsight_ledger",
            (DEFAULT_EXPORT_CHANNEL,))
    if legacy:
        db.execute("DROP TABLE bridge_hindsight_ledger")


def _channel(args) -> str:
    """The export channel for this invocation; never empty, never a wildcard."""
    return (getattr(args, "channel", "") or "").strip() or DEFAULT_EXPORT_CHANNEL


def _prune_ledger(db, row_id: str, kept: int) -> int:
    """Drop ledger rows for message seqs that no longer exist in the cache.

    A re-indexed transcript that got *shorter* used to leave pending rows for
    the vanished seqs. `export` inner-joins session_messages, so those rows
    could never be emitted and never left 'pending' — the ledger reported
    work outstanding forever. Re-index prunes them instead.
    """
    return db.execute(
        "DELETE FROM bridge_export_ledger WHERE session_row=? AND seq>=?",
        (row_id, kept)).rowcount


def _message_key(row_id: str, seq: int) -> str:
    return f"{row_id}:{seq}"


def _ingest_store(args) -> dict:
    """Run one incremental ingest pass using autopilot's plan core + guard."""
    apply_mode = bool(getattr(args, "apply", False)) or bool(getattr(args, "watch", False))
    plan, files = ap._session_plan(args)
    indexable = [f for f in files if f["status"] == "indexed" and not f.get("unchanged")]
    kinds = sorted({k for f in indexable for k in f.get("secret_kinds", [])})
    redact = bool(getattr(args, "redact", False))
    allow = bool(getattr(args, "allow_secret", False))
    applied = 0
    if apply_mode and indexable:
        if kinds:
            with ap.conn() as db:
                ap.audit(db, "session", plan["source"],
                         "secret_blocked" if not (redact or allow) else
                         ("secret_redacted" if redact else "secret_allowed"),
                         {"files": len(indexable), "kinds": kinds})
            if not (redact or allow):
                raise SystemExit(
                    f"refusing to ingest credential-shaped session content ({', '.join(kinds)}); "
                    "re-run with --redact or --allow-secret")
        t = ap.now()
        with ap.conn() as db:
            _ensure_bridge(db)
            for f in indexable:
                msgs = f["_parsed"]["messages"]
                if redact:
                    msgs = [{**m, "content": ap._redact_secrets(m["content"])} for m in msgs]
                row_id = f["_row_id"]
                db.execute("DELETE FROM session_messages WHERE session_row=?", (row_id,))
                # Changed content invalidates any prior export state.
                old = {r["seq"]: r["content_hash"] for r in db.execute(
                    "SELECT seq,content_hash FROM bridge_export_ledger WHERE session_row=?",
                    (row_id,)).fetchall()}
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
                    ch = hashlib.sha256((m["role"] + "\x00" + m["content"]).encode()).hexdigest()
                    db.execute(
                        "INSERT INTO session_messages(session_row,seq,role,content,content_hash,at) "
                        "VALUES(?,?,?,?,?,?)",
                        (row_id, seq, m["role"], m["content"], ch, m["at"]))
                    if old.get(seq) != ch:
                        db.execute(
                            "INSERT OR REPLACE INTO bridge_export_ledger(message_key,"
                            "session_row,seq,role,at,content_hash,channel,state,attempts,"
                            "last_error_kind,exported_at,updated_at) "
                            "VALUES(?,?,?,?,?,?,?,'pending',0,'','',?)",
                            (_message_key(row_id, seq), row_id, seq, m["role"], m["at"],
                             ch, _channel(args), t))
                _prune_ledger(db, row_id, len(msgs))
                ap.audit(db, "session", row_id, "session_ingested",
                         {"source": plan["source"], "path": f["path"], "messages": len(msgs),
                          "tool_results_skipped": f["tool_results_skipped"],
                          "malformed_lines": f["_parsed"]["malformed_lines"],
                          "duplicates_collapsed": f["_parsed"]["duplicates_collapsed"],
                          "redacted": redact})
                applied += 1
    plan["dry_run"] = not apply_mode
    plan["applied_files"] = applied if apply_mode else 0
    return plan


def bridge_sqlite_sync(args):
    """Near-real-time ingest of one live Hermes session by session_id.

    Native SQLite adapter over HERMES_HOME/state.db (sessions + messages),
    read-only. Same guard ladder, ledger, provenance and idempotence as the
    JSONL sync path.
    """
    result = ingest_session(args)
    result["bridge"] = {"command": "sqlite-sync", "trigger": "on_session_end"}
    ap.json_out(result)


def bridge_sync(args):
    """Near-real-time on-demand ingest of current/changed Hermes sessions."""
    plan = _ingest_store(args)
    plan["bridge"] = {"command": "bridge-sync", "trigger": "on-demand"}
    ap.json_out(plan)


def bridge_watch(args):
    """Poll a store root and ingest changes on an interval (near-real-time)."""
    interval = max(1.0, float(args.interval))
    deadline = time.time() + (args.for_seconds if args.for_seconds > 0 else float("inf"))
    runs = 0
    while True:
        args.apply = True
        plan = _ingest_store(args)
        runs += 1
        print(json.dumps({"watch_run": runs, "applied_files": plan["applied_files"],
                          "totals": plan["totals"]}, sort_keys=True), flush=True)
        if time.time() >= deadline:
            break
        time.sleep(interval)


def bridge_export(args):
    """Provenance-complete JSONL export of not-yet-exported cached messages.

    Deterministic: fixed ordering (session_row, seq), bounded count, and the
    digest covers the exact emitted lines. Ledger rows flip to exported only
    after the manifest is durably in place, so a failed write leaves them
    pending and the next run re-emits them.
    """
    channel = _channel(args)
    out = Path(args.out).expanduser()
    limit = max(1, int(args.limit))
    with ap.conn() as db:
        _ensure_bridge(db)
        sql = ("SELECT l.message_key,l.session_row,l.seq,l.role,l.at,l.content_hash,s.session_id,"
               "s.source,s.profile,s.project,m.content FROM bridge_export_ledger l "
               "JOIN sessions s ON s.id=l.session_row JOIN session_messages m "
               "ON m.session_row=l.session_row AND m.seq=l.seq "
               "WHERE l.state='pending' AND l.channel=? "
               "ORDER BY l.session_row ASC, l.seq ASC LIMIT ?")
        rows = [dict(r) for r in db.execute(sql, (channel, limit)).fetchall()]
        redact = bool(getattr(args, "redact", False))
        allow = bool(getattr(args, "allow_secret", False))
        # Findings are collected per row, so redaction touches only the rows
        # that actually carry a finding rather than rewriting every line
        # whenever any one line trips the guard.
        per_row = {r["message_key"]: sorted({f["kind"] for f in
                                             ap._secret_findings(r["content"])})
                   for r in rows}
        findings_kinds = sorted({k for ks in per_row.values() for k in ks})
        if findings_kinds and not (redact or allow):
            raise SystemExit(
                f"refusing to export credential-shaped content ({', '.join(findings_kinds)}); "
                "re-run with --redact or --allow-secret")
        lines = []
        for r in rows:
            content = r["content"]
            if redact and per_row[r["message_key"]]:
                content = ap._redact_secrets(content)
            rec = {
                "format": BRIDGE_EXPORT_FORMAT, "channel": channel,
                "provenance": {
                    "source": r["source"], "hermes_profile": r["profile"],
                    "project": r["project"], "session_id": r["session_id"],
                    "session_row": r["session_row"], "message_seq": r["seq"],
                    "message_key": r["message_key"], "role": r["role"],
                    "timestamp": r["at"], "content_hash": r["content_hash"],
                },
                "text": content,
            }
            lines.append(json.dumps(rec, sort_keys=True))
        digest = hashlib.sha256("\n".join(lines).encode()).hexdigest() if lines else ""
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".tmp")
        # Opened 0o600 rather than chmod-ed after the fact: the manifest can
        # carry conversation content, and a world-readable window between the
        # write and the chmod is a window.
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + ("\n" if lines else ""))
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(out)
        t = ap.now()
        for r in rows:
            db.execute(
                "UPDATE bridge_export_ledger SET state='exported',attempts=attempts+1,"
                "exported_at=?,updated_at=? WHERE message_key=?", (t, t, r["message_key"]))
        ap.audit(db, "session", "bridge-export",
                 "bridge_export_manifest_written",
                 {"channel": channel, "messages": len(rows),
                  "kinds": findings_kinds, "digest": digest})
    ap.json_out({"ok": True, "out": str(out), "channel": channel,
                 "messages": len(rows), "digest": digest,
                 "secret_kinds": findings_kinds,
                 "note": ("manifest written for downstream import" if rows
                          else "nothing pending")})


def bridge_export_status(args):
    """Ledger state for one export channel. Purely local; probes nothing."""
    channel = _channel(args)
    with ap.conn() as db:
        _ensure_bridge(db)
        counts = {st: db.execute(
            "SELECT COUNT(*) n FROM bridge_export_ledger WHERE channel=? AND state=?",
            (channel, st)).fetchone()["n"]
            for st in ("pending", "exported", "failed", "skipped")}
    ap.json_out({"channel": channel, "ledger": counts,
                 "export_state": "current" if counts["pending"] == 0 else "pending"})


def bridge_promote(args):
    """Explicit promotion of one cached message into structured control-plane state.

    Only ever invoked deliberately per message key; raw cache is untouched and
    the promotion carries a provenance citation so execution truth stays
    traceable back to session evidence.
    """
    kind = args.kind
    if kind not in ("note", "task"):
        raise SystemExit("--kind must be note or task (facts use fact-assert directly)")
    row_id, _, seq = args.message_key.rpartition(":")
    if not row_id or not seq.isdigit():
        raise SystemExit("--message-key must be <session_row>:<seq>")
    with ap.conn() as db:
        _ensure_bridge(db)
        msg = db.execute(
            "SELECT m.content,m.role,m.at,s.source,s.profile,s.project,s.session_id "
            "FROM session_messages m JOIN sessions s ON s.id=m.session_row "
            "WHERE m.session_row=? AND m.seq=?", (row_id, int(seq))).fetchone()
        if not msg:
            raise SystemExit("message not found in session cache")
        kinds = [f["kind"] for f in ap._secret_findings(msg["content"])]
        if kinds and not (args.redact or args.allow_secret):
            raise SystemExit(
                f"refusing credential-shaped content ({', '.join(sorted(set(kinds)))}); "
                "use --redact or --allow-secret")
        content = ap._redact_secrets(msg["content"]) if kinds and args.redact else msg["content"]
        citation = (f"[{msg['source']}:{msg['profile']}/{msg['session_id']}:"
                    f"{seq}@{msg['at']}#{hashlib.sha256((msg['role']+chr(0)+msg['content']).encode()).hexdigest()[:12]}]")
        t = ap.now()
        if kind == "note":
            if not args.task:
                raise SystemExit("--task is required for --kind note")
            ap.task_row(db, args.task)
            nid = __import__("uuid").uuid4().hex
            h = ap._note_hash(args.task, f"{citation} {content}")
            db.execute(
                "INSERT INTO notes(id,task_id,kind,content,source,content_hash,"
                "created_at,pinned,expires_at) VALUES(?,?,?,?,?,?,?,0,'')",
                (nid, args.task, "evidence", f"{citation} {content}",
                 "mindos-bridge", h, t))
            promoted = {"type": "note", "id": nid, "task": args.task}
        else:
            task_id = ((args.project or msg["project"] or "Inbox").lower().replace(" ", "-")
                       + "-" + __import__("uuid").uuid4().hex[:8])
            db.execute(
                "INSERT INTO tasks(id,project,title,description,owner,status,priority,"
                "next_action,due_at,not_before,tags,requires_receipts,created_at,updated_at) "
                "VALUES(?,?,?,?,'hermes','queued','P2','','','','[]','[]',?,?)",
                (task_id, args.project or msg["project"] or "Inbox",
                 args.title or content[:80], content, t, t))
            promoted = {"type": "task", "id": task_id}
        ap.audit(db, "session", args.message_key, "bridge_promoted",
                 {"kind": promoted["type"], "citation_digest":
                  hashlib.sha256(citation.encode()).hexdigest()[:12]})
    promoted["citation"] = citation
    ap.json_out({"ok": True, **promoted})


def main():
    p = argparse.ArgumentParser(description="MindOS live Hermes conversation bridge")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp, apply_flag=True):
        sp.add_argument("--source", required=True)
        sp.add_argument("--root", required=True)
        sp.add_argument("--profile", default="")
        sp.add_argument("--project", default="")
        sp.add_argument("--since", default="")
        sp.add_argument("--max-file-bytes", dest="max_file_bytes",
                        type=int, default=ap.DEFAULT_SESSION_MAX_FILE_BYTES)
        if apply_flag:
            sp.add_argument("--apply", action="store_true")
        sp.add_argument("--channel", default=DEFAULT_EXPORT_CHANNEL,
                        help="export channel recorded in the ledger")
        sp.add_argument("--redact", action="store_true")
        sp.add_argument("--allow-secret", dest="allow_secret", action="store_true")

    sp = sub.add_parser("sync", help="incremental ingest of Hermes-style session stores")
    common(sp)
    sp.set_defaults(fn=bridge_sync)

    sp = sub.add_parser("sqlite-sync",
                        help="ingest one live Hermes session by session_id from state.db")
    sp.add_argument("--state-db", dest="state_db", default="",
                    help="Hermes state.db path (default: HERMES_HOME/state.db)")
    sp.add_argument("--sqlite-session-id", dest="sqlite_session_id", required=True)
    sp.add_argument("--max-messages", dest="max_messages", type=int, default=5000)
    sp.add_argument("--profile", default="")
    sp.add_argument("--project", default="")
    sp.add_argument("--channel", default=DEFAULT_EXPORT_CHANNEL)
    sp.add_argument("--redact", action="store_true")
    sp.add_argument("--allow-secret", dest="allow_secret", action="store_true")
    sp.set_defaults(fn=bridge_sqlite_sync)

    sp = sub.add_parser("watch", help="poll-and-ingest on an interval (near-real-time)")
    common(sp, apply_flag=False)
    sp.add_argument("--interval", type=float, default=30.0)
    sp.add_argument("--for-seconds", dest="for_seconds", type=float, default=-1)
    sp.set_defaults(fn=bridge_watch)

    sp = sub.add_parser("export", help="provenance JSONL export manifest")
    sp.add_argument("--channel", default=DEFAULT_EXPORT_CHANNEL)
    sp.add_argument("--out", required=True)
    sp.add_argument("--limit", type=int, default=1000)
    sp.add_argument("--redact", action="store_true")
    sp.add_argument("--allow-secret", dest="allow_secret", action="store_true")
    sp.set_defaults(fn=bridge_export)

    sp = sub.add_parser("export-status", help="local export-ledger status for a channel")
    sp.add_argument("--channel", default=DEFAULT_EXPORT_CHANNEL)
    sp.set_defaults(fn=bridge_export_status)

    sp = sub.add_parser("promote", help="explicitly promote one message to note/task")
    sp.add_argument("message_key")
    sp.add_argument("--kind", choices=["note", "task"], required=True)
    sp.add_argument("--task", default="")
    sp.add_argument("--project", default="")
    sp.add_argument("--title", default="")
    sp.add_argument("--redact", action="store_true")
    sp.add_argument("--allow-secret", dest="allow_secret", action="store_true")
    sp.set_defaults(fn=bridge_promote)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
