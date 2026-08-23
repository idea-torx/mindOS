#!/usr/bin/env python3
"""MindOS managed intra-bot message layer (provider-neutral envelopes).

Ingests and coordinates managed bot-to-bot messages without confusing them
with ordinary user chat and without allowing silent loops:

1. Stable envelope `mindos-bot-envelope-v1`: message_id, correlation_id,
   in_reply_to, sender (bot/harness/profile), recipient (bot/profile),
   direction, capability_epoch, timestamp, content_class, content, optional
   autonomy_level / model_binding / provider / provenance. Cross-harness:
   canonical nested form, a flat variant, and Hermes-style dm text all parse
   to the same normalized envelope.
2. Peer allowlist with capability epochs (`bot_peers`): ingress requires a
   registered, unrevoked, unexpired peer whose epoch matches, whose
   capabilities cover the content class, and whose source profile is allowed.
3. Idempotent ingest + delivery receipts (`bot_messages`, `bot_receipts`):
   accepted | rejected | duplicate | expired | failed, one receipt row per
   (message,status) with attempt counting; replays are duplicates, never
   re-stored.
4. Loop/replay budgets: self-addressed refusal, correlation-chain cap,
   same-payload replay inside the chain window refused.
5. Secret guard before any storage: refuse by default, --redact stores
   [REDACTED:<kind>], audited --allow-secret. Values never logged.
6. Bounded context inclusion with provenance and exact-profile scoping; a
   matching section is exposed through mindos_context_pack.

This tool never sends messages and never touches live state; run it against
an explicit MindOS home (HERMES_AUTOPILOT_HOME).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import autopilot as ap  # noqa: E402  (home resolution, schema, guards)

ENVELOPE_FORMAT = "mindos-bot-envelope-v1"
CONTENT_CLASSES = ("bot_chat", "user_relay", "handoff", "task_receipt")
DIRECTIONS = ("inbound", "outbound")
RECEIPT_STATUSES = ("accepted", "rejected", "duplicate", "expired", "failed")
DEFAULT_MAX_CHAIN = 16
DEFAULT_MAX_AGE_HOURS = 24.0
MAX_FUTURE_SECONDS = 300.0
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{2,127}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_peers (
  peer_id TEXT PRIMARY KEY,
  harness TEXT NOT NULL DEFAULT '',
  bot_name TEXT NOT NULL,
  url TEXT NOT NULL DEFAULT '',
  key_ref TEXT NOT NULL DEFAULT '',
  capabilities TEXT NOT NULL DEFAULT '[]',
  capability_epoch INTEGER NOT NULL DEFAULT 1,
  allowed_profiles TEXT NOT NULL DEFAULT '[]',
  expires_at TEXT NOT NULL DEFAULT '',
  revoked INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bot_messages (
  message_id TEXT PRIMARY KEY,
  correlation_id TEXT NOT NULL,
  in_reply_to TEXT NOT NULL DEFAULT '',
  sender_peer TEXT NOT NULL,
  sender_bot TEXT NOT NULL,
  recipient_bot TEXT NOT NULL,
  direction TEXT NOT NULL,
  source_profile TEXT NOT NULL DEFAULT '',
  target_profile TEXT NOT NULL DEFAULT '',
  capability_epoch INTEGER NOT NULL DEFAULT 0,
  content_class TEXT NOT NULL,
  content TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  autonomy_level TEXT NOT NULL DEFAULT '',
  model_binding TEXT NOT NULL DEFAULT '',
  provider TEXT NOT NULL DEFAULT '',
  provenance_json TEXT NOT NULL DEFAULT '{}',
  redacted INTEGER NOT NULL DEFAULT 0,
  at TEXT NOT NULL,
  ingested_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_botmsg_corr ON bot_messages(correlation_id);
CREATE INDEX IF NOT EXISTS idx_botmsg_target ON bot_messages(target_profile);
CREATE TABLE IF NOT EXISTS bot_receipts (
  receipt_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL,
  status TEXT NOT NULL,
  reason_kind TEXT NOT NULL DEFAULT '',
  detail_json TEXT NOT NULL DEFAULT '{}',
  attempts INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_botreceipt_msg ON bot_receipts(message_id);
"""


def _ensure_botmail(db) -> None:
    db.executescript(SCHEMA)


class EnvelopeError(Exception):
    """Structurally invalid envelope; `kind` names the failure class."""

    def __init__(self, kind: str, detail: str = ""):
        super().__init__(detail or kind)
        self.kind = kind


def peer_id(harness: str, bot: str) -> str:
    return f"{harness}:{bot}"


def _iso_parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _norm_sender_recipient(env: dict) -> dict:
    """Accept canonical nested or flat single-object envelope variants."""
    out = dict(env)
    for side in ("sender", "recipient"):
        if not isinstance(out.get(side), dict):
            prefix = "sender" if side == "sender" else "recipient"
            out[side] = {
                "bot": out.pop(f"{prefix}_bot", ""),
                "harness": out.pop(f"{prefix}_harness", ""),
                "profile": out.pop(f"{prefix}_profile", ""),
            }
    return out


def parse_envelope(raw) -> dict:
    """Validate + normalize any supported cross-harness envelope shape."""
    if isinstance(raw, str):
        raw = envelope_from_dm_text(raw)
    if not isinstance(raw, dict):
        raise EnvelopeError("not_an_object")
    env = _norm_sender_recipient(raw)
    fmt = str(env.get("format", ENVELOPE_FORMAT))
    if fmt != ENVELOPE_FORMAT:
        raise EnvelopeError("unknown_format", fmt)
    mid = str(env.get("message_id", ""))
    corr = str(env.get("correlation_id", ""))
    if not _ID_RE.match(mid):
        raise EnvelopeError("bad_message_id")
    if not _ID_RE.match(corr):
        raise EnvelopeError("bad_correlation_id")
    s, r = env.get("sender") or {}, env.get("recipient") or {}
    sb, sh = str(s.get("bot", "")), str(s.get("harness", "")).lower()
    rb = str(r.get("bot", ""))
    sp, tp = str(s.get("profile", "")), str(r.get("profile", ""))
    for name, val in (("sender_bot", sb), ("recipient_bot", rb)):
        if not _ID_RE.match(val):
            raise EnvelopeError(f"bad_{name}")
    if sh and not _ID_RE.match(sh):
        raise EnvelopeError("bad_sender_harness")
    direction = str(env.get("direction", "inbound"))
    if direction not in DIRECTIONS:
        raise EnvelopeError("bad_direction")
    cc = str(env.get("content_class", "bot_chat"))
    if cc not in CONTENT_CLASSES:
        raise EnvelopeError("bad_content_class")
    try:
        epoch = int(env.get("capability_epoch", 0))
    except (TypeError, ValueError):
        raise EnvelopeError("bad_capability_epoch")
    if epoch < 1:
        raise EnvelopeError("bad_capability_epoch")
    ts = str(env.get("timestamp", ""))
    try:
        ts_dt = _iso_parse(ts)
    except ValueError:
        raise EnvelopeError("bad_timestamp")
    content = env.get("content")
    if not isinstance(content, str) or not content.strip():
        raise EnvelopeError("empty_content")
    auto = str(env.get("autonomy_level", ""))
    if auto and auto not in ap.AUTONOMY_LEVELS:
        raise EnvelopeError("bad_autonomy_level")
    model = str(env.get("model_binding", ""))
    if model:
        try:
            model = ap._valid_model_binding(model)
        except SystemExit:
            raise EnvelopeError("bad_model_binding")
    prov = env.get("provenance", {})
    if not isinstance(prov, dict):
        raise EnvelopeError("bad_provenance")
    return {
        "format": ENVELOPE_FORMAT, "message_id": mid, "correlation_id": corr,
        "in_reply_to": str(env.get("in_reply_to", "")),
        "sender": {"bot": sb, "harness": sh, "profile": sp},
        "recipient": {"bot": rb, "profile": tp},
        "direction": direction, "capability_epoch": epoch,
        "timestamp": ts_dt.astimezone(timezone.utc).isoformat(),
        "content_class": cc, "content": content,
        "autonomy_level": auto, "model_binding": model,
        "provider": str(env.get("provider", "")), "provenance": prov,
    }


_DM_TEXT_RE = re.compile(
    r"^Message from\s+(?:🤖\s*)?(?P<bot>[A-Za-z0-9._-]+)"
    r"(?:\s+\(@(?P<handle>[A-Za-z0-9._-]+)\))?:\s*(?P<body>.+)$",
    re.S)


DM_RECIPIENT = "local-agent"


def envelope_from_dm_text(text: str) -> dict:
    """Hermes-style dm text → envelope dict (cross-harness ingress shim).

    The dm text names only the remote sender; the receiving side is the
    local MindOS agent placeholder `local-agent`.
    """
    m = _DM_TEXT_RE.match((text or "").strip())
    if not m:
        raise EnvelopeError("unparseable_dm_text")
    body = m.group("body").strip()
    if not body:
        raise EnvelopeError("unparseable_dm_text")
    import uuid
    return {
        "format": ENVELOPE_FORMAT,
        "message_id": "dm-" + uuid.uuid5(uuid.NAMESPACE_URL,
                                         f"{m.group('bot')}|{body}").hex[:16],
        "correlation_id": m.group("handle") or m.group("bot"),
        "sender": {"bot": m.group("handle") or m.group("bot"),
                   "harness": "hermes", "profile": ""},
        "recipient": {"bot": DM_RECIPIENT, "profile": ""},
        "direction": "inbound", "capability_epoch": 1,
        "timestamp": ap.now(), "content_class": "bot_chat", "content": body,
    }


def _peer_row(db, pid: str):
    return db.execute("SELECT * FROM bot_peers WHERE peer_id=?", (pid,)).fetchone()


def _record_receipt(db, message_id: str, status: str, reason_kind: str = "",
                    detail: dict | None = None) -> dict:
    rid = hashlib.sha256(
        f"{message_id}\x00{status}".encode()).hexdigest()[:32]
    t = ap.now()
    row = db.execute("SELECT attempts FROM bot_receipts WHERE receipt_id=?",
                     (rid,)).fetchone()
    db.execute(
        "INSERT INTO bot_receipts(receipt_id,message_id,status,reason_kind,"
        "detail_json,attempts,created_at,updated_at) VALUES(?,?,?,?,?,1,?,?) "
        "ON CONFLICT(receipt_id) DO UPDATE SET attempts=attempts+1,"
        "reason_kind=excluded.reason_kind,detail_json=excluded.detail_json,"
        "updated_at=excluded.updated_at",
        (rid, message_id, status, reason_kind,
         json.dumps(detail or {}, sort_keys=True), t, t))
    return {"receipt_id": rid, "status": status,
            "attempts": (row["attempts"] + 1) if row else 1,
            "reason_kind": reason_kind}


def authorize(db, env: dict) -> tuple[str, str]:
    """Peer authorization + capability/epoch/expiry validation.

    Returns (ok_status, reason_kind); ok_status is '' when authorized,
    otherwise 'rejected' or 'expired'.
    """
    pid = peer_id(env["sender"]["harness"], env["sender"]["bot"])
    row = _peer_row(db, pid)
    if row is None:
        return "rejected", "peer_not_allowed"
    if int(row["revoked"] or 0):
        return "rejected", "peer_revoked"
    caps = json.loads(row["capabilities"])
    if env["content_class"] not in caps:
        return "rejected", "capability_missing"
    if int(row["capability_epoch"]) != int(env["capability_epoch"]):
        return "rejected", "epoch_mismatch"
    profiles = json.loads(row["allowed_profiles"])
    src = env["sender"]["profile"]
    if profiles and src not in profiles:
        return "rejected", "profile_not_allowed"
    exp = row["expires_at"]
    if exp:
        try:
            if _iso_parse(exp) <= datetime.now(timezone.utc):
                return "expired", "capability_expired"
        except ValueError:
            return "rejected", "bad_peer_expiry"
    return "", ""


def budget_check(db, env: dict, max_chain: int) -> tuple[str, str]:
    """Loop/replay budgets. Returns ('', '') or (reject_status, reason)."""
    if env["sender"]["bot"] == env["recipient"]["bot"]:
        return "rejected", "self_loop"
    n = db.execute("SELECT COUNT(*) n FROM bot_messages WHERE correlation_id=?",
                   (env["correlation_id"],)).fetchone()["n"]
    if n >= max_chain:
        return "rejected", "correlation_budget"
    ch = hashlib.sha256(
        f"{env['sender']['bot']}\x00{env['recipient']['bot']}\x00"
        f"{env['correlation_id']}\x00{env['content']}".encode()).hexdigest()
    dup = db.execute(
        "SELECT 1 FROM bot_messages WHERE correlation_id=? AND content_hash=? "
        "LIMIT 1", (env["correlation_id"], ch)).fetchone()
    if dup:
        return "rejected", "replay_budget"
    return "", ""


def ingest_envelope(args) -> dict:
    """Full ingress pipeline: parse → authorize → budgets → guard → store.

    Fail-open: unexpected internal errors become durable `failed` receipts +
    audit events instead of unhandled crashes. Guard refusals still exit
    non-zero honestly (no silent secret ingestion).
    """
    max_chain = max(1, int(getattr(args, "max_chain", DEFAULT_MAX_CHAIN)))
    max_age_h = float(getattr(args, "max_age_hours", DEFAULT_MAX_AGE_HOURS))
    redact = bool(getattr(args, "redact", False))
    allow = bool(getattr(args, "allow_secret", False))
    payload = json.loads(Path(args.envelope).read_text()) \
        if getattr(args, "envelope", "") else json.load(sys.stdin)
    result = {"command": "botmail-ingest", "ok": False}
    try:
        env = parse_envelope(payload)
    except EnvelopeError as e:
        result.update(receipt={"status": "rejected", "reason_kind": e.kind})
        return result
    result["envelope"] = {"message_id": env["message_id"],
                          "correlation_id": env["correlation_id"],
                          "sender": env["sender"], "recipient": env["recipient"],
                          "content_class": env["content_class"]}
    mid = env["message_id"]
    # Secret guard runs before any state-changing transaction so the refusal
    # can be audited durably without holding a write lock across the exit.
    findings = sorted({f["kind"] for f in ap._secret_findings(env["content"])})
    if findings and not (redact or allow):
        with ap.conn() as db:
            _ensure_botmail(db)
            rec = _record_receipt(db, mid, "rejected", "secret_guard")
            ap.audit(db, "bot_message", mid, "secret_blocked",
                     {"kinds": findings})
        result.update(receipt=rec)
        raise SystemExit(
            f"refusing credential-shaped bot message content "
            f"({', '.join(findings)}); re-run with --redact or --allow-secret")
    try:
        with ap.conn() as db:
            _ensure_botmail(db)
            existing = db.execute(
                "SELECT 1 FROM bot_messages WHERE message_id=?", (mid,)).fetchone()
            if existing:
                rec = _record_receipt(db, mid, "duplicate", "message_id_seen")
                ap.audit(db, "bot_message", mid, "duplicate_refused",
                         {"correlation_id": env["correlation_id"]})
                result.update(ok=True, receipt=rec)
                return result
            status, kind = "", ""
            try:
                ts_dt = _iso_parse(env["timestamp"])
                now_dt = datetime.now(timezone.utc)
                if ts_dt > now_dt + timedelta(seconds=MAX_FUTURE_SECONDS):
                    status, kind = "rejected", "future_timestamp"
                elif ts_dt < now_dt - timedelta(hours=max_age_h):
                    status, kind = "expired", "stale_envelope"
            except ValueError:
                status, kind = "rejected", "bad_timestamp"
            if not status:
                status, kind = authorize(db, env)
            if not status:
                status, kind = budget_check(db, env, max_chain)
            if status:
                rec = _record_receipt(db, mid, status, kind)
                ap.audit(db, "bot_message", mid, f"ingest_{status}",
                         {"reason_kind": kind,
                          "correlation_id": env["correlation_id"],
                          "content_class": env["content_class"]})
                result.update(ok=status == "expired", receipt=rec)
                return result
            stored = env["content"]
            redacted = False
            if findings and redact:
                stored = ap._redact_secrets(stored)
                redacted = True
            elif findings and allow:
                ap.audit(db, "bot_message", mid, "secret_allowed",
                         {"kinds": findings})
            ch = hashlib.sha256(
                f"{env['sender']['bot']}\x00{env['recipient']['bot']}\x00"
                f"{env['correlation_id']}\x00{stored}".encode()).hexdigest()
            t = ap.now()
            db.execute(
                "INSERT INTO bot_messages(message_id,correlation_id,in_reply_to,"
                "sender_peer,sender_bot,recipient_bot,direction,source_profile,"
                "target_profile,capability_epoch,content_class,content,"
                "content_hash,autonomy_level,model_binding,provider,"
                "provenance_json,redacted,at,ingested_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (mid, env["correlation_id"], env["in_reply_to"],
                 peer_id(env["sender"]["harness"], env["sender"]["bot"]),
                 env["sender"]["bot"], env["recipient"]["bot"], env["direction"],
                 env["sender"]["profile"], env["recipient"]["profile"],
                 env["capability_epoch"], env["content_class"], stored, ch,
                 env["autonomy_level"], env["model_binding"], env["provider"],
                 json.dumps(env["provenance"], sort_keys=True),
                 1 if redacted else 0, env["timestamp"], t))
            rec = _record_receipt(db, mid, "accepted")
            ap.audit(db, "bot_message", mid, "ingest_accepted",
                     {"correlation_id": env["correlation_id"],
                      "content_class": env["content_class"],
                      "content_hash": ch[:16],
                      "sender_peer": peer_id(env["sender"]["harness"],
                                             env["sender"]["bot"]),
                      "capability_epoch": env["capability_epoch"],
                      "redacted": redacted,
                      "autonomy_level": env["autonomy_level"],
                      "model_binding": env["model_binding"],
                      "provider": env["provider"]})
            result.update(ok=True, receipt=rec)
            return result
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 — fail-open durable failure state
        with ap.conn() as db:
            _ensure_botmail(db)
            rec = _record_receipt(db, mid, "failed", type(e).__name__)
            ap.audit(db, "bot_message", mid, "ingest_failed",
                     {"error_kind": type(e).__name__})
        result.update(ok=False, receipt=rec,
                      error_kind=type(e).__name__)
        return result


def cmd_ingest(args):
    if (getattr(args, "envelope", "-") or "-") == "-":
        args.envelope = ""  # stdin
    ap.json_out(ingest_envelope(args))


def cmd_peer_add(args):
    caps = [c for c in (args.capabilities or "").split(",") if c]
    bad = [c for c in caps if c not in CONTENT_CLASSES]
    if bad:
        raise SystemExit(f"unknown capabilities: {', '.join(bad)}")
    profiles = [p for p in (args.profiles or "").split(",") if p]
    if args.expires_at:
        try:
            _iso_parse(args.expires_at)
        except ValueError:
            raise SystemExit("--expires-at must be ISO 8601")
    pid = peer_id(args.harness.lower(), args.bot)
    t = ap.now()
    with ap.conn() as db:
        _ensure_botmail(db)
        row = _peer_row(db, pid)
        old_epoch = int(row["capability_epoch"]) if row else 0
        # Every roster change bumps the epoch so envelopes referencing a
        # stale epoch are rejected until senders catch up (--keep-epoch to
        # preserve it, e.g. for pure metadata edits).
        epoch = max(1, old_epoch + (0 if args.keep_epoch else 1))
        db.execute(
            "INSERT INTO bot_peers(peer_id,harness,bot_name,url,key_ref,"
            "capabilities,capability_epoch,allowed_profiles,expires_at,revoked,"
            "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(peer_id) DO UPDATE SET url=excluded.url,"
            "key_ref=excluded.key_ref,capabilities=excluded.capabilities,"
            "capability_epoch=excluded.capability_epoch,"
            "allowed_profiles=excluded.allowed_profiles,"
            "expires_at=excluded.expires_at,revoked=excluded.revoked,"
            "updated_at=excluded.updated_at",
            (pid, args.harness.lower(), args.bot, args.url, args.key_ref,
             json.dumps(caps), epoch, json.dumps(profiles), args.expires_at,
             1 if args.revoke else 0, t, t))
        ap.audit(db, "bot_peer", pid, "peer_upserted",
                 {"capabilities": caps, "capability_epoch": epoch,
                  "profiles": profiles, "revoked": bool(args.revoke),
                  "expires_at": args.expires_at})
    ap.json_out({"ok": True, "peer_id": pid, "capability_epoch": epoch})


def cmd_peer_list(_args):
    with ap.conn() as db:
        _ensure_botmail(db)
        rows = [dict(r) for r in db.execute(
            "SELECT * FROM bot_peers ORDER BY peer_id")]
    for r in rows:
        r["capabilities"] = json.loads(r["capabilities"])
        r["allowed_profiles"] = json.loads(r["allowed_profiles"])
    ap.json_out({"ok": True, "peers": rows})


def cmd_receipts(args):
    with ap.conn() as db:
        _ensure_botmail(db)
        conds, vals = [], []
        if args.message_id:
            conds.append("message_id=?")
            vals.append(args.message_id)
        if args.status:
            conds.append("status=?")
            vals.append(args.status)
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        rows = [dict(r) for r in db.execute(
            f"SELECT * FROM bot_receipts{where} ORDER BY updated_at DESC, "
            "receipt_id ASC LIMIT ?", (*vals, max(1, args.limit)))]
    ap.json_out({"ok": True, "receipts": rows})


def cmd_context(args):
    """Bounded bot-chat context block for one target profile."""
    block = build_context_block(
        profile=args.profile, query=args.query, limit=args.limit,
        max_bytes=args.max_bytes, redact=args.redact,
        allow_secret=args.allow_secret)
    ap.json_out(block)


def build_context_block(profile: str = "", query: str = "", limit: int = 6,
                        max_bytes: int = 2048, redact: bool = False,
                        allow_secret: bool = False) -> dict:
    """Profile-scoped bounded bot-chat candidates (mirrors pack semantics).

    Exact profile match on target_profile when a profile is given — bot chat
    for another profile can never be included. Guard ladder applies before
    packing; digest covers the emitted block minus generated_at.
    """
    items: list = []
    try:
        with ap.conn() as db:
            _ensure_botmail(db)
            conds, vals = [], []
            if profile:
                conds.append("target_profile=?")
                vals.append(profile)
            toks = [t for t in re.findall(r"[a-z0-9]+", (query or "").lower())
                    if t] if query else []
            if toks:
                conds.append("(" + " OR ".join("content LIKE ?" for _ in toks) + ")")
                vals.extend("%" + t + "%" for t in toks)
            where = (" WHERE " + " AND ".join(conds)) if conds else ""
            sql = ("SELECT message_id,sender_bot,correlation_id,content_class,"
                   "content,at,autonomy_level,model_binding,provider,redacted "
                   "FROM bot_messages" + where +
                   " ORDER BY at DESC, message_id ASC LIMIT ?")
            rows = db.execute(sql, (*vals, max(0, limit))).fetchall()
    except Exception as e:  # noqa: BLE001 — honest degradation
        return {"status": f"unavailable ({type(e).__name__})", "items": [],
                "matched_items": 0}
    matched = len(rows)
    used = 0
    truncated = False
    for r in rows:
        item = {k: r[k] for k in ("message_id", "sender_bot", "correlation_id",
                                  "content_class", "at", "autonomy_level",
                                  "model_binding", "provider")}
        g, refused = {}, None
        for k, v in {**item, "content": r["content"]}.items():
            if isinstance(v, str):
                kinds = [f["kind"] for f in ap._secret_findings(v)]
                if kinds:
                    if allow_secret:
                        pass
                    elif redact:
                        v = ap._redact_secrets(v)
                    else:
                        refused = sorted(set(kinds))[0]
                        break
            g[k] = v
        if refused:
            continue
        cost = len(json.dumps(g, sort_keys=True, separators=(",", ":")))
        if used + cost > max_bytes:
            truncated = True
            break
        used += cost
        items.append(g)
    status = "ok" if items else ("refused-secret" if matched and not items
                                 else "empty")
    block = {"format": ENVELOPE_FORMAT, "status": status, "items": items,
             "matched_items": matched, "used_bytes": used,
             "truncated": truncated, "profile_scope": profile or "*"}
    block["digest"] = hashlib.sha256(json.dumps(
        block, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return block


def main():
    p = argparse.ArgumentParser(
        description="MindOS managed intra-bot communication layer")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("ingest", help="validate, authorize and store an envelope")
    sp.add_argument("envelope", nargs="?", default="-",
                    help="path to envelope JSON, or - for stdin")
    sp.add_argument("--max-chain", dest="max_chain", type=int,
                    default=DEFAULT_MAX_CHAIN)
    sp.add_argument("--max-age-hours", dest="max_age_hours", type=float,
                    default=DEFAULT_MAX_AGE_HOURS)
    sp.add_argument("--redact", action="store_true")
    sp.add_argument("--allow-secret", dest="allow_secret", action="store_true")
    sp.set_defaults(fn=cmd_ingest)

    pp = sub.add_parser("peer-add", help="register/update an allowed peer")
    pp.add_argument("--harness", required=True)
    pp.add_argument("--bot", required=True)
    pp.add_argument("--url", default="")
    pp.add_argument("--key-ref", dest="key_ref", default="",
                    help="where the credential lives (never the value)")
    pp.add_argument("--capabilities", default="bot_chat",
                    help="comma list of: " + ", ".join(CONTENT_CLASSES))
    pp.add_argument("--profiles", default="",
                    help="comma list of allowed source profiles (empty = any)")
    pp.add_argument("--expires-at", dest="expires_at", default="")
    pp.add_argument("--revoke", action="store_true")
    pp.add_argument("--keep-epoch", dest="keep_epoch", action="store_true")
    pp.set_defaults(fn=cmd_peer_add)

    pl = sub.add_parser("peer-list", help="list registered peers")
    pl.set_defaults(fn=cmd_peer_list)

    pr = sub.add_parser("receipts", help="delivery receipt lifecycle")
    pr.add_argument("--message-id", dest="message_id", default="")
    pr.add_argument("--status", default="", choices=[""] + list(RECEIPT_STATUSES))
    pr.add_argument("--limit", type=int, default=50)
    pr.set_defaults(fn=cmd_receipts)

    pc = sub.add_parser("context", help="bounded bot-chat context block")
    pc.add_argument("--profile", default="")
    pc.add_argument("--query", default="")
    pc.add_argument("--limit", type=int, default=6)
    pc.add_argument("--max-bytes", dest="max_bytes", type=int, default=2048)
    pc.add_argument("--redact", action="store_true")
    pc.add_argument("--allow-secret", dest="allow_secret", action="store_true")
    pc.set_defaults(fn=cmd_context)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
