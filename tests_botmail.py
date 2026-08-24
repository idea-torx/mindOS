#!/usr/bin/env python3
"""Executable verification for MindOS managed intra-bot communication.

Disposable fixtures only: every run uses HERMES_AUTOPILOT_HOME on a temp dir.
Live ~/.hermes, gateways, and peers are never touched.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).parent

with tempfile.TemporaryDirectory() as td:
    env = os.environ.copy()
    env["HERMES_AUTOPILOT_HOME"] = str(Path(td) / "mindos-home")

    def bm(*a, expect_fail=False):
        p = subprocess.run([sys.executable, str(ROOT / "mindos_botmail.py"), *a],
                           env=env, text=True, capture_output=True)
        if expect_fail:
            assert p.returncode != 0, ("expected failure", a, p.stdout, p.stderr)
            return p.stderr
        assert p.returncode == 0, (a, p.stdout, p.stderr)
        return json.loads(p.stdout)

    def db_rows(sql, vals=()):
        with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
            db.row_factory = sqlite3.Row
            return [dict(r) for r in db.execute(sql, vals)]

    def env_path(envdict):
        p = Path(td) / f"env-{envdict['message_id']}.json"
        p.write_text(json.dumps(envdict))
        return str(p)

    def now_iso(**delta):
        return (datetime.now(timezone.utc) +
                timedelta(**delta)).isoformat().replace("+00:00", "Z")

    def envelope(mid, **kw):
        e = {
            "format": "mindos-bot-envelope-v1",
            "message_id": mid,
            "correlation_id": kw.pop("correlation_id", "corr-" + mid),
            "in_reply_to": "",
            "sender": {"bot": kw.pop("sender_bot", "spark"),
                       "harness": kw.pop("sender_harness", "hermes"),
                       "profile": kw.pop("source_profile", "telegram-leo")},
            "recipient": {"bot": kw.pop("recipient_bot", "dixie"),
                          "profile": kw.pop("target_profile", "default")},
            "direction": "inbound",
            "capability_epoch": kw.pop("capability_epoch", 1),
            "timestamp": kw.pop("timestamp", now_iso()),
            "content_class": kw.pop("content_class", "bot_chat"),
            "content": kw.pop("content", f"status check {mid}"),
        }
        e.update(kw)
        return e

    # -- Peer registration + capability epochs ---------------------------------
    r = bm("peer-add", "--harness", "hermes", "--bot", "spark", "--url",
           "http://spark.lan:8377", "--key-ref", "~/.hermes/.env:API_SERVER_KEY",
           "--capabilities", "bot_chat,handoff", "--profiles", "telegram-leo")
    assert r["capability_epoch"] == 1, r
    lst = bm("peer-list")
    assert len(lst["peers"]) == 1 and lst["peers"][0]["peer_id"] == "hermes:spark"
    print("PASS peer registry")

    # -- Accepted ingest (canonical form) + idempotent duplicates ----------------
    e1 = envelope("msg-accept-1")
    out = bm("ingest", env_path(e1))
    assert out["ok"] is True and out["receipt"]["status"] == "accepted", out
    rows = db_rows("SELECT * FROM bot_messages WHERE message_id='msg-accept-1'")
    assert len(rows) == 1 and rows[0]["sender_peer"] == "hermes:spark"
    assert rows[0]["content_class"] == "bot_chat"
    out2 = bm("ingest", env_path(e1))
    assert out2["receipt"]["status"] == "duplicate", out2
    out3 = bm("ingest", env_path(e1))
    assert out3["receipt"]["attempts"] == 2, out3  # attempt counting, still no re-store
    assert len(db_rows("SELECT * FROM bot_messages")) == 1
    recs = bm("receipts", "--message-id", "msg-accept-1")["receipts"]
    assert {r["status"] for r in recs} == {"accepted", "duplicate"}, recs
    print("PASS idempotent delivery receipts")

    # -- Rejections: unknown peer, epoch mismatch, capability, profile, loop ----
    rej = [
        ("peer_not_allowed", envelope("msg-rp", sender_bot="stranger")),
        ("epoch_mismatch", envelope("msg-re", capability_epoch=99)),
        ("capability_missing",
         envelope("msg-rc", content_class="user_relay")),
        ("profile_not_allowed",
         envelope("msg-rf", source_profile="other-profile")),
        ("self_loop", envelope("msg-rs", recipient_bot="spark")),
    ]
    for kind, e in rej:
        o = bm("ingest", env_path(e))
        assert o["ok"] is False and o["receipt"]["status"] == "rejected", (kind, o)
        assert o["receipt"]["reason_kind"] == kind, (kind, o)
    assert len(db_rows("SELECT * FROM bot_messages")) == 1
    print("PASS rejection ladder (peer/epoch/capability/profile/self-loop)")

    # -- Expiry: stale envelope + expired peer capability ------------------------
    o = bm("ingest", env_path(envelope("msg-stale", timestamp=now_iso(hours=-48))))
    assert o["ok"] is True and o["receipt"]["status"] == "expired", o
    assert o["receipt"]["reason_kind"] == "stale_envelope", o
    bm("peer-add", "--harness", "hermes", "--bot", "ghost", "--capabilities",
       "bot_chat", "--expires-at", now_iso(minutes=-5))
    o = bm("ingest", env_path(envelope("msg-exp-peer", sender_bot="ghost")))
    assert o["ok"] is True and o["receipt"]["status"] == "expired", o
    assert o["receipt"]["reason_kind"] == "capability_expired", o
    assert not db_rows(
        "SELECT 1 FROM bot_messages WHERE message_id='msg-exp-peer'")
    print("PASS expiry (stale envelope / expired peer capability)")

    # -- Capability epoch bump invalidates envelopes minted at the old epoch -----
    r2 = bm("peer-add", "--harness", "hermes", "--bot", "spark", "--url",
            "http://spark.lan:8377", "--capabilities", "bot_chat,handoff",
            "--profiles", "telegram-leo")
    assert r2["capability_epoch"] == 2, r2
    o = bm("ingest", env_path(envelope("msg-old-epoch")))
    assert o["receipt"]["reason_kind"] == "epoch_mismatch", o
    E2 = {"capability_epoch": 2}
    o = bm("ingest", env_path(envelope("msg-new-epoch", **E2)))
    assert o["receipt"]["status"] == "accepted", o
    print("PASS capability/epoch validation across roster changes")

    # -- Loop/replay budgets ------------------------------------------------------
    o = bm("ingest", env_path(envelope("msg-loop-a", correlation_id="corr-loop",
                                       content="ping loop", **E2)))
    assert o["receipt"]["status"] == "accepted", o
    o = bm("ingest", env_path(envelope("msg-loop-b", correlation_id="corr-loop",
                                       content="ping loop", **E2)))
    assert o["receipt"]["status"] == "rejected", o
    assert o["receipt"]["reason_kind"] == "replay_budget", o  # same payload replay
    o = bm("ingest", env_path(envelope("msg-chain-x", correlation_id="corr-big",
                                       content="unique payload x", **E2)))
    assert o["receipt"]["status"] == "accepted", o
    for i in range(20):
        o = bm("ingest", env_path(envelope(f"msg-big-{i}",
                                           correlation_id="corr-big",
                                           content=f"growth payload {i}", **E2)))
    assert o["receipt"]["status"] == "rejected" and \
        o["receipt"]["reason_kind"] == "correlation_budget", o
    n_big = db_rows("SELECT COUNT(*) n FROM bot_messages "
                    "WHERE correlation_id='corr-big'")[0]["n"]
    assert n_big <= 16, n_big
    print("PASS loop/replay budgets")

    # -- Secret guard: refuse default / redact explicit / audited allow ----------
    SECRET_VALUE = "AKIA" + "IOSFODNN7EXAMPLE"
    esec = envelope("msg-secret", content=f"use key {SECRET_VALUE} now", **E2)
    err = bm("ingest", env_path(esec), expect_fail=True)
    assert "refusing" in err and SECRET_VALUE not in err, err
    assert not db_rows("SELECT 1 FROM bot_messages WHERE message_id='msg-secret'")
    out = bm("ingest", env_path(esec), "--redact")
    assert out["receipt"]["status"] == "accepted", out
    row = db_rows("SELECT content FROM bot_messages "
                  "WHERE message_id='msg-secret'")[0]
    assert "[REDACTED:aws_access_key]" in row["content"], row
    audit_payloads = "".join(r["payload_json"] for r in db_rows(
        "SELECT payload_json FROM audit_events"))
    all_msgs = "".join(r["content"] for r in db_rows(
        "SELECT content FROM bot_messages"))
    assert SECRET_VALUE not in audit_payloads and SECRET_VALUE not in all_msgs
    print("PASS secret guard (refuse/redact/no value leakage)")

    # -- Profile isolation in bounded context ------------------------------------
    ctx_a = bm("context", "--profile", "default", "--query", "status check")
    assert ctx_a["status"] == "ok" and len(ctx_a["items"]) >= 1, ctx_a
    d1 = ctx_a["digest"]
    ctx_a2 = bm("context", "--profile", "default", "--query", "status check")
    assert ctx_a2["digest"] == d1
    # A message addressed to another profile never appears in this scope.
    bm("peer-add", "--harness", "opencode", "--bot", "fable",
       "--capabilities", "bot_chat")
    o = bm("ingest", env_path(envelope(
        "msg-other-profile", sender_bot="fable", sender_harness="opencode",
        target_profile="private-profile",
        content="orchid secrets of profile B")))
    assert o["receipt"]["status"] == "accepted", o
    ctx_b = bm("context", "--profile", "default", "--query", "orchid")
    assert ctx_b["items"] == [], ctx_b
    assert "orchid" not in json.dumps(ctx_a)
    print("PASS bounded context + profile isolation + digest determinism")

    # -- Bot chat reaches the session-start context pack, profile-scoped ----------
    pk = subprocess.run([sys.executable, str(ROOT / "mindos_context_pack.py"),
                         "session-pack", "--profile", "default",
                         "--query", "status check", "--max-bytes", "8192"],
                        env=env, text=True, capture_output=True)
    pack = json.loads(pk.stdout)
    assert pack["sources"]["bot_chat"]["status"] == "ok", pack["sources"]
    assert all(i.get("correlation_id") for i in pack["sections"]["bot_chat"])
    pk_b = json.loads(subprocess.run(
        [sys.executable, str(ROOT / "mindos_context_pack.py"), "session-pack",
         "--profile", "private-profile", "--query", "orchid",
         "--max-bytes", "8192"],
        env=env, text=True, capture_output=True).stdout)
    assert pk_b["sections"]["bot_chat"], pk_b["sections"]
    assert "orchid" not in json.dumps(pack["sections"])  # no cross-profile leak
    print("PASS context-pack bot_chat section (bounded, provenance, isolated)")

    # -- Fail-open durable failure state -------------------------------------------
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        db.execute("DROP TABLE bot_messages")
        db.execute("""CREATE TABLE bot_messages (
            message_id TEXT PRIMARY KEY, correlation_id TEXT NOT NULL,
            in_reply_to TEXT NOT NULL DEFAULT '', sender_peer TEXT NOT NULL,
            sender_bot TEXT NOT NULL, recipient_bot TEXT NOT NULL,
            direction TEXT NOT NULL, source_profile TEXT NOT NULL DEFAULT '',
            target_profile TEXT NOT NULL, capability_epoch INTEGER NOT NULL,
            content_class TEXT NOT NULL, content TEXT NOT NULL,
            content_hash TEXT NOT NULL, autonomy_level TEXT NOT NULL DEFAULT '',
            model_binding TEXT NOT NULL DEFAULT '', provider TEXT NOT NULL DEFAULT '',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            redacted INTEGER NOT NULL, at TEXT NOT NULL,
            ingested_at TEXT NOT NULL, forced_break TEXT NOT NULL)""")
        db.commit()
    o = bm("ingest", env_path(envelope("msg-failopen", **E2)))
    assert o["ok"] is False and o["receipt"]["status"] == "failed", o
    assert o["receipt"]["reason_kind"] == "IntegrityError", o
    recs = bm("receipts", "--status", "failed")["receipts"]
    assert any(r["message_id"] == "msg-failopen" for r in recs), recs
    print("PASS fail-open durable failure receipt")

    # -- Cross-harness envelope parsing ---------------------------------------------
    probe = """
import json, sys
sys.path.insert(0, %r)
import mindos_botmail as mb
flat = mb.parse_envelope({
    "format": mb.ENVELOPE_FORMAT, "message_id": "codex-flat-1",
    "correlation_id": "corr-flat", "sender_bot": "claude",
    "sender_harness": "claude", "recipient_bot": "dixie",
    "capability_epoch": 3, "content_class": "handoff",
    "timestamp": "2026-08-22T10:00:00+02:00",
    "content": "handoff body", "autonomy_level": "L1"})
assert flat["sender"]["harness"] == "claude"
assert flat["timestamp"].endswith("+00:00") and flat["autonomy_level"] == "L1"
assert flat["model_binding"] == ""
rich = mb.parse_envelope({
    "format": mb.ENVELOPE_FORMAT, "message_id": "dsh-rich-1",
    "correlation_id": "corr-rich", "sender": {"bot": "planner",
        "harness": "codex", "profile": "p1"},
    "recipient": {"bot": "builder", "profile": "p2"},
    "capability_epoch": 1, "timestamp": "2026-08-22T08:00:00Z",
    "content_class": "task_receipt", "content": "done",
    "autonomy_level": "L2", "model_binding": "x-preview-f-free",
    "provider": "opencode", "provenance": {"run": "r1"}})
assert rich["model_binding"] == "x-preview-f-free"
assert rich["provider"] == "opencode" and rich["provenance"] == {"run": "r1"}
dm = mb.parse_envelope("Message from " + chr(0x1F916) +
                       " dixie (@dixie): disk status?")
assert dm["sender"]["bot"] == "dixie" and dm["content"] == "disk status?"
assert dm["sender"]["harness"] == "hermes"
assert dm["message_id"].startswith("dm-")
for bad in [{"format": "x"}, {"message_id": ".."},
            {"content_class": "nope"}, {"autonomy_level": "L9"},
            {"capability_epoch": 0}, {"timestamp": "not-a-time"},
            {"direction": "sideways"}]:
    base = {"format": mb.ENVELOPE_FORMAT, "message_id": "m-1",
            "correlation_id": "c-1", "sender": {"bot": "a", "harness": "b"},
            "recipient": {"bot": "c"}, "capability_epoch": 1,
            "timestamp": "2026-08-22T10:00:00Z", "content_class": "bot_chat",
            "content": "hi"}
    base.update(bad)
    try:
        mb.parse_envelope(base); raise AssertionError("should reject: " + str(bad))
    except mb.EnvelopeError:
        pass
print(json.dumps({"ok": True}))
""" % str(ROOT)
    p = subprocess.run([sys.executable, "-c", probe], env=env,
                       text=True, capture_output=True)
    assert p.returncode == 0 and json.loads(p.stdout)["ok"], p.stderr[-800:]
    print("PASS cross-harness parsing (flat/rich variants, dm text, rejects)")

print("botmail tests: PASS")
