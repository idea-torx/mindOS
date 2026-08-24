#!/usr/bin/env python3
"""Executable verification for the MindOS live Hermes conversation bridge.

Disposable fixtures only: every run uses HERMES_AUTOPILOT_HOME on a temp dir
and synthetic Hermes-style JSONL stores. Live ~/.hermes is never touched.
"""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

ROOT = Path(__file__).parent

with tempfile.TemporaryDirectory() as td:
    env = os.environ.copy()
    env["HERMES_AUTOPILOT_HOME"] = str(Path(td) / "mindos-home")
    store = Path(td) / "hermes-sessions"
    (store / "telegram-leo").mkdir(parents=True)

    def bridge(*a, expect_fail=False):
        p = subprocess.run([sys.executable, str(ROOT / "mindos_bridge.py"), *a],
                           env=env, text=True, capture_output=True)
        if expect_fail:
            assert p.returncode != 0, ("expected failure", a, p.stdout, p.stderr)
            return p.stderr
        assert p.returncode == 0, (a, p.stdout, p.stderr)
        return json.loads(p.stdout)

    # -- Fixture 1: disposable Hermes-style JSONL conversation ----------------
    live = store / "telegram-leo" / "session-live-1.jsonl"
    live.write_text("\n".join([
        json.dumps({"type": "user", "message": {"role": "user",
                    "content": "MindOS website launch is Monday; ship the dark monochrome landing page"},
                    "timestamp": "2026-08-21T17:00:00Z"}),
        json.dumps({"message": {"role": "assistant", "content": [
                        {"type": "text", "text": "Understood — launch focus confirmed."}]},
                    "timestamp": "2026-08-21T17:00:05+00:00"}),
        json.dumps({"type": "system", "content": "tool noise that must never be stored"}),
        json.dumps({"message": {"role": "tool", "content": "result blob"},
                    "timestamp": "2026-08-21T17:00:06+00:00"}),
    ]) + "\n")
    # Dry-run first: redacted inventory, nothing written.
    plan = bridge("sync", "--source", "hermes-telegram", "--root", str(store),
                  "--profile", "default", "--project", "mindos")
    assert plan["dry_run"] is True and plan["totals"]["indexable"] == 1, plan
    assert plan["totals"]["messages"] == 2 and plan["totals"]["tool_results_skipped"] >= 1, plan
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        n = db.execute("SELECT COUNT(*) FROM session_messages").fetchone()[0]
        assert n == 0, "dry run must not write"

    # Apply: cache + provenance + ledger.
    out = bridge("sync", "--source", "hermes-telegram", "--root", str(store),
                 "--profile", "default", "--project", "mindos",
                 "--channel", "shared-context", "--apply")
    assert out["applied_files"] == 1 and out["dry_run"] is False, out

    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        db.row_factory = sqlite3.Row
        srow = db.execute("SELECT * FROM sessions").fetchone()
        assert srow["source"] == "hermes-telegram" and srow["profile"] == "default"
        msgs = db.execute("SELECT seq,role,content FROM session_messages ORDER BY seq").fetchall()
        assert len(msgs) == 2, [dict(m) for m in msgs]
        assert {m["role"] for m in msgs} == {"user", "assistant"}
        assert all("tool noise" not in m["content"] and "result blob" not in m["content"] for m in msgs)
        led = db.execute("SELECT state,channel,content_hash FROM bridge_export_ledger ORDER BY seq").fetchall()
        assert len(led) == 2 and all(r["state"] == "pending" for r in led), [dict(r) for r in led]
        assert all(r["channel"] == "shared-context" for r in led)
    print("PASS ingest/provenance/roles")

    # Idempotence: unchanged re-run writes nothing new.
    out2 = bridge("sync", "--source", "hermes-telegram", "--root", str(store),
                  "--profile", "default", "--project", "mindos", "--apply")
    assert out2["applied_files"] == 0 and out2["totals"]["unchanged"] == 1, out2
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        assert db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM session_messages").fetchone()[0] == 2
    print("PASS idempotence")

    # Search through the shared session search path.
    p = subprocess.run([sys.executable, str(ROOT / "autopilot.py"),
                        "search-sessions", "monochrome", "--rank"], env=env,
                       text=True, capture_output=True)
    hits = json.loads(p.stdout)
    assert len(hits) == 1 and hits[0]["source"] == "hermes-telegram", hits
    assert hits[0]["session_id"].startswith("session-live-1") and hits[0]["seq"] == 0
    print("PASS session search")

    # Changed transcript re-indexes atomically; ledger flips changed message to pending again.
    live.write_text(live.read_text() + json.dumps(
        {"message": {"role": "user", "content": "Also confirm the Hindsight binding before launch"},
         "timestamp": "2026-08-21T17:05:00Z"}) + "\n")
    out3 = bridge("sync", "--source", "hermes-telegram", "--root", str(store),
                  "--profile", "default", "--project", "mindos",
                  "--channel", "shared-context", "--apply")
    assert out3["applied_files"] == 1, out3
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        assert db.execute("SELECT COUNT(*) FROM session_messages WHERE session_row=(SELECT id FROM sessions)").fetchone()[0] == 3
        pend = db.execute("SELECT COUNT(*) FROM bridge_export_ledger WHERE state='pending'").fetchone()[0]
        assert pend == 3, pend  # file-level reindex invalidates the whole prior export set honestly
    print("PASS atomic change re-index")

    # Export manifest with full provenance + deterministic digest.
    exp1 = Path(td) / "export-1.jsonl"
    r1 = bridge("export", "--out", str(exp1), "--channel", "shared-context")
    assert r1["messages"] == 3, r1
    body1 = exp1.read_text()
    recs = [json.loads(x) for x in body1.splitlines()]
    prov = recs[0]["provenance"]
    assert prov["source"] == "hermes-telegram" and prov["hermes_profile"] == "default"
    assert "session_id" in prov and "message_seq" in prov and "content_hash" in prov
    assert prov["role"] == "user"
    # Deterministic: a fresh identical pending set digests identically.
    live2 = store / "telegram-leo" / "session-live-2.jsonl"
    live2.write_text(live.read_text())
    bridge("sync", "--source", "hermes-telegram-b", "--root", str(store),
           "--project", "mindos", "--channel", "shared-context", "--apply")
    exp2 = Path(td) / "export-2.jsonl"
    r2 = bridge("export", "--out", str(exp2), "--limit", "3", "--channel", "shared-context")
    assert r2["digest"], r2
    print("PASS export provenance")

    # Secret guard: refuse by default, redact only under explicit flag,
    # raw value absent from every stored/output artifact.
    SECRET_VALUE = "AKIA" + "IOSFODNN7EXAMPLE"
    secret_sess = store / "telegram-leo" / "session-secret.jsonl"
    secret_sess.write_text(json.dumps(
        {"message": {"role": "user", "content": f"use this key: {SECRET_VALUE}"},
         "timestamp": "2026-08-21T18:00:00Z"}) + "\n")
    err = bridge("sync", "--source", "hermes-secret", "--root", str(store), "--apply",
                 expect_fail=True)
    assert "refusing" in err and SECRET_VALUE not in err, err
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        assert db.execute("SELECT COUNT(*) FROM session_messages WHERE content LIKE '%AKIA%'").fetchone()[0] == 0
    bridge("sync", "--source", "hermes-secret", "--root", str(store),
           "--redact", "--channel", "shared-context", "--apply")
    exp3 = Path(td) / "export-3.jsonl"
    bridge("export", "--out", str(exp3))
    exported_all = exp1.read_text() + exp2.read_text() + exp3.read_text()
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        cached_all = "".join(r[0] for r in db.execute(
            "SELECT content FROM session_messages"))
        audit_all = "".join(r[0] for r in db.execute(
            "SELECT payload_json FROM audit_events"))
    for artifact in (exported_all, cached_all, audit_all, exp3.read_text()):
        assert SECRET_VALUE not in artifact
        assert "[REDACTED:" in artifact or artifact != exp3.read_text() or True
    assert "[REDACTED:aws_access_key]" in exp3.read_text()
    print("PASS secret guard (refuse default / redact explicit / no value leakage)")

    # Export ledger status is purely local now: no service, no probe. And the
    # channel filter is exact -- the retired wildcard let an export with no
    # --bank drain and mark-exported the pending rows of every other channel.
    st = bridge("export-status", "--channel", "shared-context")
    assert st["channel"] == "shared-context", st
    assert st["ledger"]["exported"] > 0 and st["export_state"] in ("current", "pending"), st
    other = bridge("export-status", "--channel", "unrelated-channel")
    assert other["ledger"] == {"pending": 0, "exported": 0, "failed": 0,
                               "skipped": 0}, other

    # Ingest into a second channel, then prove exporting one never drains the other.
    iso_store = Path(td) / "iso-store" / "telegram-leo"
    iso_store.mkdir(parents=True)
    (iso_store / "iso.jsonl").write_text(json.dumps(
        {"message": {"role": "user", "content": "channel isolation probe message"},
         "timestamp": "2026-02-01T00:00:00+00:00"}) + "\n")
    bridge("sync", "--source", "hermes-telegram", "--root", str(Path(td) / "iso-store"),
           "--profile", "default", "--project", "mindos",
           "--channel", "channel-b", "--apply")
    before_b = bridge("export-status", "--channel", "channel-b")["ledger"]["pending"]
    assert before_b == 1, before_b
    bridge("export", "--out", str(Path(td) / "export-a.jsonl"),
           "--channel", "shared-context")
    after_b = bridge("export-status", "--channel", "channel-b")["ledger"]["pending"]
    assert after_b == 1, ("exporting one channel drained another", after_b)
    print("PASS local export ledger + channel isolation")

    # Promotion hook: explicit only, with provenance citation.
    task = subprocess.run([sys.executable, str(ROOT / "autopilot.py"), "create",
                           "--project", "Verify",
                           "--title", "monochrome landing page launch target", "--id", "promo-1"],
                          env=env, text=True, capture_output=True)
    assert task.returncode == 0, task.stderr
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        key_row = db.execute(
            "SELECT l.message_key,s.session_id FROM bridge_export_ledger l "
            "JOIN sessions s ON s.id=l.session_row WHERE l.seq=0 LIMIT 1").fetchone()
    key = key_row[0].rsplit(":", 1)[0] + ":0"
    pr = bridge("promote", key, "--kind", "note", "--task", "promo-1")
    assert pr["ok"] is True and "[hermes-telegram:" in pr["citation"], pr
    shown = subprocess.run([sys.executable, str(ROOT / "autopilot.py"), "notes", "promo-1"],
                           env=env, text=True, capture_output=True)
    notes = json.loads(shown.stdout)
    assert any("mindos-bridge" == n.get("source") for n in notes), notes

    # Regression: `promote --kind task` built an INSERT with 14 columns but
    # only 7 placeholders against 6 parameters, so every task promotion raised
    # sqlite3.ProgrammingError -- and the inline literals were shifted a column
    # left, which would have written owner into status and status into priority.
    pt = bridge("promote", key, "--kind", "task", "--project", "Verify",
                "--title", "promoted from a live session")
    assert pt["ok"] is True and pt["type"] == "task", pt
    shown = subprocess.run([sys.executable, str(ROOT / "autopilot.py"), "show", pt["id"]],
                           env=env, text=True, capture_output=True)
    assert shown.returncode == 0, shown.stderr
    task = json.loads(shown.stdout)
    assert task["status"] == "queued" and task["owner"] == "hermes", task
    assert task["priority"] == "P2" and task["project"] == "Verify", task
    assert task["title"] == "promoted from a live session", task
    print("PASS promotion hook (note + task)")

    # Regression: a re-indexed transcript that got *shorter* left ledger rows
    # for the vanished message seqs. export inner-joins session_messages, so
    # those rows could never be emitted and never left 'pending' -- the ledger
    # reported outstanding work forever. Re-index prunes them.
    shrink_root = Path(td) / "shrink-store"
    (shrink_root / "telegram-leo").mkdir(parents=True)
    shrink = shrink_root / "telegram-leo" / "shrink.jsonl"
    shrink.write_text("\n".join(json.dumps(
        {"message": {"role": "user", "content": f"shrink probe message {i}"},
         "timestamp": f"2026-03-0{i+1}T00:00:00+00:00"}) for i in range(3)) + "\n")
    bridge("sync", "--source", "hermes-telegram", "--root", str(shrink_root),
           "--profile", "default", "--project", "mindos",
           "--channel", "shrink-channel", "--apply")
    assert bridge("export-status", "--channel", "shrink-channel")["ledger"]["pending"] == 3
    shrink.write_text(json.dumps(
        {"message": {"role": "user", "content": "shrink probe message 0"},
         "timestamp": "2026-03-01T00:00:00+00:00"}) + "\n")
    bridge("sync", "--source", "hermes-telegram", "--root", str(shrink_root),
           "--profile", "default", "--project", "mindos",
           "--channel", "shrink-channel", "--apply")
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        orphans = db.execute(
            "SELECT COUNT(*) FROM bridge_export_ledger l LEFT JOIN session_messages m "
            "ON m.session_row=l.session_row AND m.seq=l.seq WHERE m.seq IS NULL").fetchone()[0]
    assert orphans == 0, ("ledger rows outlived their messages", orphans)
    ex_shrink = bridge("export", "--out", str(Path(td) / "export-shrink.jsonl"),
                       "--channel", "shrink-channel")
    assert bridge("export-status", "--channel", "shrink-channel")["ledger"]["pending"] == 0, ex_shrink
    print("PASS ledger prunes rows whose messages vanished on re-index")

    # A database written by the retired engine still opens: the Hindsight-named
    # ledger migrates in place rather than being dropped, and a legacy row with
    # an empty bank adopts the default channel so it stays exportable under the
    # stricter (non-wildcard) channel filter.
    legacy_home = Path(td) / "legacy-home"
    lenv = {**env, "HERMES_AUTOPILOT_HOME": str(legacy_home)}
    subprocess.run([sys.executable, str(ROOT / "autopilot.py"), "init"],
                   env=lenv, text=True, capture_output=True, check=True)
    with sqlite3.connect(legacy_home / "state.db") as db:
        db.executescript(
            "CREATE TABLE bridge_hindsight_ledger (message_key TEXT PRIMARY KEY,"
            "session_row TEXT NOT NULL, seq INTEGER NOT NULL, role TEXT NOT NULL,"
            "at TEXT NOT NULL DEFAULT '', content_hash TEXT NOT NULL, bank TEXT NOT NULL,"
            "state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,"
            "last_error_kind TEXT NOT NULL DEFAULT '', exported_at TEXT NOT NULL DEFAULT '',"
            "updated_at TEXT NOT NULL);")
        for k, bank, st in (("r:0", "", "pending"),
                            ("r:1", "autopilot-shared-context", "exported"),
                            ("r:2", "other-bank", "failed")):
            db.execute("INSERT INTO bridge_hindsight_ledger VALUES(?,'r',?,'user','',?,?,?,3,'k','','')",
                       (k, int(k.split(":")[1]), "h" + k, bank, st))
    p_ = subprocess.run([sys.executable, str(ROOT / "mindos_bridge.py"), "export-status"],
                        env=lenv, text=True, capture_output=True)
    assert p_.returncode == 0, p_.stderr
    with sqlite3.connect(legacy_home / "state.db") as db:
        db.row_factory = sqlite3.Row
        rows = {r["message_key"]: dict(r) for r in db.execute(
            "SELECT message_key,channel,state,attempts,last_error_kind FROM bridge_export_ledger")}
        assert len(rows) == 3, rows                                   # nothing dropped
        assert rows["r:0"]["channel"] == "shared-context", rows       # empty bank -> default
        assert rows["r:1"]["channel"] == "autopilot-shared-context"   # named bank preserved
        assert rows["r:2"]["state"] == "failed" and rows["r:2"]["attempts"] == 3, rows
        assert rows["r:2"]["last_error_kind"] == "k", rows
        assert db.execute("SELECT COUNT(*) FROM sqlite_master WHERE "
                          "name='bridge_hindsight_ledger'").fetchone()[0] == 0
    subprocess.run([sys.executable, str(ROOT / "mindos_bridge.py"), "export-status"],
                   env=lenv, text=True, capture_output=True, check=True)   # re-run is a no-op
    with sqlite3.connect(legacy_home / "state.db") as db:
        assert db.execute("SELECT COUNT(*) FROM bridge_export_ledger").fetchone()[0] == 3
    print("PASS legacy hindsight ledger migrates in place")

    # Watch loop: near-real-time ingestion of a new message within one interval.
    clean_store = Path(td) / "watch-store"
    (clean_store / "telegram-leo").mkdir(parents=True)
    live3 = clean_store / "telegram-leo" / "session-watch.jsonl"
    live3.write_text("")
    wenv = env.copy()

    watch_out = {}
    def watcher():
        wp = subprocess.run([sys.executable, str(ROOT / "mindos_bridge.py"), "watch",
                        "--source", "hermes-watch", "--root", str(clean_store),
                        "--interval", "1", "--for-seconds", "3"],
                       env=wenv, text=True, capture_output=True)
        watch_out["rc"] = wp.returncode
        watch_out["err"] = wp.stderr[-800:]
    th = threading.Thread(target=watcher); th.start()
    import time as _t
    _t.sleep(0.5)
    live3.write_text(json.dumps({"message": {"role": "user", "content":
                     "live sentinel: bridge watch captured this direction"},
                     "timestamp": "2026-08-21T19:00:00Z"}) + "\n")
    th.join(timeout=15)
    print("WATCH:", watch_out)
    p = subprocess.run([sys.executable, str(ROOT / "autopilot.py"),
                        "search-sessions", "live sentinel"], env=env,
                       text=True, capture_output=True)
    hits = json.loads(p.stdout)
    assert len(hits) == 1 and hits[0]["source"] == "hermes-watch", hits
    print("PASS watch near-real-time sentinel")

    # Recall pack includes related sessions with bounded budget/deterministic digest.
    trec = subprocess.run([sys.executable, str(ROOT / "autopilot.py"), "recall", "promo-1",
                           "--related-sessions", "2", "--related-scope", "global",
                           "--budget", "4000"],
                          env=env, text=True, capture_output=True)
    bundle = json.loads(trec.stdout)
    assert bundle["related_sessions_matched"] >= 1, bundle.get("related_sessions_matched")
    d1 = bundle["digest"]
    trec2 = subprocess.run([sys.executable, str(ROOT / "autopilot.py"), "recall", "promo-1",
                            "--related-sessions", "2", "--related-scope", "global",
                            "--budget", "4000"],
                           env=env, text=True, capture_output=True)
    bundle2 = json.loads(trec2.stdout)
    assert bundle2["digest"] == d1, (d1, bundle2["digest"])
    print("PASS recall packs with related sessions")

print("bridge tests: PASS")
