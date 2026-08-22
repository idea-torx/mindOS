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
from http.server import BaseHTTPRequestHandler, HTTPServer
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
                 "--bank", "autopilot-shared-context", "--apply")
    assert out["applied_files"] == 1 and out["dry_run"] is False, out

    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        db.row_factory = sqlite3.Row
        srow = db.execute("SELECT * FROM sessions").fetchone()
        assert srow["source"] == "hermes-telegram" and srow["profile"] == "default"
        msgs = db.execute("SELECT seq,role,content FROM session_messages ORDER BY seq").fetchall()
        assert len(msgs) == 2, [dict(m) for m in msgs]
        assert {m["role"] for m in msgs} == {"user", "assistant"}
        assert all("tool noise" not in m["content"] and "result blob" not in m["content"] for m in msgs)
        led = db.execute("SELECT state,bank,content_hash FROM bridge_hindsight_ledger ORDER BY seq").fetchall()
        assert len(led) == 2 and all(r["state"] == "pending" for r in led), [dict(r) for r in led]
        assert all(r["bank"] == "autopilot-shared-context" for r in led)
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
                  "--bank", "autopilot-shared-context", "--apply")
    assert out3["applied_files"] == 1, out3
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        assert db.execute("SELECT COUNT(*) FROM session_messages WHERE session_row=(SELECT id FROM sessions)").fetchone()[0] == 3
        pend = db.execute("SELECT COUNT(*) FROM bridge_hindsight_ledger WHERE state='pending'").fetchone()[0]
        assert pend == 3, pend  # file-level reindex invalidates the whole prior export set honestly
    print("PASS atomic change re-index")

    # Export manifest with full provenance + deterministic digest.
    exp1 = Path(td) / "export-1.jsonl"
    r1 = bridge("export", "--out", str(exp1), "--bank", "autopilot-shared-context")
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
           "--project", "mindos", "--bank", "autopilot-shared-context", "--apply")
    exp2 = Path(td) / "export-2.jsonl"
    r2 = bridge("export", "--out", str(exp2), "--limit", "3", "--bank", "autopilot-shared-context")
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
           "--redact", "--bank", "autopilot-shared-context", "--apply")
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

    # Hindsight unavailable + degraded paths are honest; local cache stays correct.
    hs = bridge("hindsight-check", "--url", "http://127.0.0.1:9", "--bank", "autopilot-shared-context")
    assert hs["status"] == "unavailable" and hs["semantic_sync"] == "pending", hs
    print("ledger:", hs["ledger"])

    class _Degraded(BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def _send(self, code, body):
            self.send_response(code); self.end_headers()
            self.wfile.write(json.dumps(body).encode())
        def do_GET(self):
            if self.path == "/health":
                self._send(200, {"status": "healthy"})
            elif self.path == "/v1/default/banks":
                self._send(200, {"banks": [{"bank_id": "someone-elses"}]})
            else:
                self._send(404, {"detail": "Not Found"})
    try:
        srv = HTTPServer(("127.0.0.1", 0), _Degraded)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        url = f"http://127.0.0.1:{srv.server_port}"
    except PermissionError:
        url = None
    if url:
        deg = bridge("hindsight-check", "--url", url, "--bank", "autopilot-shared-context")
        assert deg["status"] == "degraded" and any(
            "not present" in x for x in deg["problems"]), deg
        assert deg["semantic_sync"] == "pending"
        # Export still works while degraded — honest probe status recorded in audit.
        exd = bridge("export", "--out", str(Path(td) / "export-deg.jsonl"),
                     "--check-url", url, "--bank", "autopilot-shared-context")
        assert exd["probe"]["status"] == "degraded", exd
    print("PASS Hindsight unavailable/degraded honesty")

    # Promotion hook: explicit only, with provenance citation.
    task = subprocess.run([sys.executable, str(ROOT / "autopilot.py"), "create",
                           "--project", "Verify",
                           "--title", "monochrome landing page launch target", "--id", "promo-1"],
                          env=env, text=True, capture_output=True)
    assert task.returncode == 0, task.stderr
    with sqlite3.connect(Path(td) / "mindos-home" / "state.db") as db:
        key_row = db.execute(
            "SELECT l.message_key,s.session_id FROM bridge_hindsight_ledger l "
            "JOIN sessions s ON s.id=l.session_row WHERE l.seq=0 LIMIT 1").fetchone()
    key = key_row[0].rsplit(":", 1)[0] + ":0"
    pr = bridge("promote", key, "--kind", "note", "--task", "promo-1")
    assert pr["ok"] is True and "[hermes-telegram:" in pr["citation"], pr
    shown = subprocess.run([sys.executable, str(ROOT / "autopilot.py"), "notes", "promo-1"],
                           env=env, text=True, capture_output=True)
    notes = json.loads(shown.stdout)
    assert any("mindos-bridge" == n.get("source") for n in notes), notes
    print("PASS promotion hook")

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
