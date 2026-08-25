#!/usr/bin/env python3
"""Executable verification for the native Hermes SQLite session adapter.

Disposable fixtures only: a synthetic Hermes-shaped state.db (same sessions/
messages schema) is built in a temp dir; HERMES_HOME and HERMES_AUTOPILOT_HOME
point there. The live ~/.hermes store is opened read-only at most for a
shape-check and is never written.

Proves:
1. sqlite-sync ingests a disposable sentinel session end-to-end into
   sessions/session_messages with provenance
2. re-run is idempotent (unchanged stream => no-op, no duplicate rows)
3. a changed session re-indexes and flips ledger rows back to pending
4. secret guard: refuse by default, --redact stores [REDACTED:*] and the raw
   value never reaches cache or export manifest
5. tool rows / compacted / inactive rows are skipped, never cached
6. HERMES_HOME isolation: adapter resolves state.db from HERMES_HOME
7. missing session_id fails honestly; JSONL fixture path still works
   (regression) and the gateway hook prefers sqlite when a sid is present
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).parent
BRIDGE = ROOT / "mindos_bridge.py"
HOOK = ROOT / "mindos_gateway_hook.py"

HERMES_SCHEMA_SESSIONS = """
CREATE TABLE sessions (
  id TEXT PRIMARY KEY, source TEXT NOT NULL, profile_name TEXT,
  started_at REAL NOT NULL, ended_at REAL
);
"""
HERMES_SCHEMA_MESSAGES = """
CREATE TABLE messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  role TEXT NOT NULL, content TEXT, timestamp REAL NOT NULL,
  active INTEGER NOT NULL DEFAULT 1, compacted INTEGER NOT NULL DEFAULT 0
);
"""


def make_hermes_home(td: Path) -> Path:
    """Synthetic Hermes home with a state.db shaped like the live schema."""
    home = td / "hermes-home"
    home.mkdir()
    db = sqlite3.connect(home / "state.db")
    db.executescript(HERMES_SCHEMA_SESSIONS + HERMES_SCHEMA_MESSAGES)
    t0 = 1787370000.0
    db.execute("INSERT INTO sessions VALUES (?,?,?,?,?)",
               ("sess_sentinel_1", "cli", "default", t0, t0 + 60))
    msgs = [
        ("user", "SQLITE-SENTINEL: mindos website launch is Monday", t0 + 1, 1, 0),
        ("assistant", "On it — dark monochrome landing page queued.", t0 + 2, 1, 0),
        ("assistant", None, t0 + 3, 1, 0),          # tool-call-only turn: skip
        ("tool", "stdout noise", t0 + 4, 1, 0),     # tool row: skip
        ("user", "compacted away", t0 + 5, 1, 1),   # compacted: skip
        ("user", "inactive row", t0 + 6, 0, 0),     # inactive: skip
        ("assistant", "On it — dark monochrome landing page queued.", t0 + 2, 1, 0),  # dup collapse
    ]
    db.executemany(
        "INSERT INTO messages(session_id,role,content,timestamp,active,compacted) "
        "VALUES (?,?,?,?,?,?)",
        [("sess_sentinel_1", r, c, ts, a, cp) for r, c, ts, a, cp in msgs])
    db.commit()
    db.close()
    return home


def bridge(env, *a, expect_fail=False):
    """Run mindos_bridge.py; returns parsed JSON dict (or error text on expected failure)."""
    p = subprocess.run([sys.executable, str(BRIDGE), *a], env=env,
                       text=True, capture_output=True, timeout=60)
    if expect_fail:
        assert p.returncode != 0, ("expected failure", a, p.stdout)
        return p.stderr + p.stdout
    assert p.returncode == 0, (a, p.stdout, p.stderr)
    out = json.loads(p.stdout)
    assert isinstance(out, dict)
    return out


with tempfile.TemporaryDirectory() as td:
    tdp = Path(td)
    hermes_home = make_hermes_home(tdp)
    mindos_home = tdp / "mindos-home"

    env = os.environ.copy()
    env["HERMES_HOME"] = str(hermes_home)
    env["HERMES_AUTOPILOT_HOME"] = str(mindos_home)

    def counts():
        with sqlite3.connect(mindos_home / "state.db") as db:
            s = db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            m = db.execute("SELECT COUNT(*) FROM session_messages").fetchone()[0]
            pend = db.execute(
                "SELECT COUNT(*) FROM bridge_export_ledger WHERE state='pending'"
            ).fetchone()[0]
            return s, m, pend

    # -- 1. sentinel ingest end-to-end ----------------------------------------
    r = bridge(env, "sqlite-sync", "--sqlite-session-id", "sess_sentinel_1",
               "--profile", "default", "--project", "mindos",
               "--channel", "shared-context", "--redact")
    assert r["status"] == "indexed" and r["messages"] == 2, r
    assert r["adapter"] == "hermes-sqlite" and r["applied"] is True
    s, m, pend = counts()
    assert s == 1 and m == 2 and pend == 2, (s, m, pend)
    with sqlite3.connect(mindos_home / "state.db") as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT source,profile,session_id,path FROM sessions").fetchone()
        assert row["source"] == "hermes-sqlite" and row["profile"] == "default"
        assert row["session_id"] == "sess_sentinel_1"
        assert row["path"].startswith("sqlite:")
        contents = [r2[0] for r2 in db.execute(
            "SELECT content FROM session_messages ORDER BY seq")]
    assert any("SQLITE-SENTINEL" in c for c in contents)
    assert not any("compacted away" in c or "inactive row" in c or
                   "stdout noise" in c for c in contents)
    print("PASS sqlite sentinel ingested with provenance; non-conversational rows skipped")

    # -- 2. idempotent re-run --------------------------------------------------
    r2 = bridge(env, "sqlite-sync", "--sqlite-session-id", "sess_sentinel_1",
                "--profile", "default", "--project", "mindos",
                "--channel", "shared-context", "--redact")
    assert r2.get("unchanged") is True and r2.get("messages") == 2, r2
    assert counts() == (1, 2, 2), "idempotent re-run must not add rows"
    print("PASS re-run idempotent (unchanged stream skipped)")

    # -- 3. changed session re-indexes; ledger flips back to pending -----------
    db = sqlite3.connect(hermes_home / "state.db")
    db.execute("INSERT INTO messages(session_id,role,content,timestamp,active,compacted) "
               "VALUES ('sess_sentinel_1','user','follow-up: ship the bridge',?,1,0)",
               (1787370100.0,))
    db.commit(); db.close()
    r3 = bridge(env, "sqlite-sync", "--sqlite-session-id", "sess_sentinel_1",
                "--profile", "default", "--project", "mindos",
                "--channel", "shared-context", "--redact")
    assert r3["applied"] is True and r3["messages"] == 3, r3
    s, m, pend = counts()
    assert (s, m, pend) == (1, 3, 3), (s, m, pend)
    print("PASS changed session re-indexed; ledger rows back to pending")

    # Export manifest honors pending rows (honest pending/export semantics).
    out = mindos_home / "bridge-exports" / "latest.jsonl"
    ex = bridge(env, "export", "--out", str(out), "--channel", "shared-context")
    assert ex["messages"] == 3, ex
    recs = [json.loads(l) for l in out.read_text().splitlines()]
    assert recs[0]["provenance"]["source"] == "hermes-sqlite"
    assert recs[0]["provenance"]["session_id"] == "sess_sentinel_1"
    s, m, pend = counts()
    assert pend == 0, "export must mark rows exported"
    print("PASS provenance export manifest; pending -> exported (GET-only honesty)")

    # -- 4. secret guard -------------------------------------------------------
    SECRET_VALUE = "AKIA" + "IOSFODNN7EXAMPLE"
    db = sqlite3.connect(hermes_home / "state.db")
    db.execute("INSERT INTO sessions VALUES (?,?,?,?,?)",
               ("sess_secret_1", "cli", "default", 1787370200.0,
                1787370260.0))
    db.execute("INSERT INTO messages(session_id,role,content,timestamp,active,compacted) "
               "VALUES ('sess_secret_1','user','use key: " + SECRET_VALUE + "',?,1,0)",
               (1787370201.0,))
    db.commit(); db.close()
    err = bridge(env, "sqlite-sync", "--sqlite-session-id", "sess_secret_1",
                 expect_fail=True)
    assert "refusing" in err and "credential" in err, err
    with sqlite3.connect(mindos_home / "state.db") as db:
        n = db.execute("SELECT COUNT(*) FROM sessions WHERE session_id='sess_secret_1'"
                       ).fetchone()[0]
    assert n == 0, "refusal must not write cache"
    r4 = bridge(env, "sqlite-sync", "--sqlite-session-id", "sess_secret_1", "--redact")
    assert r4["messages"] == 1 and r4["secret_kinds"], r4
    with sqlite3.connect(mindos_home / "state.db") as db:
        cached = "".join(r[0] for r in db.execute(
            "SELECT content FROM session_messages WHERE session_row=?",
            (r4["row_id"],)))
    assert SECRET_VALUE not in cached and "[REDACTED:" in cached
    bridge(env, "export", "--out", str(out), "--channel", "shared-context")
    exp = out.read_text()
    assert SECRET_VALUE not in cached + exp, "raw secret leaked"
    print("PASS secret guard: refuse by default, redact stores [REDACTED:*], no leakage")

    # -- 5. HERMES_HOME isolation ----------------------------------------------
    env_b = env.copy()
    env_b["HERMES_HOME"] = str(tdp / "empty-hermes-home")
    (tdp / "empty-hermes-home").mkdir()
    err = bridge(env_b, "sqlite-sync", "--sqlite-session-id", "sess_sentinel_1",
                 expect_fail=True)
    assert "not found" in err, err  # state.db missing in that home
    print("PASS state.db resolved from HERMES_HOME (profile isolation)")

    # -- 6. missing session fails honestly -------------------------------------
    err = bridge(env, "sqlite-sync", "--sqlite-session-id", "sess_nope",
                 expect_fail=True)
    assert "not found" in err, err
    print("PASS unknown session_id fails honestly")

    # -- 7. JSONL fixture path unregressed + hook prefers sqlite ----------------
    store = tdp / "session-store" / "gateway-leo"
    store.mkdir(parents=True)
    (store / "sess-jsonl.jsonl").write_text(json.dumps({
        "message": {"role": "user", "content": "JSONL-SENTINEL still works"},
        "timestamp": "2026-08-21T21:00:00Z"}) + "\n")
    rj = bridge(env, "sync", "--source", "hermes-gateway", "--root",
                str(store.parent), "--apply", "--redact")
    assert rj["totals"]["indexable"] == 1, rj
    print("PASS JSONL fixture path unregressed")

    hook_env = env.copy()
    hook_env["HERMES_AUTOPILOT_HOME"] = str(tdp / "hook-mindos-home")
    p = subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps({"session_id": "sess_sentinel_1"}), env=hook_env,
        text=True, capture_output=True, timeout=30)
    assert p.returncode == 0, p.stderr
    cfg_home = Path(hook_env["HERMES_HOME"])
    (cfg_home / "config.yaml").write_text(
        "mindos_bridge:\n  enabled: true\n  profile: default\n"
        "  channel: shared-context\n  redact: true\n")
    p = subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps({"session_id": "sess_sentinel_1"}), env=hook_env,
        text=True, capture_output=True, timeout=30)
    assert p.returncode == 0, p.stderr
    deadline = time.time() + 30
    hook_db = Path(hook_env["HERMES_AUTOPILOT_HOME"]) / "state.db"
    while time.time() < deadline:
        if hook_db.exists():
            with sqlite3.connect(hook_db) as db:
                if db.execute("SELECT COUNT(*) FROM session_messages").fetchone()[0]:
                    break
        time.sleep(0.5)
    with sqlite3.connect(hook_db) as db:
        src = db.execute("SELECT DISTINCT source FROM sessions").fetchall()
        n = db.execute("SELECT COUNT(*) FROM session_messages").fetchone()[0]
    assert [r[0] for r in src] == ["hermes-sqlite"] and n == 3, (src, n)
    print("PASS gateway hook ingests live session via sqlite adapter (session_id path)")

print("sqlite adapter tests: PASS")
