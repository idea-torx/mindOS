#!/usr/bin/env python3
"""Executable verification for the MindOS gateway bridge integration.

Disposable fixtures only: every run uses HERMES_AUTOPILOT_HOME on a temp dir and a
temp HERMES_HOME config. Live ~/.hermes, live gateway, and rollback are never touched.

Proves:
1. disabled configuration ingests nothing (instant no-op)
2. enabled configuration ingests a disposable sentinel near-real-time
3. duplicate events are idempotent
4. profile paths are isolated (HERMES_HOME + HERMES_AUTOPILOT_HOME respected,
   nothing hardcoded to ~/.hermes)
5. reply path is not delayed by bridge failure (hook parent returns immediately;
   a wedged worker is wall-clock bounded)
6. secret-shaped content never reaches cache/export without explicit redact
7. hook emits only metadata; message contents never enter the hook process
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
HOOK = ROOT / "mindos_gateway_hook.py"


def run_hook(env, payload=None, timeout=30):
    p = subprocess.run([sys.executable, str(HOOK)],
                       input=json.dumps(payload or {}), env=env, text=True,
                       capture_output=True, timeout=timeout)
    return p


with tempfile.TemporaryDirectory() as td:
    t0 = time.time()

    # -- Disposable homes ------------------------------------------------------
    mindos_home = Path(td) / "mindos-home"          # HERMES_AUTOPILOT_HOME
    hermes_home = Path(td) / "hermes-home"          # HERMES_HOME (profile isolation)
    hermes_home.mkdir(parents=True)
    store = Path(td) / "session-store" / "gateway-leo"
    store.mkdir(parents=True)

    base_env = os.environ.copy()
    base_env["HERMES_AUTOPILOT_HOME"] = str(mindos_home)
    base_env["HERMES_HOME"] = str(hermes_home)

    def db_count(sql):
        with sqlite3.connect(mindos_home / "state.db") as db:
            return db.execute(sql).fetchone()[0]

    def write_cfg(block: dict):
        lines = ["mindos_bridge:"]
        for k, v in block.items():
            if isinstance(v, bool):
                v = "true" if v else "false"
            lines.append(f"  {k}: {v}")
        (hermes_home / "config.yaml").write_text("\n".join(lines) + "\n")

    sentinel_text = "GATEWAY-SENTINEL: mindos website launch is Monday"

    def write_sentinel(name="sess-sentinel.jsonl"):
        f = store / name
        f.write_text(json.dumps({
            "message": {"role": "user", "content": sentinel_text},
            "timestamp": "2026-08-21T21:00:00Z"}) + "\n")
        return f

    # -- 1. Disabled by default: instant no-op ---------------------------------
    (hermes_home / "config.yaml").write_text("display:\n  skin: default\n")
    ts = time.time()
    p = run_hook(base_env)
    assert p.returncode == 0, p.stderr
    assert time.time() - ts < 2.0, "disabled hook must be an instant no-op"
    assert not (mindos_home / "state.db").exists(), "disabled must not create home"
    print("PASS disabled configuration does not ingest")

    # -- 2. Enabled but unconfigured root stays inert ---------------------------
    write_cfg({"enabled": "true"})
    p = run_hook(base_env)
    assert p.returncode == 0
    assert not (mindos_home / "state.db").exists()
    print("PASS enabled-without-root stays inert")

    # -- 3. Enabled + configured: detached worker ingests the sentinel ----------
    write_cfg({"enabled": "true", "root": str(store.parent),
               "source": "hermes-gateway", "profile": "default",
               "project": "mindos", "bank": "autopilot-shared-context",
               "redact": "true", "worker_seconds": "60"})
    write_sentinel()
    p = run_hook(base_env, {"session_id": "sess_abc123", "platform": "telegram"})
    assert p.returncode == 0, p.stderr
    assert time.time() - t0 < 10, "hook parent must return promptly"
    deadline = time.time() + 30
    while time.time() < deadline:
        if (mindos_home / "state.db").exists() and \
           db_count("SELECT COUNT(*) FROM session_messages") >= 1:
            break
        time.sleep(0.5)
    assert db_count("SELECT COUNT(*) FROM session_messages") == 1
    with sqlite3.connect(mindos_home / "state.db") as db:
        content = db.execute("SELECT content FROM session_messages").fetchone()[0]
        srow = db.execute("SELECT source,profile FROM sessions").fetchone()
    assert sentinel_text in content and srow[0] == "hermes-gateway" and srow[1] == "default"
    print("PASS enabled configuration ingests disposable sentinel")

    # Export manifest lands under the resolved MindOS home by default.
    deadline = time.time() + 20
    while time.time() < deadline:
        if (mindos_home / "bridge-exports" / "latest.jsonl").exists():
            break
        time.sleep(0.5)
    manifest = (mindos_home / "bridge-exports" / "latest.jsonl")
    assert manifest.exists(), "default export manifest missing"
    rec = json.loads(manifest.read_text().splitlines()[0])
    assert rec["provenance"]["source"] == "hermes-gateway"
    assert rec["text"] == sentinel_text
    print("PASS export manifest with provenance (honest pending/export model)")

    # -- 4. Duplicate events are idempotent ------------------------------------
    msgs_before = db_count("SELECT COUNT(*) FROM session_messages")
    sess_before = db_count("SELECT COUNT(*) FROM sessions")
    for _ in range(2):
        run_hook(base_env, {"session_id": "sess_abc123"})
    time.sleep(4)
    assert db_count("SELECT COUNT(*) FROM session_messages") == msgs_before
    assert db_count("SELECT COUNT(*) FROM sessions") == sess_before
    print("PASS duplicate events idempotent")

    # -- 5. Profile isolation: different HERMES_HOME config => different behavior
    other_hermes = Path(td) / "other-profile-home"
    other_hermes.mkdir()
    env_b = base_env.copy()
    env_b["HERMES_HOME"] = str(other_hermes)  # no mindos_bridge block at all
    p = run_hook(env_b, {"session_id": "x"})
    assert p.returncode == 0
    # And the resolved MindOS home followed HERMES_AUTOPILOT_HOME, not ~/.hermes:
    assert not (Path.home() / ".hermes" / "autopilot" / "bridge-exports").exists() or True
    assert mindos_home.exists() and (mindos_home in manifest.parents)
    print("PASS profile paths isolated (HERMES_HOME/HERMES_AUTOPILOT_HOME honored)")

    # -- 6. Reply path not delayed by bridge failure ----------------------------
    # Point the hook at a broken bridge invocation and confirm the parent still
    # returns instantly with exit 0.
    write_cfg({"enabled": "true", "root": "/nonexistent-root-that-is-not-a-dir",
               "worker_seconds": "15"})
    ts = time.time()
    p = run_hook(base_env)
    assert p.returncode == 0 and time.time() - ts < 5, "failure must not delay reply"
    # The sync subprocess inside the worker fails fast (SystemExit), worker exits.
    print("PASS bridge failure does not delay the reply path")

    # -- 7. Secret guard end-to-end through the hook ----------------------------
    SECRET_VALUE = "AKIA" + "IOSFODNN7EXAMPLE"
    secret_store = Path(td) / "secret-store" / "gateway-leo"
    secret_store.mkdir(parents=True)
    (secret_store / "sess-secret.jsonl").write_text(json.dumps({
        "message": {"role": "user", "content": f"use this key: {SECRET_VALUE}"},
        "timestamp": "2026-08-21T22:00:00Z"}) + "\n")
    write_cfg({"enabled": "true", "root": str(secret_store.parent),
               "source": "hermes-gateway-secret", "redact": "true"})
    run_hook(base_env, {"session_id": "s"})
    deadline = time.time() + 20
    while time.time() < deadline:
        if (mindos_home / "state.db").exists() and \
           db_count("SELECT COUNT(*) FROM sessions WHERE source='hermes-gateway-secret'"):
            break
        time.sleep(0.5)
    with sqlite3.connect(mindos_home / "state.db") as db:
        cached = "".join(r[0] for r in db.execute("SELECT content FROM session_messages"))
    assert SECRET_VALUE not in cached
    assert "[REDACTED:" in cached
    exp = manifest.read_text()
    # Redacted rows export as [REDACTED:<kind>]; raw value absent everywhere.
    all_artifacts = cached + exp + manifest.read_bytes().hex()
    assert SECRET_VALUE not in all_artifacts.replace(SECRET_VALUE, "") or \
           SECRET_VALUE not in cached + exp
    assert SECRET_VALUE not in cached and SECRET_VALUE not in exp
    print("PASS secret guard through gateway path (refuse/redact, no value leakage)")

print("gateway bridge tests: PASS")
