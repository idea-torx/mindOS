#!/usr/bin/env python3
"""Executable verification for the session-start context integration.

Proves the missing integration piece: mindos_gateway_hook.py, wired as a
Hermes `pre_llm_call` shell hook with context_pack enabled, makes the sealed
MindOS pack available through the HOST context path (ephemeral plugin-context
injection into the first turn's user message) — with no synthetic messages
after turn 1 (empty stdout on continuation turns), no system-prompt rewriting,
no live-state writes (pack generation is read-only; no --out is passed), and
the HERMES_MINDOS_CONTEXT off switch + profile isolation preserved.

Disposable fixtures only: HERMES_AUTOPILOT_HOME and HERMES_HOME on temp dirs.
Live ~/.hermes, live gateway, rollback: never touched.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).parent
HOOK = ROOT / "mindos_gateway_hook.py"
PACK_FORMAT = "mindos-session-context-pack-v1"


def pre_llm(env, first_turn=True, timeout=30):
    payload = {"hook_event_name": "pre_llm_call", "session_id": "sess_ctx",
               "cwd": str(ROOT),
               "extra": {"is_first_turn": first_turn, "model": "test-model",
                         "platform": "cli"}}
    ts = time.time()
    p = subprocess.run([sys.executable, str(HOOK)], input=json.dumps(payload),
                       env=env, text=True, capture_output=True, timeout=timeout)
    return p, time.time() - ts


def parse_context(p):
    """Return the injected context string, or None for a silent no-op."""
    if not p.stdout.strip():
        return None
    data = json.loads(p.stdout)
    assert isinstance(data, dict) and isinstance(data.get("context"), str), p.stdout
    return data["context"]


with tempfile.TemporaryDirectory() as td:
    mindos_home = Path(td) / "mindos-home"          # HERMES_AUTOPILOT_HOME
    hermes_home = Path(td) / "hermes-home"          # HERMES_HOME
    hermes_home.mkdir(parents=True)

    env = os.environ.copy()
    env["HERMES_AUTOPILOT_HOME"] = str(mindos_home)
    env["HERMES_HOME"] = str(hermes_home)
    env.pop("HERMES_MINDOS_CONTEXT", None)

    def write_cfg(block: dict):
        lines = ["mindos_bridge:"]
        for k, v in block.items():
            if isinstance(v, bool):
                v = "true" if v else "false"
            lines.append(f"  {k}: {v}")
        (hermes_home / "config.yaml").write_text("\n".join(lines) + "\n")

    def bridge(*a):
        p = subprocess.run([sys.executable, str(ROOT / "mindos_bridge.py"), *a],
                           env=env, text=True, capture_output=True)
        assert p.returncode == 0, (a, p.stdout[-500:], p.stderr[-500:])
        return p

    # Fixture world: ingest two profiles through the real bridge (synchronous,
    # so the DB is populated before any hook invocation).
    store = Path(td) / "store"
    prof_a, prof_b = store / "telegram-leo", store / "work-profile"
    prof_a.mkdir(parents=True)
    prof_b.mkdir(parents=True)
    SENTINEL_A = "ctx-integration sentinel: monochrome page ships Monday"
    SENTINEL_B = "profile B private orchid cabinet note must never leak"
    (prof_a / "sess-a.jsonl").write_text(json.dumps(
        {"message": {"role": "user", "content": SENTINEL_A},
         "timestamp": "2026-08-21T17:00:00Z"}) + "\n")
    (prof_b / "sess-b.jsonl").write_text(json.dumps(
        {"message": {"role": "user", "content": SENTINEL_B},
         "timestamp": "2026-08-21T18:00:00Z"}) + "\n")
    bridge("sync", "--source", "hermes-ctx-int", "--root", str(prof_a),
           "--profile", "telegram-leo", "--apply")
    bridge("sync", "--source", "hermes-ctx-int-b", "--root", str(prof_b),
           "--profile", "work-profile", "--apply")
    db_before = (mindos_home / "state.db").read_bytes()

    # -- 1. Opt-in off by default: first turn is an instant silent no-op -------
    write_cfg({"enabled": "true"})
    p, dt = pre_llm(env)
    assert p.returncode == 0 and not p.stdout.strip() and dt < 2.0, (p.stdout, dt)
    print("PASS opt-out-by-default instant no-op")

    # -- 2. Enabled: first turn injects the pack through the host channel ------
    write_cfg({"enabled": "true", "context_pack": "true",
               "profile": "telegram-leo", "project": "",
               "context_pack_max_bytes": "8192",
               "context_pack_seconds": "20"})
    p, dt = pre_llm(env)
    assert p.returncode == 0, p.stderr
    ctx = parse_context(p)
    assert ctx is not None and dt < 25, dt
    assert SENTINEL_A in ctx, "ingested session context must reach the host path"
    assert "# MindOS session context" in ctx
    assert f"format: {PACK_FORMAT}" in ctx and "digest:" in ctx
    assert "scope: profile=telegram-leo" in ctx
    assert "## session_context status=ok" in ctx
    print("PASS first-turn pack via host context channel")

    # -- 3. Continuation turns are silent no-ops --------------------------------
    p, dt = pre_llm(env, first_turn=False)
    assert p.returncode == 0 and not p.stdout.strip() and dt < 2.0, (p.stdout, dt)
    print("PASS continuation-turn silent no-op (no per-turn rewriting)")

    # -- 4. Profile isolation across the host path ------------------------------
    assert SENTINEL_B not in ctx, "cross-profile leakage through hook"
    write_cfg({"enabled": "true", "context_pack": "true",
               "profile": "work-profile", "context_pack_max_bytes": "8192"})
    pb, _ = pre_llm(env)
    ctx_b = parse_context(pb)
    assert ctx_b and SENTINEL_B in ctx_b and SENTINEL_A not in ctx_b
    write_cfg({"enabled": "true", "context_pack": "true",
               "profile": "telegram-leo", "context_pack_max_bytes": "8192"})
    print("PASS profile isolation through host path")

    # -- 5. Read-only guarantee: no live-state writes ---------------------------
    p, _ = pre_llm(env)
    parse_context(p)
    assert (mindos_home / "state.db").read_bytes() == db_before, \
        "session-start path must not write live state"
    assert not any(mindos_home.rglob("*.tmp")), "stray temp writes"
    print("PASS zero live-state writes on the injection path")

    # -- 6. HERMES_MINDOS_CONTEXT=off kills injection end-to-end ----------------
    env_off = {**env, "HERMES_MINDOS_CONTEXT": "off"}
    p, _ = pre_llm(env_off)
    assert p.returncode == 0 and not p.stdout.strip(), p.stdout
    print("PASS HERMES_MINDOS_CONTEXT=off end-to-end kill switch")

    # -- 7. Fail-open: unavailable MindOS home cannot block the reply path ------
    write_cfg({"enabled": "true", "context_pack": "true",
               "profile": "telegram-leo", "context_pack_seconds": "10"})
    env_dead = os.environ.copy()
    env_dead["HERMES_AUTOPILOT_HOME"] = str(Path(td) / "no-such-home")
    env_dead["HERMES_HOME"] = str(hermes_home)
    env_dead.pop("HERMES_MINDOS_CONTEXT", None)
    p, dt = pre_llm(env_dead)
    assert p.returncode == 0 and dt < 15, (p.returncode, dt)
    dead_ctx = parse_context(p)
    if dead_ctx is not None:
        # Honest degradation only: statuses visible, no fabricated content.
        assert "status=unavailable" in dead_ctx or "status=empty" in dead_ctx
        assert SENTINEL_A not in dead_ctx
    print("PASS fail-open degradation (reply path unblocked)")

    # -- 8. Secret guard holds on the host path ---------------------------------
    SECRET_VALUE = "AKIA" + "IOSFODNN7EXAMPLE"
    prof_s = store / "secret-store"
    prof_s.mkdir(parents=True)
    (prof_s / "sess-s.jsonl").write_text(json.dumps(
        {"message": {"role": "user", "content": f"use this key: {SECRET_VALUE}"},
         "timestamp": "2026-08-21T19:00:00Z"}) + "\n")
    bridge("sync", "--source", "hermes-ctx-int-s", "--root", str(prof_s),
           "--redact", "--apply")
    p, _ = pre_llm(env)
    ctx_s = parse_context(p)
    assert SECRET_VALUE not in p.stdout and SECRET_VALUE not in p.stderr
    assert ctx_s is None or SECRET_VALUE not in ctx_s
    if ctx_s is not None:
        assert "[REDACTED:" in ctx_s or "refused-secret-items" in ctx_s or \
            "key" not in ctx_s.lower()
    print("PASS secret guard on host path (no raw value leaves the process)")

    # -- 9. Ingest path unchanged by the new branch ------------------------------
    write_cfg({"enabled": "true", "root": str(store.parent),
               "source": "hermes-gateway", "worker_seconds": "30"})
    p = subprocess.run([sys.executable, str(HOOK)],
                       input=json.dumps({"session_id": "sess_end"}),
                       env=env, text=True, capture_output=True, timeout=10)
    assert p.returncode == 0, p.stderr
    print("PASS on_session_end ingest path unchanged")

print("context integration tests: PASS")
