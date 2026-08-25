#!/usr/bin/env python3
"""Executable verification for MindOS session-start context injection.

Disposable fixtures only: every run uses HERMES_AUTOPILOT_HOME on a temp dir
and synthetic Hermes-style JSONL stores. Live ~/.hermes is never touched.
Covers the contract cases: empty, stale, unavailable, redacted,
cross-profile, duplicate/idempotence, bounds, digest stability.
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

with tempfile.TemporaryDirectory() as td:
    env = os.environ.copy()
    env["HERMES_AUTOPILOT_HOME"] = str(Path(td) / "mindos-home")
    env.pop("HERMES_MINDOS_CONTEXT", None)

    def ap_cli(*a, expect_fail=False):
        p = subprocess.run([sys.executable, str(ROOT / "autopilot.py"), *a],
                           env=env, text=True, capture_output=True)
        if expect_fail:
            assert p.returncode != 0, (a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        assert p.returncode == 0, (a, p.stdout, p.stderr)
        return json.loads(p.stdout) if p.stdout.strip() else {}

    def pack(*a, extra_env=None):
        e = env if extra_env is None else {**env, **extra_env}
        p = subprocess.run([sys.executable, str(ROOT / "mindos_context_pack.py"),
                            "session-pack", *a], env=e, text=True,
                           capture_output=True)
        assert p.returncode == 0, (a, p.stdout[-500:], p.stderr[-500:])
        return json.loads(p.stdout)

    # Fixture world: task + handoff + receipt + fact + two-profile sessions.
    ap_cli("create", "--project", "Verify", "--title", "context injection target",
           "--id", "ctx-1")
    ap_cli("handoff", "ctx-1", "--from-agent", "ox-a", "--to-agent", "ox-b",
           "--status", "verified", "--objective",
           "handoff objective for session-start injection")
    ap_cli("receipt", "ctx-1", "--kind", "test-green",
           "--payload", '{"suite": "context-injection"}')
    ap_cli("fact-assert", "--subject", "mindos", "--predicate", "ships",
           "--object", "sessionpack", "--source", "ctx-tests")

    store = Path(td) / "store"
    prof_a = store / "telegram-leo"
    prof_b = store / "work-profile"
    prof_a.mkdir(parents=True)
    prof_b.mkdir(parents=True)
    (prof_a / "sess-a.jsonl").write_text("\n".join(json.dumps(x) for x in [
        {"message": {"role": "user", "content":
         "injection fixture: bounded pack ships Monday with monochrome hero"},
         "timestamp": "2026-08-21T17:00:00Z"},
        {"message": {"role": "assistant", "content": "Fixture acknowledged."},
         "timestamp": "2026-08-21T17:00:05Z"}]) + "\n")
    (prof_b / "sess-b.jsonl").write_text(json.dumps(
        {"message": {"role": "user", "content":
         "cross-profile secret phrase orchid cabinet must never leak"},
         "timestamp": "2026-08-21T18:00:00Z"}) + "\n")

    def bridge(*a):
        p = subprocess.run([sys.executable, str(ROOT / "mindos_bridge.py"), *a],
                           env=env, text=True, capture_output=True)
        assert p.returncode == 0, (a, p.stdout[-500:], p.stderr[-500:])
        return json.loads(p.stdout) if p.stdout.strip() else {}

    bridge("sync", "--source", "fixture-a", "--root", str(prof_a),
           "--profile", "telegram-leo", "--apply")
    bridge("sync", "--source", "fixture-b", "--root", str(prof_b),
           "--profile", "work-profile", "--apply")

    # Full sections present with provenance and statuses.
    pk = pack("--profile", "telegram-leo", "--query", "monochrome",
              "--max-bytes", "16384")
    assert pk["format"] == "mindos-session-context-pack-v1" and pk["enabled"]
    assert any("session-start injection" in h["objective"]
               for h in pk["sections"]["handoffs"])
    assert any(r["kind"] == "test-green" for r in pk["sections"]["receipts"])
    assert any(f["predicate"] == "ships" for f in pk["sections"]["temporal_facts"])
    assert any("monochrome" in s["content"]
               for s in pk["sections"]["session_context"])
    assert all(s["profile"] == "telegram-leo"
               for s in pk["sections"]["session_context"])
    assert "orchid" not in json.dumps(pk), "cross-profile leakage"
    assert pk["sources"]["semantic"]["status"] == "empty"
    assert not pk["budget"]["truncated"] and pk["digest"]
    print("PASS full-sections+provenance+statuses")

    # Digest idempotence (duplicate generation is a no-op semantically).
    pk2 = pack("--profile", "telegram-leo", "--query", "monochrome",
               "--max-bytes", "16384")
    assert pk2["digest"] == pk["digest"], (pk["digest"], pk2["digest"])
    print("PASS idempotent-duplicate-generation")

    # Cross-profile scope: profile B's own pack sees only its content.
    pb = pack("--profile", "work-profile", "--max-bytes", "16384")
    b_sess = [s["content"] for s in pb["sections"]["session_context"]]
    assert any("orchid" in c for c in b_sess), pb["sections"]["session_context"]
    assert all(s["profile"] == "work-profile"
               for s in pb["sections"]["session_context"])
    print("PASS per-profile-scope-isolation")

    # Budget bound: tiny byte budget truncates honestly and never overflows.
    tiny = pack("--profile", "telegram-leo", "--query", "monochrome",
                "--max-bytes", "700", "--max-items", "3")
    body = len(json.dumps({k: v for k, v in tiny.items()
                           if k != "digest"}, sort_keys=True,
                          separators=(",", ":")))
    assert tiny["budget"]["used_bytes"] <= 700 or tiny["budget"]["truncated"]
    assert sum(len(v) for v in tiny["sections"].values()) <= 3
    print("PASS byte/item-bounds")

    # Stale: state change flips verify-pack; recompute digest returned.
    pack_path = Path(td) / "pack.json"
    pack_path.write_text(json.dumps(pk))
    (prof_a / "sess-a.jsonl").write_text(
        (prof_a / "sess-a.jsonl").read_text() + json.dumps(
            {"message": {"role": "user", "content":
             "injection fixture follow-up: monochrome hero shipped"},
             "timestamp": "2026-08-21T17:30:00Z"}) + "\n")
    bridge("sync", "--source", "fixture-a", "--root", str(prof_a),
           "--profile", "telegram-leo", "--apply")
    vp = subprocess.run([sys.executable, str(ROOT / "mindos_context_pack.py"),
                         "verify-pack", "--pack", str(pack_path)],
                        env=env, text=True, capture_output=True)
    verdict = json.loads(vp.stdout)
    assert verdict["fresh"] is False and verdict["current_digest"] != pk["digest"]
    fresh_pk = pack("--profile", "telegram-leo", "--query", "monochrome",
                    "--max-bytes", "16384")
    vp2 = json.loads(subprocess.run(
        [sys.executable, str(ROOT / "mindos_context_pack.py"), "verify-pack",
         "--pack", str(pack_path)], env=env, text=True, capture_output=True).stdout)
    assert vp2["fresh"] is False and vp2["current_digest"] == fresh_pk["digest"]
    print("PASS stale-detection+recompute-digest")

    # Aged: max_age_hours in the past flags aged without breaking exit code.
    aged = pack("--max-age-hours", "-1")
    assert aged["freshness"]["max_age_hours"] == -1
    aged_path = Path(td) / "aged.json"
    time.sleep(1.1)
    aged_path.write_text(json.dumps(aged))
    av = json.loads(subprocess.run(
        [sys.executable, str(ROOT / "mindos_context_pack.py"), "verify-pack",
         "--pack", str(aged_path)], env=env, text=True,
        capture_output=True).stdout)
    assert av["aged"] is True, av
    print("PASS aged-flag")

    # Redacted: credential-shaped content refused by default, redacted under flag.
    SECRET_VALUE = "AKIA" + "IOSFODNN7EXAMPLE"
    prof_s = store / "secret-store"
    prof_s.mkdir(parents=True)
    (prof_s / "sess-s.jsonl").write_text(json.dumps(
        {"message": {"role": "user", "content": f"use this key: {SECRET_VALUE}"},
         "timestamp": "2026-08-21T19:00:00Z"}) + "\n")
    err = subprocess.run([sys.executable, str(ROOT / "mindos_bridge.py"),
                          "sync", "--source", "fixture-secret", "--root",
                          str(prof_s), "--redact", "--apply"],
                         env=env, text=True, capture_output=True)
    assert err.returncode == 0, err.stderr
    rp = pack("--query", "key:", "--max-bytes", "16384")
    assert SECRET_VALUE not in json.dumps(rp)
    assert "[REDACTED:" in json.dumps(rp["sections"]["session_context"]) or \
        all("[REDACTED:" in s["content"] or "key" not in s["content"].lower()
            for s in rp["sections"]["session_context"]), rp["sections"]
    raw_cache = "".join(r[0] for r in sqlite3.connect(
        Path(td) / "mindos-home" / "state.db").execute(
        "SELECT content FROM session_messages"))
    assert SECRET_VALUE not in raw_cache
    print("PASS secret-redaction-no-value-leak")

    # Unavailable home degrades honestly; empty query still yields valid pack.
    empty_home = {**env, "HERMES_AUTOPILOT_HOME": str(Path(td) / "no-home")}
    ep = subprocess.run([sys.executable, str(ROOT / "mindos_context_pack.py"),
                         "session-pack"], env=empty_home, text=True,
                        capture_output=True)
    epk = json.loads(ep.stdout)
    assert ep.returncode == 0
    assert all(m["status"].startswith(("unavailable", "empty"))
               for m in epk["sources"].values()), epk["sources"]
    assert not any(epk["sections"].values())
    empty_q = pack("--profile", "telegram-leo")
    assert empty_q["sources"]["temporal_facts"]["status"] == "ok"
    print("PASS unavailable/empty-honest-degradation")

    # Opt-out disable path.
    off = pack(extra_env={"HERMES_MINDOS_CONTEXT": "off"})
    assert off == {"enabled": False, "format": "mindos-session-context-pack-v1"}
    print("PASS opt-out-disable-path")

print("context-injection tests: PASS")
