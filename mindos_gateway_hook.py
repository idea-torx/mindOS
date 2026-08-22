#!/usr/bin/env python3
"""MindOS gateway bridge — on_session_end shell-hook entry point.

Wired from config.yaml (ACTIVE Hermes home, honors HERMES_HOME)::

    mindos_bridge:
      enabled: false            # opt-in; safe/off by default
      sqlite_mode: true         # with a session_id, ingest the live session
                                # from HERMES_HOME/state.db (native adapter);
                                # false = legacy JSONL root sync only
      state_db: ""              # explicit state.db path; default HERMES_HOME/state.db
      source: hermes-gateway
      root: ""                  # JSONL store root (fallback when no session_id)
      profile: ""               # Hermes profile label recorded as provenance
      project: ""
      bank: autopilot-shared-context
      redact: true              # store [REDACTED:<kind>] copies of credential-shaped text
      export_on_sync: true      # also write the pending-export manifest (GET-only honesty)
      export_out: ""            # manifest path (default <mindos home>/bridge-exports/latest.jsonl)
      worker_seconds: 120       # hard wall-clock cap for the detached worker

Contract honored:
- Reads ONLY shell-hook stdin metadata (session_id, platform). Message contents never
  enter this process and are never logged.
- MindOS home resolves exclusively through autopilot._resolve_home() — env
  HERMES_AUTOPILOT_HOME > reversible selector > default. Never hardcodes ~/.hermes.
- Duplicate events are idempotent: unchanged transcript files are skipped by content
  hash inside the incremental sync.
- Non-blocking: actual work runs in a detached grandchild process with a hard
  wall-clock kill; this parent exits immediately so the gateway reply path is never
  delayed by bridge failure or slowness. The batch `watch` fallback remains available.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

HOOK_DIR = Path(__file__).resolve().parent


def _config() -> dict:
    """Read mindos_bridge settings from the ACTIVE Hermes home config.yaml.

    Honors HERMES_HOME so profiles stay isolated; falls back to the default home.
    Missing/unreadable config => disabled (safe default).
    """
    try:
        import yaml  # type: ignore
    except ImportError:
        yaml = None
    home = Path(os.environ.get("HERMES_HOME", "").strip() or
                (Path.home() / ".hermes"))
    cfg_path = home / "config.yaml"
    raw: dict = {}
    if cfg_path.is_file():
        try:
            if yaml is not None:
                raw = yaml.safe_load(cfg_path.read_text()) or {}
            else:
                # Minimal fallback parser for the documented scalar-only schema.
                in_block = False
                for line in cfg_path.read_text().splitlines():
                    if not line.strip() or line.lstrip().startswith("#"):
                        continue
                    if not line.startswith(" "):
                        in_block = line.strip() == "mindos_bridge:"
                        continue
                    if in_block and ":" in line:
                        k, _, v = line.strip().partition(":")
                        v = v.split("#", 1)[0].strip().strip("\"'")
                        raw.setdefault("mindos_bridge", {})[k.strip()] = v
        except Exception:
            return {}
    block = raw.get("mindos_bridge") if isinstance(raw, dict) else None
    return block if isinstance(block, dict) else {}


def _as_bool(v, default=False) -> bool:
    if v is None:
        return bool(default)
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _build_commands(cfg: dict) -> tuple[list[str] | None, list[str] | None]:
    """Primary ingest command + optional fallback command.

    Native SQLite adapter first when a session_id is present (sqlite_mode
    defaults on); if that attempt exits non-zero (e.g. no state.db in this
    profile home), the worker falls back to the legacy JSONL root sync so
    both adapters coexist.
    """
    if not _as_bool(cfg.get("enabled"), False):
        return None, None

    def _common(cmd):
        for flag, key in (("--profile", "profile"), ("--project", "project"),
                          ("--bank", "bank")):
            val = str(cfg.get(key, "") or "").strip()
            if val:
                cmd += [flag, val]
        if _as_bool(cfg.get("redact"), True):
            cmd.append("--redact")
        return cmd

    session_id = str(cfg.get("_session_id", "") or "").strip()
    state_db = str(cfg.get("state_db", "") or "").strip()
    root = str(cfg.get("root", "") or "").strip()

    root_cmd = None
    if root:
        root_cmd = _common([
            sys.executable, str(HOOK_DIR / "mindos_bridge.py"), "sync",
            "--source", str(cfg.get("source", "hermes-gateway")),
            "--root", root, "--apply"])

    if session_id and _as_bool(cfg.get("sqlite_mode"), True):
        sql_cmd = [sys.executable, str(HOOK_DIR / "mindos_bridge.py"), "sqlite-sync",
                   "--sqlite-session-id", session_id]
        if state_db:
            sql_cmd += ["--state-db", state_db]
        return _common(sql_cmd), root_cmd

    if not root:
        return None, None  # misconfigured -> refuse to guess paths; stay inert
    return root_cmd, None


def _build_command(cfg: dict) -> list[str] | None:
    return _build_commands(cfg)[0]


def _export_command(cfg: dict) -> list[str] | None:
    if not _as_bool(cfg.get("export_on_sync"), True):
        return None
    bank = str(cfg.get("bank", "") or "autopilot-shared-context").strip()
    out = str(cfg.get("export_out", "") or "").strip()
    if not out:
        try:
            sys.path.insert(0, str(HOOK_DIR))
            import autopilot as ap  # noqa: E402  (profile-safe home resolution)
            out = str(Path(ap.ROOT) / "bridge-exports" / "latest.jsonl")
        except Exception:
            return None  # cannot resolve home honestly -> skip export this pass
    cmd = [sys.executable, str(HOOK_DIR / "mindos_bridge.py"),
           "export", "--out", out, "--limit", "500"]
    if bank:
        cmd += ["--bank", bank]
    return cmd


def main() -> int:
    # Drain stdin (hook protocol) but keep only metadata — never message content.
    try:
        raw_in = sys.stdin.read()
        data = json.loads(raw_in) if raw_in.strip() else {}
    except Exception:
        data = {}

    cfg = _config()
    sid = str(data.get("session_id", "")).strip() if isinstance(data, dict) else ""
    # sqlite_mode (default on): with a session_id, ingest the live session
    # directly from Hermes state.db via the native adapter; without one (or
    # with sqlite_mode: false) fall back to the JSONL store-root sync.
    if sid:
        cfg["_session_id"] = sid
    cmd, fallback_cmd = _build_commands(cfg)
    if not cmd and not fallback_cmd:
        return 0  # disabled or unconfigured: instant no-op, fail open

    job = {"cmd": cmd,
           "fallback_cmd": fallback_cmd,
           "export_cmd": _export_command(cfg),
           "deadline_s": max(10, int(cfg.get("worker_seconds", 120))),
           "session_id": str(data.get("session_id", "")) if isinstance(data, dict) else ""}
    try:
        subprocess.Popen(
            [sys.executable, __file__, "--worker"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True,
        ).stdin.write(json.dumps(job).encode())  # type: ignore[union-attr]
    except Exception:
        # Last-resort synchronous fallback with a tiny bound; still non-fatal.
        try:
            subprocess.run(cmd or fallback_cmd or [], timeout=10,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
    return 0


def _worker_main() -> int:
    """Runs in the detached process: execute the queued commands with hard caps.

    Ingest order: primary cmd; if it exits non-zero and a JSONL fallback_cmd
    exists, run that too. Export always attempts (honest pending semantics).
    """
    try:
        job = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    import time
    deadline = time.time() + max(10, int(job.get("deadline_s", 120)))
    steps = [job.get("cmd"), job.get("fallback_cmd"), job.get("export_cmd")]
    for c in steps:
        if not isinstance(c, list) or not c:
            continue
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            r = subprocess.run(c, timeout=remaining, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
            # A successful primary ingest makes the fallback redundant.
            if c is job.get("cmd") and r.returncode == 0:
                job["fallback_cmd"] = None
        except Exception:
            continue
    return 0


if __name__ == "__main__":
    sys.exit(_worker_main() if "--worker" in sys.argv else main())
