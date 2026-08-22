#!/usr/bin/env python3
"""MindOS gateway bridge — on_session_end shell-hook entry point.

Wired from config.yaml (ACTIVE Hermes home, honors HERMES_HOME)::

    mindos_bridge:
      enabled: false            # opt-in; safe/off by default
      source: hermes-gateway
      root: ""                  # session-store root to ingest (required when enabled)
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


def _build_command(cfg: dict) -> list[str] | None:
    if not _as_bool(cfg.get("enabled"), False):
        return None
    root = str(cfg.get("root", "") or "").strip()
    if not root:
        return None  # misconfigured -> refuse to guess paths; stay inert
    cmd = [sys.executable, str(HOOK_DIR / "mindos_bridge.py"), "sync",
           "--source", str(cfg.get("source", "hermes-gateway")),
           "--root", root, "--apply"]
    for flag, key in (("--profile", "profile"), ("--project", "project"),
                      ("--bank", "bank")):
        val = str(cfg.get(key, "") or "").strip()
        if val:
            cmd += [flag, val]
    if _as_bool(cfg.get("redact"), True):
        cmd.append("--redact")
    return cmd


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
    cmd = _build_command(cfg)
    if not cmd:
        return 0  # disabled or unconfigured: instant no-op, fail open

    job = {"cmd": cmd,
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
            subprocess.run(cmd, timeout=10, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        except Exception:
            pass
    return 0


def _worker_main() -> int:
    """Runs in the detached process: execute the queued commands with hard caps."""
    try:
        job = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    import time
    deadline = time.time() + max(10, int(job.get("deadline_s", 120)))
    for step in ("cmd", "export_cmd"):
        c = job.get(step)
        if not isinstance(c, list) or not c:
            continue
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            subprocess.run(c, timeout=remaining, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(_worker_main() if "--worker" in sys.argv else main())
