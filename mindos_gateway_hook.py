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
      channel: shared-context   # export channel (legacy key `bank` still read)
      redact: true              # store [REDACTED:<kind>] copies of credential-shaped text
      export_on_sync: true      # also write the pending-export manifest
      export_out: ""            # manifest path (default <mindos home>/bridge-exports/latest.jsonl)
      worker_seconds: 120       # hard wall-clock cap for the detached worker
      context_pack: false       # opt-in: answer pre_llm_call first-turn with a
                                # MindOS session-start pack via the host's
                                # ephemeral plugin-context channel (read-only)
      context_pack_max_bytes: 4096
      context_pack_seconds: 15  # wall-clock cap for pack generation

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

Session-start context injection (opt-in, separate from ingest):
- Hermes fires the shell hook `pre_llm_call` every turn; `extra.is_first_turn`
  is true exactly once, at the first turn of a brand-new session. That is the
  new-session boundary used here. (`on_session_start` exists but its return
  value is discarded by agent/conversation_loop.py — it has no context
  channel; `pre_llm_call` results shaped {"context": "..."} are injected by
  agent/turn_context.py ephemerally into the current turn's user message,
  never persisted to the session DB and never touching the system prompt.)
- With `context_pack: true` in the mindos_bridge block (default off) and only
  on the first turn, this hook runs mindos_context_pack.py session-pack
  synchronously under a hard wall-clock cap and prints {"context": <pack
  markdown>} so the HOST injects it once at session start. No synthetic
  messages are added after turn 1 (empty stdout), no per-turn rewriting, no
  prompt-cache breakage (system prompt byte-stable), no role alternation
  change (host appends into the existing user message), and zero writes:
  pack generation is read-only and no --out is passed.
- HERMES_MINDOS_CONTEXT=off|0|false|no is honored end-to-end: the pack tool
  prints {"enabled": false} and this hook emits nothing.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

HOOK_DIR = Path(__file__).resolve().parent
PACK_TOOL = HOOK_DIR / "mindos_context_pack.py"
PACK_FORMAT = "mindos-session-context-pack-v1"


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
        for flag, key in (("--profile", "profile"), ("--project", "project")):
            val = str(cfg.get(key, "") or "").strip()
            if val:
                cmd += [flag, val]
        cmd += ["--channel", _channel(cfg)]
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


def _channel(cfg: dict) -> str:
    """Export channel from config; `bank` is still read so a config written
    for the retired Hindsight naming keeps working unchanged."""
    return (str(cfg.get("channel", "") or "").strip()
            or str(cfg.get("bank", "") or "").strip()
            or "shared-context")


def _export_command(cfg: dict) -> list[str] | None:
    if not _as_bool(cfg.get("export_on_sync"), True):
        return None
    out = str(cfg.get("export_out", "") or "").strip()
    if not out:
        try:
            sys.path.insert(0, str(HOOK_DIR))
            import autopilot as ap  # noqa: E402  (profile-safe home resolution)
            out = str(Path(ap.ROOT) / "bridge-exports" / "latest.jsonl")
        except Exception:
            return None  # cannot resolve home honestly -> skip export this pass
    return [sys.executable, str(HOOK_DIR / "mindos_bridge.py"),
            "export", "--out", out, "--limit", "500",
            "--channel", _channel(cfg)]


def _render_context_markdown(pack: dict) -> str:
    """Deterministic bounded markdown rendering of a sealed context pack.

    Provenance survives injection: format, digest, generated_at, scope,
    budget, per-section statuses (ok/empty/unavailable/refused-secret) and
    the refused-secret counts. Item lines are compact sorted-key JSON so an
    unchanged state renders byte-stably.
    """
    budget = pack.get("budget", {})
    lines = [
        "# MindOS session context",
        f"format: {pack.get('format', '')}",
        f"digest: {pack.get('digest', '')}",
        f"generated_at: {pack.get('generated_at', '')}",
        f"scope: profile={pack.get('profile', '*')} project={pack.get('project', '*')}",
        f"budget: used={budget.get('used_bytes', 0)}/{budget.get('max_bytes', 0)} bytes "
        f"items<={budget.get('max_items', 0)} truncated={bool(budget.get('truncated'))}",
    ]
    sources = pack.get("sources", {})
    for section in ("temporal_facts", "handoffs", "receipts", "session_context",
                    "semantic"):
        meta = sources.get(section, {})
        lines.append(
            f"## {section} status={meta.get('status', 'unknown')} "
            f"packed={meta.get('packed_items', 0)}/{meta.get('requested_items', 0)}")
        for item in pack.get("sections", {}).get(section, []):
            lines.append("- " + json.dumps(item, sort_keys=True,
                                           separators=(",", ":")))
    refused = pack.get("refused_secret_items") or {}
    if refused:
        lines.append("refused-secret-items: " + json.dumps(refused, sort_keys=True))
    return "\n".join(lines)


def _session_context_response(cfg: dict, data: dict) -> str | None:
    """First-turn-only MindOS pack via the host pre_llm_call context channel.

    Returns the stdout JSON string {"context": ...} or None for a silent
    no-op. Fail-open: any failure prints nothing and returns exit 0 — the
    reply path can never be blocked or delayed beyond the wall-clock cap.
    """
    if not _as_bool(cfg.get("context_pack"), False):
        return None
    extra = data.get("extra") if isinstance(data.get("extra"), dict) else {}
    if not _as_bool(extra.get("is_first_turn"), False):
        return None  # continuation turn: silent no-op, cache stays warm

    cmd = [sys.executable, str(PACK_TOOL), "session-pack"]
    for flag, key in (("--profile", "profile"), ("--project", "project")):
        val = str(cfg.get(key, "") or "").strip()
        if val:
            cmd += [flag, val]
    if _as_bool(cfg.get("redact"), True):
        cmd.append("--redact")
    try:
        max_bytes = max(256, int(cfg.get("context_pack_max_bytes", 4096)))
    except (TypeError, ValueError):
        max_bytes = 4096
    cmd += ["--max-bytes", str(max_bytes)]
    try:
        cap = max(5, int(cfg.get("context_pack_seconds", 15)))
    except (TypeError, ValueError):
        cap = 15
    try:
        r = subprocess.run(cmd, timeout=cap, text=True,
                           capture_output=True)
    except Exception:
        return None
    if r.returncode != 0:
        return None
    try:
        pack = json.loads(r.stdout)
    except Exception:
        return None
    if not isinstance(pack, dict) or \
            pack.get("format") != PACK_FORMAT or pack.get("enabled") is not True:
        # Includes the HERMES_MINDOS_CONTEXT=off kill-switch response.
        return None
    return json.dumps({"context": _render_context_markdown(pack)})


def main() -> int:
    # Drain stdin (hook protocol) but keep only metadata — never message content.
    try:
        raw_in = sys.stdin.read()
        data = json.loads(raw_in) if raw_in.strip() else {}
    except Exception:
        data = {}

    cfg = _config()
    event = str(data.get("hook_event_name", "") or "").strip() \
        if isinstance(data, dict) else ""
    if event == "pre_llm_call":
        # Session-start context path: synchronous but wall-clock capped,
        # read-only, fail-open. Empty stdout = silent no-op for the host.
        try:
            out = _session_context_response(cfg, data)
        except Exception:
            out = None
        if out:
            sys.stdout.write(out + "\n")
        return 0
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
        proc = subprocess.Popen(
            [sys.executable, __file__, "--worker"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True,
        )
        # The worker blocks on sys.stdin.read() until EOF, so the pipe must be
        # closed explicitly. Letting the Popen fall out of scope only works by
        # CPython refcount finalization; inside a long-lived gateway process on
        # any other runtime the worker would hang holding an open pipe.
        assert proc.stdin is not None
        with proc.stdin as w:
            w.write(json.dumps(job).encode())
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
