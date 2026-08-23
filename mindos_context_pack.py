#!/usr/bin/env python3
"""MindOS session-start context pack — bounded relevant-context injection.

Produces a sealed, bounded JSON context pack for NEW Hermes session starts
from MindOS shared state: temporal facts, live handoffs, latest receipts,
profile-scoped ingested session snippets, and Hindsight semantic recall.
Never synthetic per-turn messages, never system-prompt rewriting; the host
injects the pack once at session start so prompt caching is preserved.

Safety and honesty contract:
- Bounded by --max-bytes / --max-items with per-section counts and a global
  truncated flag; one huge transcript cannot eat the budget.
- Profile-safe: session content requires an exact profile match when
  --profile is given; cross-profile sessions can never be packed.
- Secret guard ladder before any text is packed: default drops the item and
  reports it (fail closed), --redact packs [REDACTED:<kind>] copies, audited
  --allow-secret passes verbatim. Values never reach output or errors.
- Every section reports ok/empty/unavailable/refused-secret — unavailable
  sources degrade honestly instead of fabricating context.
- Deterministic digest (sha256 over the pack minus generated_at/digest):
  identical state regenerates byte-stable packs; verify-pack detects stale
  or aged packs and returns the recomputed digest.
- Opt-out: HERMES_MINDOS_CONTEXT=off|0|false|no prints {"enabled": false}
  and exits 0 without reading anything.

Read-only against all sources; run against an explicit MindOS home via
HERMES_AUTOPILOT_HOME. Does not deploy, install, or touch live state.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import autopilot as ap  # noqa: E402  (home resolution, schema, secret guard)

PACK_FORMAT = "mindos-session-context-pack-v1"
DISABLE_VALUES = {"off", "0", "false", "no"}
SNIPPET_CHARS = 400


def disabled() -> bool:
    return os.environ.get("HERMES_MINDOS_CONTEXT", "").strip().lower() in DISABLE_VALUES


def _snippet(text: str) -> str:
    return text if len(text) <= SNIPPET_CHARS else text[:SNIPPET_CHARS - 1] + "…"


def _guard_text(text: str, redact: bool, allow: bool):
    """Guard ladder for one candidate item's text.

    Returns (text_to_pack_or_None, refused_kind_or_None). Default refuses
    (None) on credential-shaped content; redact substitutes placeholders;
    explicit audited allow passes verbatim.
    """
    kinds = [f["kind"] for f in ap._secret_findings(text)]
    if not kinds:
        return text, None
    if allow:
        return text, None
    if redact:
        return ap._redact_secrets(text), None
    return None, sorted(set(kinds))[0]


def _tokens(text: str) -> list:
    toks = []
    for raw in (text or "").split():
        tok = raw.replace('"', "")
        if tok and any(c.isalnum() for c in tok):
            toks.append(tok)
    return toks


def _fact_candidates(db, limit: int) -> tuple[list, str]:
    try:
        rows = [dict(r) for r in db.execute(
            "SELECT id,subject,predicate,object,source,task_id,valid_from,valid_until "
            "FROM facts WHERE valid_until='' OR valid_until> ? "
            "ORDER BY valid_from DESC, id ASC LIMIT ?", (ap.now(), max(0, limit)))]
        return rows, ("ok" if rows else "empty")
    except Exception as e:  # noqa: BLE001 — degrade honestly, name the failure class
        return [], f"unavailable ({type(e).__name__})"


def _handoff_candidates(db, limit: int) -> tuple[list, str]:
    try:
        rows = [dict(r) for r in db.execute(
            "SELECT h.id,h.task_id,h.from_agent,h.to_agent,h.status,h.objective,"
            "h.commit_ref,h.created_at,t.title AS task_title "
            "FROM handoffs h LEFT JOIN tasks t ON t.id=h.task_id "
            "WHERE h.superseded_by='' ORDER BY h.created_at DESC, h.id ASC LIMIT ?",
            (max(0, limit),))]
        return rows, ("ok" if rows else "empty")
    except Exception as e:  # noqa: BLE001
        return [], f"unavailable ({type(e).__name__})"


def _receipt_candidates(db, limit: int) -> tuple[list, str]:
    try:
        rows = []
        for r in db.execute(
                "SELECT id,task_id,kind,created_at FROM receipts "
                "ORDER BY created_at DESC, id DESC LIMIT ?", (max(0, limit),)):
            d = dict(r)
            d["payload_digest"] = hashlib.sha256(
                json.dumps(json.loads(db.execute(
                    "SELECT payload_json FROM receipts WHERE id=?", (d["id"],)
                ).fetchone()[0]), sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
            rows.append(d)
        return rows, ("ok" if rows else "empty")
    except Exception as e:  # noqa: BLE001
        return [], f"unavailable ({type(e).__name__})"


def _session_candidates(db, query: str, profile: str, project: str,
                        limit: int) -> tuple[list, str]:
    if limit <= 0:
        return [], "empty"
    toks = _tokens(query) or ["*"]
    try:
        cols = ("SELECT s.session_id,s.source,s.profile,s.project,"
                "sm.seq,sm.role,sm.at,sm.content")
        conds, vals = [], []
        if profile:
            conds.append("s.profile=?")
            vals.append(profile)
        if project:
            conds.append("s.project=?")
            vals.append(project)
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        if ap._sessions_fts_ready(db) and toks != ["*"]:
            match = " OR ".join('"%s"' % t for t in toks)
            sql = (cols + " FROM session_messages_fts f JOIN session_messages sm "
                   "ON sm.rowid=f.rowid JOIN sessions s ON s.id=sm.session_row "
                   "WHERE session_messages_fts MATCH ?" +
                   (" AND " + " AND ".join(conds) if conds else "") +
                   " ORDER BY sm.at DESC, sm.session_row ASC, sm.seq ASC LIMIT ?")
            rows = db.execute(sql, [match, *vals, max(0, limit)]).fetchall()
        else:
            likes = "" if toks == ["*"] else "(" + " OR ".join(
                "sm.content LIKE ?" for _ in toks) + ")"
            sql = (cols + " FROM session_messages sm JOIN sessions s "
                   "ON s.id=sm.session_row" + where +
                   (" AND " + likes if likes else "") +
                   " ORDER BY sm.at DESC, sm.session_row ASC, sm.seq ASC LIMIT ?")
            like_vals = ["%" + t + "%" for t in toks] if likes != "" else []
            rows = db.execute(sql, [*like_vals, *vals, max(0, limit)]).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["content"] = _snippet(d["content"])
            out.append(d)
        return out, ("ok" if out else "empty")
    except Exception as e:  # noqa: BLE001
        return [], f"unavailable ({type(e).__name__})"


def _bot_candidates(db, profile: str, query: str,
                    limit: int) -> tuple[list, str]:
    """Managed intra-bot messages scoped to one target profile.

    Exact-profile match only — bot chat addressed to another profile can
    never enter this pack. The table is owned by mindos_botmail.py and may
    not exist yet; that degrades honestly instead of failing the pack.
    """
    try:
        conds, vals = [], []
        if profile:
            conds.append("target_profile=?")
            vals.append(profile)
        toks = [t for t in re.findall(r"[a-z0-9]+", (query or "").lower())
                if t] if query else []
        if toks:
            conds.append("(" + " OR ".join("content LIKE ?" for _ in toks) + ")")
            vals.extend("%" + t + "%" for t in toks)
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        rows = db.execute(
            "SELECT message_id,sender_bot,correlation_id,content_class,"
            "content,at,autonomy_level,model_binding,provider "
            "FROM bot_messages" + where +
            " ORDER BY at DESC, message_id ASC LIMIT ?",
            (*vals, max(0, limit))).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["content"] = _snippet(d["content"])
            out.append(d)
        return out, ("ok" if out else "empty")
    except Exception as e:  # noqa: BLE001
        return [], f"unavailable ({type(e).__name__})"


def _semantic_candidates(query: str, limit: int) -> tuple[list, str]:
    if limit <= 0 or not ap._hindsight_available():
        return [], ("empty" if limit <= 0 else "unavailable (no bank configured)")
    try:
        mems = []
        low_toks = [t.lower() for t in _tokens(query)]
        with ap._hindsight_bank_path().open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                m = json.loads(line)
                text = str(m.get("text", ""))
                if low_toks and not any(t in text.lower() for t in low_toks):
                    continue
                mems.append({"id": str(m.get("id", "")), "engine": ap.HINDSIGHT_ENGINE_TAG,
                             "kind": str(m.get("kind", "")), "project": str(m.get("project", "")),
                             "at": str(m.get("created_at", "")), "tags": sorted(m.get("tags", [])),
                             "content": _snippet(text)})
        mems.sort(key=lambda m: (m["at"], m["id"]), reverse=True)
        mems = mems[:max(0, limit)]
        return mems, ("ok" if mems else "empty")
    except Exception as e:  # noqa: BLE001
        return [], f"unavailable ({type(e).__name__})"


def build_pack(profile: str = "", project: str = "", query: str = "",
               max_bytes: int = 4096, max_items: int = 24,
               facts_limit: int = 8, handoffs_limit: int = 4,
               receipts_limit: int = 4, sessions_limit: int = 6,
               bots_limit: int = 4, semantic_limit: int = 4, max_age_hours: float = 12.0,
               redact: bool = False, allow_secret: bool = False) -> dict:
    """Assemble the bounded session-start pack. Read-only; never raises."""
    max_bytes = max(256, int(max_bytes))
    sections: dict = {}
    sources: dict = {}
    refused_secret_items: dict = {}
    used_bytes = 64 + len(PACK_FORMAT)
    truncated = False

    def cost(obj) -> int:
        return len(json.dumps(obj, sort_keys=True, separators=(",", ":")))

    def fit(section: str, cands: list, requested: int,
            base_status: str, extra: dict | None = None) -> None:
        """Guard, budget-fit and pack one section's candidates."""
        nonlocal used_bytes, truncated
        packed = 0
        refused = None
        for item in cands:
            guarded, refused = {}, None
            for k, v in item.items():
                if isinstance(v, str):
                    g, r = _guard_text(v, redact, allow_secret)
                    if g is None:
                        refused = r
                        break
                    guarded[k] = g
                else:
                    guarded[k] = v
            if refused:
                refused_secret_items.setdefault(section, []).append(
                    {"ref": item.get("id") or item.get("session_id", ""), "kind": refused})
                continue
            need = cost(guarded)
            items_so_far = sum(len(v) for v in sections.values()) + packed
            if used_bytes + need > max_bytes or (max_items and items_so_far >= max_items):
                truncated = True
                break
            used_bytes += need
            sections[section].append(guarded)
            packed += 1
        st = base_status
        if refused_secret_items.get(section) and packed == 0:
            st = "refused-secret"
        elif packed or len(cands):
            st = "ok" if base_status == "ok" else base_status
        meta = {"status": st, "requested_items": max(0, requested),
                "matched_items": len(cands), "packed_items": packed}
        if extra:
            meta.update(extra)
        sources[section] = meta

    enabled_db = True
    try:
        with ap.conn() as db:
            facts, st = _fact_candidates(db, facts_limit)
            sections["temporal_facts"] = []
            fit("temporal_facts", facts, facts_limit, st)
            handoffs, st = _handoff_candidates(db, handoffs_limit)
            sections["handoffs"] = []
            fit("handoffs", handoffs, handoffs_limit, st)
            receipts, st = _receipt_candidates(db, receipts_limit)
            sections["receipts"] = []
            fit("receipts", receipts, receipts_limit, st)
            sessions, st = _session_candidates(db, query, profile, project,
                                               sessions_limit)
            sections["session_context"] = []
            fit("session_context", sessions, sessions_limit, st,
                {"profile_scope": profile or "*"})
            bots, st = _bot_candidates(db, profile, query, bots_limit)
            sections["bot_chat"] = []
            fit("bot_chat", bots, bots_limit, st,
                {"profile_scope": profile or "*"})
    except Exception as e:  # noqa: BLE001 — no MindOS home/DB at all: honest degradation
        enabled_db = False
        for s in ("temporal_facts", "handoffs", "receipts", "session_context",
                  "bot_chat"):
            sources[s] = {"status": f"unavailable ({type(e).__name__})",
                          "requested_items": 0, "matched_items": 0, "packed_items": 0}
            sections[s] = []

    sem, st = _semantic_candidates(query, semantic_limit)
    sections["semantic"] = []
    fit("semantic", sem, semantic_limit, st)

    envelope = {
        "format": PACK_FORMAT,
        "enabled": True,
        "profile": profile or "*",
        "project": project or "*",
        "query": query,
        # Exact build parameters so verify-pack can reproduce this pack
        # byte-stably; the display profile/project above use "*" sentinels
        # and must never be re-fed as literal scope filters.
        "params": {"profile": profile, "project": project, "query": query,
                   "max_bytes": max_bytes, "max_items": max_items,
                   "facts_limit": facts_limit, "handoffs_limit": handoffs_limit,
                   "receipts_limit": receipts_limit,
                   "sessions_limit": sessions_limit,
                   "bots_limit": bots_limit,
                   "semantic_limit": semantic_limit,
                   "max_age_hours": max_age_hours,
                   "redact": redact, "allow_secret": allow_secret},
        "generated_at": ap.now(),
        "budget": {"max_bytes": max_bytes, "used_bytes": used_bytes,
                   "max_items": max_items, "truncated": truncated},
        "freshness": {"max_age_hours": max_age_hours},
        "sources": sources,
        "refused_secret_items": {k: len(v) for k, v in refused_secret_items.items()},
        "_refusal_detail": refused_secret_items,
        "sections": sections,
    }
    digest = hashlib.sha256(json.dumps(
        {k: v for k, v in envelope.items() if k not in ("generated_at",)},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    envelope["digest"] = digest
    return envelope


def _recompute_staleness(pack: dict, current: dict) -> dict:
    """Compare a stored pack against freshly regenerated state."""
    from datetime import datetime, timezone
    verdict = {"fresh": None, "aged": False, "current_digest": current["digest"]}
    try:
        gen = datetime.fromisoformat(pack["generated_at"])
        age_h = (datetime.now(timezone.utc) - gen).total_seconds() / 3600.0
        verdict["age_hours"] = round(age_h, 3)
        verdict["aged"] = age_h > float(pack["freshness"]["max_age_hours"])
    except Exception:  # noqa: BLE001
        verdict["age_hours"] = None
    verdict["fresh"] = pack["digest"] == current["digest"]
    return verdict


def cmd_session_pack(args):
    if disabled():
        print(json.dumps({"enabled": False, "format": PACK_FORMAT}))
        return
    if args.allow_secret:
        with ap.conn() as db:
            ap.audit(db, "session", "context-pack", "secret_allowed",
                     {"fields": ["sections/*"], "tool": PACK_FORMAT})
    pack = build_pack(profile=args.profile, project=args.project, query=args.query,
                      max_bytes=args.max_bytes, max_items=args.max_items,
                      facts_limit=args.facts, handoffs_limit=args.handoffs,
                    receipts_limit=args.receipts, sessions_limit=args.sessions,
                    bots_limit=args.bots,
                    semantic_limit=args.semantic, max_age_hours=args.max_age_hours,
                      redact=args.redact, allow_secret=args.allow_secret)
    if getattr(args, "out", ""):
        out = Path(args.out).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".tmp")
        tmp.write_text(json.dumps(pack, sort_keys=True, indent=1) + "\n")
        tmp.chmod(0o600)
        tmp.replace(out)
    print(json.dumps(pack, sort_keys=True, indent=1))


def cmd_verify_pack(args):
    if disabled():
        print(json.dumps({"enabled": False, "format": PACK_FORMAT}))
        return
    pack = json.loads(Path(args.pack).expanduser().read_text())
    if pack.get("format") != PACK_FORMAT:
        raise SystemExit(f"unknown pack format: {pack.get('format')!r}")
    # Rebuild with the exact parameters the stored pack was generated with.
    # The envelope's display profile/project carry "*" sentinels for unset
    # scope; re-feeding them as literal filters would silently drop matched
    # items and make every such pack verify as stale (or falsely fresh).
    params = pack.get("params", {})

    def scope(key: str) -> str:
        raw = params.get(key, pack.get(key, ""))
        return "" if raw == "*" else str(raw)

    current = build_pack(
        profile=scope("profile"), project=scope("project"),
        query=str(params.get("query", pack.get("query", ""))),
        max_bytes=pack["budget"]["max_bytes"],
        max_items=pack["budget"]["max_items"],
        facts_limit=int(params.get("facts_limit", 8)),
        handoffs_limit=int(params.get("handoffs_limit", 4)),
        receipts_limit=int(params.get("receipts_limit", 4)),
        sessions_limit=int(params.get("sessions_limit", 6)),
        bots_limit=int(params.get("bots_limit", 4)),
        semantic_limit=int(params.get("semantic_limit", 4)),
        max_age_hours=float(params.get(
            "max_age_hours",
            pack.get("freshness", {}).get("max_age_hours", 12.0))),
        redact=bool(params.get("redact", bool(pack.get("refused_secret_items")))),
        allow_secret=bool(params.get("allow_secret", False)))
    verdict = _recompute_staleness(pack, current)
    print(json.dumps({"ok": True, "format": PACK_FORMAT, **verdict}, sort_keys=True))


def cmd_sentinel(args):
    """End-to-end proof in a fully disposable fixture world (never live).

    Builds a temp MindOS home + synthetic Hermes store with two profiles and
    a Hindsight bank, ingests through the real bridge, then proves: pack
    delivery for a new session, digest idempotence, profile isolation,
    secret refusal/redaction, the disable path, and unavailable-source
    degradation. Zero writes outside the temp dir.
    """
    import subprocess
    import tempfile
    root = Path(__file__).parent
    checks = []
    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        env = os.environ.copy()
        env["HERMES_AUTOPILOT_HOME"] = str(tdp / "mindos-home")
        env["HERMES_HINDSIGHT_HOME"] = str(tdp / "hindsight")
        env.pop("HERMES_MINDOS_CONTEXT", None)
        store_root = tdp / "store"
        store_a = store_root / "telegram-leo"
        store_b = store_root / "other-profile"
        store_s = store_root / "secret-store"
        store_a.mkdir(parents=True)
        store_b.mkdir(parents=True)
        store_s.mkdir(parents=True)
        (store_a / "sess-a.jsonl").write_text("\n".join(json.dumps(x) for x in [
            {"message": {"role": "user", "content":
             "sentinel context injection check: launch the monochrome page Monday"},
             "timestamp": "2026-08-21T17:00:00Z"},
            {"message": {"role": "assistant", "content": "Launch focus confirmed."},
             "timestamp": "2026-08-21T17:00:05Z"}]) + "\n")
        (store_b / "sess-b.jsonl").write_text(json.dumps(
            {"message": {"role": "user", "content": "profile B private note about orchids"},
             "timestamp": "2026-08-21T18:00:00Z"}) + "\n")
        SECRET_VALUE = "AKIA" + "IOSFODNN7EXAMPLE"
        (store_s / "sess-secret.jsonl").write_text(json.dumps(
            {"message": {"role": "user", "content": f"use this key: {SECRET_VALUE}"},
             "timestamp": "2026-08-21T19:00:00Z"}) + "\n")

        def run(tool, *a, extra_env=None):
            e = env if extra_env is None else {**env, **extra_env}
            p = subprocess.run([sys.executable, str(root / tool), *a],
                               env=e, text=True, capture_output=True)
            assert p.returncode == 0, (tool, a, p.stdout[-500:], p.stderr[-500:])
            return json.loads(p.stdout) if p.stdout.strip() else {}

        # 1. Ingest through the real bridge (secret session refused by default).
        run("mindos_bridge.py", "sync", "--source", "hermes-sentinel",
            "--root", str(store_a), "--profile", "telegram-leo", "--apply")
        run("mindos_bridge.py", "sync", "--source", "hermes-sentinel-b",
            "--root", str(store_b), "--profile", "profile-b", "--apply")
        err = subprocess.run(
            [sys.executable, str(root / "mindos_bridge.py"), "sync", "--source",
             "hermes-sentinel-s", "--root", str(store_s), "--apply"],
            env=env, text=True, capture_output=True)
        assert err.returncode != 0 and SECRET_VALUE not in err.stderr
        checks.append("ingest+secret-refusal")

        # 2. New-session pack for profile A contains injected context.
        pack = run("mindos_context_pack.py", "session-pack",
                   "--profile", "telegram-leo", "--query", "monochrome",
                   "--max-bytes", "8192")
        assert pack["format"] == PACK_FORMAT and pack["enabled"] is True
        assert any("monochrome" in i["content"]
                   for i in pack["sections"]["session_context"])
        assert all(i.get("profile") == "telegram-leo"
                   for i in pack["sections"]["session_context"])
        assert "orchids" not in json.dumps(pack)  # cross-profile isolation
        assert pack["sources"]["temporal_facts"]["status"] == "empty"
        checks.append("new-session-pack+provenance+profile-isolation")

        # 3. Idempotence: unchanged state regenerates an identical digest.
        pack2 = run("mindos_context_pack.py", "session-pack",
                    "--profile", "telegram-leo", "--query", "monochrome",
                    "--max-bytes", "8192")
        assert pack2["digest"] == pack["digest"] and \
            pack2["generated_at"] >= pack["generated_at"]
        checks.append("digest-idempotence")

        # 4. Staleness: state change flips verify-pack to stale; recompute heals.
        pack_path = tdp / "pack.json"
        pack_path.write_text(json.dumps(pack))
        (store_a / "sess-a.jsonl").write_text(
            (store_a / "sess-a.jsonl").read_text() + json.dumps(
                {"message": {"role": "user", "content": "follow-up: monochrome hero shipped"},
                 "timestamp": "2026-08-21T17:30:00Z"}) + "\n")
        run("mindos_bridge.py", "sync", "--source", "hermes-sentinel",
            "--root", str(store_a), "--profile", "telegram-leo", "--apply")
        vp = json.loads(subprocess.run(
            [sys.executable, str(root / "mindos_context_pack.py"),
             "verify-pack", "--pack", str(pack_path)],
            env=env, text=True, capture_output=True).stdout)
        assert vp["fresh"] is False and vp["current_digest"], vp
        checks.append("stale-detection+recompute")

        # 5. Redaction path packs [REDACTED:*] copies; raw value stays absent.
        run("mindos_bridge.py", "sync", "--source", "hermes-sentinel-s",
            "--root", str(store_s), "--redact", "--apply")
        rpack = run("mindos_context_pack.py", "session-pack",
                    "--query", "key:", "--max-bytes", "8192")
        assert SECRET_VALUE not in json.dumps(rpack)
        checks.append("redact-no-leak")

        # 6. Disable env is an honest no-op.
        off = run("mindos_context_pack.py", "session-pack",
                  extra_env={"HERMES_MINDOS_CONTEXT": "off"})
        assert off == {"enabled": False, "format": PACK_FORMAT}, off
        checks.append("opt-out-disable-path")

        # 7. Unavailable sources degrade honestly (empty MindOS home + no bank).
        empty_env = {**env, "HERMES_AUTOPILOT_HOME": str(tdp / "no-home"),
                     "HERMES_HINDSIGHT_HOME": str(tdp / "no-bank")}
        ep = subprocess.run(
            [sys.executable, str(root / "mindos_context_pack.py"),
             "session-pack", "--max-bytes", "2048"],
            env=empty_env, text=True, capture_output=True)
        assert ep.returncode == 0, ep.stderr[-500:]
        epack = json.loads(ep.stdout)
        statuses = {s: meta["status"] for s, meta in epack["sources"].items()}
        assert all(st.startswith(("unavailable", "empty")) for st in statuses.values()), statuses
        assert not any(epack["sections"].values()), epack["sections"]
        checks.append("honest-degradation")
    result = {"ok": True, "sentinel": "mindos-context-pack-v1",
              "live_homes_touched": False, "checks": checks}
    if getattr(args, "json", False):
        print(json.dumps(result, indent=1))
    else:
        for c in checks:
            print(f"PASS sentinel: {c}")
        print("sentinel: PASS")


def main():
    p = argparse.ArgumentParser(description="MindOS session-start context pack")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("session-pack", help="generate the bounded session-start pack")
    sp.add_argument("--profile", default="", help="Hermes profile scope (exact match)")
    sp.add_argument("--project", default="")
    sp.add_argument("--query", default="", help="focus text for session/semantic recall")
    sp.add_argument("--max-bytes", dest="max_bytes", type=int, default=4096)
    sp.add_argument("--max-items", dest="max_items", type=int, default=24)
    sp.add_argument("--facts", type=int, default=8)
    sp.add_argument("--handoffs", type=int, default=4)
    sp.add_argument("--receipts", type=int, default=4)
    sp.add_argument("--sessions", type=int, default=6)
    sp.add_argument("--bots", type=int, default=4)
    sp.add_argument("--semantic", type=int, default=4)
    sp.add_argument("--max-age-hours", dest="max_age_hours", type=float, default=12.0)
    sp.add_argument("--redact", action="store_true")
    sp.add_argument("--allow-secret", dest="allow_secret", action="store_true")
    sp.add_argument("--out", default="")
    sp.set_defaults(fn=cmd_session_pack)

    vp = sub.add_parser("verify-pack", help="freshness/staleness check for a stored pack")
    vp.add_argument("--pack", required=True)
    vp.set_defaults(fn=cmd_verify_pack)

    st = sub.add_parser("sentinel",
                        help="disposable end-to-end proof (never touches live homes)")
    st.add_argument("--json", action="store_true", dest="json")
    st.set_defaults(fn=cmd_sentinel)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
