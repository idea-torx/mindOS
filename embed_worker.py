#!/usr/bin/env python3
"""Out-of-process embedding worker for autopilot's semantic memory.

Why a subprocess at all. The embedding model lives in a Python 3.11
virtualenv with torch, numpy and sentence-transformers; autopilot.py runs on
the system interpreter and is deliberately dependency-free -- it must keep
working on a machine where none of that exists. Importing across the two is
not possible, and vendoring torch into autopilot's runtime would trade a
2,000-line stdlib-only tool for a multi-gigabyte dependency tree. So the two
halves talk over a pipe: JSON in on stdin, JSON out on stdout, nothing else.

The division of labour is not arbitrary. This worker computes and *reads*;
it never writes to the database. Every mutation still flows through
autopilot.py, which appends the hash-chained audit event in the same
transaction. A second writer here would be a second author of the chain --
exactly the concurrent-writer fork that had to be fixed in audit().

Offline is enforced, not hoped for. HF_HUB_OFFLINE is set before the import
so a missing model cache fails loudly and locally instead of silently
reaching for the network mid-cron; nothing here downloads or installs.

Modes (single JSON object on stdin):
  {"mode":"probe"}
      -> {"ok":true,"model":...,"dim":N}
  {"mode":"embed","texts":[...]}
      -> {"ok":true,"model":...,"dim":N,"vectors":[b64 float32 ...]}
  {"mode":"search","db":PATH,"query":TEXT,"limit":N,
   "project":P|null,"min_score":F,"include_superseded":bool}
      -> {"ok":true,"model":...,"hits":[{"memory_id":...,"score":...}]}
  {"mode":"cluster","db":PATH,"threshold":F,"project":P|null,"max_clusters":N}
      -> {"ok":true,"model":...,"clusters":[[id,...]],"scored":N}

Vectors are L2-normalised at embed time, so cosine similarity is a plain dot
product and search needs no per-query renormalisation.
"""
import base64
import json
import os
import sqlite3
import sys

# Set before any transformers import: these are read at module import time.
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("TQDM_DISABLE", "1")

DEFAULT_MODEL = os.environ.get("AUTOPILOT_EMBED_MODEL", "BAAI/bge-small-en-v1.5")

_MODEL = None


def _fail(reason: str, detail: str = "") -> None:
    """Every failure leaves stdout as one well-formed JSON object.

    The caller degrades gracefully on {"ok": false}; a crash that wrote a
    traceback to stdout would instead surface as a parse error and be
    reported as a bug rather than as an unavailable optional dependency.
    """
    json.dump({"ok": False, "reason": reason, "detail": detail[:500]}, sys.stdout)
    sys.stdout.write("\n")
    sys.exit(0)


def _model(name: str):
    global _MODEL
    if _MODEL is None:
        try:
            from sentence_transformers import SentenceTransformer
        except Exception as exc:                       # torch/ST absent
            _fail("import_failed", f"{type(exc).__name__}: {exc}")
        try:
            _MODEL = SentenceTransformer(name)
        except Exception as exc:                       # model not in local cache
            _fail("model_unavailable", f"{type(exc).__name__}: {exc}")
    return _MODEL


def _encode(name: str, texts):
    """Encode to L2-normalised float32. Batched so a large backlog does not
    balloon resident memory; the model load dominates either way."""
    import numpy as np
    m = _model(name)
    out = m.encode(list(texts), batch_size=64, show_progress_bar=False,
                   normalize_embeddings=True, convert_to_numpy=True)
    return np.asarray(out, dtype=np.float32)


def _b64(vec) -> str:
    return base64.b64encode(vec.tobytes()).decode("ascii")


def main() -> None:
    try:
        req = json.load(sys.stdin)
    except Exception as exc:
        _fail("bad_request", str(exc))
    if not isinstance(req, dict):
        _fail("bad_request", "expected a JSON object")
    mode = req.get("mode") or "probe"
    name = req.get("model") or DEFAULT_MODEL

    if mode == "probe":
        # Loads the model, which is the only honest availability check: the
        # package can import cleanly and still have no weights cached.
        vec = _encode(name, ["probe"])
        json.dump({"ok": True, "model": name, "dim": int(vec.shape[1])}, sys.stdout)

    elif mode == "embed":
        texts = req.get("texts") or []
        if not isinstance(texts, list) or not all(isinstance(t, str) for t in texts):
            _fail("bad_request", "texts must be a list of strings")
        if not texts:
            json.dump({"ok": True, "model": name, "dim": 0, "vectors": []}, sys.stdout)
            sys.stdout.write("\n")
            return
        vecs = _encode(name, texts)
        json.dump({"ok": True, "model": name, "dim": int(vecs.shape[1]),
                   "vectors": [_b64(v) for v in vecs]}, sys.stdout)

    elif mode == "search":
        import numpy as np
        db_path = req.get("db") or ""
        query = (req.get("query") or "").strip()
        if not db_path or not query:
            _fail("bad_request", "db and query are required")
        limit = max(1, int(req.get("limit") or 10))
        project = req.get("project")
        min_score = float(req.get("min_score") or 0.0)
        include_superseded = bool(req.get("include_superseded"))
        try:
            # Read-only URI: this process must not be able to write, even by
            # accident. Writes belong to autopilot.py and its audit chain.
            db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        except sqlite3.Error as exc:
            _fail("db_unavailable", str(exc))
        sql = ("SELECT v.memory_id, v.vec FROM memory_vectors v "
               "JOIN memories m ON m.id = v.memory_id WHERE v.model = ?")
        vals = [name]
        if not include_superseded:
            sql += " AND m.superseded_by = ''"
        if project:
            # Scope is enforced, never relaxed on an empty result -- a
            # project-scoped search that matches nothing returns nothing.
            sql += " AND m.project = ?"
            vals.append(project)
        try:
            rows = db.execute(sql, vals).fetchall()
        except sqlite3.Error as exc:
            _fail("db_unavailable", str(exc))
        if not rows:
            json.dump({"ok": True, "model": name, "hits": [], "searched": 0},
                      sys.stdout)
            sys.stdout.write("\n")
            return
        ids = [r[0] for r in rows]
        mat = np.frombuffer(b"".join(r[1] for r in rows),
                            dtype=np.float32).reshape(len(rows), -1)
        q = _encode(name, [query])[0]
        if q.shape[0] != mat.shape[1]:
            _fail("dim_mismatch",
                  f"query dim {q.shape[0]} != stored dim {mat.shape[1]}")
        # Both sides are unit vectors, so the dot product is the cosine.
        scores = mat @ q
        order = np.argsort(-scores)[:limit]
        hits = [{"memory_id": ids[int(i)], "score": round(float(scores[int(i)]), 6)}
                for i in order if float(scores[int(i)]) >= min_score]
        json.dump({"ok": True, "model": name, "hits": hits,
                   "searched": len(rows)}, sys.stdout)

    elif mode == "cluster":
        # Near-duplicate detection for the consolidation session. The whole
        # point of doing it here is that the expensive, mechanical half --
        # "which of these thousands of memories are plausibly the same fact" --
        # is arithmetic, not judgement. An LLM asked to find duplicates by
        # reading the entire store would burn its context on the easy part and
        # miss pairs at distance; it should receive candidate groups and spend
        # its tokens on the one thing it is actually better at, deciding
        # whether two similar-looking facts really are one fact.
        import numpy as np
        db_path = req.get("db") or ""
        if not db_path:
            _fail("bad_request", "db is required")
        threshold = float(req.get("threshold") or 0.85)
        project = req.get("project")
        max_clusters = max(1, int(req.get("max_clusters") or 25))
        try:
            db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        except sqlite3.Error as exc:
            _fail("db_unavailable", str(exc))
        sql = ("SELECT v.memory_id, v.vec FROM memory_vectors v "
               "JOIN memories m ON m.id = v.memory_id "
               "WHERE v.model = ? AND m.superseded_by = ''")
        vals = [name]
        if project:
            sql += " AND m.project = ?"
            vals.append(project)
        # Kinds the caller has declared un-mergeable. Consolidation assumes a
        # cluster is one fact restated; for an immutable event record that
        # assumption is false, and merging would delete history rather than
        # tidy it.
        exclude_kinds = [k for k in (req.get("exclude_kinds") or []) if k]
        if exclude_kinds:
            sql += " AND m.kind NOT IN (%s)" % ",".join("?" * len(exclude_kinds))
            vals.extend(exclude_kinds)
        sql += " ORDER BY m.created_at ASC, m.id ASC"
        try:
            rows = db.execute(sql, vals).fetchall()
        except sqlite3.Error as exc:
            _fail("db_unavailable", str(exc))
        if len(rows) < 2:
            json.dump({"ok": True, "model": name, "clusters": [],
                       "scored": len(rows)}, sys.stdout)
            sys.stdout.write("\n")
            return
        ids = [r[0] for r in rows]
        mat = np.frombuffer(b"".join(r[1] for r in rows),
                            dtype=np.float32).reshape(len(rows), -1)
        # Blocked rather than one N x N matrix: at 20k memories the full
        # matrix is 1.6 GB, and this runs on a laptop inside a cron tick.
        parent = list(range(len(ids)))

        def find(i):
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        def union(i, j):
            ri, rj = find(i), find(j)
            if ri != rj:
                parent[max(ri, rj)] = min(ri, rj)

        block = 512
        for start in range(0, len(ids), block):
            chunk = mat[start:start + block]
            sims = chunk @ mat.T
            for local, row in enumerate(sims):
                i = start + local
                # Upper triangle only: each pair is considered exactly once,
                # and a memory is never compared against itself.
                for j in np.nonzero(row[i + 1:] >= threshold)[0]:
                    union(i, i + 1 + int(j))
        groups = {}
        for i in range(len(ids)):
            groups.setdefault(find(i), []).append(ids[i])
        clusters = [g for g in groups.values() if len(g) > 1]
        # Largest first: the biggest redundancies are worth the session's
        # attention before the marginal pairs.
        clusters.sort(key=lambda g: (-len(g), g[0]))
        json.dump({"ok": True, "model": name,
                   "clusters": clusters[:max_clusters],
                   "cluster_total": len(clusters),
                   "scored": len(ids)}, sys.stdout)

    else:
        _fail("bad_request", f"unknown mode: {mode}")
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
