#!/usr/bin/env python3
"""Autopilot v1.1 safe operations: recovery, approvals, reconciliation, reports."""
from __future__ import annotations
import json, os, re, sqlite3, subprocess, sys, tempfile, urllib.request, hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import autopilot

DB = autopilot.DB

def db(): return autopilot.conn()
def utc(): return autopilot.now()
def run(cmd, cwd=None):
    try:
        p=subprocess.run(cmd,cwd=cwd,text=True,capture_output=True,timeout=20)
        return p.returncode,p.stdout.strip(),p.stderr.strip()
    except Exception as e: return 1,"",str(e)

def _backoff_deadline(retry_count: int, base: int, cap: int) -> str:
    """Deterministic exponential cooldown after the Nth recovery: base * 2^(N-1), capped."""
    if base <= 0:
        return ''
    delay = min(base * (2 ** (retry_count - 1)), cap)
    dt = datetime.now(timezone.utc) + timedelta(seconds=delay)
    return dt.replace(microsecond=0).isoformat()

def recover(args=None):
    now=utc(); recovered=[]; failed=[]
    max_retries = getattr(args, 'max_retries', 3) if args is not None else 3
    dry_run = bool(getattr(args, 'dry_run', False))
    backoff_base = getattr(args, 'backoff_base', 60)
    backoff_cap = getattr(args, 'backoff_cap', 3600)
    plan = []
    with db() as c:
        rows=c.execute("SELECT id,lease_owner,lease_expires_at,status,retry_count FROM tasks WHERE lease_expires_at!='' AND lease_expires_at<=? AND status IN ('claimed','running','waiting_for_agent')",(now,)).fetchall()
        for r in rows:
            new_retry = r['retry_count'] + 1
            if new_retry > max_retries:
                # Retry budget exhausted: fail the task instead of looping forever.
                plan.append(('failed', r, new_retry, ''))
            else:
                # Exponential cooldown so a repeatedly failing task cannot
                # hot-loop through dispatch; --backoff-base 0 disables it.
                plan.append(('recovered', r, new_retry,
                             _backoff_deadline(new_retry, backoff_base, backoff_cap)))
        if dry_run:
            # Non-destructive preview: report what the next real pass would do.
            print(json.dumps({'ok':True,'dry_run':True,
                'would_recover':[r['id'] for kind,r,_,_ in plan if kind=='recovered'],
                'would_fail':[r['id'] for kind,r,_,_ in plan if kind=='failed'],
                'backoff':{r['id']:ra for kind,r,_,ra in plan if kind=='recovered' and ra},
                'count':len(plan)}))
            return
        for kind, r, new_retry, ra in plan:
            if kind == 'failed':
                c.execute("UPDATE tasks SET status='failed',lease_owner='',lease_expires_at='',retry_count=?,blocked_reason='max lease retries exceeded',updated_at=? WHERE id=?",(new_retry,now,r['id']))
                autopilot.audit(c, 'task', r['id'], 'lease_failed', {'previous_owner': r['lease_owner'], 'previous_status': r['status'], 'retry_count': new_retry})
                failed.append(r['id'])
            else:
                c.execute("UPDATE tasks SET status='queued',lease_owner='',lease_expires_at='',retry_count=?,recover_after=?,blocked_reason='stale lease recovered',updated_at=? WHERE id=?",(new_retry,ra,now,r['id']))
                autopilot.audit(c, 'task', r['id'], 'lease_recovered', {'previous_owner': r['lease_owner'], 'previous_status': r['status'], 'retry_count': new_retry, 'recover_after': ra})
                recovered.append(r['id'])
    print(json.dumps({'ok':True,'recovered':recovered,'failed':failed,
                      'backoff':{r['id']:ra for kind,r,_,ra in plan if kind=='recovered' and ra},
                      'count':len(recovered)+len(failed)}))

ESCALATION_ORDER = ['P3', 'P2', 'P1', 'P0']

def escalate(args=None):
    """Deadline SLA sweep: overdue non-terminal tasks climb one priority level.

    Dispatch already orders by priority then earliest deadline, but a stale P3
    task that misses its deadline keeps losing dispatch races to fresh P2 work
    forever. Each `escalate` pass moves every non-terminal task whose due_at has
    passed up exactly one priority level (P3→P2→P1→P0); tasks already at P0 are
    reported as `already_p0` instead of being silently stuck. Repeated passes
    therefore converge an ignored overdue task toward the front of the queue,
    and every bump is audited as `priority_escalated` with the deadline as
    provenance. `--dry-run` previews without mutating.
    """
    dry = bool(getattr(args, 'dry_run', False))
    t = utc()
    changed = []
    already_p0 = []
    with db() as c:
        rows = c.execute(
            "SELECT id,priority,due_at FROM tasks "
            "WHERE due_at!='' AND due_at<=? AND status NOT IN ('completed','failed','cancelled') "
            "ORDER BY due_at", (t,)).fetchall()
        for r in rows:
            idx = ESCALATION_ORDER.index(r['priority']) if r['priority'] in ESCALATION_ORDER else -1
            if idx < 0 or idx == len(ESCALATION_ORDER) - 1:
                already_p0.append(r['id'])
                continue
            changed.append({'task_id': r['id'], 'from_priority': r['priority'],
                            'to_priority': ESCALATION_ORDER[idx + 1], 'due_at': r['due_at']})
        if not dry:
            for ch in changed:
                c.execute("UPDATE tasks SET priority=?,updated_at=? WHERE id=?",
                          (ch['to_priority'], t, ch['task_id']))
                autopilot.audit(c, 'task', ch['task_id'], 'priority_escalated',
                                {'from_priority': ch['from_priority'], 'to_priority': ch['to_priority'],
                                 'due_at': ch['due_at'], 'reason': 'overdue'})
    print(json.dumps({'ok': True, 'dry_run': dry, 'generated_at': t,
                      'escalated': changed, 'already_p0': already_p0,
                      'count': len(changed)}, sort_keys=True))

def approval(args):
    status={'approve':'waiting_for_review','reject':'blocked','block':'blocked'}[args.action]
    reason=args.reason or ('approval rejected' if args.action=='reject' else '')
    with db() as c:
        row=c.execute('SELECT * FROM tasks WHERE id=?',(args.id,)).fetchone()
        if not row: raise SystemExit('task not found: '+args.id)
        c.execute("UPDATE tasks SET status=?,blocked_reason=?,next_action=?,updated_at=? WHERE id=?",(status,reason,args.next_action or row['next_action'],utc(),args.id))
        rid=f'approval-{args.id}-{int(datetime.now().timestamp())}-{autopilot.uuid.uuid4().hex[:6]}'
        c.execute("INSERT INTO receipts(id,task_id,kind,payload_json,created_at) VALUES(?,?,?,?,?)",(rid,args.id,'approval',json.dumps({'action':args.action,'by':args.by,'reason':reason}),utc()))
        autopilot.audit(c, 'task', args.id, 'approval', {'action': args.action, 'by': args.by, 'reason': reason})
    print(json.dumps({'ok':True,'id':args.id,'status':status,'by':args.by}))

def processes(args=None):
    rc,out,err=run(['ps','-axo','pid=,etime=,command='])
    rows=[]
    for line in out.splitlines():
        if re.search(r'claude\s+-p|claude\s+--print|hermes_cli\.main.*(?:chat|cron|serve|gateway)',line,re.I):
            rows.append(re.sub(r'\s+',' ',line.strip())[:300])
    print(json.dumps({'ok':rc==0,'active_agent_processes':rows,'count':len(rows)}))

def github(args=None):
    changes=[]
    with db() as c:
        rows=c.execute("SELECT id,pr_url,status FROM tasks WHERE pr_url!='' AND status NOT IN ('completed','cancelled')").fetchall()
        for r in rows:
            m=re.search(r'github\.com/([^/]+/[^/]+)/pull/(\d+)',r['pr_url'])
            if not m: continue
            repo,num=m.groups(); rc,out,err=run(['gh','pr','view',num,'--repo',repo,'--json','state,mergeStateStatus,statusCheckRollup,url'])
            if rc!=0: continue
            try: data=json.loads(out)
            except: continue
            changes.append({'task':r['id'],'pr':data})
    print(json.dumps({'ok':True,'tracked':changes},sort_keys=True))

def sentry(args=None):
    # Read-only intake. Creates queued tasks; never resolves or edits Sentry.
    token_cmd=['security','find-generic-password','-a','trove','-s','sentry','-w']
    rc,tok,_=run(token_cmd)
    if rc!=0 or not tok: print(json.dumps({'ok':False,'error':'sentry credential unavailable'})); return
    url='https://sentry.io/api/0/organizations/trove-ur/issues/?query=is%3Aunresolved&project=trove&limit=100&sort=date'
    req=urllib.request.Request(url,headers={'Authorization':'Bearer '+tok,'Accept':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=30) as r: issues=json.load(r)
    except Exception as e:
        print(json.dumps({'ok':False,'error':type(e).__name__})); return
    created=[]
    with db() as c:
        for i in issues:
            sid=i.get('shortId','unknown'); tid='sentry-'+sid.lower().replace('-','-')
            if c.execute('SELECT 1 FROM tasks WHERE id=?',(tid,)).fetchone(): continue
            title=i.get('title') or 'Sentry issue'
            t=utc(); c.execute("INSERT INTO tasks(id,project,title,description,owner,status,priority,next_action,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(tid,'Trove',title,f"Sentry {sid}: {i.get('permalink','')}",'hermes','queued','P1','Fetch issue evidence and classify',t,t)); created.append(tid)
    print(json.dumps({'ok':True,'unresolved':len(issues),'created':created}))

def morning(args=None):
    print('IDEATORX AUTOPILOT MORNING BRIEF — '+utc())
    autopilot.dashboard(argparse.Namespace())
    print('\nSAFE AUTOMATION: read-only reconciliation, no deploy/merge/external submission.')

SNAPSHOT_TABLES = ('tasks', 'heartbeats', 'receipts', 'notes', 'handoffs', 'task_deps', 'audit_events')
RESTORE_ORDER = ('tasks', 'task_deps', 'heartbeats', 'receipts', 'notes', 'handoffs', 'audit_events')

def _snapshot_doc():
    data = {}
    with db() as c:
        for table in SNAPSHOT_TABLES:
            data[table] = [dict(r) for r in c.execute(f'SELECT * FROM {table}')]
    body = {'format': 'autopilot-snapshot-v1', 'created_at': utc(), 'tables': data}
    digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    return {'snapshot': body, 'sha256': digest}

def snapshot(args=None):
    """Consistent point-in-time JSON export of every table, integrity-sealed."""
    if args is not None and getattr(args, 'out', None):
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        out_path = autopilot.ROOT / 'backups'
        out_path.mkdir(parents=True, exist_ok=True)
        out_path = out_path / f"snapshot-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    doc = _snapshot_doc()
    fd, tmp = tempfile.mkstemp(prefix='.snapshot.', dir=str(out_path.parent))
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(json.dumps(doc, sort_keys=True)); f.flush(); os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, str(out_path))
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    counts = {t: len(doc['snapshot']['tables'][t]) for t in SNAPSHOT_TABLES}
    print(json.dumps({'ok': True, 'path': str(out_path), 'sha256': doc['sha256'], 'counts': counts}))

def _load_snapshot(path: Path):
    """Read and integrity-check a snapshot file; returns (body, counts)."""
    if not path.exists(): raise SystemExit(f'snapshot not found: {path}')
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f'snapshot is not valid JSON: {e}')
    body = doc.get('snapshot')
    if not body or body.get('format') != 'autopilot-snapshot-v1':
        raise SystemExit('unrecognized snapshot format')
    recomputed = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    if recomputed != doc.get('sha256'):
        raise SystemExit('snapshot integrity check failed; refusing to use a tampered file')
    return body, {t: len(body.get('tables', {}).get(t, [])) for t in SNAPSHOT_TABLES}

def snapshot_restore(args):
    """Disaster-recovery restore: rebuild the database from an integrity-checked snapshot.

    Refuses to touch a non-empty target unless --force. After loading, foreign-key
    consistency and the audit hash chain are re-verified.
    """
    body, counts = _load_snapshot(Path(args.path))
    autopilot.ensure()
    with sqlite3.connect(DB, timeout=10) as c:
        c.row_factory = sqlite3.Row
        existing = sum(c.execute(f'SELECT COUNT(*) n FROM {t}').fetchone()['n'] for t in SNAPSHOT_TABLES)
        if existing and not getattr(args, 'force', False):
            raise SystemExit(f'target database is not empty ({existing} rows); pass --force to overwrite')
        c.execute('PRAGMA foreign_keys=OFF')
        c.execute('BEGIN')
        for t in RESTORE_ORDER:
            c.execute(f'DELETE FROM {t}')
            for row in body.get('tables', {}).get(t, []):
                cols = list(row.keys())
                c.execute(f'INSERT INTO {t}({",".join(cols)}) VALUES({",".join("?" * len(cols))})',
                          [row[k] for k in cols])
        fk_violations = [dict(r) for r in c.execute('PRAGMA foreign_key_check').fetchall()]
        chain_problems = autopilot.audit_chain_problems(c)
        c.commit()
    ok = not fk_violations and not chain_problems
    print(json.dumps({'ok': ok, 'path': str(args.path), 'restored': counts,
                      'total_rows': sum(counts.values()),
                      'fk_violations': fk_violations, 'audit_chain_problems': chain_problems}))
    if not ok: sys.exit(1)

def snapshot_check(args):
    """Verify a snapshot file's self-hash without touching any database."""
    path = Path(args.path)
    if not path.exists(): raise SystemExit(f'snapshot not found: {path}')
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f'snapshot is not valid JSON: {e}')
    body = doc.get('snapshot')
    if not body or body.get('format') != 'autopilot-snapshot-v1':
        raise SystemExit('unrecognized snapshot format')
    recomputed = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    ok = recomputed == doc.get('sha256')
    counts = {t: len(body.get('tables', {}).get(t, [])) for t in SNAPSHOT_TABLES}
    print(json.dumps({'ok': ok, 'path': str(path),
                      'expected_sha256': doc.get('sha256'), 'actual_sha256': recomputed,
                      'created_at': body.get('created_at'), 'counts': counts}))
    if not ok: sys.exit(1)

ARCHIVE_TABLES = ('tasks', 'task_deps', 'heartbeats', 'receipts', 'notes', 'handoffs')
ARCHIVE_FORMAT = 'autopilot-archive-v1'

def archive(args=None):
    """Retention / memory consolidation: seal terminal tasks into an
    integrity-checked archive file, then remove them from the live database.

    Tasks with status completed/failed/cancelled and updated_at <= --before
    are eligible. Refuses when a live (non-archived) task still depends on an
    archived candidate. Audit events are *retained* in the live database — the
    hash chain is append-only tamper-evident history — but copies are included
    in the archive for reference. Receipt files move into the archive document.
    """
    before = autopilot._normalize_iso(getattr(args, 'before', ''), '--before')
    if not before:
        raise SystemExit('--before requires an ISO 8601 cutoff applied to updated_at')
    dry = bool(getattr(args, 'dry_run', False))
    with db() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT * FROM tasks WHERE status IN ('completed','failed','cancelled') AND updated_at<=? "
            "ORDER BY updated_at", (before,)).fetchall()]
        ids = [r['id'] for r in rows]
        if not ids:
            print(json.dumps({'ok': True, 'archived': [], 'dry_run': dry, 'path': None, 'counts': {}}))
            return
        ph = ','.join('?' * len(ids))
        blockers = [dict(r) for r in c.execute(
            f"SELECT DISTINCT task_id, depends_on FROM task_deps "
            f"WHERE depends_on IN ({ph}) AND task_id NOT IN ({ph})", (*ids, *ids)).fetchall()]
        if blockers:
            detail = ', '.join(f"{b['task_id']} -> {b['depends_on']}" for b in blockers)
            raise SystemExit('refusing to archive: live tasks still depend on terminal candidates: ' + detail)
        def fetch(sql, params=None):
            return [dict(r) for r in c.execute(sql, params if params is not None else ids).fetchall()]
        tables = {
            'tasks': rows,
            'task_deps': fetch(f'SELECT * FROM task_deps WHERE task_id IN ({ph}) OR depends_on IN ({ph})', (*ids, *ids)),
            'heartbeats': fetch(f'SELECT * FROM heartbeats WHERE task_id IN ({ph})'),
            'receipts': fetch(f'SELECT * FROM receipts WHERE task_id IN ({ph})'),
            'notes': fetch(f'SELECT * FROM notes WHERE task_id IN ({ph})'),
            'handoffs': fetch(f'SELECT * FROM handoffs WHERE task_id IN ({ph})'),
        }
        audit_retained = c.execute(
            f"SELECT COUNT(*) n FROM audit_events WHERE entity_type='task' AND entity_id IN ({ph})",
            ids).fetchone()['n']
    receipt_files = {}
    for r in tables['receipts']:
        p = autopilot.RECEIPTS / (r['id'] + '.json')
        receipt_files[r['id']] = p.read_text() if p.exists() else None
    doc = {'format': ARCHIVE_FORMAT, 'created_at': utc(), 'before': before,
           'tables': tables, 'receipt_files': receipt_files,
           'audit_events_retained': audit_retained}
    digest = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()
    doc['sha256'] = digest
    counts = {t: len(tables[t]) for t in ARCHIVE_TABLES}
    if dry:
        print(json.dumps({'ok': True, 'dry_run': True, 'task_ids': ids, 'counts': counts,
                          'receipt_files': len(receipt_files), 'audit_events_retained': audit_retained}))
        return
    if getattr(args, 'out', None):
        out_path = Path(args.out); out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        out_path = autopilot.ROOT / 'backups'
        out_path.mkdir(parents=True, exist_ok=True)
        out_path = out_path / f"archive-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    fd, tmp = tempfile.mkstemp(prefix='.archive.', dir=str(out_path.parent))
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(json.dumps(doc, sort_keys=True)); f.flush(); os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, str(out_path))
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    # Seal-then-destroy: the verified archive exists on disk before any deletion.
    with db() as c:
        c.execute(f'DELETE FROM task_deps WHERE task_id IN ({ph}) OR depends_on IN ({ph})', (*ids, *ids))
        for t in ('heartbeats', 'receipts', 'notes', 'handoffs'):
            c.execute(f'DELETE FROM {t} WHERE task_id IN ({ph})', ids)
        c.execute(f'DELETE FROM tasks WHERE id IN ({ph})', ids)
    removed = 0
    for rid, text in receipt_files.items():
        p = autopilot.RECEIPTS / (rid + '.json')
        if text is not None and p.exists():
            p.unlink(); removed += 1
    print(json.dumps({'ok': True, 'archived': ids, 'path': str(out_path), 'sha256': digest,
                      'counts': counts, 'receipt_files_removed': removed,
                      'audit_events_retained': audit_retained}))

def _load_archive(path: Path):
    """Read and integrity-check an archive file; returns its body."""
    if not path.exists(): raise SystemExit(f'archive not found: {path}')
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f'archive is not valid JSON: {e}')
    expected = doc.get('sha256')
    body = {k: v for k, v in doc.items() if k != 'sha256'}
    if body.get('format') != ARCHIVE_FORMAT:
        raise SystemExit('unrecognized archive format')
    actual = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    if actual != expected:
        raise SystemExit('archive integrity check failed; refusing to use a tampered file')
    return body

def archive_check(args):
    """Verify an archive file's self-hash without touching any database."""
    body = _load_archive(Path(args.path))
    tables = body.get('tables', {})
    print(json.dumps({'ok': True, 'path': str(args.path), 'created_at': body.get('created_at'),
                      'before': body.get('before'),
                      'counts': {t: len(tables.get(t, [])) for t in ARCHIVE_TABLES},
                      'audit_events_retained': body.get('audit_events_retained', 0)}))

def archive_restore(args):
    """Re-import archived tasks into the live database after verifying integrity.

    Refuses when any archived task id already exists unless --force (which
    replaces those tasks and their dependent rows via cascade). Receipt files
    are recreated atomically; FTS triggers re-index restored notes/tasks.
    """
    body = _load_archive(Path(args.path))
    tables = body.get('tables', {})
    ids = [t['id'] for t in tables.get('tasks', [])]
    force = bool(getattr(args, 'force', False))
    autopilot.ensure()
    ph = ','.join('?' * len(ids)) if ids else ''
    with sqlite3.connect(DB, timeout=10) as c:
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA foreign_keys=ON')
        existing = [r[0] for r in c.execute(
            f'SELECT id FROM tasks WHERE id IN ({ph})', ids).fetchall()] if ids else []
        if existing and not force:
            raise SystemExit('tasks already exist in database: %s; pass --force to replace'
                             % ', '.join(sorted(existing)))
        c.execute('BEGIN')
        if existing:
            c.execute(f'DELETE FROM tasks WHERE id IN ({ph})', existing)
        for t in ARCHIVE_TABLES:
            for row in tables.get(t, []):
                cols = list(row.keys())
                c.execute(f'INSERT INTO {t}({",".join(cols)}) VALUES({",".join("?" * len(cols))})',
                          [row[k] for k in cols])
        fk_violations = [dict(r) for r in c.execute('PRAGMA foreign_key_check').fetchall()]
        c.commit()
    written = 0
    for rid, text in (body.get('receipt_files') or {}).items():
        if text is None:
            continue
        target = autopilot.RECEIPTS / (rid + '.json')
        if target.exists() and not force:
            continue
        fd, tmp = tempfile.mkstemp(prefix=f'.{rid}.', dir=str(autopilot.RECEIPTS))
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(text); f.flush(); os.fsync(f.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, str(target))
            written += 1
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
    ok = not fk_violations
    print(json.dumps({'ok': ok, 'path': str(args.path), 'restored_tasks': ids,
                      'restored': {t: len(tables.get(t, [])) for t in ARCHIVE_TABLES},
                      'replaced': sorted(existing), 'receipt_files_written': written,
                      'fk_violations': fk_violations}))
    if not ok: sys.exit(1)

def doctor(args=None):
    """Read-only consistency sweep: orphan deps, receipt index/files, audit chain, stale leases."""
    problems = []
    with db() as c:
        for r in c.execute(
            "SELECT d.task_id,d.depends_on FROM task_deps d "
            "LEFT JOIN tasks a ON a.id=d.task_id LEFT JOIN tasks b ON b.id=d.depends_on "
            "WHERE a.id IS NULL OR b.id IS NULL"):
            problems.append({'kind': 'orphan_dependency', 'task_id': r['task_id'], 'depends_on': r['depends_on']})
        indexed = set()
        for r in c.execute("SELECT id FROM receipts"):
            indexed.add(r['id'])
            if not (autopilot.RECEIPTS / f"{r['id']}.json").exists():
                problems.append({'kind': 'receipt_file_missing', 'receipt_id': r['id']})
        if autopilot.RECEIPTS.exists():
            for p in autopilot.RECEIPTS.glob('*.json'):
                if p.stem not in indexed:
                    problems.append({'kind': 'receipt_row_missing', 'path': p.name})
        problems.extend(autopilot.audit_chain_problems(c))
        stale = c.execute(
            "SELECT id FROM tasks WHERE lease_expires_at!='' AND lease_expires_at<=? "
            "AND status IN ('claimed','running','waiting_for_agent')", (utc(),)).fetchall()
        for r in stale:
            problems.append({'kind': 'stale_lease', 'task_id': r['id']})
        dangling = c.execute(
            "SELECT t.id FROM tasks t WHERE t.last_receipt!='' AND NOT EXISTS("
            "SELECT 1 FROM receipts r WHERE r.id=t.last_receipt)").fetchall()
        for r in dangling:
            problems.append({'kind': 'last_receipt_dangling', 'task_id': r['id']})
        for r in c.execute(
            "SELECT n.id FROM notes n LEFT JOIN tasks t ON t.id=n.task_id WHERE t.id IS NULL"):
            problems.append({'kind': 'orphan_note', 'note_id': r['id']})
        for r in c.execute(
            "SELECT n.id,n.superseded_by FROM notes n "
            "LEFT JOIN notes s ON s.id=n.superseded_by "
            "WHERE n.superseded_by!='' AND s.id IS NULL"):
            problems.append({'kind': 'supersede_target_missing', 'note_id': r['id'], 'superseded_by': r['superseded_by']})
        for r in c.execute(
            "SELECT h.id FROM handoffs h LEFT JOIN tasks t ON t.id=h.task_id WHERE t.id IS NULL"):
            problems.append({'kind': 'orphan_handoff', 'handoff_id': r['id']})
        for r in c.execute(
            "SELECT h.id,h.superseded_by FROM handoffs h "
            "LEFT JOIN handoffs s ON s.id=h.superseded_by "
            "WHERE h.superseded_by!='' AND s.id IS NULL"):
            problems.append({'kind': 'handoff_supersede_target_missing', 'handoff_id': r['id'], 'superseded_by': r['superseded_by']})
        for r in c.execute(
            "SELECT task_id,COUNT(*) n FROM handoffs WHERE superseded_by='' GROUP BY task_id HAVING n>1"):
            problems.append({'kind': 'multiple_live_handoffs', 'task_id': r['task_id'], 'count': r['n']})
        if autopilot._fts_ready(c):
            for table, fts in (('notes', 'notes_fts'), ('tasks', 'tasks_fts')):
                src = c.execute(f'SELECT COUNT(*) n FROM {table}').fetchone()['n']
                idx = c.execute(f'SELECT COUNT(*) n FROM {fts}').fetchone()['n']
                if src != idx:
                    problems.append({'kind': 'fts_index_drift', 'table': table, 'rows': src, 'indexed': idx})
    print(json.dumps({'ok': not problems, 'problems': problems, 'count': len(problems)}, sort_keys=True))

def policy(args):
    path=Path.home()/'.hermes/autopilot/policies'/f'{args.project.lower()}.yaml'
    if not path.exists(): print(json.dumps({'allowed':False,'reason':'no project policy'})); return
    text=path.read_text(); key=args.action+'_requires_user: true'
    requires=key in text
    print(json.dumps({'project':args.project,'action':args.action,'allowed':not requires,'requires_user':requires,'policy':str(path)}))

import argparse
p=argparse.ArgumentParser(); s=p.add_subparsers(dest='cmd',required=True)
for name,fn in [('processes',processes),('github',github),('sentry',sentry),('morning',morning),('doctor',doctor)]:
 x=s.add_parser(name); x.set_defaults(fn=fn)
x=s.add_parser('recover'); x.add_argument('--max-retries',type=int,default=3); x.add_argument('--backoff-base',type=int,default=60); x.add_argument('--backoff-cap',type=int,default=3600); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=recover)
x=s.add_parser('escalate'); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=escalate)
x=s.add_parser('snapshot'); x.add_argument('--out',default=None); x.set_defaults(fn=snapshot)
x=s.add_parser('snapshot-check'); x.add_argument('path'); x.set_defaults(fn=snapshot_check)
x=s.add_parser('snapshot-restore'); x.add_argument('path'); x.add_argument('--force',action='store_true'); x.set_defaults(fn=snapshot_restore)
x=s.add_parser('archive'); x.add_argument('--before',required=True); x.add_argument('--out',default=None); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=archive)
x=s.add_parser('archive-check'); x.add_argument('path'); x.set_defaults(fn=archive_check)
x=s.add_parser('archive-restore'); x.add_argument('path'); x.add_argument('--force',action='store_true'); x.set_defaults(fn=archive_restore)
x=s.add_parser('approval'); x.add_argument('action',choices=['approve','reject','block']); x.add_argument('id'); x.add_argument('--by',default='leo'); x.add_argument('--reason',default=''); x.add_argument('--next-action',default=''); x.set_defaults(fn=approval)
x=s.add_parser('policy'); x.add_argument('project'); x.add_argument('action'); x.set_defaults(fn=policy)
args=p.parse_args(); args.fn(args)
