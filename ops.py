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

CHECKPOINT_FORMAT = 'autopilot-checkpoint-v1'

def _checkpoint_doc():
    """Pin the audit chain head: the seal that makes tail truncation detectable."""
    with db() as c:
        row = c.execute('SELECT COUNT(*) n, COALESCE(MAX(id),0) mx FROM audit_events').fetchone()
        total, last_id = row['n'], row['mx']
        head = c.execute('SELECT hash FROM audit_events WHERE id=?', (last_id,)).fetchone() if last_id else None
    body = {'format': CHECKPOINT_FORMAT, 'created_at': utc(),
            'last_event_id': last_id,
            'last_event_hash': head['hash'] if head else '',
            'total_events': total}
    digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    return {'checkpoint': body, 'sha256': digest}

def checkpoint(args=None):
    """Seal a point-in-time audit-chain checkpoint.

    The hash chain is tamper-evident for *modification* but blind to tail
    truncation: deleting the newest events leaves every remaining link valid.
    A checkpoint pins the head hash + event id + count at a moment in time;
    later `verify-chain --checkpoint`, `checkpoint-check`, and `doctor` compare
    against it so history rewriting or event deletion becomes observable.
    """
    if args is not None and getattr(args, 'out', None):
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        out_path = autopilot.ROOT / 'backups'
        out_path.mkdir(parents=True, exist_ok=True)
        out_path = out_path / f"checkpoint-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    doc = _checkpoint_doc()
    fd, tmp = tempfile.mkstemp(prefix='.checkpoint.', dir=str(out_path.parent))
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(json.dumps(doc, sort_keys=True)); f.flush(); os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, str(out_path))
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    body = doc['checkpoint']
    print(json.dumps({'ok': True, 'path': str(out_path), 'sha256': doc['sha256'],
                      'last_event_id': body['last_event_id'], 'total_events': body['total_events']}))

def checkpoint_check(args):
    """Verify a checkpoint file's integrity and its containment in the live chain."""
    path = Path(args.path)
    cp = autopilot._load_checkpoint(str(path))
    problems = []
    with db() as c:
        problems.extend(autopilot.audit_chain_problems(c))
        problems.extend(autopilot.checkpoint_problems(c, cp))
    print(json.dumps({'ok': not problems, 'path': str(path),
                      'last_event_id': cp['last_event_id'],
                      'last_event_hash': cp['last_event_hash'],
                      'total_events': cp['total_events'], 'created_at': cp['created_at'],
                      'problems': problems}, sort_keys=True))
    if problems: sys.exit(1)

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

WORKORDER_FORMAT = 'autopilot-workorder-v1'
# Child tables carried by a work order; 'tasks' is exported separately as
# 'task' since it gets sanitization and merge semantics of its own.
WORKORDER_TABLES = ('task_deps', 'heartbeats', 'receipts', 'notes', 'handoffs')
WORKORDER_ACTIVE_STATUSES = ('claimed', 'running', 'waiting_for_agent')

def _doc_secret_kinds(node):
    """Credential kinds present anywhere in a JSON-ish structure (kind only)."""
    kinds = set()
    if isinstance(node, str):
        for f in autopilot._secret_findings(node):
            kinds.add(f['kind'])
    elif isinstance(node, dict):
        for v in node.values():
            kinds |= _doc_secret_kinds(v)
    elif isinstance(node, list):
        for v in node:
            kinds |= _doc_secret_kinds(v)
    return kinds

def _redact_doc(node):
    if isinstance(node, str):
        return autopilot._redact_secrets(node)
    if isinstance(node, dict):
        return {k: _redact_doc(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_redact_doc(v) for v in node]
    return node

def _apply_workorder_secret_policy(tables, redact, allow, task_id, action):
    """Shared privacy boundary for export/import: block by default, redact or
    allow explicitly. Returns (kinds, tables_after). Audits the decision."""
    kinds = sorted(_doc_secret_kinds(tables))
    if not kinds:
        return [], tables
    if allow:
        autopilot.audit(autopilot.conn(), 'task', task_id, 'secret_allowed',
                        {'fields': ['workorder'], 'kinds': kinds})
        return kinds, tables
    if redact:
        autopilot.audit(autopilot.conn(), 'task', task_id, 'secret_redacted',
                        {'fields': ['workorder'], 'kinds': kinds})
        return kinds, _redact_doc(tables)
    autopilot.audit(autopilot.conn(), 'task', task_id, 'secret_blocked',
                    {'fields': ['workorder'], 'kinds': kinds})
    raise SystemExit(
        'refusing to %s credential-shaped content (%s); re-run with --redact '
        'to %s a redacted copy or --allow-secret to override'
        % (action, ', '.join(kinds), 'write' if action == 'export' else 'import'))

def export_task(args=None):
    """Seal one task's full execution state into a portable work-order file.

    A work order is the provider-neutral unit of cross-boundary recovery: it
    carries the task row, dependency edges in both directions, complete note
    and handoff history, receipts (with their sealed files), and the heartbeat,
    all under one sha256 seal so any Autopilot home can verify integrity before
    importing. The same secret guard that protects shared-memory writes guards
    the export boundary — a credential never leaves the database unredacted.
    """
    tid = args.id
    with db() as c:
        task = c.execute('SELECT * FROM tasks WHERE id=?', (tid,)).fetchone()
        if not task:
            raise SystemExit('task not found: ' + tid)
        def fetch(sql):
            return [dict(r) for r in c.execute(sql, (tid,)).fetchall()]
        tables = {
            'task_deps': [dict(r) for r in c.execute(
                'SELECT * FROM task_deps WHERE task_id=? OR depends_on=?', (tid, tid)).fetchall()],
            'heartbeats': fetch('SELECT * FROM heartbeats WHERE task_id=?'),
            'receipts': fetch('SELECT * FROM receipts WHERE task_id=?'),
            'notes': fetch('SELECT * FROM notes WHERE task_id=? ORDER BY created_at, rowid'),
            'handoffs': fetch('SELECT * FROM handoffs WHERE task_id=? ORDER BY created_at, rowid'),
        }
    receipt_files = {}
    for r in tables['receipts']:
        p = autopilot.RECEIPTS / (r['id'] + '.json')
        receipt_files[r['id']] = p.read_text() if p.exists() else None
    body = {'format': WORKORDER_FORMAT, 'version': 1, 'exported_at': utc(),
            'task_id': tid, 'task': dict(task), 'tables': tables,
            'receipt_files': receipt_files}
    kinds, tables = _apply_workorder_secret_policy(
        tables, getattr(args, 'redact', False), getattr(args, 'allow_secret', False),
        tid, 'export')
    if kinds:
        body['tables'] = tables
        body['secret_kinds'] = kinds
    digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    doc = {**body, 'sha256': digest}
    counts = {t: len(tables[t]) for t in WORKORDER_TABLES}
    with db() as c:
        autopilot.audit(c, 'task', tid, 'workorder_exported',
                        {'sha256': digest, 'counts': counts,
                         **({'secret_kinds': kinds} if kinds else {})})
    if getattr(args, 'out', None):
        out_path = Path(args.out); out_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix='.workorder.', dir=str(out_path.parent))
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(json.dumps(doc, sort_keys=True)); f.flush(); os.fsync(f.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, str(out_path))
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
        print(json.dumps({'ok': True, 'task_id': tid, 'path': str(out_path),
                          'sha256': digest, 'counts': counts,
                          'receipt_files': len(receipt_files),
                          **({'secret_kinds': kinds} if kinds else {})}, sort_keys=True))
    else:
        print(json.dumps(doc, sort_keys=True))

def _load_workorder(path: Path):
    """Read and integrity-check a work-order file; returns its body."""
    if not path.exists(): raise SystemExit(f'work order not found: {path}')
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f'work order is not valid JSON: {e}')
    expected = doc.get('sha256')
    body = {k: v for k, v in doc.items() if k != 'sha256'}
    if body.get('format') != WORKORDER_FORMAT:
        raise SystemExit('unrecognized work-order format')
    actual = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    if actual != expected:
        raise SystemExit('work-order integrity check failed; refusing a tampered file')
    body['sha256'] = expected   # re-attach the verified seal for provenance
    if not isinstance(body.get('task'), dict) or not body['task'].get('id'):
        raise SystemExit('work order has no task row')
    return body

def import_task(args=None):
    """Idempotently merge a verified work order into this Autopilot home.

    Recovery across boundaries must be safe to retry: an identical re-import
    deduplicates instead of duplicating; a changed task refuses without --force,
    and --force merges rather than clobbers (local child rows are preserved).
    An imported task can never arrive still leased — active statuses are reset
    to queued with lease fields cleared, because the previous owner does not
    exist here. Dependency edges are inserted only when both endpoints exist
    locally; dangling ones are reported, never silently dropped. The secret
    guard runs again on import so an override at the source cannot leak
    credentials into this home unnoticed.
    """
    body = _load_workorder(Path(args.path))
    tid = body['task']['id']
    dry = bool(getattr(args, 'dry_run', False))
    force = bool(getattr(args, 'force', False))
    kinds, tables = _apply_workorder_secret_policy(
        body.get('tables', {}), getattr(args, 'redact', False),
        getattr(args, 'allow_secret', False), tid, 'import')
    task_in = dict(body['task'])
    sanitized = task_in['status'] in WORKORDER_ACTIVE_STATUSES
    if sanitized:
        task_in.update(status='queued', lease_owner='', lease_expires_at='',
                       blocked_reason='imported from work order')
    autopilot.ensure()
    with sqlite3.connect(DB, timeout=10) as c:
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA foreign_keys=ON')
        existing = c.execute('SELECT * FROM tasks WHERE id=?', (tid,)).fetchone()
        existing = dict(existing) if existing else None
        identical = False
        if existing:
            identical = existing == task_in and all(
                {r['id']: r for r in tables.get(t, [])} ==
                {r['id']: dict(r) for r in c.execute(
                    f'SELECT * FROM {t} WHERE '
                    + ('task_id=?' if t != 'task_deps' else 'task_id=? OR depends_on=?'),
                    (tid, tid) if t == 'task_deps' else (tid,)).fetchall()}
                for t in ('notes', 'handoffs', 'receipts'))
        if dry:
            print(json.dumps({'ok': True, 'dry_run': True, 'task_id': tid,
                              'exists': existing is not None, 'identical': identical,
                              'would_sanitize_lease': sanitized,
                              'seal_verified': True, 'counts':
                              {t: len(tables.get(t, [])) for t in WORKORDER_TABLES}}))
            return
        if identical:
            with db() as ac:
                autopilot.audit(ac, 'task', tid, 'workorder_import_deduplicated',
                                {'sha256': body['sha256'], 'exported_at': body.get('exported_at')})
            print(json.dumps({'ok': True, 'task_id': tid, 'deduplicated': True,
                              'sanitized': False, 'inserted': {}, 'skipped_deps': []}))
            return
        if existing and not force:
            raise SystemExit(
                f'task {tid} already exists with different state; pass --force to merge')
        c.execute('BEGIN')
        if existing:
            cols = list(task_in.keys())
            c.execute(f'UPDATE tasks SET {",".join(f"{col}=?" for col in cols)} WHERE id=?',
                      [*[task_in[col] for col in cols], tid])
        else:
            cols = list(task_in.keys())
            c.execute(f'INSERT INTO tasks({",".join(cols)}) VALUES({",".join("?" * len(cols))})',
                      [task_in[col] for col in cols])
        inserted = {}
        for t in ('notes', 'handoffs', 'receipts'):
            n = 0
            for row in tables.get(t, []):
                cols = list(row.keys())
                cur = c.execute(
                    f'INSERT OR IGNORE INTO {t}({",".join(cols)}) VALUES({",".join("?" * len(cols))})',
                    [row[k] for k in cols])
                n += cur.rowcount
            inserted[t] = n
        hb = tables.get('heartbeats', [])
        for row in hb:
            cols = list(row.keys())
            c.execute(f'INSERT OR REPLACE INTO heartbeats({",".join(cols)}) VALUES({",".join("?" * len(cols))})',
                      [row[k] for k in cols])
        skipped_deps = []
        for row in tables.get('task_deps', []):
            both = c.execute(
                "SELECT (SELECT COUNT(*) FROM tasks WHERE id IN (?,?)) n",
                (row['task_id'], row['depends_on'])).fetchone()['n'] == 2
            if not both:
                skipped_deps.append(f"{row['task_id']}->{row['depends_on']}")
                continue
            cur = c.execute('INSERT OR IGNORE INTO task_deps(task_id,depends_on,created_at) VALUES(?,?,?)',
                            (row['task_id'], row['depends_on'], row['created_at']))
            inserted['task_deps'] = inserted.get('task_deps', 0) + cur.rowcount
        fk_violations = [dict(r) for r in c.execute('PRAGMA foreign_key_check').fetchall()]
        c.commit()
        autopilot.audit(c, 'task', tid, 'task_imported',
                        {'sha256': body['sha256'], 'exported_at': body.get('exported_at'),
                         'replaced': existing is not None, 'sanitized_lease': sanitized,
                         'inserted': inserted, 'skipped_deps': skipped_deps,
                         **({'secret_kinds': kinds} if kinds else {})})
    written = 0
    for rid, text in (body.get('receipt_files') or {}).items():
        if text is None:
            continue
        target = autopilot.RECEIPTS / (rid + '.json')
        if target.exists():
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
    print(json.dumps({'ok': ok, 'task_id': tid, 'deduplicated': False,
                      'replaced': existing is not None, 'sanitized': sanitized,
                      'inserted': inserted, 'skipped_deps': skipped_deps,
                      'receipt_files_written': written, 'fk_violations': fk_violations}))
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
            p = autopilot.RECEIPTS / f"{r['id']}.json"
            if not p.exists():
                problems.append({'kind': 'receipt_file_missing', 'receipt_id': r['id']})
                continue
            # Tamper-evident receipts: rows sealed with a file_hash must match
            # the bytes on disk. Legacy unsealed rows (file_hash='') are skipped.
            sealed = c.execute("SELECT file_hash FROM receipts WHERE id=?", (r['id'],)).fetchone()
            if sealed and sealed['file_hash']:
                actual = hashlib.sha256(p.read_bytes()).hexdigest()
                if actual != sealed['file_hash']:
                    problems.append({'kind': 'receipt_file_hash_mismatch', 'receipt_id': r['id'],
                                     'expected_sha256': sealed['file_hash'], 'actual_sha256': actual})
        if autopilot.RECEIPTS.exists():
            for p in autopilot.RECEIPTS.glob('*.json'):
                if p.stem not in indexed:
                    problems.append({'kind': 'receipt_row_missing', 'path': p.name})
        problems.extend(autopilot.audit_chain_problems(c))
        # Sealed chain checkpoints: any divergence from a pinned head proves
        # tail truncation or history rewriting (the chain alone cannot).
        backups = autopilot.ROOT / 'backups'
        if backups.exists():
            for p in sorted(backups.glob('checkpoint-*.json')):
                try:
                    cp = autopilot._load_checkpoint(str(p))
                except SystemExit:
                    problems.append({'kind': 'checkpoint_file_invalid', 'path': p.name})
                    continue
                problems.extend(autopilot.checkpoint_problems(c, cp))
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

def handoff_check(args=None):
    """Protocol lint over live handoffs: enforce the handoff contract read-only.

    The handoff protocol promises that every durable handoff carries an
    objective, is addressed to a recipient, carries verified evidence or next
    actions, and — when it cites a `--recall-digest` — that the cited context
    is still fresh. Nothing enforces those promises today; this sweep turns
    violations into observable problems instead of silent drift:

    - unaddressed: no to_agent, so no inbox will ever surface it;
    - missing_objective: the resume point has no stated goal;
    - sparse: neither evidence nor next_actions (a self-report without proof);
    - unproven_recall_digest: the handoff cites a --recall-digest that never
      appears in the audited recall stream (`context_recalled` or
      `session_resumed`) — a fabricated or mistyped citation (self-report
      without proof);
    - older_than_latest_recall: the cited digest was genuinely recalled but a
      newer audited recall for the task exists since, so the handoff may have
      been written against outdated context;
    - terminal_task_handoff: a live handoff on a completed/failed/cancelled
      task — stale recovery bait;
    - stale_unacknowledged: an addressed live handoff older than the ack SLA
      (--ack-sla-hours, default 24) that its recipient never acknowledged —
      inbound work nobody picked up.

    Read-only: reports problems, never mutates. Digest freshness relative to
    *current* durable state is `recall-verify`'s job; this lint checks
    provenance (was it really recalled, and is it the latest recall).
    """
    t = utc()
    sla_hours = getattr(args, 'ack_sla_hours', 24) if args is not None else 24
    try:
        sla_hours = float(sla_hours)
    except (TypeError, ValueError):
        raise SystemExit('--ack-sla-hours must be a number')
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=sla_hours)) \
        .replace(microsecond=0).isoformat()
    problems = []
    with db() as c:
        q = ("SELECT h.id,h.task_id,h.to_agent,h.objective,h.evidence,h.next_actions,"
             "h.recall_digest,h.acked_by,h.created_at,t.status AS task_status FROM handoffs h "
             "JOIN tasks t ON t.id=h.task_id WHERE h.superseded_by=''")
        vals = []
        if getattr(args, 'task', ''):
            q += " AND h.task_id=?"
            vals.append(args.task)
        for r in c.execute(q + " ORDER BY h.created_at DESC", vals).fetchall():
            reasons = []
            if not r['to_agent'].strip():
                reasons.append('unaddressed')
            if not r['objective'].strip():
                reasons.append('missing_objective')
            try:
                evidence = json.loads(r['evidence'] or '[]')
                next_actions = json.loads(r['next_actions'] or '[]')
            except json.JSONDecodeError:
                evidence, next_actions = [], []
            if not evidence and not next_actions:
                reasons.append('sparse_no_evidence_or_next_actions')
            if r['recall_digest']:
                proven = c.execute(
                    "SELECT 1 FROM audit_events WHERE entity_type='task' AND entity_id=? "
                    "AND action IN ('context_recalled','session_resumed') "
                    "AND payload_json LIKE ? LIMIT 1",
                    (r['task_id'], '%"digest": "' + r['recall_digest'] + '"%')).fetchone()
                if not proven:
                    reasons.append('unproven_recall_digest')
                else:
                    latest = c.execute(
                        "SELECT payload_json FROM audit_events WHERE entity_type='task' "
                        "AND entity_id=? AND action IN ('context_recalled','session_resumed') "
                        "ORDER BY id DESC LIMIT 1", (r['task_id'],)).fetchone()
                    if latest and json.loads(latest['payload_json']).get('digest') != r['recall_digest']:
                        reasons.append('older_than_latest_recall')
            if r['task_status'] in ('completed', 'failed', 'cancelled'):
                reasons.append('terminal_task_handoff')
            if r['to_agent'].strip() and not r['acked_by'] and r['created_at'] <= cutoff:
                reasons.append('stale_unacknowledged')
            if reasons:
                problems.append({'handoff_id': r['id'], 'task_id': r['task_id'],
                                 'reasons': sorted(reasons)})
    print(json.dumps({'ok': not problems, 'generated_at': t,
                      'problems': problems, 'count': len(problems)}, sort_keys=True))

def recall_stale(args=None):
    """Fleet-wide freshness sweep: which live handoffs cite drifted context?

    `recall-verify` answers freshness for one task and one digest the caller
    already holds; this sweep answers the operator question across the whole
    fleet: for every *live* handoff citing a --recall-digest, recompute the
    task's current recall bundle exactly as it was originally recalled (the
    audited event stores the bundle parameters — budget, related, scope,
    agent, and the optional temporal-rerank settings) and compare digests.

    Per-item states:
    - fresh: the cited recall's *core* context (everything except the handoff
      section — the agent's own post-recall handoff is not self-drift) still
      matches current durable state;
    - stale: notes, receipts, lease state, or deps have moved since the cited
      recall; the item carries the recomputed `current_digest` so the next
      agent can re-recall before acting;
    - unproven_recall_digest: no audited recall/resume event ever produced
      the cited digest (fabricated or mistyped);
    - unknown_recall_params: the digest was proven by a legacy event recorded
      before parameter/core capture; freshness cannot be recomputed exactly.

    Read-only: reports problems, never mutates.
    """
    t = utc()
    items = []
    with db() as c:
        rows = c.execute(
            "SELECT h.id AS handoff_id,h.task_id,h.recall_digest "
            "FROM handoffs h JOIN tasks t ON t.id=h.task_id "
            "WHERE h.superseded_by='' AND h.recall_digest!='' ORDER BY h.task_id,h.id").fetchall()
        for r in rows:
            item = {'handoff_id': r['handoff_id'], 'task_id': r['task_id'],
                    'recall_digest': r['recall_digest']}
            ev = c.execute(
                "SELECT payload_json FROM audit_events WHERE entity_type='task' AND entity_id=? "
                "AND action IN ('context_recalled','session_resumed') "
                "AND payload_json LIKE ? ORDER BY id DESC LIMIT 1",
                (r['task_id'], '%"digest": "' + r['recall_digest'] + '"%')).fetchone()
            if not ev:
                item['state'] = 'unproven_recall_digest'
            else:
                payload = json.loads(ev['payload_json'])
                params = {k: payload.get(k) for k in ('budget', 'related', 'related_scope')}
                # --related-handoffs is optional (added after the provenance
                # loop); absent means the original bundle was built without it.
                rel_handoffs = payload.get('related_handoffs') or 0
                # Rerank parameters are optional (feature added after the
                # provenance loop); absent keys mean the original bundle was
                # built without rerank, which is exactly how it must be
                # recomputed for the digest comparison to be meaningful.
                rerank = bool(payload.get('rerank'))
                half_life = payload.get('recency_half_life_hours')
                boost = payload.get('pinned_boost')
                if rerank and (half_life is None or boost is None):
                    item['state'] = 'unknown_recall_params'
                    items.append(item)
                    continue
                if any(v is None for v in params.values()) or not payload.get('core_digest'):
                    item['state'] = 'unknown_recall_params'
                else:
                    bundle = autopilot._build_recall_bundle(
                        c, r['task_id'], payload.get('agent') or '',
                        params['budget'], params['related'], params['related_scope'],
                        rerank=rerank,
                        recency_half_life_hours=168.0 if half_life is None else half_life,
                        pinned_boost=0.5 if boost is None else boost,
                        rel_handoffs=rel_handoffs)
                    item['state'] = ('fresh' if bundle['core_digest'] == payload['core_digest']
                                     else 'stale')
                    item['current_digest'] = bundle['digest']
            items.append(item)
    counts = {}
    for i in items:
        counts[i['state']] = counts.get(i['state'], 0) + 1
    print(json.dumps({'ok': True, 'generated_at': t, 'checked': len(items),
                      'states': counts, 'items': items}, sort_keys=True))

def notes_expired(args=None):
    """Fleet-wide memory-hygiene sweep: live notes past their TTL.

    Complements `metrics`'s `notes_expired_live` count with the actual rows so
    an operator can see *which* facts aged out of packs and retrieval:

    - unpinned expired notes are retired: hidden from context packs, recall
      bundles, and search by the runtime itself — listed here for review;
    - pinned expired notes stay packed (pinned facts are immortal by design)
      but carry `expired: true`; they need an explicit supersede-note, so they
      are flagged `action: supersede` while retired ones get `action: revive`
      (re-adding the content revives them) or explicit retirement.

    Read-only: reports, never mutates.
    """
    t = utc()
    items = []
    with db() as c:
        rows = c.execute(
            "SELECT n.id,n.task_id,t.project,n.kind,n.content,n.source,n.created_at,"
            "n.pinned,n.expires_at FROM notes n JOIN tasks t ON t.id=n.task_id "
            "WHERE n.superseded_by='' AND n.expires_at!='' AND n.expires_at<=? "
            "ORDER BY n.expires_at,n.id", (t,)).fetchall()
        for r in rows:
            items.append({**dict(r), 'expired': True,
                          'retired': not r['pinned'],
                          'action': 'supersede' if r['pinned'] else 'revive'})
    print(json.dumps({'ok': True, 'generated_at': t, 'count': len(items),
                      'pinned_expired': sum(1 for i in items if i['pinned']),
                      'items': items}, sort_keys=True))

def secret_scan(args=None):
    """Fleet sweep for credential-shaped content already stored in shared memory.

    The write path (`note`, `supersede-note`, `handoff`) blocks credential-shaped
    content unless explicitly redacted or overridden — but rows written before
    that guard existed (or via --allow-secret) can still sit in shared memory,
    packing into every context bundle. This sweep scans live notes and live
    handoffs (objective + list fields) with the same detector the write path
    uses and reports *where* and *what kind* — never the value itself.

    Read-only: remediation is a history-preserving supersede (--redact), so
    operators keep the audit trail while the secret leaves rotation.
    `--all` includes superseded rows, tagged `live: false`.
    """
    t = utc()
    include_all = bool(getattr(args, 'all', False))
    items = []
    with db() as c:
        nq = ("SELECT n.id,n.task_id,t.project,n.kind,n.content,n.created_at,n.superseded_by "
              "FROM notes n JOIN tasks t ON t.id=n.task_id")
        if not include_all:
            nq += " WHERE n.superseded_by=''"
        for r in c.execute(nq + " ORDER BY n.rowid"):
            kinds = sorted({f['kind'] for f in autopilot._secret_findings(r['content'])})
            if kinds:
                items.append({'type': 'note', 'id': r['id'], 'task_id': r['task_id'],
                              'project': r['project'], 'fields': ['content'], 'kinds': kinds,
                              'live': r['superseded_by'] == '',
                              'action': 'supersede-note %s --content <redacted> --redact' % r['id']})
        hq = "SELECT id,task_id,objective,evidence,constraints,decisions,next_actions,risks,superseded_by FROM handoffs"
        if not include_all:
            hq += " WHERE superseded_by=''"
        for r in c.execute(hq + " ORDER BY rowid"):
            fields = {}
            def _scan(field, text):
                if text:
                    kinds = sorted({f['kind'] for f in autopilot._secret_findings(text)})
                    if kinds:
                        fields[field] = kinds
            _scan('objective', r['objective'])
            for col in ('evidence', 'constraints', 'decisions', 'next_actions', 'risks'):
                try:
                    entries = json.loads(r[col])
                except (json.JSONDecodeError, TypeError):
                    continue
                for i, entry in enumerate(entries if isinstance(entries, list) else []):
                    _scan('%s[%d]' % (col, i), entry)
            if fields:
                items.append({'type': 'handoff', 'id': r['id'], 'task_id': r['task_id'],
                              'fields': sorted(fields),
                              'kinds': sorted({k for ks in fields.values() for k in ks}),
                              'live': r['superseded_by'] == '',
                              'action': 'record a redacted replacement handoff'})
    print(json.dumps({'ok': True, 'generated_at': t, 'count': len(items),
                      'notes_flagged': sum(1 for i in items if i['type'] == 'note'),
                      'handoffs_flagged': sum(1 for i in items if i['type'] == 'handoff'),
                      'items': items}, sort_keys=True))

def _cluster_live_notes(rows, threshold):
    """Greedy near-duplicate clustering over one task's live notes.

    Rows arrive oldest→newest; each note joins the first cluster whose
    canonical note it matches at >= threshold token-Jaccard similarity,
    else founds a new cluster. Deterministic: same rows, same clusters.
    """
    clusters = []
    for r in rows:
        toks = autopilot._tokens(r['content'])
        for c in clusters:
            sim = autopilot._jaccard(toks, autopilot._tokens(c['kept']['content']))
            if sim >= threshold:
                c['members'].append((r, round(sim, 3)))
                break
        else:
            clusters.append({'kept': r, 'members': []})
    return clusters

def consolidate(args=None):
    """Memory consolidation: merge near-duplicate live notes into canonical facts.

    The `note` command's near-duplicate guard only *flags* rephrased
    restatements; over weeks of agent traffic shared memory still accumulates
    near-identical facts that all pack into context budgets and all surface in
    retrieval. This sweep finishes the job: live notes on a task are clustered
    by token-Jaccard similarity (same measure as the guard, default threshold
    from AUTOPILOT_NEAR_DUP_THRESHOLD, override with --threshold), and every
    non-canonical member is superseded into its cluster's canonical note —
    the pinned note when the cluster has one, else the newest. Superseded
    losers keep their audit trail via note-history; retrieval and context
    packs shrink immediately because they filter on superseded_by.

    Retired (expired unpinned) notes are invisible already and never cluster.
    Each consolidation is guarded (`WHERE superseded_by=''`) so a concurrent
    supersede wins safely, and audited as `note_consolidated`. `--dry-run`
    previews the plan without mutating; `--task` scopes the sweep to one task.
    """
    t = utc()
    threshold = getattr(args, 'threshold', None) if args is not None else None
    if threshold is None:
        threshold = autopilot._near_dup_threshold()
    try:
        threshold = float(threshold)
    except (TypeError, ValueError):
        raise SystemExit('--threshold must be a number between 0 and 1')
    if not 0 < threshold <= 1:
        raise SystemExit('--threshold must be a number between 0 and 1')
    dry = bool(getattr(args, 'dry_run', False))
    scope_task = getattr(args, 'task', '') or ''
    clusters_out = []
    consolidated = 0
    tasks_scanned = 0
    with db() as c:
        q = ("SELECT n.id,n.task_id,n.kind,n.content,n.source,n.created_at,n.pinned,n.expires_at,n.rowid AS ord "
             "FROM notes n WHERE n.superseded_by='' "
             "AND (n.expires_at='' OR n.expires_at>? OR n.pinned=1)")
        vals = [t]
        if scope_task:
            if not c.execute("SELECT 1 FROM tasks WHERE id=?", (scope_task,)).fetchone():
                raise SystemExit(f'task not found: {scope_task}')
            q += " AND n.task_id=?"
            vals.append(scope_task)
        q += " ORDER BY n.task_id,n.created_at,n.rowid"
        by_task = {}
        for r in c.execute(q, vals).fetchall():
            by_task.setdefault(r['task_id'], []).append(dict(r))
        for task_id in sorted(by_task):
            rows = by_task[task_id]
            tasks_scanned += 1
            for cl in _cluster_live_notes(rows, threshold):
                members = cl['members']
                if not members:
                    continue
                # Canonical: pinned beats unpinned, then newest wins. It may be
                # a member rather than the founder, so the loser pool must
                # include the founder too. rowid breaks same-second ties
                # deterministically (created_at is second-precision; ids are
                # random uuids).
                candidates = [cl['kept']] + [m for m, _ in members]
                kept = max(candidates, key=lambda r: (r['pinned'], r['created_at'], r['ord']))
                pool = [(cl['kept'], round(autopilot._jaccard(
                            autopilot._tokens(kept['content']),
                            autopilot._tokens(cl['kept']['content'])), 3))]
                pool += sorted(members, key=lambda x: x[0]['id'])
                losers = [(m, sim) for m, sim in pool if m['id'] != kept['id']]
                if not losers:
                    continue
                entry = {'task_id': task_id, 'kept_note_id': kept['id'],
                         'consolidated': [{'note_id': m['id'], 'kind': m['kind'],
                                           'pinned': bool(m['pinned']), 'similarity': sim}
                                          for m, sim in losers]}
                clusters_out.append(entry)
                if dry:
                    continue
                for m, sim in losers:
                    if m['id'] == kept['id']:
                        continue
                    cur = c.execute(
                        "UPDATE notes SET superseded_by=? WHERE id=? AND superseded_by=''",
                        (kept['id'], m['id']))
                    if cur.rowcount != 1:
                        continue  # concurrently superseded; the winner keeps history
                    autopilot.audit(c, 'task', task_id, 'note_consolidated',
                                    {'note_id': m['id'], 'kept_note_id': kept['id'],
                                     'kind': m['kind'], 'similarity': sim})
                    consolidated += 1
    print(json.dumps({'ok': True, 'dry_run': dry, 'generated_at': t,
                      'threshold': threshold, 'tasks_scanned': tasks_scanned,
                      'clusters': clusters_out, 'consolidated_count': consolidated},
                     sort_keys=True))

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
x=s.add_parser('handoff-check'); x.add_argument('--task',default=''); x.add_argument('--ack-sla-hours',dest='ack_sla_hours',type=float,default=24); x.set_defaults(fn=handoff_check)
x=s.add_parser('recall-stale'); x.set_defaults(fn=recall_stale)
x=s.add_parser('notes-expired'); x.set_defaults(fn=notes_expired)
x=s.add_parser('secret-scan'); x.add_argument('--all',action='store_true'); x.set_defaults(fn=secret_scan)
x=s.add_parser('consolidate'); x.add_argument('--task',default=''); x.add_argument('--threshold',type=float,default=None); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=consolidate)
x=s.add_parser('recover'); x.add_argument('--max-retries',type=int,default=3); x.add_argument('--backoff-base',type=int,default=60); x.add_argument('--backoff-cap',type=int,default=3600); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=recover)
x=s.add_parser('escalate'); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=escalate)
x=s.add_parser('snapshot'); x.add_argument('--out',default=None); x.set_defaults(fn=snapshot)
x=s.add_parser('snapshot-check'); x.add_argument('path'); x.set_defaults(fn=snapshot_check)
x=s.add_parser('checkpoint'); x.add_argument('--out',default=None); x.set_defaults(fn=checkpoint)
x=s.add_parser('checkpoint-check'); x.add_argument('path'); x.set_defaults(fn=checkpoint_check)
x=s.add_parser('snapshot-restore'); x.add_argument('path'); x.add_argument('--force',action='store_true'); x.set_defaults(fn=snapshot_restore)
x=s.add_parser('archive'); x.add_argument('--before',required=True); x.add_argument('--out',default=None); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=archive)
x=s.add_parser('archive-check'); x.add_argument('path'); x.set_defaults(fn=archive_check)
x=s.add_parser('archive-restore'); x.add_argument('path'); x.add_argument('--force',action='store_true'); x.set_defaults(fn=archive_restore)
x=s.add_parser('export-task'); x.add_argument('id'); x.add_argument('--out',default=None); x.add_argument('--redact',action='store_true'); x.add_argument('--allow-secret',action='store_true'); x.set_defaults(fn=export_task)
x=s.add_parser('import-task'); x.add_argument('path'); x.add_argument('--force',action='store_true'); x.add_argument('--redact',action='store_true'); x.add_argument('--allow-secret',action='store_true'); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=import_task)
x=s.add_parser('approval'); x.add_argument('action',choices=['approve','reject','block']); x.add_argument('id'); x.add_argument('--by',default='leo'); x.add_argument('--reason',default=''); x.add_argument('--next-action',default=''); x.set_defaults(fn=approval)
x=s.add_parser('policy'); x.add_argument('project'); x.add_argument('action'); x.set_defaults(fn=policy)
args=p.parse_args(); args.fn(args)
