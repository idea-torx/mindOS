#!/usr/bin/env python3
"""Autopilot v1.1 safe operations: recovery, approvals, reconciliation, reports."""
from __future__ import annotations
import json, os, re, shlex, sqlite3, subprocess, sys, tempfile, urllib.request, urllib.parse, hashlib
import contextlib, io, uuid
from types import SimpleNamespace
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
    now=utc(); recovered=[]; failed=[]; skipped=[]
    max_retries = getattr(args, 'max_retries', 3) if args is not None else 3
    dry_run = bool(getattr(args, 'dry_run', False))
    backoff_base = getattr(args, 'backoff_base', 60)
    backoff_cap = getattr(args, 'backoff_cap', 3600)
    plan = []
    with db() as c:
        rows=c.execute("SELECT id,lease_owner,lease_expires_at,status,retry_count FROM tasks WHERE lease_expires_at!='' AND lease_expires_at<=? AND status IN ('claimed','running','waiting_for_agent') ORDER BY id",(now,)).fetchall()
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
            # Guarded mutation: only reclaim while the lease is exactly as the
            # snapshot saw it. A worker that claimed the task between the sweep's
            # SELECT and this UPDATE keeps its fresh lease — the stale snapshot
            # must not clobber it.
            if kind == 'failed':
                cur=c.execute("UPDATE tasks SET status='failed',lease_owner='',lease_expires_at='',retry_count=?,blocked_reason='max lease retries exceeded',updated_at=? WHERE id=? AND lease_owner=? AND lease_expires_at<=?",(new_retry,now,r['id'],r['lease_owner'],now))
                if cur.rowcount != 1:
                    skipped.append(r['id']); continue
                autopilot.audit(c, 'task', r['id'], 'lease_failed', {'previous_owner': r['lease_owner'], 'previous_status': r['status'], 'retry_count': new_retry})
                failed.append(r['id'])
            else:
                cur=c.execute("UPDATE tasks SET status='queued',lease_owner='',lease_expires_at='',retry_count=?,recover_after=?,blocked_reason='stale lease recovered',updated_at=? WHERE id=? AND lease_owner=? AND lease_expires_at<=?",(new_retry,ra,now,r['id'],r['lease_owner'],now))
                if cur.rowcount != 1:
                    skipped.append(r['id']); continue
                autopilot.audit(c, 'task', r['id'], 'lease_recovered', {'previous_owner': r['lease_owner'], 'previous_status': r['status'], 'retry_count': new_retry, 'recover_after': ra})
                recovered.append(r['id'])
    print(json.dumps({'ok':True,'recovered':recovered,'failed':failed,'skipped':skipped,
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
    skipped = []
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
                # Guarded mutation: the bump only lands while priority and open
                # status are exactly as swept — a task that settled (or was
                # re-prioritized) mid-sweep is skipped, not double-bumped.
                cur = c.execute(
                    "UPDATE tasks SET priority=?,updated_at=? WHERE id=? AND priority=? "
                    "AND status NOT IN ('completed','failed','cancelled')",
                    (ch['to_priority'], t, ch['task_id'], ch['from_priority']))
                if cur.rowcount != 1:
                    skipped.append(ch['task_id'])
                    continue
                autopilot.audit(c, 'task', ch['task_id'], 'priority_escalated',
                                {'from_priority': ch['from_priority'], 'to_priority': ch['to_priority'],
                                 'due_at': ch['due_at'], 'reason': 'overdue'})
    print(json.dumps({'ok': True, 'dry_run': dry, 'generated_at': t,
                      'escalated': [ch for ch in changed if ch['task_id'] not in skipped],
                      'already_p0': already_p0, 'skipped': skipped,
                      'count': len(changed) - len(skipped)}, sort_keys=True))

def approval(args):
    status={'approve':'waiting_for_review','reject':'blocked','block':'blocked'}[args.action]
    reason=args.reason or ('approval rejected' if args.action=='reject' else '')
    rid=f'approval-{args.id}-{int(datetime.now().timestamp())}-{autopilot.uuid.uuid4().hex[:6]}'
    created=utc()
    payload={'action':args.action,'by':args.by,'reason':reason}
    # Seal the approval receipt exactly like `receipt` does: without a written,
    # hash-sealed file every later doctor sweep would report receipt_file_missing
    # for this row forever.
    data=json.dumps({'id':rid,'task_id':args.id,'kind':'approval','created_at':created,'payload':payload},indent=2,sort_keys=True)+'\n'
    file_hash=hashlib.sha256(data.encode()).hexdigest()
    with db() as c:
        row=c.execute('SELECT * FROM tasks WHERE id=?',(args.id,)).fetchone()
        if not row: raise SystemExit('task not found: '+args.id)
        c.execute("UPDATE tasks SET status=?,blocked_reason=?,next_action=?,updated_at=? WHERE id=?",(status,reason,args.next_action or row['next_action'],utc(),args.id))
        c.execute("INSERT INTO receipts(id,task_id,kind,payload_json,created_at,file_hash) VALUES(?,?,?,?,?,?)",(rid,args.id,'approval',json.dumps(payload,sort_keys=True),created,file_hash))
        c.execute("UPDATE tasks SET last_receipt=?,updated_at=? WHERE id=?", (rid, created, args.id))
        autopilot.audit(c, 'task', args.id, 'approval', {'action': args.action, 'by': args.by, 'reason': reason})
    target=autopilot.RECEIPTS/f'{rid}.json'
    fd,tmp=tempfile.mkstemp(prefix=f'.{rid}.',dir=str(autopilot.RECEIPTS))
    try:
        with os.fdopen(fd,'w') as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.chmod(tmp,0o600)
        os.replace(tmp,str(target))
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    print(json.dumps({'ok':True,'id':args.id,'status':status,'by':args.by,'receipt_id':rid}))

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

SNAPSHOT_TABLES = ('tasks', 'heartbeats', 'receipts', 'notes', 'handoffs', 'facts', 'task_deps', 'audit_events')
RESTORE_ORDER = ('tasks', 'task_deps', 'heartbeats', 'receipts', 'notes', 'handoffs', 'facts', 'audit_events')

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
        # Fact-graph provenance is detached, not deleted: fleet knowledge
        # survives the task's retirement, with the affected rows recorded in
        # the archive document for auditability.
        facts_detached = [dict(r) for r in c.execute(
            f"SELECT * FROM facts WHERE task_id IN ({ph})", ids).fetchall()]
    receipt_files = {}
    for r in tables['receipts']:
        p = autopilot.RECEIPTS / (r['id'] + '.json')
        receipt_files[r['id']] = p.read_text() if p.exists() else None
    doc = {'format': ARCHIVE_FORMAT, 'created_at': utc(), 'before': before,
           'tables': tables, 'receipt_files': receipt_files,
           'facts_detached': facts_detached,
           'audit_events_retained': audit_retained}
    digest = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()
    doc['sha256'] = digest
    counts = {t: len(tables[t]) for t in ARCHIVE_TABLES}
    if dry:
        print(json.dumps({'ok': True, 'dry_run': True, 'task_ids': ids, 'counts': counts,
                          'receipt_files': len(receipt_files),
                          'facts_detached': len(facts_detached),
                          'audit_events_retained': audit_retained}))
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
        # Facts are fleet knowledge: detach provenance instead of deleting.
        c.execute(f"UPDATE facts SET task_id='' WHERE task_id IN ({ph})", ids)
        c.execute(f'DELETE FROM tasks WHERE id IN ({ph})', ids)
    removed = 0
    for rid, text in receipt_files.items():
        p = autopilot.RECEIPTS / (rid + '.json')
        if text is not None and p.exists():
            p.unlink(); removed += 1
    print(json.dumps({'ok': True, 'archived': ids, 'path': str(out_path), 'sha256': digest,
                      'counts': counts, 'receipt_files_removed': removed,
                      'facts_detached': len(facts_detached),
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
WORKORDER_TABLES = ('task_deps', 'heartbeats', 'receipts', 'notes', 'handoffs', 'facts')
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
    and handoff history, receipts (with their sealed files), the heartbeat,
    and every temporal fact provenance-linked to the task (validity windows
    intact), all under one sha256 seal so any Autopilot home can verify
    integrity before importing. The same secret guard that protects
    shared-memory writes guards the export boundary — a credential never
    leaves the database unredacted.
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
            'facts': fetch('SELECT * FROM facts WHERE task_id=? ORDER BY created_at, rowid'),
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
    locally; dangling ones are reported, never silently dropped. Provenance-
    linked temporal facts arrive with validity windows intact (facts are fleet
    knowledge keyed by their own id, so INSERT OR IGNORE deduplicates).
    Work orders sealed before the fact graph simply carry no facts key and
    import unchanged. The secret guard runs again on import so an override at
    the source cannot leak credentials into this home unnoticed.
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
                for t in ('notes', 'handoffs', 'receipts', 'facts'))
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
        for t in ('notes', 'handoffs', 'receipts', 'facts'):
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

def _fts_index_docs(c, fts: str):
    """Rowids actually present in an external-content FTS5 index.

    COUNT(*) over an external-content FTS5 table reads through to the content
    table, so it cannot see stale or missing index entries; the fts5vocab
    'instance' table enumerates what the inverted index really holds. The
    vocab table is created in the temp database, so the swept database file
    itself is never written.
    """
    vt = f'temp._drift_vocab_{fts}'
    c.execute(f"CREATE VIRTUAL TABLE {vt} USING fts5vocab('main','{fts}','instance')")
    try:
        return {r[0] for r in c.execute(f'SELECT DISTINCT doc FROM {vt}')}
    finally:
        c.execute(f'DROP TABLE {vt}')

def doctor(args=None):
    """Read-only consistency sweep: orphan deps, receipt index/files, audit chain, stale leases."""
    problems = []
    notes = []
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
        # Fact-graph soft references: archive detaches task provenance when a
        # task retires, so a dangling ref means out-of-band surgery happened.
        for r in c.execute(
            "SELECT f.id,f.task_id FROM facts f LEFT JOIN tasks t ON t.id=f.task_id "
            "WHERE f.task_id!='' AND t.id IS NULL"):
            problems.append({'kind': 'fact_task_missing', 'fact_id': r['id'], 'task_id': r['task_id']})
        for table, fts in (('notes', 'notes_fts'), ('tasks', 'tasks_fts'), ('handoffs', 'handoffs_fts'),
                           ('session_messages', 'session_messages_fts'), ('facts', 'facts_fts'),
                           ('memories', 'memories_fts')):
            ready = (autopilot._handoffs_fts_ready(c) if fts == 'handoffs_fts'
                     else autopilot._sessions_fts_ready(c) if fts == 'session_messages_fts'
                     else autopilot._facts_fts_ready(c) if fts == 'facts_fts'
                     else autopilot._memories_fts_ready(c) if fts == 'memories_fts'
                     else autopilot._fts_ready(c))
            if not ready:
                continue
            src = {r[0] for r in c.execute(f'SELECT rowid FROM {table}')}
            try:
                idx = _fts_index_docs(c, fts)
            except sqlite3.Error:
                idx = None   # fts5vocab unavailable: degrade to the coarse count check
            if idx is None:
                n_idx = c.execute(f'SELECT COUNT(*) n FROM {fts}').fetchone()['n']
                if len(src) != n_idx:
                    problems.append({'kind': 'fts_index_drift', 'table': table,
                                     'rows': len(src), 'indexed': n_idx})
                continue
            if src != idx:
                problems.append({'kind': 'fts_index_drift', 'table': table,
                                 'rows': len(src), 'indexed': len(idx),
                                 'missing_from_index': sorted(src - idx)[:20],
                                 'stale_in_index': sorted(idx - src)[:20]})
        # Semantic memory: a healthy-with-note report on the local store. An
        # empty store degrades --related-semantic to a no-op and must never
        # fail the doctor sweep, so it lands in `notes`, never in `problems`.
        try:
            live = c.execute(
                "SELECT COUNT(*) FROM memories WHERE superseded_by=''").fetchone()[0]
            retracted = c.execute(
                "SELECT COUNT(*) FROM memories WHERE superseded_by<>''").fetchone()[0]
            notes.append({'kind': 'memory_store',
                          'status': 'available' if live else 'empty',
                          'engine': autopilot.MEMORY_ENGINE_TAG,
                          'memories': live, 'retracted': retracted,
                          'fts': autopilot._memories_fts_ready(c),
                          **({} if live else
                             {'note': 'no memories retained; --related-semantic is a no-op'})})
        except sqlite3.Error as e:
            notes.append({'kind': 'memory_store_unreadable', 'error': str(e),
                          'note': '--related-semantic degrades to a no-op'})
    print(json.dumps({'ok': not problems, 'problems': problems, 'count': len(problems),
                      'notes': notes}, sort_keys=True))

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
                # --dep-context is optional the same way; absent means the
                # original bundle was built without prerequisite evidence.
                dep_ctx_n = payload.get('dep_context') or 0
                # --related-sessions / --related-facts are optional the same
                # way; absent means the original bundle was built without them.
                rel_sess_n = payload.get('related_sessions') or 0
                rel_facts_n = payload.get('related_facts') or 0
                # --related-semantic is optional the same way; absent means the
                # original bundle was built without the Hindsight section.
                rel_sema_n = payload.get('related_semantic') or 0
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
                        rel_handoffs=rel_handoffs,
                        dep_context=dep_ctx_n,
                        rel_sessions=rel_sess_n,
                        rel_facts=rel_facts_n,
                        rel_semantic=rel_sema_n)
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

def dup_tasks(args=None):
    """Fleet sweep: cluster open tasks whose text restates the same work.

    Task-layer memory hygiene (the mirror of `consolidate` for notes): two
    open tasks describing the same work split agent effort across two seams,
    both surface in dispatch, and neither inherits the other's context. Open
    (non-terminal) tasks are grouped per project and clustered by token-Jaccard
    similarity over title+description — the same measure and greedy algorithm
    as note consolidation. Unlike notes, tasks cannot be auto-superseded:
    merging them is a lifecycle decision (cancel one, or fold it via dep), so
    this sweep is strictly read-only and reports clusters with a suggested
    action. Deterministic: same rows, same clusters, canonical = oldest.
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
    clusters_out = []
    with db() as c:
        rows = c.execute(
            "SELECT id,project,title,description,status,priority,created_at FROM tasks "
            "WHERE status NOT IN ('completed','failed','cancelled') "
            "ORDER BY project,created_at,id").fetchall()
        by_project = {}
        for r in rows:
            d = dict(r)
            # Reuse the note-clustering machinery: it clusters on 'content'.
            d['content'] = f"{r['title']} {r['description']}".strip()
            by_project.setdefault(r['project'], []).append(d)
        for project in sorted(by_project):
            prows = by_project[project]
            if len(prows) < 2:
                continue
            for cl in _cluster_live_notes(prows, threshold):
                members = cl['members']
                if not members:
                    continue
                kept = cl['kept']
                clusters_out.append({
                    'project': project,
                    'canonical': {'task_id': kept['id'], 'title': kept['title'],
                                  'status': kept['status'], 'created_at': kept['created_at']},
                    'duplicates': [{'task_id': m['id'], 'title': m['title'],
                                    'status': m['status'], 'similarity': sim}
                                   for m, sim in sorted(members, key=lambda x: x[0]['id'])],
                    'suggested_action': 'cancel the duplicates or fold them into the '
                                        'canonical task via dep; record why in a handoff',
                })
    print(json.dumps({'ok': True, 'generated_at': t, 'threshold': threshold,
                      'clusters': clusters_out, 'cluster_count': len(clusters_out),
                      'duplicate_tasks': sum(len(cl['duplicates']) for cl in clusters_out)},
                     sort_keys=True))

def unverified_completions(args=None):
    """Fleet sweep: completions whose execution truth is missing or broken.

    A self-report is never execution truth without a receipt. Two problem
    kinds make that contract observable instead of aspirational:

    - no_receipts: a completed task with zero receipts — the completion is a
      bare agent claim with nothing sealed to back it;
    - evidence_receipt_missing: an audited `completed` event cites
      `--receipt` evidence ids that no longer exist in the receipts table
      (deleted rows, partial restore, archive drift) — the cited proof
      vanished after the fact.

    Read-only: reports, never mutates. Remediation for no_receipts is posting
    the missing evidence (receipt) or re-opening the task if none exists.
    """
    t = utc()
    items = []
    with db() as c:
        rows = c.execute(
            "SELECT t.id,t.project,t.title,t.updated_at,"
            "(SELECT COUNT(*) FROM receipts r WHERE r.task_id=t.id) n "
            "FROM tasks t WHERE t.status='completed' ORDER BY t.updated_at").fetchall()
        for r in rows:
            if r['n'] == 0:
                items.append({'kind': 'no_receipts', 'task_id': r['id'],
                              'project': r['project'], 'title': r['title'],
                              'updated_at': r['updated_at']})
        have = {x['id'] for x in c.execute('SELECT id FROM receipts')}
        for ev in c.execute(
                "SELECT entity_id,payload_json,created_at FROM audit_events "
                "WHERE action='completed' AND payload_json LIKE '%evidence_receipts%' "
                "ORDER BY id").fetchall():
            try:
                payload = json.loads(ev['payload_json'])
            except json.JSONDecodeError:
                continue
            missing = [rid for rid in payload.get('evidence_receipts', [])
                       if rid not in have]
            if missing:
                items.append({'kind': 'evidence_receipt_missing',
                              'task_id': ev['entity_id'], 'receipt_ids': missing,
                              'completed_at': ev['created_at']})
    print(json.dumps({'ok': True, 'generated_at': t, 'count': len(items),
                      'unverified': sum(1 for i in items if i['kind'] == 'no_receipts'),
                      'items': items}, sort_keys=True))

MIGRATION_INVENTORY_FORMAT = 'autopilot-migration-inventory-v1'
# Bounded discovery: an inventory sweep must never run away on a huge tree.
_SCAN_SKIP_DIRS = {'.git', 'node_modules', '__pycache__', '.venv', 'venv',
                   '.curator_backups', 'cache', 'logs', 'output'}
_SCAN_TEXT_EXTS = {'.md', '.json', '.txt', '.yaml', '.yml'}
_SCAN_MAX_FILE_BYTES = 512 * 1024
_SCAN_MAX_FILES_PER_SOURCE = 500
# Durable-source kinds mapped to their migration plan step, in execution order.
_MIGRATION_PLAN_STEPS = (
    ('autopilot_sqlite', 0,
     'back up the source database (snapshot semantics), verify its seal, then '
     'restore or per-task import-task into the target Autopilot home'),
    ('hindsight_bank', 1,
     'legacy semantic-memory bank: import with `autopilot.py memory-import '
     '<bank.jsonl> --apply` into the local in-database memory store '
     '(content-addressed, so a partial import is simply re-run)'),
    ('hermes_home', 2,
     'register profiles, skills, cron definitions, and ownership contracts '
     'after control-plane initialization'),
    ('obsidian_vault', 3,
     'optional human archive input only: index as provenance-tagged memory; '
     'never imported as execution truth'),
    ('unknown_sqlite', 4,
     'ambiguous source: resolve manually before any migration step'),
)

def _scan_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()

def _open_source_sqlite(path: Path):
    """Open a durable SQLite source without ever taking a write lock.

    Normal read-only SQLite is preferred because it can see a live WAL. Some
    sandboxed macOS runners deny the lock operation even for `mode=ro`; when
    there is no WAL/SHM sidecar (therefore no uncheckpointed transactional
    state to miss), immutable read-only mode is an equally read-only snapshot
    fallback. A live WAL refuses rather than risking an inconsistent view.
    """
    uri = f'file:{path}?mode=ro'
    try:
        c = sqlite3.connect(uri, uri=True, timeout=5)
        # sqlite3 opens read-only URIs lazily on this platform: force a schema
        # read here so a denied lock is caught before callers classify a source.
        c.execute('PRAGMA schema_version').fetchone()
        return c
    except sqlite3.Error as first:
        try:
            c.close()
        except (UnboundLocalError, sqlite3.Error):
            pass
        if any(Path(str(path) + suffix).exists() for suffix in ('-wal', '-shm')):
            raise first
        c = sqlite3.connect(f'file:{path}?mode=ro&immutable=1', uri=True, timeout=5)
        c.execute('PRAGMA schema_version').fetchone()
        return c

def _classify_sqlite(path: Path):
    """Open a candidate database strictly read-only and classify it.

    Returns (status, problems, counts, open_mode): status is 'ok' for a healthy Autopilot
    database, 'corrupted' when SQLite cannot read it or integrity fails, and
    'ambiguous' for valid SQLite that is not an Autopilot schema — the caller
    fails closed on anything other than 'ok'.
    """
    # D1 (R7 live-fleet finding): a database touched by any WAL-mode reader
    # carries -shm/-wal sidecars that a plain mode=ro connection cannot attach
    # (SQLite must create/write the shm file), so discovery of exactly the
    # live homes this migrator exists to read failed closed as 'corrupted'.
    # Note sqlite3.connect is lazy - the open failure surfaces at the first
    # statement - so each candidate URI is fully exercised (integrity check
    # included) before falling back. Fallback order, all strictly read-only
    # against the SOURCE:
    #   1. mode=ro  - the normal case, WAL-aware when sidecars are writable;
    #   2. mode=ro&immutable=1 - zero-sidecar snapshot view; only trusted
    #     after an explicit integrity_check passes on that same connection,
    #     so a torn/WAL-divergent file is still reported corrupted rather
    #     than silently read at a possibly stale snapshot. The open mode
    #     actually used is returned so the sealed inventory can report it
    #     honestly instead of implying a plain read-only attach.
    opened, mode_note, open_err = None, '', None
    for uri in (f'file:{path}?mode=ro', f'file:{path}?mode=ro&immutable=1'):
        try:
            c = sqlite3.connect(uri, uri=True, timeout=5)
            if c.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('integrity_check failed')   # never trust an unsound snapshot
            c.close()
            opened = uri
            mode_note = 'immutable=1 fallback' if 'immutable' in uri else ''
            break
        except (sqlite3.Error, ValueError) as e:
            open_err = e
            continue
    if opened is None:
        detail = f'{type(open_err).__name__}: {open_err}' if open_err else 'unable to open database file read-only'
        return 'corrupted', [detail], {}, ''
    c = sqlite3.connect(opened, uri=True, timeout=5)
    try:
        result = c.execute('PRAGMA integrity_check').fetchone()[0]
        if result != 'ok':
            return 'corrupted', [f'integrity_check: {result}'], {}, mode_note
        names = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'tasks' not in names:
            return 'ambiguous', ['valid sqlite without the Autopilot schema'], {}, mode_note
        counts = {t: c.execute(f'SELECT COUNT(*) AS n FROM "{t}"').fetchone()[0]
                  for t in ('tasks', 'notes', 'handoffs', 'receipts',
                            'heartbeats', 'task_deps', 'audit_events')
                  if t in names}
        return 'ok', [], counts, mode_note
    except sqlite3.Error as e:
        return 'corrupted', [f'{type(e).__name__}: {e}'], {}, mode_note
    finally:
        c.close()

def _scan_source_files(base: Path, target: Path, include=None):
    """Checksum every file under `target`, secret-scanning readable text.

    Bounded (file count and size caps) so a giant vault cannot stall the sweep;
    caps overflow is reported, never silent. Secret findings are kind-only —
    the inventory must redact values even from its own manifest. Returns
    (files, truncated).
    """
    if target.is_file():
        candidates = [target]
    else:
        candidates = []
        for dirpath, dirnames, filenames in os.walk(target):
            d = Path(dirpath)
            dirnames[:] = sorted(x for x in dirnames
                                 if x not in _SCAN_SKIP_DIRS and not (d / x).is_symlink())
            candidates.extend(d / name for name in sorted(filenames)
                              if (d / name).is_file() and not (d / name).is_symlink())
    files, truncated = [], False
    for p in candidates:
        try:
            relative = p.relative_to(base)
        except ValueError:
            continue
        if include and not include(relative):
            continue
        if len(files) >= _SCAN_MAX_FILES_PER_SOURCE:
            truncated = True
            break
        try:
            size = p.stat().st_size
            entry = {'path': str(relative), 'bytes': size,
                     'sha256': _scan_sha256(p), 'secret_kinds': []}
        except OSError:
            continue
        if p.suffix.lower() in _SCAN_TEXT_EXTS and size <= _SCAN_MAX_FILE_BYTES:
            try:
                text = p.read_text(errors='replace')
            except OSError:
                text = ''
            entry['secret_kinds'] = sorted({f['kind'] for f in autopilot._secret_findings(text)})
        files.append(entry)
    return files, truncated

def _discover_migration_sources(root: Path):
    """Walk `root` once, classifying durable-source candidates.

    Matched directories are pruned from further descent (their contents belong
    to that source's own bounded scan), symlinks are never followed outside the
    tree, and traversal order is deterministic so identical trees produce
    identical inventories. Yields (kind, path) tuples.
    """
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        d = Path(dirpath)
        dirnames[:] = sorted(x for x in dirnames
                             if x not in _SCAN_SKIP_DIRS and not (d / x).is_symlink())
        matched = None
        if d.name == 'autopilot' and (d / 'state.db').is_file():
            found.append(('autopilot_sqlite', d / 'state.db'))
            matched = True
        elif (d / '.obsidian').is_dir():
            found.append(('obsidian_vault', d))
            matched = True
        elif d.name == 'hindsight':
            found.append(('hindsight_bank', d))
            matched = True
        elif d.name == '.hermes':
            found.append(('hermes_home', d))
            matched = True
        if matched:
            dirnames[:] = []   # do not re-classify inside a consumed source
            continue
        for name in sorted(filenames):
            if name.endswith(('.db', '.sqlite')):
                found.append(('unknown_sqlite', d / name))
    return found

def migrate_inventory(args=None):
    """Discover durable migration sources under --root and seal a plan manifest.

    Stage one of the installer/migrator: strictly read-only discovery that
    classifies Autopilot databases, Hindsight banks, Hermes homes, Obsidian
    vaults, and unrecognized SQLite files; checksums and secret-scans their
    contents (kind-only findings); verifies database integrity; and emits a
    versioned, sha256-sealed inventory manifest carrying an ordered migration
    plan. Sources are never modified and no data leaves the machine.

    Fail-closed: any corrupted or ambiguous source marks the whole inventory
    `fail_closed` and exits non-zero — an operator must resolve ambiguity or
    corruption before any import runs, and the sealed manifest records exactly
    what was blocked and why. Re-running against unchanged sources reproduces
    an identical plan section, so interrupted migrations can resume safely.
    """
    t = utc()
    root = Path(getattr(args, 'root', '')).expanduser()
    if not root.is_dir():
        raise SystemExit(f'--root is not a directory: {root}')
    root = root.resolve()
    sources, fail_closed = [], False
    for kind, path in _discover_migration_sources(root):
        status, problems, counts = 'ok', [], {}
        if kind in ('autopilot_sqlite', 'unknown_sqlite'):
            status, problems, counts, open_mode = _classify_sqlite(path)
            # D1 honesty: when discovery needed the immutable=1 fallback, the
            # sealed inventory says so (and that integrity_check passed on
            # that view) instead of presenting a plain read-only attach.
            if status == 'ok' and open_mode:
                problems = [f'discovery used {open_mode} after mode=ro could not attach; '
                            'integrity_check passed on the immutable snapshot']
        files, truncated = _scan_source_files(
            path.parent if path.is_file() else path.parent if kind == 'autopilot_sqlite' else path,
            path)
        # D3 (R7 live-fleet finding): migrate-import restores receipt FILES
        # from <db_dir>/receipts/, so those bytes are part of the source's
        # migration surface even though they are not adjacent to state.db.
        # Include them in the same checksummed, secret-scanned scope so a
        # missing or drifted receipt file is visible at stage one instead of
        # surfacing later as doctor receipt_file_missing findings post-import.
        if kind == 'autopilot_sqlite':
            receipts_dir = path.parent / 'receipts'
            if receipts_dir.is_dir():
                extra, extra_trunc = _scan_source_files(path.parent, receipts_dir)
                seen = {f['path'] for f in files}
                files += [f for f in extra if f['path'] not in seen]
                truncated = truncated or extra_trunc
                files.sort(key=lambda f: f['path'])
        kinds = sorted({k for f in files for k in f['secret_kinds']})
        if problems or truncated:
            problems = list(problems)
            if truncated:
                problems.append(f'file cap {_SCAN_MAX_FILES_PER_SOURCE} reached; contents partially scanned')
        if status != 'ok':
            fail_closed = True
        sid = 'src-' + hashlib.sha256(str(path).encode()).hexdigest()[:12]
        sources.append({'id': sid, 'kind': kind, 'path': str(path), 'status': status,
                        'problems': problems, 'counts': counts, 'files': files,
                        'secret_kinds': kinds, **({'truncated': True} if truncated else {})})
    # Deterministic ordering: plan-step rank, then path — identical inputs,
    # identical manifest (modulo created_at), so re-runs diff to nothing.
    rank = {k: i for i, (k, _, _) in enumerate(_MIGRATION_PLAN_STEPS)}
    sources.sort(key=lambda s: (rank.get(s['kind'], 99), s['path']))
    actions = {k: a for k, _, a in _MIGRATION_PLAN_STEPS}
    plan = [{'order': i, 'source_id': s['id'], 'kind': s['kind'],
             'action': 'SKIP — resolve before migrating' if s['status'] != 'ok' else actions.get(s['kind'], 'review')}
            for i, s in enumerate(sources)]
    body = {'format': MIGRATION_INVENTORY_FORMAT, 'created_at': t, 'root': str(root),
            'sources': sources, 'plan': plan, 'fail_closed': fail_closed,
            'summary': {
                'sources': len(sources),
                'healthy': sum(1 for s in sources if s['status'] == 'ok'),
                'blocked': sum(1 for s in sources if s['status'] != 'ok'),
                'secret_kinds': sorted({k for s in sources for k in s['secret_kinds']})}}
    # The seal excludes created_at so identical trees reproduce a byte-identical
    # manifest (modulo the timestamp), letting interrupted migrations resume
    # against the same sealed plan; every content field is still covered.
    digest = hashlib.sha256(json.dumps(
        {k: v for k, v in body.items() if k != 'created_at'}, sort_keys=True).encode()).hexdigest()
    doc = {**body, 'sha256': digest}
    autopilot.ensure()
    with db() as c:
        autopilot.audit(c, 'system', 'migration-inventory', 'migration_inventory_sealed',
                        {'sha256': digest, 'root': str(root), 'sources': len(sources),
                         'fail_closed': fail_closed})
    out = getattr(args, 'out', None)
    if out:
        out_path = Path(out); out_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix='.inventory.', dir=str(out_path.parent))
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(json.dumps(doc, sort_keys=True)); f.flush(); os.fsync(f.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, str(out_path))
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
        print(json.dumps({'ok': not fail_closed, 'path': str(out_path), 'sha256': digest,
                          'summary': body['summary'], 'fail_closed': fail_closed,
                          'sources': [{k: s[k] for k in ('id', 'kind', 'path', 'status')}
                                      for s in sources]}, sort_keys=True))
    else:
        print(json.dumps(doc, sort_keys=True))
    if fail_closed:
        blocked = [f"{s['id']} ({s['path']}): {'; '.join(s['problems'])}"
                   for s in sources if s['status'] != 'ok']
        sys.exit('migration inventory failed closed:\n  ' + '\n  '.join(blocked))

def _load_inventory(path: Path):
    """Read and seal-verify an inventory manifest; returns its body."""
    if not path.exists(): raise SystemExit(f'inventory not found: {path}')
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f'inventory is not valid JSON: {e}')
    expected = doc.get('sha256')
    body = {k: v for k, v in doc.items() if k != 'sha256'}
    if body.get('format') != MIGRATION_INVENTORY_FORMAT:
        raise SystemExit('unrecognized inventory format')
    # Mirror the sealing rule: created_at is outside the seal (see
    # migrate_inventory), so re-verification excludes it too.
    actual = hashlib.sha256(json.dumps(
        {k: v for k, v in body.items() if k != 'created_at'}, sort_keys=True).encode()).hexdigest()
    if actual != expected:
        raise SystemExit('inventory integrity check failed; refusing a tampered manifest')
    body['sha256'] = expected   # re-attach the verified seal for provenance
    return body

def migrate_inventory_check(args):
    """Verify a sealed inventory manifest without touching any database."""
    body = _load_inventory(Path(args.path))
    print(json.dumps({'ok': True, 'path': str(args.path), 'created_at': body.get('created_at'),
                      'root': body.get('root'), 'summary': body.get('summary'),
                      'fail_closed': body.get('fail_closed', False),
                      'sha256': body.get('sha256')}, sort_keys=True))

MIGRATION_RESULT_FORMAT = 'autopilot-migration-result-v1'
# Execution-truth tables imported by migrate-import, in FK-safe insert order.
# Heartbeats are deliberately excluded: liveness state for owners who do not
# exist on this machine is disposable cache, not history worth carrying.
# Facts carry no hard FK (task_id is a soft reference), so they insert last
# and can never dangle.
_MIGRATION_TABLES = ('tasks', 'task_deps', 'receipts', 'notes', 'handoffs', 'facts')
_MIGRATION_REQUIRED_COLS = {
    'tasks': ('id',), 'task_deps': ('task_id', 'depends_on'),
    'receipts': ('id',), 'notes': ('id',), 'handoffs': ('id',),
    'facts': ('id',),
}

def _rechain_audit(c):
    """Recompute the audit hash chain over every event in id order.

    Only ever needed when foreign events are merged into a ledger that already
    has its own chain (the --relink-audit path); a fresh home imports the
    source chain verbatim and never touches existing seals.
    """
    prev = ''
    for row in c.execute('SELECT id FROM audit_events ORDER BY id').fetchall():
        r = c.execute('SELECT entity_type,entity_id,action,payload_json,created_at '
                      'FROM audit_events WHERE id=?', (row[0],)).fetchone()
        h = autopilot._chain_hash(prev, *tuple(r))
        c.execute('UPDATE audit_events SET prev_hash=?,hash=? WHERE id=?', (prev, h, row[0]))
        prev = h

_MIGRATION_BOOKKEEPING_SECRET_ACTIONS = ('secret_blocked', 'secret_redacted', 'secret_allowed')

def _foreign_audit_total(c):
    """Count target audit events that are genuine local history.

    System-scoped migration/onboarding bookkeeping (inventory seals, this
    migrator's own secret decisions, onboarding reports) never makes a home
    "lived-in" — the canonical init -> migrate-inventory -> onboard flow must
    work with no special flags, including the dry-run -> apply progression.
    Any other event is operator/agent history that demands the explicit
    --relink-audit decision before a foreign chain can be merged.
    """
    return c.execute(
        "SELECT COUNT(*) FROM audit_events WHERE NOT (entity_type='system' AND ("
        "(action LIKE 'migration\\_%' ESCAPE '\\') OR (action LIKE 'onboarding\\_%' ESCAPE '\\') "
        "OR action IN (?,?,?)))",
        _MIGRATION_BOOKKEEPING_SECRET_ACTIONS).fetchone()[0]

def _migration_secret_policy(rows_by_table, redact, allow, source_id, applying):
    """Shared-memory secret guard over the rows about to be imported.

    Same boundary as work orders: refuse credential-shaped content by default,
    --redact stores [REDACTED:<kind>] copies, --allow-secret overrides. The
    decision is audited kind-only. Returns (kinds, rows_after_policy).
    """
    kinds = sorted(_doc_secret_kinds({t: rs for t, rs in rows_by_table.items()}))
    if not kinds:
        return [], rows_by_table
    if allow:
        if applying:
            with autopilot.conn() as c:
                autopilot.audit(c, 'system', source_id, 'secret_allowed',
                                {'fields': ['migration_import'], 'kinds': kinds})
        return kinds, rows_by_table
    if redact:
        if applying:
            with autopilot.conn() as c:
                autopilot.audit(c, 'system', source_id, 'secret_redacted',
                                {'fields': ['migration_import'], 'kinds': kinds})
        return kinds, {t: _redact_doc(rs) for t, rs in rows_by_table.items()}
    if applying:
        with autopilot.conn() as c:
            autopilot.audit(c, 'system', source_id, 'secret_blocked',
                            {'fields': ['migration_import'], 'kinds': kinds})
    raise SystemExit(
        'refusing to import credential-shaped content (%s); re-run with --redact '
        'to import redacted copies or --allow-secret to override' % ', '.join(kinds))

def migrate_import(args=None):
    """Import Autopilot execution truth from an inventoried source into this home.

    Stage two of the installer/migrator: binds to the sealed stage-one
    inventory (`--inventory` + `--source-id`), refuses fail-closed manifests,
    re-verifies both the seal and the source database checksum so drift since
    discovery is caught before any data moves, then merges every execution
    table idempotently (INSERT OR IGNORE on natural keys — an identical re-run
    deduplicates to nothing). Tasks that arrive mid-flight are sanitized to
    queued with leases cleared, because the prior owner does not exist here.
    The temporal fact graph travels with execution truth (validity windows
    intact); a source predating the fact table imports cleanly as legacy shape.
    The audit hash chain is preserved verbatim when this home's ledger is
    empty; merging into a live ledger breaks every link and is refused unless
    `--relink-audit` explicitly relinks the whole merged chain.

    Any source-side foreign-key violation is refused before planning or
    mutation, naming the dangling rows. Source remediation is a separate
    approved operation; this importer never repairs, attaches, or invents
    execution-truth rows.

    Dry-run is the default: nothing is written until `--apply` is passed.
    Everything lands in one transaction (rollback on any failure), receipt
    files are restored from the source home with their sealed hashes
    verified, and a post-apply health report (integrity, FK, chain, coverage)
    exits non-zero on any problem. Sources are only ever opened read-only.
    """
    inv = _load_inventory(Path(args.inventory))
    if inv.get('fail_closed'):
        raise SystemExit('inventory failed closed; resolve blocked sources before importing')
    sid = args.source_id
    src_meta = next((s_ for s_ in inv.get('sources', []) if s_.get('id') == sid), None)
    if src_meta is None:
        raise SystemExit(f'source {sid} not found in inventory')
    if src_meta['kind'] != 'autopilot_sqlite':
        raise SystemExit(f"source {sid} is kind '{src_meta['kind']}'; only autopilot_sqlite sources can be imported")
    if src_meta['status'] != 'ok':
        raise SystemExit(f"source {sid} is not healthy ({'; '.join(src_meta['problems'])}); resolve it first")
    db_path = Path(src_meta['path'])
    entry = next((f for f in src_meta.get('files', [])
                  if (db_path.parent / f['path']) == db_path), None)
    if entry is None:
        raise SystemExit(f'source database {db_path} has no checksum in the inventory; re-run migrate-inventory')
    if _scan_sha256(db_path) != entry['sha256']:
        raise SystemExit(
            f'source database changed since the inventory was sealed ({db_path}); re-run migrate-inventory')
    dry = not getattr(args, 'apply', False)
    autopilot.ensure()
    try:
        src = _open_source_sqlite(db_path)
        src.row_factory = sqlite3.Row
        integrity = src.execute('PRAGMA integrity_check').fetchone()[0]
        if integrity != 'ok':
            raise SystemExit(f'source integrity check failed at import time: {integrity}')
        # D2 (R7 live-fleet finding): the live fleet carried receipts and a
        # heartbeat pointing at deleted tasks. The dry-run plan reported them
        # as importable and --apply died mid-transaction on the FK constraint
        # (clean rollback, but no migration). Fail closed HERE, in both
        # dry-run and apply, naming every dangling row — detection and an
        # explicit refusal is this tool's whole job; live remediation of
        # orphan rows stays a separate approved operation.
        fk_violations = []
        for r in src.execute('PRAGMA foreign_key_check').fetchall():
            # Column count varies by SQLite build (3 vs 6 columns); normalize.
            v = {'table': r[0], 'rowid': r[1], 'referred_table': r[2],
                 'fkid': r[3] if len(r) > 3 else '', 'parent': r[4] if len(r) > 4 else '',
                 'fk': r[5] if len(r) > 5 else ''}
            fk_violations.append(v)
        if fk_violations:
            raise SystemExit(
                'source database fails PRAGMA foreign_key_check; refusing to import '
                '(apply would abort mid-transaction and the dry-run plan would be false):\n  '
                + '\n  '.join(f"{v['table']} rowid={v['rowid']} -> missing "
                              f"{v['referred_table']} ({v['parent']})"
                              for v in sorted(map(dict, fk_violations), key=lambda v: (
                                  v['table'], v['rowid'])))
                + '\nResolve orphaned rows in the source first (an approved remediation '
                  'operation); this tool detects and refuses but never repairs live data.')
        # D3 companion gate: receipt ROWS whose sealed file does not exist in
        # the source home would import fine and then fail doctor with
        # receipt_file_missing forever. Inventory checksums the receipts
        # directory (above), so absence/drift is visible at stage one; here we
        # refuse to offer an import that would insert evidence without files.
        # Every receipt row is covered by the source FK gate above.
        tgt = sqlite3.connect(DB, timeout=10)
        tgt.row_factory = sqlite3.Row
        tgt.execute('PRAGMA foreign_keys=ON')
        # Schema-drift tolerance: import the intersection of columns, but never
        # a row whose identity cannot be represented locally. A source whose
        # schema predates a table entirely (e.g. the temporal fact graph) is
        # legacy shape, not drift — it carries zero rows there.
        src_tables = {r[0] for r in src.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        cols = {}
        for t in _MIGRATION_TABLES:
            tcols = {r[1] for r in tgt.execute(f'PRAGMA table_info({t})')}
            if t not in src_tables:
                cols[t] = []
                continue
            scols = {r[1] for r in src.execute(f'PRAGMA table_info({t})')}
            missing = [c_ for c_ in _MIGRATION_REQUIRED_COLS[t] if c_ not in scols or c_ not in tcols]
            if missing:
                raise SystemExit(f'source schema for {t} is missing required columns: {missing}')
            cols[t] = sorted(scols & tcols)
        rows = {}
        for t in _MIGRATION_TABLES:
            if not cols[t]:
                rows[t] = []
                continue
            sel = ','.join(f'"{c_}"' for c_ in cols[t])
            order = 'id' if t == 'tasks' else 'rowid'
            rows[t] = [dict(r) for r in src.execute(f'SELECT {sel} FROM {t} ORDER BY {order}')]
        hb_count = src.execute('SELECT COUNT(*) FROM heartbeats').fetchone()[0] \
            if 'heartbeats' in {r[0] for r in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")} else 0
        # D3 companion gate: a receipt ROW whose sealed file does not exist in
        # the source home would import fine and then fail doctor with
        # receipt_file_missing forever. Inventory checksums the receipts
        # directory (above), so absence/drift is visible at stage one; here we
        # refuse to offer an import that would insert evidence without files.
        # Receipt rows are covered by the source FK and file gates above.
        src_task_ids = {r['id'] for r in rows['tasks']}
        missing_receipt_files = sorted(
            r['id'] for r in rows['receipts']
            if r.get('task_id') in src_task_ids
            and not (db_path.parent / 'receipts' / f"{r['id']}.json").exists())
        if missing_receipt_files:
            raise SystemExit(
                'source carries receipt rows without their sealed files '
                f'(<source>/receipts/): {", ".join(missing_receipt_files)}; importing '
                'would create unresolvable receipt_file_missing findings. Restore the '
                'receipt files or quarantine those rows before migrating.')
        sanitized_ids = sorted(r['id'] for r in rows['tasks']
                               if r['status'] in WORKORDER_ACTIVE_STATUSES)
        for r in rows['tasks']:
            if r['status'] in WORKORDER_ACTIVE_STATUSES:
                r.update(status='queued', lease_owner='', lease_expires_at='',
                         blocked_reason='migrated active lease sanitized')
        # The fact graph joins the secret guard even though its tokens cannot
        # be credential-shaped by construction: the free-form `source` field
        # is operator text and gets exactly the same boundary as notes.
        secret_tables = {t: rows.get(t, []) for t in ('notes', 'handoffs', 'receipts', 'facts')}
        pre_secret = {r['id']: json.dumps(r, sort_keys=True) for r in secret_tables['receipts']}
        kinds, secret_tables = _migration_secret_policy(
            secret_tables, getattr(args, 'redact', False),
            getattr(args, 'allow_secret', False), sid, applying=not dry)
        rows.update(secret_tables)
        # A redacted receipt row no longer matches its verbatim source file;
        # restoring that file would reintroduce the very secret --redact
        # removed, so those files are withheld and reported instead.
        withheld_receipts = sorted(
            rid for rid, j in pre_secret.items()
            if json.dumps(next(r for r in rows['receipts'] if r['id'] == rid),
                          sort_keys=True) != j)
        src_audit = [dict(r) for r in src.execute(
            'SELECT id,entity_type,entity_id,action,payload_json,created_at,prev_hash,hash '
            'FROM audit_events ORDER BY id')]
        present = {t: tgt.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
                   for t in _MIGRATION_TABLES}
        target_audit_total = tgt.execute('SELECT COUNT(*) FROM audit_events').fetchone()[0]
        # Baseline FK state: a target may legitimately carry pre-existing
        # dangling dep edges (missing prerequisites are a modeled state), so
        # the post-apply health check must fail on violations the import
        # INTRODUCED, not on damage that was already there.
        baseline_fk = {tuple(r) for r in tgt.execute('PRAGMA foreign_key_check').fetchall()}
        if dry:
            print(json.dumps({
                'ok': True, 'dry_run': True, 'source_id': sid, 'source_path': str(db_path),
                'inventory_sha256': inv['sha256'],
                'tables': {t: {'source_rows': len(rows[t]),
                               'already_present': present[t]} for t in _MIGRATION_TABLES},
                'sanitized_tasks': sanitized_ids, 'secret_kinds': kinds,
                **({'receipt_files_withheld': withheld_receipts} if withheld_receipts else {}),
                'heartbeats_skipped_disposable': hb_count,
                'audit_events_source': len(src_audit),
                'target_audit_ledger_empty': target_audit_total == 0,
                'relink_required': _foreign_audit_total(tgt) > 0}, sort_keys=True))
            return
        relink = bool(getattr(args, 'relink_audit', False))
        # Interrupted-migration resume: if every source event already exists
        # here with identical content, this ledger already absorbed that chain
        # and a re-run must deduplicate quietly rather than refuse or
        # double-insert. Chain fields are deliberately excluded from the
        # comparison: a prior --relink-audit merge legitimately rewrote them.
        twins = 0
        for r in src_audit:
            hit = tgt.execute(
                'SELECT 1 FROM audit_events WHERE entity_type=? AND entity_id=? AND '
                'action=? AND payload_json=? AND created_at=?',
                (r['entity_type'], r['entity_id'], r['action'], r['payload_json'],
                 r['created_at'])).fetchone()
            twins += 1 if hit else 0
        resume = bool(src_audit) and twins == len(src_audit)
        if target_audit_total > 0 and src_audit and not resume:
            # Stage one's own bookkeeping (inventory seals, migration secret
            # decisions) does not make a home "lived-in": the canonical
            # init -> migrate-inventory -> migrate-import flow must work with
            # no special flags. Only genuine operator/agent history demands
            # the explicit --relink-audit decision.
            foreign_total = _foreign_audit_total(tgt)
            if foreign_total > 0 and not relink:
                raise SystemExit(
                    'this home already has audit history; importing a foreign chain would break '
                    'tamper evidence. Start from a fresh home, or pass --relink-audit to '
                    'explicitly merge and relink the combined ledger.')
        try:
            tgt.execute('BEGIN')
            inserted, skipped_existing = {}, {}
            # Rollback journal: exactly what this apply inserted, with the
            # content hash of every row at insert time, so a later
            # migrate-rollback can undo this import precisely — and refuse
            # when a row has since become live local execution truth.
            journal = {t: {'cols': cols[t], 'keys': [], 'hashes': {}} for t in _MIGRATION_TABLES}
            for t in _MIGRATION_TABLES:
                n = 0
                for r in rows[t]:
                    cur = tgt.execute(
                        f'INSERT OR IGNORE INTO {t}({",".join(cols[t])}) '
                        f'VALUES({",".join("?" * len(cols[t]))})',
                        [r[c_] for c_ in cols[t]])
                    if cur.rowcount:
                        key = [r['task_id'], r['depends_on']] if t == 'task_deps' else r['id']
                        kj = json.dumps(key, sort_keys=True)
                        journal[t]['keys'].append(key)
                        journal[t]['hashes'][kj] = hashlib.sha256(json.dumps(
                            [r[c_] for c_ in cols[t]], sort_keys=True).encode()).hexdigest()
                    n += cur.rowcount
                inserted[t] = n
                skipped_existing[t] = len(rows[t]) - n
            audit_offset = 0
            if target_audit_total > 0 and src_audit:
                audit_offset = tgt.execute(
                    'SELECT COALESCE(MAX(id),0) FROM audit_events').fetchone()[0]
            audit_imported = 0
            journal_audit_ids = []
            for r in src_audit:
                # Per-event content twin: an event already absorbed by a prior
                # import (verbatim or relink-shifted) is skipped rather than
                # re-inserted, making re-runs and interrupted resumes
                # idempotent over the audit ledger itself.
                if tgt.execute(
                        'SELECT 1 FROM audit_events WHERE entity_type=? AND entity_id=? AND '
                        'action=? AND payload_json=? AND created_at=?',
                        (r['entity_type'], r['entity_id'], r['action'], r['payload_json'],
                         r['created_at'])).fetchone():
                    continue
                cur = tgt.execute(
                    'INSERT OR IGNORE INTO audit_events(id,entity_type,entity_id,action,'
                    'payload_json,created_at,prev_hash,hash) VALUES(?,?,?,?,?,?,?,?)',
                    (r['id'] + audit_offset, r['entity_type'], r['entity_id'], r['action'],
                     r['payload_json'], r['created_at'], r['prev_hash'], r['hash']))
                if cur.rowcount:
                    journal_audit_ids.append(r['id'] + audit_offset)
                audit_imported += cur.rowcount
            autopilot.audit(tgt, 'system', sid, 'migration_import_applied',
                            {'inventory_sha256': inv['sha256'], 'source_id': sid,
                             'source_path': str(db_path), 'inserted': inserted,
                             'skipped_existing': skipped_existing,
                             'sanitized_tasks': sanitized_ids,
                             **({'secret_kinds': kinds} if kinds else {}),
                             **({'receipt_files_withheld': withheld_receipts}
                                if withheld_receipts else {}),
                             'audit_relinked': bool(audit_offset and audit_imported)})
            if audit_offset and audit_imported:
                _rechain_audit(tgt)
            tgt.commit()
        except Exception:
            tgt.rollback()
            raise
        # Restore verification: receipt files reappear here exactly as sealed,
        # byte-checked against the file_hash recorded in each imported row.
        written, hash_mismatches = 0, []
        journal_receipt_files = []
        src_receipts = db_path.parent / 'receipts'
        for r in rows['receipts']:
            rid = r['id']
            if rid in withheld_receipts:
                continue
            target_file = autopilot.RECEIPTS / f'{rid}.json'
            if target_file.exists():
                continue
            source_file = src_receipts / f'{rid}.json'
            if not source_file.exists():
                continue
            text = source_file.read_text()
            if r.get('file_hash') and hashlib.sha256(text.encode()).hexdigest() != r['file_hash']:
                hash_mismatches.append(rid)
                continue
            fd, tmp = tempfile.mkstemp(prefix=f'.{rid}.', dir=str(autopilot.RECEIPTS))
            try:
                with os.fdopen(fd, 'w') as f:
                    f.write(text); f.flush(); os.fsync(f.fileno())
                os.chmod(tmp, 0o600)
                os.replace(tmp, str(target_file))
                written += 1
                journal_receipt_files.append({'id': rid, 'file_hash': r.get('file_hash', '')})
            finally:
                if os.path.exists(tmp): os.unlink(tmp)
        health_problems = []
        if tgt.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            health_problems.append('integrity_check failed on the target database')
        current_fk = {tuple(r) for r in tgt.execute('PRAGMA foreign_key_check').fetchall()}
        new_fk = current_fk - baseline_fk
        pre_existing_fk = len(baseline_fk & current_fk)
        health_problems += [f'fk_violation: {v}' for v in sorted(new_fk)]
        chain_problems = autopilot.audit_chain_problems(tgt)
        health_problems += [f'audit chain: {p}' for p in chain_problems]
        coverage = {}
        for t in _MIGRATION_TABLES:
            s_count = src.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] \
                if t in src_tables else 0
            t_count = tgt.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
            coverage[t] = {'source': s_count, 'target': t_count}
            if t_count < s_count:
                health_problems.append(f'{t}: target has {t_count} rows, source has {s_count}')
        health_problems += [f'receipt file hash mismatch: {rid}' for rid in hash_mismatches]
        deduplicated = all(v == 0 for v in inserted.values()) and audit_imported == 0
        result = {
            'ok': not health_problems, 'applied': True, 'deduplicated': deduplicated,
            'source_id': sid, 'source_path': str(db_path),
            'inventory_sha256': inv['sha256'],
            'inserted': inserted, 'skipped_existing': skipped_existing,
            'sanitized_tasks': sanitized_ids,
            **({'secret_kinds': kinds} if kinds else {}),
            'audit_events_imported': audit_imported,
            'audit_relinked': bool(audit_offset and audit_imported),
            'heartbeats_skipped_disposable': hb_count,
            'receipt_files_written': written,
            **({'receipt_files_withheld': withheld_receipts} if withheld_receipts else {}),
            'rollback': {'tables': journal, 'audit_event_ids': sorted(journal_audit_ids),
                         'receipt_files': journal_receipt_files},
            'health': {'problems': health_problems, 'coverage': coverage,
                       'chain_problem_count': len(chain_problems),
                       'pre_existing_fk_violations': pre_existing_fk}}
        out = getattr(args, 'out', None)
        if out:
            body_doc = {k: v for k, v in result.items() if k != 'ok'}
            doc = {**body_doc, 'format': MIGRATION_RESULT_FORMAT, 'created_at': utc(),
                   'sha256': hashlib.sha256(
                       json.dumps(body_doc, sort_keys=True).encode()).hexdigest()}
            out_path = Path(out); out_path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix='.migration-result.', dir=str(out_path.parent))
            try:
                with os.fdopen(fd, 'w') as f:
                    f.write(json.dumps(doc, sort_keys=True)); f.flush(); os.fsync(f.fileno())
                os.chmod(tmp, 0o600)
                os.replace(tmp, str(out_path))
            finally:
                if os.path.exists(tmp): os.unlink(tmp)
            result['result_doc'] = str(out_path)
        print(json.dumps(result, sort_keys=True))
        if health_problems:
            sys.exit('migration import failed its health check:\n  '
                     + '\n  '.join(health_problems))
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            tgt.close()
        except Exception:
            pass

def _load_result_doc(path: Path):
    """Load and seal-verify a sealed autopilot-migration-result-v1 document."""
    if not path.exists(): raise SystemExit(f'result document not found: {path}')
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f'result document is not valid JSON: {e}')
    if doc.get('format') != MIGRATION_RESULT_FORMAT:
        raise SystemExit(f'not a {MIGRATION_RESULT_FORMAT} document: {path}')
    body = {k: v for k, v in doc.items() if k not in ('sha256', 'format', 'created_at')}
    if hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest() != doc.get('sha256'):
        raise SystemExit('integrity check failed: result document seal does not match its content')
    return doc

def migrate_rollback(args=None):
    """Undo a migration import, precisely and fail-closed.

    Stage three of the installer/migrator: consumes the sealed rollback
    journal a prior `migrate-import --apply --out` recorded — exactly the
    rows it inserted (with their content hashes at insert time), the audit
    events it merged, and the receipt files it restored. Dry-run is the
    default. Refuses when an imported row has drifted since import (it is
    now live local execution truth someone changed) or when local rows that
    were never imported depend on an imported task — including facts whose
    soft provenance would dangle; --force is the explicit override and
    cascades those dependents away with the task. Removed
    audit events are relinked into a continuous chain, receipt files are
    deleted only when they still match their sealed hash (locally changed
    files are withheld and reported), re-runs deduplicate to nothing, and
    a post-rollback health report exits non-zero on any problem.
    """
    doc = _load_result_doc(Path(args.result))
    if not doc.get('applied'):
        raise SystemExit('this result document records a dry-run; nothing was applied')
    rb = doc.get('rollback')
    if not isinstance(rb, dict) or 'tables' not in rb:
        raise SystemExit(
            'result document carries no rollback journal; re-run the import with --out '
            'to regenerate one before rolling back')
    dry = not getattr(args, 'apply', False)
    force = bool(getattr(args, 'force', False))
    autopilot.ensure()
    c = sqlite3.connect(DB, timeout=10)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA foreign_keys=ON')
    try:
        present, gone, drift = {}, {}, []
        for t in _MIGRATION_TABLES:
            jcols = rb['tables'][t].get('cols') or _MIGRATION_REQUIRED_COLS[t]
            for key in rb['tables'][t]['keys']:
                kj = json.dumps(key, sort_keys=True)
                if t == 'task_deps':
                    row = c.execute('SELECT * FROM task_deps WHERE task_id=? AND depends_on=?',
                                    tuple(key)).fetchone()
                else:
                    row = c.execute(f'SELECT * FROM {t} WHERE id=?', (key,)).fetchone()
                if row is None:
                    gone.setdefault(t, []).append(key)
                    continue
                present.setdefault(t, []).append(key)
                h = hashlib.sha256(json.dumps(
                    [row[c_] for c_ in jcols if c_ in row.keys()], sort_keys=True).encode()).hexdigest()
                if h != rb['tables'][t]['hashes'].get(kj):
                    drift.append({'table': t, 'key': key})
        # Local rows that were never imported but reference imported tasks:
        # removing the task would cascade them away, so they block by default.
        # Facts are soft references, but rolling back a task that a local fact
        # points at would leave the dangling provenance doctor flags as
        # out-of-band surgery — so they block exactly like hard children.
        journal_ids = {t: {json.dumps(k, sort_keys=True) for k in rb['tables'][t]['keys']}
                       for t in _MIGRATION_TABLES}
        task_keys = [k[0] if isinstance(k, list) else k for k in rb['tables']['tasks']['keys']]
        blockers = []
        for tid in task_keys:
            for t in ('notes', 'handoffs', 'receipts', 'facts'):
                for r in c.execute(f'SELECT id FROM {t} WHERE task_id=?', (tid,)):
                    if json.dumps(r['id'], sort_keys=True) not in journal_ids[t]:
                        blockers.append({'table': t, 'id': r['id'], 'task_id': tid})
            for r in c.execute(
                    'SELECT task_id,depends_on FROM task_deps WHERE task_id=? OR depends_on=?',
                    (tid, tid)):
                if json.dumps([r['task_id'], r['depends_on']], sort_keys=True) \
                        not in journal_ids['task_deps']:
                    blockers.append({'table': 'task_deps',
                                     'key': [r['task_id'], r['depends_on']], 'task_id': tid})
        plan = {
            'ok': True, 'dry_run': True, 'source_id': doc.get('source_id'),
            'result_doc': str(args.result),
            'would_remove': {t: present.get(t, []) for t in _MIGRATION_TABLES},
            'already_removed': {t: len(gone.get(t, [])) for t in _MIGRATION_TABLES},
            'audit_events_would_remove': sum(
                1 for aid in rb.get('audit_event_ids', [])
                if c.execute('SELECT 1 FROM audit_events WHERE id=?', (aid,)).fetchone()),
            'receipt_files_would_delete': [
                f['id'] for f in rb.get('receipt_files', [])
                if (autopilot.RECEIPTS / f"{f['id']}.json").exists()],
            'drifted_rows': drift, 'local_dependents': blockers,
            'force_required': bool(drift or blockers)}
        if dry:
            print(json.dumps(plan, sort_keys=True))
            return
        if (drift or blockers) and not force:
            detail = '; '.join(
                [f"drifted: {d['table']} {d['key']}" for d in drift]
                + [f"local dependent: {b['table']} {b.get('id') or b.get('key')} -> {b['task_id']}"
                   for b in blockers])
            raise SystemExit(
                'refusing to roll back changed execution truth'
                + (' (pass --force to cascade it away anyway)' if not force else '')
                + f': {detail}')
        try:
            c.execute('BEGIN')
            removed = {}
            n = 0
            for key in rb['tables']['task_deps']['keys']:
                n += c.execute('DELETE FROM task_deps WHERE task_id=? AND depends_on=?',
                               tuple(key)).rowcount
            removed['task_deps'] = n
            for t in ('receipts', 'notes', 'handoffs', 'facts'):
                n = 0
                for key in rb['tables'][t]['keys']:
                    cur = c.execute(f'DELETE FROM {t} WHERE id=?', (key,))
                    n += cur.rowcount
                removed[t] = n
            n = 0
            for key in rb['tables']['tasks']['keys']:
                cur = c.execute('DELETE FROM tasks WHERE id=?', (key,))
                n += cur.rowcount
            removed['tasks'] = n
            # Facts are soft references with no FK cascade: under --force,
            # local facts pointing at removed tasks are deleted explicitly so
            # their provenance cannot dangle (hard children cascade via FK).
            cascaded_facts = 0
            for b in blockers:
                if b.get('table') == 'facts':
                    cascaded_facts += c.execute(
                        'DELETE FROM facts WHERE id=?', (b['id'],)).rowcount
            removed['facts'] = removed.get('facts', 0) + cascaded_facts
            audit_removed = 0
            for aid in rb.get('audit_event_ids', []):
                cur = c.execute('DELETE FROM audit_events WHERE id=?', (aid,))
                audit_removed += cur.rowcount
            if audit_removed:
                _rechain_audit(c)
            autopilot.audit(c, 'system', doc.get('source_id', ''), 'migration_rollback_applied',
                            {'result_doc_sha256': doc.get('sha256'),
                             'source_id': doc.get('source_id'),
                             'removed': removed, 'audit_events_removed': audit_removed,
                             'forced': force})
            c.commit()
        except Exception:
            c.rollback()
            raise
        # Receipt files: delete only what this import wrote and only while it
        # still matches its sealed hash; locally changed files are kept and
        # reported rather than destroyed.
        deleted_files, withheld_files = [], []
        for f in rb.get('receipt_files', []):
            path = autopilot.RECEIPTS / f"{f['id']}.json"
            if not path.exists():
                continue
            text = path.read_text()
            expected = f.get('file_hash', '')
            if expected and hashlib.sha256(text.encode()).hexdigest() != expected:
                withheld_files.append(f['id'])
                continue
            path.unlink()
            deleted_files.append(f['id'])
        health_problems = []
        if c.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            health_problems.append('integrity_check failed on the target database')
        chain_problems = autopilot.audit_chain_problems(c)
        health_problems += [f'audit chain: {p}' for p in chain_problems]
        result = {
            'ok': not health_problems, 'rolled_back': True,
            'source_id': doc.get('source_id'), 'removed': removed,
            'already_removed': {t: len(gone.get(t, [])) for t in _MIGRATION_TABLES},
            'audit_events_removed': audit_removed,
            **({'cascade_removed': len(blockers)} if force and blockers else {}),
            'receipt_files_deleted': deleted_files,
            **({'receipt_files_withheld': withheld_files} if withheld_files else {}),
            'health': {'problems': health_problems,
                       'chain_problem_count': len(chain_problems)}}
        print(json.dumps(result, sort_keys=True))
        if health_problems:
            sys.exit('migration rollback failed its health check:\n  '
                     + '\n  '.join(health_problems))
    finally:
        c.close()

ONBOARDING_FORMAT = 'autopilot-onboarding-v1'
_ONBOARD_PROBE_ID = 'onboard-probe'

def _capture_json(fn, fn_args=None):
    """Run an ops stage in-process and parse the JSON it prints."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        fn(fn_args)
    try:
        return json.loads(buf.getvalue())
    except json.JSONDecodeError as e:
        raise SystemExit(f'{getattr(fn, "__name__", "stage")} returned non-JSON output: {e}')

def _onboard_preflight():
    con = sqlite3.connect(':memory:')
    try:
        fts5 = bool(con.execute("SELECT sqlite_compileoption_used('ENABLE_FTS5')").fetchone()[0])
    finally:
        con.close()
    root = autopilot.ROOT
    writable = os.access(root if root.exists() else root.parent, os.W_OK)
    warnings = []
    if not writable:
        problems = [f'autopilot home is not writable: {root}']
    else:
        problems = []
    if not fts5:
        # Graceful degradation is supported everywhere; onboarding proceeds
        # but the report records that ranked retrieval runs in LIKE mode.
        warnings.append('sqlite build lacks FTS5; ranked retrieval degrades to substring search')
    return {'python': sys.version.split()[0], 'sqlite': sqlite3.sqlite_version,
            'fts5': fts5, 'home': str(root), 'home_writable': bool(writable),
            'warnings': warnings, 'problems': problems}

def _onboard_probe():
    """Prove the cross-agent protocol end-to-end through the real CLI surface.

    Two distinct agent identities (hermes hands off, codex picks up) exercise
    handoff -> recall -> ack -> resume -> receipt -> complete against a
    dedicated probe task, so "all agents can recall and hand off the same
    context" is verified execution, not a claim. Idempotent: an already-done
    probe task from a prior onboarding is skipped, and an interrupted probe
    resumes (handoff dedupes, resume reclaims) instead of duplicating.
    """
    script = Path(autopilot.__file__).resolve()

    def cli(*a):
        code, out, err = run([sys.executable, str(script), *a])
        if code != 0:
            raise SystemExit(f"probe step '{a[0]}' failed: {err or out}")
        return json.loads(out)

    with db() as c:
        row = c.execute('SELECT status FROM tasks WHERE id=?', (_ONBOARD_PROBE_ID,)).fetchone()
    if row and row['status'] == 'completed':
        return {'status': 'skipped', 'reason': f'{_ONBOARD_PROBE_ID} already done by a prior onboarding'}
    if not row:
        cli('create', '--project', 'meta', '--title', 'onboarding protocol probe',
            '--description', 'created by ops.py onboard --probe; safe to archive afterwards',
            '--id', _ONBOARD_PROBE_ID)
    h = cli('handoff', _ONBOARD_PROBE_ID, '--from-agent', 'hermes', '--to-agent', 'codex',
            '--objective', 'onboarding cross-agent protocol probe')
    bundle = cli('recall', _ONBOARD_PROBE_ID, '--agent', 'codex')
    digest = bundle['digest']
    cli('ack', _ONBOARD_PROBE_ID, '--agent', 'codex', '--recall-digest', digest)
    cli('resume', _ONBOARD_PROBE_ID, '--agent', 'codex')
    rec = cli('receipt', _ONBOARD_PROBE_ID, '--kind', 'verification',
              '--payload', '{"probe": "cross-agent-handoff"}')
    cli('complete', _ONBOARD_PROBE_ID, '--owner', 'codex',
        '--note', 'protocol probe: hermes handed off, codex recalled, acked, completed with evidence',
        '--receipt', rec['receipt_id'])
    with db() as c:
        done = c.execute("SELECT status FROM tasks WHERE id=?", (_ONBOARD_PROBE_ID,)).fetchone()
        acked = c.execute(
            "SELECT acked_by FROM handoffs WHERE id=? AND superseded_by=''", (h['id'],)).fetchone()
    if not done or done['status'] != 'completed':
        raise SystemExit('protocol probe completed its steps but the task did not settle to completed')
    if not acked or acked['acked_by'] != 'codex':
        raise SystemExit('protocol probe handoff was not acknowledged by codex')
    return {'status': 'ok', 'task_id': _ONBOARD_PROBE_ID, 'handoff_id': h['id'],
            'deduplicated': h.get('deduplicated', False), 'recall_digest': digest,
            'receipt_id': rec['receipt_id']}

def onboard(args):
    """One command from sealed inventory to a verified working Autopilot home.

    The installer's front door: preflight environment checks, idempotent
    control-plane initialization, seal verification of the stage-one manifest
    with fail-closed refusal, unambiguous source selection, a dry-run import
    plan, then — only under `--apply` — the real import, doctor's full
    consistency sweep, and optionally (`--probe`) an end-to-end cross-agent
    protocol exercise through the real CLI. Every stage lands in a sealed
    autopilot-onboarding-v1 report; any failure stops onboarding before later
    stages run and exits non-zero naming what failed.

    Dry-run writes nothing but its own bookkeeping (the same convention as
    migrate-inventory), so `--probe` requires `--apply`: a probe mutates the
    target home by design. Re-running is safe — init, import, doctor, and the
    probe are all idempotent, which makes an interrupted onboarding resumable
    by simply running the same command again.
    """
    t = utc()
    stages = []
    inv = None
    sid = ''
    abort_msg = ''
    ok = True
    try:
        pre = _onboard_preflight()
        problems = pre.pop('problems')
        stages.append({'stage': 'preflight', 'status': 'failed' if problems else 'ok',
                       **pre, **({'problems': problems} if problems else {})})
        if problems:
            raise SystemExit('; '.join(problems))
        autopilot.ensure()
        stages.append({'stage': 'init_control_plane', 'status': 'ok'})
        inv = _load_inventory(Path(args.inventory))
        if inv.get('fail_closed'):
            raise SystemExit('inventory failed closed; resolve blocked sources before onboarding')
        candidates = [s_ for s_ in inv.get('sources', [])
                      if s_['kind'] == 'autopilot_sqlite' and s_['status'] == 'ok']
        sid = getattr(args, 'source_id', '') or (candidates[0]['id'] if len(candidates) == 1 else '')
        if not sid:
            known = ', '.join(s_['id'] for s_ in inv.get('sources', [])
                              if s_['kind'] == 'autopilot_sqlite')
            raise SystemExit(
                f'{len(candidates)} healthy autopilot_sqlite source(s): ambiguous; pass '
                f'--source-id explicitly (candidates: {known or "none"})')
        if not any(s_['id'] == sid for s_ in candidates):
            raise SystemExit(f'source {sid} is not a healthy autopilot_sqlite source in this inventory')
        stages.append({'stage': 'select_source', 'status': 'ok', 'source_id': sid})
        ns = SimpleNamespace(inventory=args.inventory, source_id=sid, apply=False, out=None,
                             redact=getattr(args, 'redact', False),
                             allow_secret=getattr(args, 'allow_secret', False),
                             relink_audit=getattr(args, 'relink_audit', False))
        plan = _capture_json(migrate_import, ns)
        stages.append({'stage': 'import_plan', 'status': 'ok', 'tables': plan.get('tables'),
                       'secret_kinds': plan.get('secret_kinds', []),
                       'sanitized_tasks': plan.get('sanitized_tasks', [])})
        result_doc = ''
        if getattr(args, 'apply', False):
            result_doc = str(autopilot.ROOT / 'migrations' /
                             f'migration-result-{inv["sha256"][:8]}.json')
            ns.apply = True
            ns.out = result_doc
            applied = _capture_json(migrate_import, ns)
            hp = applied.get('health', {}).get('problems', [])
            result_doc = applied.get('result_doc', result_doc)
            stages.append({'stage': 'import_apply', 'status': 'failed' if hp else 'ok',
                           'inserted': applied.get('inserted'),
                           'skipped_existing': applied.get('skipped_existing'),
                           'deduplicated': applied.get('deduplicated'),
                           'audit_events_imported': applied.get('audit_events_imported'),
                           'result_doc': result_doc,
                           **({'health_problems': hp} if hp else {})})
            if hp:
                raise SystemExit('import failed its post-apply health check')
        doc_report = _capture_json(doctor, None)
        dp = doc_report.get('problems', [])
        stages.append({'stage': 'doctor', 'status': 'failed' if dp else 'ok',
                       'problem_count': len(dp),
                       **({'problems': dp[:20]} if dp else {})})
        if dp:
            raise SystemExit(f'doctor found {len(dp)} problem(s); fix them before relying on this home')
        if getattr(args, 'probe', False):
            if not getattr(args, 'apply', False):
                raise SystemExit('--probe requires --apply: a probe mutates the home by design')
            stages.append({'stage': 'protocol_probe', **_onboard_probe()})
    except SystemExit as e:
        ok = False
        abort_msg = str(e)
        if not stages or stages[-1].get('status') != 'failed':
            stages.append({'stage': 'aborted', 'status': 'failed', 'error': abort_msg})
    summary = {
        'stages_total': len(stages),
        'stages_ok': sum(1 for s in stages if s.get('status') in ('ok', 'skipped')),
        'applied': bool(getattr(args, 'apply', False)),
        'probed': any(s.get('stage') == 'protocol_probe' for s in stages)}
    body = {'format': ONBOARDING_FORMAT, 'created_at': t, 'home': str(autopilot.ROOT),
            'inventory_path': str(args.inventory),
            'inventory_sha256': inv['sha256'] if inv else '',
            'source_id': sid, 'stages': stages, 'summary': summary}
    # Same sealing convention as the inventory: created_at stays outside the
    # digest so identical onboarding states reproduce an identical report.
    digest = hashlib.sha256(json.dumps(
        {k: v for k, v in body.items() if k != 'created_at'}, sort_keys=True).encode()).hexdigest()
    doc = {**body, 'sha256': digest}
    try:
        autopilot.ensure()
        with db() as c:
            autopilot.audit(c, 'system', 'onboarding',
                            'onboarding_completed' if ok else 'onboarding_failed',
                            {'sha256': digest, 'source_id': sid, 'applied': summary['applied'],
                             'stages': [s['stage'] for s in stages]})
    except Exception:
        pass   # reporting must never mask the stage outcome it describes
    out = getattr(args, 'out', None)
    compact = {'ok': ok, 'sha256': digest, 'home': str(autopilot.ROOT), 'summary': summary,
               'stages': [{'stage': s.get('stage'), 'status': s.get('status')} for s in stages]}
    if out:
        out_path = Path(out); out_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix='.onboarding.', dir=str(out_path.parent))
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(json.dumps(doc, sort_keys=True)); f.flush(); os.fsync(f.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, str(out_path))
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
        print(json.dumps({**compact, 'path': str(out_path)}, sort_keys=True))
    else:
        print(json.dumps(doc, sort_keys=True))
    if not ok:
        sys.exit('onboarding failed:\n  ' + abort_msg)

BRAIN_INVENTORY_FORMAT = 'mindos-brain-inventory-v1'
# The end-to-end brain is not one store: each inventoried source carries the
# epistemic role it plays, so a later migration can treat execution truth,
# semantic memory, temporal facts, raw session cache, and human/agent
# definitions differently instead of flattening them into one authority.
_BRAIN_ROLES = {
    'autopilot': 'execution_truth',
    'memories': 'semantic_memory',
    'temporal': 'temporal_facts',
    'claude_sync': 'sync_metadata',
    'claude_memory': 'human_archive',
    'sessions': 'session_cache',
    'profiles': 'profile_definitions',
    'skills': 'skill_definitions',
    'cron': 'cron_definitions',
}
def _brain_classify_temporal(path: Path):
    """Classify the temporal sidecar strictly read-only (mirror of
    _classify_sqlite for the sidecar's own schema)."""
    try:
        c = _open_source_sqlite(path)
    except sqlite3.Error as e:
        return 'corrupted', [f'{type(e).__name__}: {e}'], {}
    try:
        result = c.execute('PRAGMA integrity_check').fetchone()[0]
        if result != 'ok':
            return 'corrupted', [f'integrity_check: {result}'], {}
        names = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'entities' not in names:
            return 'ambiguous', ['valid sqlite without the temporal sidecar schema'], {}
        counts = {t: c.execute(f'SELECT COUNT(*) AS n FROM "{t}"').fetchone()[0]
                  for t in ('entities', 'relations', 'ingested_events') if t in names}
        return 'ok', [], counts
    except sqlite3.Error as e:
        return 'corrupted', [f'{type(e).__name__}: {e}'], {}
    finally:
        c.close()

def _brain_ro_counts(path: Path, tables):
    """Read-only row counts for tables present in an Autopilot-schema db.

    The audit_events count excludes this command's own bookkeeping events:
    each sealed inventory writes one, and counting them would make every
    re-run differ from the last — defeating the resume-by-reseal contract.
    """
    try:
        c = _open_source_sqlite(path)
    except sqlite3.Error:
        return {}
    try:
        out = {}
        for t in tables:
            if t == 'audit_events':
                out[t] = c.execute("SELECT COUNT(*) AS n FROM audit_events "
                                   "WHERE action!='brain_inventory_sealed'").fetchone()[0]
            else:
                out[t] = c.execute(f'SELECT COUNT(*) AS n FROM "{t}"').fetchone()[0]
        return out
    except sqlite3.Error:
        return {}
    finally:
        c.close()

def _brain_scan_tree(base: Path, targets, cap=_SCAN_MAX_FILES_PER_SOURCE, include=None):
    """Checksum + kind-only secret-scan several subtrees under one budget."""
    files, truncated = [], False
    for target in targets:
        if not target.exists():
            continue
        batch, more = _scan_source_files(base, target, include=include)
        truncated = truncated or more
        for entry in batch:
            if len(files) >= cap:
                return files, True
            files.append(entry)
    return files, truncated

def _brain_file_source(sid_kind, path: Path, role, targets=None, extra_counts=None,
                       cap=_SCAN_MAX_FILES_PER_SOURCE, include=None):
    """Shared shape for filesystem-backed sources: checksummed file inventory,
    secret kinds reduced to kinds-only, deterministic ordering."""
    files, truncated = _brain_scan_tree(path, targets or [path], cap, include=include)
    kinds = sorted({k for f in files for k in f['secret_kinds']})
    problems = []
    if truncated:
        problems.append(f'file cap {cap} reached; contents partially scanned')
    counts = {'files': len(files), 'bytes': sum(f['bytes'] for f in files)}
    if extra_counts:
        counts.update(extra_counts)
    return {'id': 'brain-' + hashlib.sha256(f'{sid_kind}:{path}'.encode()).hexdigest()[:12],
            'kind': sid_kind, 'role': role, 'path': str(path),
            'status': 'ok' if not truncated else 'ok_partial_scan',
            'problems': problems, 'counts': counts, 'files': files,
            'secret_kinds': kinds, **({'truncated': True} if truncated else {})}

def _brain_json_doc(path: Path):
    """Parse a JSON metadata file read-only; (doc, problem)."""
    try:
        return json.loads(path.read_text()), ''
    except FileNotFoundError:
        return None, 'missing'
    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as e:
        return None, f'{type(e).__name__}: {e}'

_BRAIN_PROFILE_DEFINITION_NAMES = {
    'profile.yaml', 'profile.yml', 'profile.json', 'profile.toml', 'profile.md',
    'config.yaml', 'config.yml', 'config.json', 'config.toml',
}

def _brain_profile_definition(relative: Path):
    """Only durable profile declarations, never a profile's runtime state."""
    return relative.name.lower() in _BRAIN_PROFILE_DEFINITION_NAMES

def _brain_skill_definition(relative: Path):
    """A skill's portable contract is its SKILL.md declaration."""
    return relative.name == 'SKILL.md'

def brain_inventory(args=None):
    """Dry-run-first end-to-end brain inventory across every durable source.

    Strictly read-only: Autopilot execution state, the temporal sidecar, the
    local semantic-memory store, the Claude memory-sync metadata, Claude project memory archives, raw session
    cache, and profile/skill/cron definitions are counted, checksummed, and
    classified without a single byte of mutation. Values never enter the
    manifest — only redacted counts, sha256 checksums, integrity/health
    verdicts, provenance roles, and kind-only secret findings. The sealed,
    versioned manifest is byte-reproducible against unchanged sources (the
    digest excludes created_at), so interrupted installs can resume against
    the same inventory, and nothing here makes a home lived-in beyond one
    digest-only audit event recording that an inventory was taken.

    Fail-closed: corrupted or ambiguous local sources block with exact
    blockers; absent optional sources (temporal sidecar, sync file) are
    recorded honestly without blocking.
    """
    t = utc()
    hermes = Path(getattr(args, 'hermes_home', '') or Path.home() / '.hermes').expanduser()
    claude = Path(getattr(args, 'claude_home', '') or Path.home() / '.claude').expanduser()
    timeout = float(getattr(args, 'timeout', 3) or 3)
    sources, fail_closed = [], False

    def add(src, blocking=False):
        nonlocal fail_closed
        if src['status'] in ('corrupted', 'ambiguous'):
            fail_closed = True
        elif blocking:
            fail_closed = True
        sources.append(src)

    # Execution truth: the Autopilot control plane database + receipt evidence.
    adb = autopilot.DB
    status, problems, counts, _open_mode = _classify_sqlite(adb)
    # audit_events deliberately re-counted through _brain_ro_counts so the
    # command's own seal events stay outside the sealed manifest (reproducibility).
    counts.update(_brain_ro_counts(adb, ('sessions', 'session_messages', 'facts',
                                         'memories', 'audit_events')))
    receipts = autopilot.RECEIPTS
    rec_files = sorted(p for p in receipts.glob('*.json') if p.is_file()) if receipts.is_dir() else []
    counts['receipt_files'] = len(rec_files)
    add({'id': 'brain-' + hashlib.sha256(f'autopilot:{adb}'.encode()).hexdigest()[:12],
         'kind': 'autopilot', 'role': _BRAIN_ROLES['autopilot'], 'path': str(adb),
         'status': status, 'problems': problems, 'counts': counts, 'files': [],
         'sha256': _scan_sha256(adb) if adb.is_file() else '',
         'secret_kinds': []}, blocking=(status == 'ambiguous'))
    # Temporal sidecar: optional by contract ("if present").
    tdb = autopilot.ROOT / 'temporal.db'
    if tdb.is_file():
        status, problems, counts = _brain_classify_temporal(tdb)
        add({'id': 'brain-' + hashlib.sha256(f'temporal:{tdb}'.encode()).hexdigest()[:12],
             'kind': 'temporal', 'role': _BRAIN_ROLES['temporal'], 'path': str(tdb),
             'status': status, 'problems': problems, 'counts': counts,
             'sha256': _scan_sha256(tdb), 'files': [],
             'secret_kinds': []})
    else:
        add({'id': 'brain-' + hashlib.sha256(f'temporal:{tdb}'.encode()).hexdigest()[:12],
             'kind': 'temporal', 'role': _BRAIN_ROLES['temporal'], 'path': str(tdb),
             'status': 'absent', 'problems': ['temporal sidecar not present'],
             'counts': {}, 'sha256': '', 'files': [], 'secret_kinds': []})
    # Semantic memory: local and in-database since the external Hindsight
    # service was retired. It is inventoried as its own epistemic source even
    # though it shares the control-plane file, because a migration must still
    # be able to treat semantic memory differently from execution truth.
    mem_counts, mem_problems, mem_status = {}, [], 'absent'
    if adb.is_file():
        try:
            mc = _open_source_sqlite(adb)
            try:
                mem_counts = {
                    'memories': mc.execute(
                        "SELECT COUNT(*) FROM memories WHERE superseded_by=''").fetchone()[0],
                    'retracted': mc.execute(
                        "SELECT COUNT(*) FROM memories WHERE superseded_by<>''").fetchone()[0]}
                mem_status = 'ok'
            finally:
                mc.close()
        except sqlite3.Error as e:
            mem_status, mem_problems = 'degraded', [f'{type(e).__name__}: {e}']
    else:
        mem_problems = ['no control-plane database; memory store not initialized']
    add({'id': 'brain-' + hashlib.sha256(f'memories:{adb}'.encode()).hexdigest()[:12],
         'kind': 'memories', 'role': _BRAIN_ROLES['memories'],
         'engine': autopilot.MEMORY_ENGINE_TAG, 'path': str(adb),
         'status': mem_status, 'problems': mem_problems, 'counts': mem_counts,
         'sha256': '', 'files': [], 'secret_kinds': []})
    # Claude memory-sync metadata: sync state keyed by memory-file paths. The
    # directory name is a legacy on-disk location, not a live service binding.
    sync_path = hermes / 'hindsight' / 'claude-memory-sync.json'
    doc, problem = _brain_json_doc(sync_path)
    sync_counts = {} if doc is None else (
        {'entries': len(doc)} if isinstance(doc, dict) else
        {'entries': len(doc)} if isinstance(doc, list) else {})
    if doc is not None and not isinstance(doc, (dict, list)):
        problem, sync_counts = 'unrecognized sync document shape', {}
    add({'id': 'brain-' + hashlib.sha256(f'claude_sync:{sync_path}'.encode()).hexdigest()[:12],
         'kind': 'claude_sync', 'role': _BRAIN_ROLES['claude_sync'],
         'path': str(sync_path),
         'status': 'absent' if problem == 'missing' else 'ok' if not problem else 'corrupted',
         'problems': ([] if problem in ('', 'missing')
                      else ['claude-memory-sync.json unreadable: ' + problem]),
         'counts': sync_counts,
         'sha256': _scan_sha256(sync_path) if sync_path.is_file() else '',
         'files': [], 'secret_kinds': []})
    # Human archive: per-project Claude memory directories, checksummed.
    projects_root = claude / 'projects'
    mem_dirs = sorted({m.parent.parent for m in projects_root.rglob('memory/MEMORY.md')}) \
        if projects_root.is_dir() else []
    mem_src = _brain_file_source('claude_memory', projects_root, _BRAIN_ROLES['claude_memory'],
                                 targets=[d / 'memory' for d in mem_dirs])
    mem_src['counts']['projects'] = len(mem_dirs)
    add(mem_src)
    # Session cache: disposable derived rows in the control plane + raw store.
    sess_dir = hermes / 'sessions'
    raw_store = sess_dir / 'raw-store'
    sess_extra = {'raw_store_present': raw_store.is_dir()}
    if adb.exists():
        cache = _brain_ro_counts(adb, ('sessions', 'session_messages'))
        sess_extra.update({f'cache_{k}': v for k, v in cache.items()})
    add(_brain_file_source('sessions', sess_dir, _BRAIN_ROLES['sessions'],
                           targets=[raw_store], extra_counts=sess_extra))
    # Profile / skill definitions: agent configuration, checksummed.
    add(_brain_file_source('profiles', hermes / 'profiles', _BRAIN_ROLES['profiles'],
                           include=_brain_profile_definition))
    add(_brain_file_source('skills', hermes / 'skills', _BRAIN_ROLES['skills'],
                           include=_brain_skill_definition))
    # Cron definitions: jobs.json is the definition surface; runtime artifacts
    # (executions.db, output/) are counted but never opened as authority.
    cron_dir = hermes / 'cron'
    jobs_path = cron_dir / 'jobs.json'
    jdoc, jproblem = _brain_json_doc(jobs_path) if jobs_path.is_file() else (None, 'missing')
    if jproblem == 'missing':
        cron_extra = {}
    elif jproblem:
        cron_extra = {}
    elif isinstance(jdoc, list):
        cron_extra = {'jobs': len(jdoc)}
    elif isinstance(jdoc, dict):
        cron_extra = {'jobs': len(jdoc.get('jobs', jdoc))}
    else:
        jproblem, cron_extra = 'unrecognized jobs document shape', {}
    cron_src = _brain_file_source('cron', cron_dir, _BRAIN_ROLES['cron'],
                                  targets=[jobs_path], extra_counts=cron_extra)
    if jproblem and jproblem != 'missing':
        cron_src['status'] = 'corrupted'
        cron_src['problems'].append('cron/jobs.json unreadable: ' + jproblem)
    elif jproblem == 'missing' and cron_dir.is_dir():
        cron_src['problems'].append('cron/jobs.json not present')
    add(cron_src)

    rank = {k: i for i, k in enumerate(_BRAIN_ROLES)}
    sources.sort(key=lambda s: (rank.get(s['kind'], 99), s['path']))
    body = {'format': BRAIN_INVENTORY_FORMAT, 'created_at': t,
            'hermes_home': str(hermes), 'claude_home': str(claude),
            'sources': sources,
            'fail_closed': fail_closed,
            'summary': {
                'sources': len(sources),
                'healthy': sum(1 for s in sources if s['status'] == 'ok'),
                'blocked': sum(1 for s in sources if s['status'] in ('corrupted', 'ambiguous')),
                'degraded_or_unavailable': sum(1 for s in sources
                                               if s['status'] in ('degraded', 'unavailable')),
                'absent': sum(1 for s in sources if s['status'] == 'absent'),
                'secret_kinds': sorted({k for s in sources for k in s['secret_kinds']})}}
    # Same sealing convention as migrate-inventory: created_at stays outside
    # the digest so unchanged sources reproduce a byte-identical manifest.
    digest = hashlib.sha256(json.dumps(
        {k: v for k, v in body.items() if k != 'created_at'}, sort_keys=True).encode()).hexdigest()
    doc_out = {**body, 'sha256': digest}
    # Unlike the older execution-state inventory, this end-to-end source
    # sweep must be genuinely read-only: the live Autopilot home is itself a
    # source and even a digest-only audit event would mutate it. The sealed
    # manifest is therefore the audit artifact for this stage.
    out = getattr(args, 'out', None)
    compact = {'ok': not fail_closed, 'sha256': digest, 'fail_closed': fail_closed,
               'summary': body['summary'],
               'sources': [{k: s[k] for k in ('id', 'kind', 'role', 'path', 'status')}
                           for s in sources]}
    if out:
        out_path = Path(out); out_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix='.brain-inventory.', dir=str(out_path.parent))
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(json.dumps(doc_out, sort_keys=True)); f.flush(); os.fsync(f.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, str(out_path))
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
        print(json.dumps({**compact, 'path': str(out_path)}, sort_keys=True))
    else:
        print(json.dumps(doc_out, sort_keys=True))
    if fail_closed:
        blocked = [f"{s['id']} ({s['path']}): {'; '.join(s['problems'])}"
                   for s in sources if s['status'] in ('corrupted', 'ambiguous')]
        sys.exit('brain inventory failed closed:\n  ' + '\n  '.join(blocked))

def brain_inventory_check(args):
    """Verify a sealed brain-inventory manifest without touching any source."""
    path = Path(args.path)
    if not path.exists(): raise SystemExit(f'manifest not found: {path}')
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f'manifest is not valid JSON: {e}')
    expected = doc.get('sha256')
    body = {k: v for k, v in doc.items() if k != 'sha256'}
    if body.get('format') != BRAIN_INVENTORY_FORMAT:
        raise SystemExit('unrecognized brain inventory format')
    actual = hashlib.sha256(json.dumps(
        {k: v for k, v in body.items() if k != 'created_at'}, sort_keys=True).encode()).hexdigest()
    if actual != expected:
        raise SystemExit('manifest integrity check failed; refusing a tampered manifest')
    print(json.dumps({'ok': True, 'path': str(path), 'created_at': body.get('created_at'),
                      'hermes_home': body.get('hermes_home'),
                      'summary': body.get('summary'),
                      'fail_closed': body.get('fail_closed', False),
                      'sources': [{'kind': s.get('kind'), 'role': s.get('role'),
                                   'status': s.get('status')} for s in body.get('sources', [])],
                      'sha256': expected}, sort_keys=True))

BRAIN_IMPORT_FORMAT = 'mindos-brain-import-v1'
_BRAIN_IMPORT_TEXT_EXTS = _SCAN_TEXT_EXTS | {'.jsonl', '.toml', '.ini', '.cfg'}

def _load_brain_inventory(path: Path):
    """Load a brain inventory only after verifying its content seal."""
    if not path.exists():
        raise SystemExit(f'brain inventory not found: {path}')
    try:
        doc = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        raise SystemExit(f'brain inventory is not valid JSON: {e}')
    if doc.get('format') != BRAIN_INVENTORY_FORMAT:
        raise SystemExit('unrecognized brain inventory format')
    body = {k: v for k, v in doc.items() if k != 'sha256'}
    actual = hashlib.sha256(json.dumps(
        {k: v for k, v in body.items() if k != 'created_at'}, sort_keys=True).encode()).hexdigest()
    if actual != doc.get('sha256'):
        raise SystemExit('brain inventory integrity check failed; refusing a tampered manifest')
    return doc

def _brain_relative(path: str):
    """Validate a manifest-relative path before it becomes a target path."""
    p = Path(path)
    if p.is_absolute() or '..' in p.parts or path in ('', '.'):
        raise SystemExit(f'unsafe relative path in brain inventory: {path!r}')
    return p

def _brain_text_redacted(data: bytes):
    """Return a redacted text copy, or None when bytes are not safe text."""
    try:
        text = data.decode('utf-8')
    except UnicodeDecodeError:
        return None
    return autopilot._redact_secrets(text).encode()

def _brain_entry_bytes(source: Path, expected_sha: str, label: str):
    """Read a source byte-for-byte only after detecting inventory drift."""
    if not source.is_file() or source.is_symlink():
        raise SystemExit(f'brain source missing or unsafe after inventory: {label}')
    actual = _scan_sha256(source)
    if not expected_sha or actual != expected_sha:
        raise SystemExit(
            f'brain source drift detected for {label}; expected inventory checksum '
            f'{expected_sha or "<missing>"}, got {actual}; take a fresh snapshot')
    return source.read_bytes()

def _brain_target_write(path: Path, data: bytes):
    """Atomic create-or-verify write. Never replaces local target truth."""
    digest = hashlib.sha256(data).hexdigest()
    if path.exists() or path.is_symlink():
        if not path.is_file() or path.is_symlink() or _scan_sha256(path) != digest:
            raise SystemExit(f'target conflict at {path}; refusing to overwrite local data')
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix='.brain-import.', dir=str(path.parent))
    except OSError as e:
        raise SystemExit(f'target is not writable at {path.parent}: {type(e).__name__}: {e}')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    return True

def _brain_plan_file(src, rel: Path, target_rel: Path, reason=''):
    """Turn one checksummed manifest file into a safe plan row."""
    source_root = Path(src['path'])
    source = source_root / rel
    expected = next((f.get('sha256') for f in src.get('files', [])
                     if f.get('path') == str(rel)), '')
    return {'source_id': src['id'], 'kind': src['kind'], 'source': str(source),
            'source_rel': str(rel), 'target_rel': str(target_rel),
            'expected_sha256': expected, 'secret_kinds': [], 'reason': reason}

def _brain_import_plan(inv):
    """Create a deterministic copy/bind/quarantine plan from a sealed inventory.

    The inventory already represents a read-only snapshot. This function does
    no I/O beyond interpreting that document; apply re-verifies every source
    checksum before it creates anything in the target.
    """
    actions, blockers = [], []
    for src in inv.get('sources', []):
        kind, status = src.get('kind'), src.get('status')
        if kind == 'memories':
            actions.append({'action': 'external_execution_import_required',
                            'source_id': src.get('id'), 'kind': kind, 'status': status,
                            'reason': 'semantic memory lives in the control-plane '
                                      'database and moves with the execution-truth import'})
            continue
        if kind == 'autopilot':
            actions.append({'action': 'external_execution_import_required', 'source_id': src.get('id'),
                            'kind': kind, 'status': status,
                            'reason': 'execution truth remains the existing migrate-import boundary'})
            continue
        if status == 'absent':
            actions.append({'action': 'skip_absent', 'source_id': src.get('id'), 'kind': kind,
                            'reason': '; '.join(src.get('problems', []))})
            continue
        if status not in ('ok', 'ok_partial_scan'):
            blockers.append(f"{src.get('id')} ({kind}) is {status}: {'; '.join(src.get('problems', []))}")
            continue
        if status == 'ok_partial_scan':
            blockers.append(f"{src.get('id')} ({kind}) was partially scanned; take a complete inventory")
            continue
        if kind in ('temporal', 'claude_sync'):
            dest = 'temporal.db' if kind == 'temporal' else 'metadata/claude-memory-sync.json'
            actions.append({'action': 'copy', 'source_id': src['id'], 'kind': kind,
                            'source': src['path'], 'target_rel': dest,
                            'expected_sha256': src.get('sha256', ''),
                            'secret_kinds': list(src.get('secret_kinds', [])), 'reason': ''})
            continue
        if kind not in ('claude_memory', 'sessions', 'profiles', 'skills', 'cron'):
            actions.append({'action': 'quarantine_unsupported_source', 'source_id': src.get('id'),
                            'kind': kind, 'reason': 'unsupported source kind is not copied'})
            continue
        prefix = {'claude_memory': 'archives/claude-memory', 'sessions': 'sessions',
                  'profiles': 'profiles', 'skills': 'skills', 'cron': 'cron'}[kind]
        for f in src.get('files', []):
            rel = _brain_relative(f.get('path', ''))
            if kind == 'sessions' and not (rel.parts and rel.parts[0] == 'raw-store'):
                actions.append({'action': 'quarantine_unsupported_file', 'source_id': src['id'],
                                'kind': kind, 'source_rel': str(rel),
                                'expected_sha256': f.get('sha256', ''),
                                'reason': 'derived session cache is rebuildable; only raw-store is portable'})
                continue
            if kind == 'cron' and rel != Path('jobs.json'):
                actions.append({'action': 'quarantine_unsupported_file', 'source_id': src['id'],
                                'kind': kind, 'source_rel': str(rel),
                                'expected_sha256': f.get('sha256', ''),
                                'reason': 'only cron/jobs.json is a portable definition'})
                continue
            row = _brain_plan_file(src, rel, Path(prefix) / rel)
            row['secret_kinds'] = list(f.get('secret_kinds', []))
            actions.append({'action': 'copy', **row})
    return actions, blockers

def brain_import(args=None):
    """Apply a sealed brain inventory into an explicit new MindOS home.

    Dry-run is the default. Sources are re-hashed immediately before copying;
    changed sources, incomplete inventories, and pre-existing different target
    bytes fail closed. Semantic memory is not copied here: it lives in the
    control-plane database and moves with the execution-truth import.
    Credential-shaped source files are quarantined by default, or copied as a
    redacted text derivative with --redact. Every applied run writes a sealed
    report and source-to-target provenance records; re-running verifies those
    exact bytes and becomes a no-op.
    """
    inv = _load_brain_inventory(Path(args.inventory))
    if inv.get('fail_closed'):
        raise SystemExit('brain inventory failed closed; resolve blocked sources before import')
    target = Path(args.target).expanduser()
    if not target.is_absolute():
        raise SystemExit('--target must be an explicit absolute new MindOS home')
    target = target.resolve()
    source_hermes = Path(inv.get('hermes_home', '')).expanduser()
    if target == source_hermes or target == source_hermes / 'autopilot':
        raise SystemExit('refusing to import into the inventoried Hermes source home')
    actions, blockers = _brain_import_plan(inv)
    if blockers:
        raise SystemExit('brain import refused:\n  ' + '\n  '.join(blockers))
    redact = bool(getattr(args, 'redact', False))
    materialized, quarantine = [], []
    # Preflight every source byte and every target conflict before a single
    # destination write. This is the snapshot-consistency boundary.
    for action in actions:
        if action['action'] != 'copy':
            continue
        data = _brain_entry_bytes(Path(action['source']), action['expected_sha256'],
                                  action['source'])
        kinds = sorted({f['kind'] for f in autopilot._secret_findings(data.decode('utf-8', errors='ignore'))}) \
            if Path(action['source']).suffix.lower() in _BRAIN_IMPORT_TEXT_EXTS else []
        kinds = sorted(set(kinds) | set(action.get('secret_kinds', [])))
        target_path = target / _brain_relative(action['target_rel'])
        if kinds:
            redacted = _brain_text_redacted(data) if redact else None
            if redacted is None:
                quarantine.append({**action, 'secret_kinds': kinds,
                                   'reason': 'credential-shaped or non-text source quarantined'})
                continue
            data = redacted
            action = {**action, 'secret_kinds': kinds, 'redacted': True}
        if target_path.exists() or target_path.is_symlink():
            digest = hashlib.sha256(data).hexdigest()
            if not target_path.is_file() or target_path.is_symlink() or _scan_sha256(target_path) != digest:
                blockers.append(f'target conflict at {target_path}')
        materialized.append((action, data))
    if blockers:
        raise SystemExit('brain import refused:\n  ' + '\n  '.join(blockers))
    for action in actions:
        if action['action'] == 'quarantine_unsupported_file':
            quarantine.append(action)
    planned = {'copy': len(materialized), 'quarantine': len(quarantine),
               'execution_import_required': any(a['action'] == 'external_execution_import_required'
                                                for a in actions)}
    if not getattr(args, 'apply', False):
        plan_actions = [{k: v for k, v in a.items() if k not in ('source',)} for a in actions]
        plan_quarantine = [{k: v for k, v in a.items() if k not in ('source',)} for a in quarantine]
        body = {'format': BRAIN_IMPORT_FORMAT, 'created_at': utc(), 'applied': False,
                'dry_run': True, 'target': str(target), 'inventory_sha256': inv['sha256'],
                'planned': planned, 'actions': plan_actions, 'quarantine': plan_quarantine}
        digest = hashlib.sha256(json.dumps({k: v for k, v in body.items() if k != 'created_at'},
                                           sort_keys=True).encode()).hexdigest()
        report = {**body, 'sha256': digest}
        out = getattr(args, 'out', '')
        if out:
            out_path = Path(out).expanduser()
            if not out_path.is_absolute():
                out_path = target / out_path
            _brain_target_write(out_path, json.dumps(report, sort_keys=True).encode())
        else:
            out_path = None
        print(json.dumps({'ok': True, 'dry_run': True, 'target': str(target),
                          'inventory_sha256': inv['sha256'], 'sha256': digest,
                          'planned': planned, 'actions': plan_actions,
                          'quarantine': plan_quarantine,
                          **({'report': str(out_path)} if out_path else {})}, sort_keys=True))
        return
    written, existing = [], []
    records = []
    for action, data in materialized:
        destination = target / _brain_relative(action['target_rel'])
        created = _brain_target_write(destination, data)
        (written if created else existing).append(str(destination))
        records.append({'source_id': action['source_id'], 'kind': action['kind'],
                        'source_path': action['source'], 'source_sha256': action['expected_sha256'],
                        'target_path': str(destination.relative_to(target)),
                        'target_sha256': hashlib.sha256(data).hexdigest(),
                        'redacted': bool(action.get('redacted')), 'secret_kinds': action.get('secret_kinds', [])})
    provenance = {'format': 'mindos-brain-provenance-v1', 'inventory_sha256': inv['sha256'],
                  'records': records, 'quarantine': [{k: v for k, v in a.items()
                                                       if k not in ('source',)} for a in quarantine]}
    provenance_bytes = json.dumps(provenance, sort_keys=True).encode()
    prov_target = target / 'provenance' / f'brain-import-{inv["sha256"]}.json'
    created = _brain_target_write(prov_target, provenance_bytes)
    (written if created else existing).append(str(prov_target))
    # The sealed report describes durable state, not the transient fact that a
    # particular re-run happened to create rather than verify an artifact.
    # That keeps the same inventory/target report reusable on an idempotent
    # resume while stdout still tells the operator what this run wrote.
    body = {'format': BRAIN_IMPORT_FORMAT, 'created_at': utc(), 'applied': True,
            'target': str(target), 'inventory_sha256': inv['sha256'], 'planned': planned,
            'artifacts': records, 'quarantine': provenance['quarantine'],
            'provenance': str(prov_target.relative_to(target))}
    digest = hashlib.sha256(json.dumps({k: v for k, v in body.items() if k != 'created_at'},
                                       sort_keys=True).encode()).hexdigest()
    report = {**body, 'sha256': digest}
    report_target = Path(getattr(args, 'out', '') or
                         target / 'migrations' / f'brain-import-{inv["sha256"][:12]}.json')
    report_target = report_target.expanduser()
    if not report_target.is_absolute():
        report_target = target / report_target
    if report_target.exists() or report_target.is_symlink():
        try:
            previous = json.loads(report_target.read_text())
        except (OSError, json.JSONDecodeError):
            raise SystemExit(f'target conflict at {report_target}; existing report is unreadable')
        previous_body = {k: v for k, v in previous.items() if k not in ('sha256', 'created_at')}
        report_body = {k: v for k, v in report.items() if k not in ('sha256', 'created_at')}
        previous_digest = hashlib.sha256(json.dumps(previous_body, sort_keys=True).encode()).hexdigest()
        if previous.get('format') != BRAIN_IMPORT_FORMAT or previous.get('sha256') != previous_digest \
                or previous_body != report_body:
            raise SystemExit(f'target conflict at {report_target}; refusing to replace a different report')
    else:
        _brain_target_write(report_target, json.dumps(report, sort_keys=True).encode())
    print(json.dumps({'ok': True, 'applied': True, 'target': str(target), 'sha256': digest,
                      'inventory_sha256': inv['sha256'], 'planned': planned,
                      'written': [str(Path(p).relative_to(target)) for p in written],
                      'already_present': [str(Path(p).relative_to(target)) for p in existing],
                      'quarantine': len(quarantine), 'report': str(report_target)}, sort_keys=True))

HOME_SELECTOR_FORMAT = 'mindos-home-selector-v1'
# Health checks a MindOS home must pass before the selector may point at it.
# Everything here is read-only: doctor-style consistency sweep plus presence
# checks for the artifacts a brain-imported home is expected to carry.

def _home_doctor_problems(root: Path):
    """Read-only health/doctor verification of an Autopilot/MindOS home."""
    import autopilot as _ap  # local alias; module-level import already exists
    problems = []
    if not (root / 'state.db').is_file():
        return [{'kind': 'state_db_missing', 'home': str(root)}]
    # Run the full consistency sweep against this home by pointing the
    # autopilot module at it for the duration (restored afterwards).
    saved = (_ap.ROOT, _ap.DB, _ap.RECEIPTS, _ap.POLICIES)
    try:
        _ap.ROOT, _ap.DB = root, root / 'state.db'
        _ap.RECEIPTS, _ap.POLICIES = root / 'receipts', root / 'policies'
        with _ap.conn() as c:
            problems.extend(_ap.audit_chain_problems(c))
            stale = c.execute(
                "SELECT id FROM tasks WHERE lease_expires_at!='' AND lease_expires_at<=? "
                "AND status IN ('claimed','running','waiting_for_agent')",
                (_ap.now(),)).fetchall()
            for r in stale:
                problems.append({'kind': 'stale_lease', 'task_id': r['id']})
            # Sealed chain checkpoints pinned under <home>/backups: divergence
            # proves tail truncation or history rewriting, exactly as doctor.
            backups = root / 'backups'
            if backups.exists():
                for p in sorted(backups.glob('checkpoint-*.json')):
                    try:
                        cp = _ap._load_checkpoint(str(p))
                    except SystemExit:
                        problems.append({'kind': 'checkpoint_file_invalid', 'path': p.name})
                        continue
                    problems.extend(_ap.checkpoint_problems(c, cp))
    finally:
        _ap.ROOT, _ap.DB, _ap.RECEIPTS, _ap.POLICIES = saved
    return problems


def home_show(args=None):
    """Report which home the runtime resolves to and why (read-only)."""
    print(json.dumps({'root': str(autopilot.ROOT), 'source': autopilot.HOME_SOURCE,
                      'default_home': str(autopilot.DEFAULT_HOME),
                      'selector_path': str(autopilot.SELECTOR_PATH),
                      'selector_present': autopilot.SELECTOR_PATH.exists()}, sort_keys=True))


def home_select(args=None):
    """Point the Hermes runtime at an explicit new MindOS home — reversibly."""
    target = Path(args.home).expanduser()
    if not target.is_absolute():
        raise SystemExit('--home must be an explicit absolute MindOS home')
    target = target.resolve()
    if not target.is_dir():
        raise SystemExit(f'home does not exist: {target}')
    if target == autopilot.DEFAULT_HOME.resolve():
        raise SystemExit('refusing to select the rollback default home; deselect instead')
    problems = _home_doctor_problems(target)
    if problems:
        raise SystemExit(f'{target} failed health verification:\n  ' +
                         '\n  '.join(p['kind'] + ': ' + json.dumps({k: v for k, v in p.items() if k != "kind"}, sort_keys=True)
                                     for p in problems))
    body = {'format': HOME_SELECTOR_FORMAT, 'source': 'mindos', 'home': str(target),
            'selected_at': utc(), 'rollback_home': str(autopilot.DEFAULT_HOME)}
    path = Path(getattr(args, 'selector', '') or autopilot.SELECTOR_PATH).expanduser()
    fd, tmp = tempfile.mkstemp(prefix='.selector.', dir=str(path.parent))
    try:
        os.write(fd, json.dumps(body, sort_keys=True).encode())
        os.close(fd); fd = None
        os.chmod(tmp, 0o600)
        os.replace(tmp, str(path))
    except OSError:
        if fd is not None:
            os.close(fd)
        raise
    print(json.dumps({'ok': True, 'selected': str(target), 'selector': str(path),
                      'rollback': body['rollback_home'], 'health': 'pass'}, sort_keys=True))


def home_deselect(args=None):
    """Revert to the immutable ~/.hermes/autopilot rollback home."""
    path = Path(getattr(args, 'selector', '') or autopilot.SELECTOR_PATH).expanduser()
    if getattr(args, 'apply', False):
        existed = path.exists() or path.is_symlink()
        if existed:
            path.unlink()
        print(json.dumps({'ok': True, 'removed_selector': existed,
                          'active_home': str(autopilot.DEFAULT_HOME),
                          'source': 'default'}, sort_keys=True))
    else:
        planned = path.exists() or path.is_symlink()
        print(json.dumps({'ok': True, 'dry_run': True, 'would_remove_selector': planned,
                          'active_home': str(autopilot.DEFAULT_HOME)}, sort_keys=True))


def home_doctor(args=None):
    """Read-only health/doctor verification of an explicit home (no selection)."""
    target = Path(args.home).expanduser().resolve()
    if not target.is_dir():
        raise SystemExit(f'home does not exist: {target}')
    problems = _home_doctor_problems(target)
    print(json.dumps({'ok': not problems, 'home': str(target), 'count': len(problems),
                      'problems': problems}, sort_keys=True))

def policy(args):
    # Resolve through autopilot.POLICIES so HERMES_AUTOPILOT_HOME is honored —
    # a hardcoded live-home path would read (or miss) the wrong policies in
    # isolated/test environments and report gate decisions about the wrong fleet.
    path = autopilot.POLICIES / f'{args.project.lower()}.yaml'
    if not path.exists(): print(json.dumps({'allowed':False,'reason':'no project policy'})); return
    text=path.read_text(); key=args.action+'_requires_user: true'
    requires=key in text
    print(json.dumps({'project':args.project,'action':args.action,'allowed':not requires,'requires_user':requires,'policy':str(path)}))

# ---------------------------------------------------------------------------
# §3 minimum slice: sense → playbook repair → breaker → learning.
#
# `sense` is ONE sweep that reuses the existing read-only checks (doctor,
# verify-chain, recall-stale, unverified-completions) and folds their outputs
# into typed findings: each carries an id, a severity, the concrete evidence,
# and the repair kind that can fix it. A finding's identity is a content hash
# over its kind + evidence, so the same defect recurring produces the same
# hash — exactly what the repair circuit breaker counts.
# ---------------------------------------------------------------------------

SEVERITY_BY_PROBLEM_KIND = {
    'audit_chain_break': 'P0',
    'receipt_file_hash_mismatch': 'P1',
    'checkpoint_file_invalid': 'P1',
    'last_receipt_dangling': 'P1',
    'stale_lease': 'P1',
    'receipt_file_missing': 'P2',
    'receipt_row_missing': 'P2',
    'fts_index_drift': 'P2',
    'fact_task_missing': 'P2',
    'multiple_live_handoffs': 'P2',
    'supersede_target_missing': 'P3',
    'handoff_supersede_target_missing': 'P3',
    'orphan_dependency': 'P3',
    'orphan_note': 'P3',
    'orphan_handoff': 'P3',
}

# Problem kinds that have a shipped playbook (everything else is reported for
# a human — sense never improvises repairs).
REPAIR_KIND_BY_PROBLEM_KIND = {
    'fts_index_drift': 'fts-rebuild',
    'stale_lease': 'stale-lease-recover',
}


def _make_finding(kind, severity, evidence, repair_kind=''):
    """Content-hashed finding identity: same defect ⇒ same hash ⇒ recurrence
    is observable by exact equality instead of fuzzy matching."""
    body = json.dumps({'kind': kind, 'evidence': evidence}, sort_keys=True)
    h = hashlib.sha256(body.encode()).hexdigest()[:16]
    return {'id': f'find-{h}', 'hash': h, 'kind': kind, 'severity': severity,
            'evidence': evidence,
            **({'suggested_repair': repair_kind} if repair_kind else {})}


def _problem_finding(p):
    kind = p.get('kind', 'unknown')
    sev = SEVERITY_BY_PROBLEM_KIND.get(
        kind, 'P2' if kind == 'audit_chain_break' else 'P3')
    return _make_finding(f'doctor_{kind}', sev, p,
                         REPAIR_KIND_BY_PROBLEM_KIND.get(kind, ''))


def sense(args=None):
    """One typed-finding sweep over the fleet's existing consistency checks.

    Read-only. Reuses doctor, verify-chain, recall-stale, and
    unverified-completions verbatim (in-process, via _capture_json) rather
    than reimplementing any check, then maps each concrete problem onto a
    typed finding with a stable content hash. Findings whose kind matches a
    shipped repair playbook carry `suggested_repair`; everything else is
    surfaced at its severity for a human.
    """
    findings = []
    doctor_doc = _capture_json(doctor)
    for p in doctor_doc.get('problems', []):
        findings.append(_problem_finding(p))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        autopilot.verify_chain(SimpleNamespace(checkpoint=''))
    chain = json.loads(buf.getvalue())
    for p in chain.get('problems', []):
        findings.append(_make_finding('audit_chain_break', 'P0', p))
    rs = _capture_json(recall_stale)
    for item in rs.get('items', []):
        if item.get('state') == 'stale':
            findings.append(_make_finding('recall_stale', 'P3', item))
    uv = _capture_json(unverified_completions)
    for item in uv.get('items', []):
        findings.append(_make_finding(f"unverified_{item.get('kind')}", 'P2', item))
    # Stalled activity: a running holder that declared a stall deadline but
    # stayed silent past it. Read-only — recovery of the lease itself stays
    # with the guarded recover sweep once it truly expires.
    stalled = []
    t = utc()
    with autopilot.conn() as c:
        for r in c.execute(
                "SELECT h.task_id AS task_id,h.last_action AS last_action,"
                "h.next_intent AS next_intent,h.progress_state AS progress_state,"
                "h.stall_deadline AS stall_deadline FROM heartbeats h "
                "JOIN tasks ta ON ta.id=h.task_id WHERE ta.status='running' "
                "AND h.stall_deadline!='' AND h.stall_deadline<=? ORDER BY h.task_id", (t,)):
            stalled.append(dict(r))
    for item in stalled:
        findings.append(_make_finding('activity_stalled', 'P2', item))
    counts = {}
    for f in findings:
        counts[f['severity']] = counts.get(f['severity'], 0) + 1
    print(json.dumps({'ok': True, 'generated_at': utc(), 'count': len(findings),
                      'by_severity': counts, 'findings': findings},
                     sort_keys=True))


def _repairs_dir():
    # Playbooks ship in-tree next to ops.py; a HERMES_AUTOPILOT_HOME
    # policies/repairs directory overrides them so isolated/test homes can
    # inject their own without touching the shipped files.
    d = autopilot.POLICIES / 'repairs'
    return d if d.exists() else Path(__file__).parent / 'policies' / 'repairs'


def _parse_flat_yaml(text):
    """Minimal flat `key: value` reader for playbook files (no nested shapes,
    no dependencies): strips comments, splits on the first ': '."""
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#') or ':' not in line:
            continue
        k, v = line.split(':', 1)
        out[k.strip()] = v.strip()
    return out


def _load_playbooks():
    d = _repairs_dir()
    if not d.exists():
        return {}
    return {p.stem: _parse_flat_yaml(p.read_text()) for p in sorted(d.glob('*.yaml'))}


def _breaker_fact_id(kind):
    return f'playbook:{kind}'


def _live_breaker(c, kind):
    t = utc()
    row = c.execute(
        "SELECT id FROM facts WHERE subject=? AND predicate='breaker-tripped' "
        f"AND {autopilot._fact_live_sql()}", (_breaker_fact_id(kind), t)).fetchone()
    return row['id'] if row else ''


def _repair_count(c, finding_hash, window_hours):
    since = (datetime.now(timezone.utc)
             - timedelta(hours=window_hours)).replace(microsecond=0).isoformat()
    return c.execute(
        "SELECT COUNT(*) n FROM audit_events WHERE action='repair_completed' "
        "AND payload_json LIKE ? AND created_at>=?",
        ('%"finding_hash": "' + finding_hash + '"%', since)).fetchone()['n']


def _trip_breaker(c, pb_name, finding_hash, window_hours):
    """A playbook stuck in a repair loop stops being a repair: disable it by
    recording a live breaker fact with a validity window, open a P0
    investigate task, and leave an audited trail."""
    t = utc()
    fid = uuid.uuid4().hex
    valid_until = (datetime.now(timezone.utc)
                   + timedelta(hours=24)).replace(microsecond=0).isoformat()
    c.execute(
        "INSERT INTO facts(id,subject,predicate,object,source,task_id,valid_from,valid_until,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (fid, _breaker_fact_id(pb_name), 'breaker-tripped', 'true', 'ops-sense',
         '', t, valid_until, t))
    autopilot.audit(c, 'fact', fid, 'circuit_breaker_tripped',
                    {'playbook': pb_name, 'finding_hash': finding_hash,
                     'repeat_window_hours': window_hours, 'valid_until': valid_until})
    task_id = f'investigate-{pb_name}-{finding_hash[:8]}'
    ct = utc()
    try:
        c.execute(
            "INSERT INTO tasks(id,project,title,description,owner,status,priority,"
            "next_action,due_at,not_before,tags,requires_receipts,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (task_id, 'Repairs', f'Investigate recurring finding {pb_name}',
             f'Circuit breaker tripped: playbook {pb_name} repaired finding '
             f'{finding_hash} repeatedly within the window; root-cause before '
             're-enabling.', 'ops-sense', 'queued', 'P0',
             'root-cause the recurring finding', '', '', '[]', '[]', ct, ct))
    except sqlite3.IntegrityError:
        cur = c.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone()
        task_id = cur['id'] if cur else task_id
    autopilot.audit(c, 'task', task_id, 'created',
                    {'project': 'Repairs', 'owner': 'ops-sense', 'priority': 'P0',
                     'reason': 'circuit_breaker_tripped'})
    return {'breaker_fact_id': fid, 'investigate_task': task_id,
            'valid_until': valid_until}


def repair(args):
    """Execute a playbook repair as a normal leased task.

    The whole lifecycle is the ordinary queue's: create → claim (lease) → run
    the playbook command → seal a receipt of the playbook's required kind →
    complete citing that receipt. Gates run before anything mutates:

    - breaker: a live breaker fact for this playbook refuses execution;
      `--break-after` repeats of the same finding hash within
      `--window-hours` trip the breaker instead of repairing again;
    - blast-radius tier policy: a playbook marked requires_user in its
      policies/repairs file refuses until --approved-by names the human
      (the same policies-file shape as merge/deploy gates);
    - idempotence: the deterministic task id makes a double-submit of the
      same repair attempt a refusal, not a duplicate task.
    """
    name = args.playbook
    pbs = _load_playbooks()
    if name not in pbs:
        raise SystemExit(f'unknown repair playbook: {name} '
                         f'(known: {", ".join(sorted(pbs)) or "none"})')
    pb = pbs[name]
    dry = bool(getattr(args, 'dry_run', False))
    threshold = getattr(args, 'break_after', 3)
    window_hours = getattr(args, 'window_hours', 24.0)
    cmd = shlex.split(pb['command'])
    rollback = shlex.split(pb.get('rollback_command', ''))
    finding_hash = getattr(args, 'finding_hash', '') or ''
    plan = {'playbook': name, 'tier': int(pb.get('blast_radius_tier', '0')),
            'command': cmd, 'rollback_command': rollback,
            'receipt_kind': pb.get('receipt_kind', '')}
    if dry:
        plan.update({'ok': True, 'dry_run': True})
        print(json.dumps(plan, sort_keys=True))
        return
    with db() as c:
        breaker = _live_breaker(c, name)
        if breaker:
            raise SystemExit(f'playbook disabled by circuit breaker (fact {breaker}); '
                             'investigate before re-enabling')
        if finding_hash:
            n = _repair_count(c, finding_hash, window_hours)
            if n >= threshold:
                # Trip on its own connection: the refusal below must not roll
                # the breaker's durable record back with the refused repair.
                with db() as bc:
                    trip = _trip_breaker(bc, name, finding_hash, window_hours)
                raise SystemExit(f'circuit breaker tripped after {n} repeats in the '
                                 f'window: {json.dumps(trip, sort_keys=True)}')
        if str(pb.get('requires_user', '')).lower() == 'true':
            approver = (getattr(args, 'approved_by', '') or '').strip()
            if not approver:
                raise SystemExit(f"playbook '{name}' is tier-{plan['tier']} "
                                 "(requires_user); re-run with --approved-by <name>")
            plan['approved_by'] = approver
        seq = _repair_count(c, finding_hash, 24 * 36500) + 1 if finding_hash else 1
        task_id = (f"repair-{name}-{finding_hash[:8]}-{seq}" if finding_hash
                   else f"repair-{name}-{uuid.uuid4().hex[:8]}")
        if c.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone():
            raise SystemExit(f'repair task already exists: {task_id}')
        ct = utc()
        c.execute(
            "INSERT INTO tasks(id,project,title,description,owner,status,priority,"
            "next_action,due_at,not_before,tags,requires_receipts,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (task_id, 'Repairs', f'Repair: {name}',
             f'Playbook repair for finding {finding_hash or "ad-hoc"} '
             f'(tier {plan["tier"]}): {" ".join(cmd)}',
             'ops-repair', 'queued', 'P2', ' '.join(cmd), '', '', '[]', '[]', ct, ct))
        autopilot.audit(c, 'task', task_id, 'created',
                        {'project': 'Repairs', 'owner': 'ops-repair', 'priority': 'P2',
                         'playbook': name, 'finding_hash': finding_hash})
    # Lease + execute outside the schema transaction: the command itself opens
    # its own connection(s) and must see the claimed state.
    autopilot.claim(SimpleNamespace(id=task_id, owner='ops-repair', minutes=30,
                                    max_active=None, force=False))
    try:
        proc = subprocess.run([sys.executable, str(Path(__file__).parent / 'ops.py'), *cmd],
                              text=True, capture_output=True, timeout=120)
    except subprocess.TimeoutExpired:
        # A hung playbook must not leave the repair task leased and silent: the
        # lease would expire into the stale-task path minutes later with no
        # record of why. Fail it here the same way a non-zero exit is failed.
        autopilot.fail(SimpleNamespace(id=task_id, owner='ops-repair',
                                       reason='playbook command timed out after 120s',
                                       no_retry=True, max_retries=0, backoff_base=60,
                                       backoff_cap=3600, epoch=None))
        raise SystemExit('repair command timed out after 120s')
    if proc.returncode != 0:
        autopilot.fail(SimpleNamespace(id=task_id, owner='ops-repair',
                                       reason=f'playbook command failed: {proc.stderr[:400]}',
                                       no_retry=True, max_retries=0, backoff_base=60,
                                       backoff_cap=3600, epoch=None))
        raise SystemExit(f'repair command failed: {proc.stderr.strip()[:400]}')
    rec = _capture_json(autopilot.receipt, SimpleNamespace(
        task_id=task_id, kind=pb.get('receipt_kind', 'repair'),
        payload=json.dumps({'playbook': name, 'finding_hash': finding_hash,
                            'command': cmd})))
    autopilot.complete(SimpleNamespace(id=task_id, owner='ops-repair', epoch=None,
                                       recall_digest='',
                                       note=f'repair via playbook {name}',
                                       evidence_receipts=[rec['receipt_id']]))
    # Learning: a successful repair asserts the finding-hash triple into the
    # fact graph so later agents can query which playbook fixed which defect.
    learn = ''
    if finding_hash:
        fa = SimpleNamespace(subject=f'finding:{finding_hash}', predicate='repaired-by',
                             object=name, source='ops-repair', task=task_id,
                             valid_hours=None)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            autopilot.fact_assert(fa)
        learn = json.loads(buf.getvalue())['id']
        with db() as c:
            autopilot.audit(c, 'task', task_id, 'repair_completed',
                            {'playbook': name, 'finding_hash': finding_hash,
                             'learning_fact_id': learn})
    plan.update({'ok': True, 'task_id': task_id, 'receipt_id': rec['receipt_id'],
                 'learning_fact_id': learn})
    print(json.dumps(plan, sort_keys=True))


def repair_list(args=None):
    """Inventory shipped playbooks (name + gate fields) for operators."""
    out = [{'name': k, **v} for k, v in _load_playbooks().items()]
    print(json.dumps({'ok': True, 'count': len(out), 'playbooks': out}, sort_keys=True))


def repair_fts_rebuild(args=None):
    """Tier-0 repair: rebuild every external-content FTS index from its table.

    The indexes are derived cache — a rebuild cannot lose source rows — but it
    is still gated behind the playbook so it only ever runs as a leased,
    receipted task. --dry-run reports current drift without writing.
    """
    tables = (('notes', 'notes_fts', autopilot._fts_ready),
              ('tasks', 'tasks_fts', autopilot._fts_ready),
              ('handoffs', 'handoffs_fts', autopilot._handoffs_fts_ready),
              ('session_messages', 'session_messages_fts', autopilot._sessions_fts_ready),
              ('facts', 'facts_fts', autopilot._facts_fts_ready),
              ('memories', 'memories_fts', autopilot._memories_fts_ready))
    rebuilt, skipped, drifted = [], [], []
    with db() as c:
        for table, fts, ready in tables:
            if not ready(c):
                skipped.append(fts)
                continue
            src = {r[0] for r in c.execute(f'SELECT rowid FROM {table}')}
            try:
                idx = _fts_index_docs(c, fts)
            except sqlite3.Error:
                idx = None
            n_idx = c.execute(f'SELECT COUNT(*) n FROM {fts}').fetchone()['n']
            if idx is None:
                if len(src) != n_idx:
                    drifted.append({'table': table, 'rows': len(src), 'indexed': n_idx})
            elif src != idx:
                drifted.append({'table': table, 'rows': len(src), 'indexed': len(idx)})
            if not getattr(args, 'dry_run', False):
                c.execute(f"INSERT INTO {fts}({fts}) VALUES('rebuild')")
                autopilot.audit(c, 'system', fts, 'fts_rebuilt', {'table': table})
                rebuilt.append(fts)
    print(json.dumps({'ok': True, 'dry_run': bool(getattr(args, 'dry_run', False)),
                      'drift_before': drifted, 'rebuilt': rebuilt,
                      'skipped_no_fts5': skipped}, sort_keys=True))

# Nanny impulse vocabulary — closed set, deterministic derivation:
#   all_clear      nothing to do, fleet healthy;
#   working        this tick took mutation action (recoveries/repairs);
#   hit_snag       findings remain after the bounded repair budget;
#   decision_needed a human decision is being asked for (breaker tripped or
#                  a requires-user playbook suggested).
NANNY_STATES = ('all_clear', 'working', 'hit_snag', 'decision_needed')
_NANNY_MAX_DETAIL = 20


def _nanny_last_tick(c):
    row = c.execute(
        "SELECT payload_json FROM audit_events WHERE action='nanny_tick' "
        "ORDER BY id DESC LIMIT 1").fetchone()
    try:
        return json.loads(row['payload_json']) if row else None
    except json.JSONDecodeError:
        return None


def _nanny_capture_repair(playbook, finding_hash):
    """Run one playbook repair in-process and return its final JSON plan.

    `repair` streams the whole leased-task lifecycle (claim/receipt/complete
    documents) before its own plan, so the capture must decode every JSON
    document and keep the last rather than parsing the buffer as one.
    """
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        repair(SimpleNamespace(playbook=playbook, finding_hash=finding_hash,
                               dry_run=False, break_after=3, window_hours=24.0,
                               approved_by=''))
    out = buf.getvalue().strip()
    dec = json.JSONDecoder()
    docs, idx = [], 0
    while idx < len(out):
        obj, end = dec.raw_decode(out, idx)
        docs.append(obj)
        idx = end
        while idx < len(out) and out[idx] in ' \n\t':
            idx += 1
    return docs[-1] if docs else {}


def _nanny_playbook_tier(name, pbs):
    pb = pbs.get(name)
    if not pb:
        return None
    tier = int(pb.get('blast_radius_tier', '0') or 0)
    requires_user = str(pb.get('requires_user', '')).lower() == 'true'
    return {'tier': tier, 'requires_user': requires_user}


def nanny(args=None):
    """One bounded self-healing tick over existing primitives — never a daemon.

    Order per tick: recover (guarded stale-lease sweep) → sense (typed
    content-hashed findings) → at most --max-repairs tier-0 playbook repairs
    matched to findings (leases, receipts, breakers, and learning all
    inherited from `repair`) → escalate (overdue SLA sweep). Double-run safe:
    every stage is a guarded sweep or a deterministically-idempotent leased
    task, so a concurrent second tick records skips instead of duplicating
    work. Each real tick audits one `nanny_tick` event carrying its open
    finding hashes, so the next tick can diff against it (`carried_over`)
    and spend narrative only on what changed — momentum memory without spam.
    """
    dry = bool(getattr(args, 'dry_run', False))
    max_repairs = getattr(args, 'max_repairs', 2)
    t = utc()
    rec = _capture_json(recover, SimpleNamespace(
        max_retries=3, dry_run=dry, backoff_base=60, backoff_cap=3600))
    sn = _capture_json(sense)
    pbs = _load_playbooks()
    findings = sn.get('findings', [])
    decisions = []
    candidates = []
    for f in findings:
        kind = f.get('suggested_repair', '')
        meta = _nanny_playbook_tier(kind, pbs) if kind else None
        if not meta:
            continue
        if meta['requires_user']:
            decisions.append({'reason': 'requires_user_playbook', 'playbook': kind,
                              'finding_hash': f.get('hash')})
            continue
        if meta['tier'] != 0:
            continue
        candidates.append((kind, f))
    # Deterministic candidate order: playbook name, then finding hash.
    seen, ordered = set(), []
    for kind, f in sorted(candidates, key=lambda c: (c[0], c[1].get('hash', ''))):
        key = (kind, f.get('hash', ''))
        if key not in seen:
            seen.add(key)
            ordered.append({'playbook': kind, 'finding': f})
    would_repair, repairs, skipped = [], [], []
    for cand in ordered[:max_repairs if not dry else len(ordered)]:
        fh = cand['finding'].get('hash', '')
        if dry:
            would_repair.append({'playbook': cand['playbook'], 'finding_hash': fh})
            continue
        try:
            out = _nanny_capture_repair(cand['playbook'], fh)
            repairs.append({'ok': True, 'playbook': cand['playbook'],
                            'finding_hash': fh, 'task_id': out.get('task_id', '')})
        except SystemExit as e:
            msg = str(e)
            if 'circuit breaker' in msg:
                decisions.append({'reason': 'circuit_breaker', 'playbook': cand['playbook'],
                                  'finding_hash': fh, 'detail': msg.split(': ', 1)[-1][:300]})
            repairs.append({'ok': False, 'playbook': cand['playbook'],
                            'finding_hash': fh, 'error': msg[:300]})
        except Exception as e:  # a broken stage must not kill the tick report
            repairs.append({'ok': False, 'playbook': cand['playbook'],
                            'finding_hash': fh, 'error': f'{type(e).__name__}: {e}'[:300]})
    esc = _capture_json(escalate, SimpleNamespace(dry_run=dry)) if not dry else None
    # Momentum memory: diff this tick's open hashes against the last audited
    # one; previously-seen findings are reported compactly, never re-narrated.
    # Findings whose repair succeeded this tick are resolved, not open — a
    # fixed snag must not read as a lingering one.
    repaired_hashes = {r['finding_hash'] for r in repairs if r['ok'] and r.get('finding_hash')}
    remaining = [f for f in findings if f.get('hash') not in repaired_hashes]
    prev = None if dry else _nanny_last_tick(db())
    prev_hashes = set((prev or {}).get('open_hashes', []))
    open_hashes = sorted({f.get('hash', '') for f in remaining if f.get('hash')})
    carried = [h for h in open_hashes if h in prev_hashes]
    fresh = [f for f in remaining if f.get('hash') not in prev_hashes]
    actions = (len(rec.get('recovered', [])) + len(rec.get('failed', []))
               + sum(1 for r in repairs if r['ok'])
               + (len(esc.get('escalated', [])) if esc else 0))
    if decisions:
        state = 'decision_needed'
    elif remaining:
        state = 'hit_snag'
    elif actions:
        state = 'working'
    else:
        state = 'all_clear'
    assert state in NANNY_STATES
    report = {'ok': True, 'generated_at': t, 'state': state, 'dry_run': dry,
              'recovered': rec.get('recovered', []),
              'recover_failed': rec.get('failed', []),
              'recover_skipped': rec.get('skipped', []),
              'repairs': repairs,
              'would_repair': would_repair,
              'escalated': (esc or {}).get('escalated', []) if not dry else [],
              'new_findings': [
                  {k: f[k] for k in f if k in ('id', 'hash', 'kind', 'severity',
                                               'suggested_repair')}
                  for f in fresh[:_NANNY_MAX_DETAIL]],
              'carried_over': carried,
              'open_hashes': open_hashes,
              'decisions': decisions,
              'counts': {'findings_open': len(remaining),
                         'repaired_this_tick': sum(1 for r in repairs if r['ok']),
                         'repair_budget': max_repairs}}
    if not dry:
        with db() as c:
            autopilot.audit(c, 'system', 'nanny', 'nanny_tick', {
                'state': state, 'open_hashes': open_hashes,
                'recovered': len(report['recovered']),
                'repaired': report['counts']['repaired_this_tick'],
                'decisions': decisions})
    print(json.dumps(report, sort_keys=True))


import argparse
p=argparse.ArgumentParser(); s=p.add_subparsers(dest='cmd',required=True)
for name,fn in [('processes',processes),('github',github),('sentry',sentry),('morning',morning),('doctor',doctor)]:
 x=s.add_parser(name); x.set_defaults(fn=fn)
x=s.add_parser('home-show'); x.set_defaults(fn=home_show)
x=s.add_parser('home-select'); x.add_argument('--home',required=True,help='explicit absolute new MindOS home; health-verified before selection'); x.add_argument('--selector',default='',help='selector file path (default ~/.hermes/autopilot-home-selector.json)'); x.set_defaults(fn=home_select)
x=s.add_parser('home-deselect'); x.add_argument('--apply',action='store_true',help='without this flag the command is a read-only dry-run plan'); x.add_argument('--selector',default=''); x.set_defaults(fn=home_deselect)
x=s.add_parser('home-doctor'); x.add_argument('--home',required=True); x.set_defaults(fn=home_doctor)
x=s.add_parser('handoff-check'); x.add_argument('--task',default=''); x.add_argument('--ack-sla-hours',dest='ack_sla_hours',type=float,default=24); x.set_defaults(fn=handoff_check)
x=s.add_parser('recall-stale'); x.set_defaults(fn=recall_stale)
x=s.add_parser('notes-expired'); x.set_defaults(fn=notes_expired)
x=s.add_parser('secret-scan'); x.add_argument('--all',action='store_true'); x.set_defaults(fn=secret_scan)
x=s.add_parser('consolidate'); x.add_argument('--task',default=''); x.add_argument('--threshold',type=float,default=None); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=consolidate)
x=s.add_parser('dup-tasks'); x.add_argument('--threshold',type=float,default=None); x.set_defaults(fn=dup_tasks)
x=s.add_parser('unverified-completions'); x.set_defaults(fn=unverified_completions)
x=s.add_parser('sense'); x.set_defaults(fn=sense)
x=s.add_parser('repair'); x.add_argument('playbook'); x.add_argument('--finding-hash',dest='finding_hash',default=''); x.add_argument('--dry-run',action='store_true'); x.add_argument('--break-after',dest='break_after',type=int,default=3,help='trip the circuit breaker after this many repeats of the same finding hash in the window'); x.add_argument('--window-hours',dest='window_hours',type=float,default=24.0); x.add_argument('--approved-by',dest='approved_by',default='',help='user approving a requires_user (tier-gated) playbook'); x.set_defaults(fn=repair)
x=s.add_parser('repair-list'); x.set_defaults(fn=repair_list)
x=s.add_parser('fts-rebuild'); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=repair_fts_rebuild)
x=s.add_parser('nanny'); x.add_argument('--dry-run',action='store_true'); x.add_argument('--max-repairs',dest='max_repairs',type=int,default=2,help='cap playbook repairs executed per tick (bounded mutation budget)'); x.set_defaults(fn=nanny)
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
x=s.add_parser('migrate-inventory'); x.add_argument('--root',required=True); x.add_argument('--out',default=None); x.set_defaults(fn=migrate_inventory)
x=s.add_parser('migrate-inventory-check'); x.add_argument('path'); x.set_defaults(fn=migrate_inventory_check)
x=s.add_parser('migrate-import'); x.add_argument('--inventory',required=True); x.add_argument('--source-id',dest='source_id',required=True); x.add_argument('--apply',action='store_true',help='without this flag the command is a read-only dry-run plan'); x.add_argument('--out',default=None,help='write a sealed autopilot-migration-result-v1 document'); x.add_argument('--redact',action='store_true'); x.add_argument('--allow-secret',action='store_true'); x.add_argument('--relink-audit',dest='relink_audit',action='store_true',help='merge into a non-empty audit ledger and relink the combined chain'); x.set_defaults(fn=migrate_import)
x=s.add_parser('migrate-rollback'); x.add_argument('result'); x.add_argument('--apply',action='store_true',help='without this flag the command is a read-only dry-run plan'); x.add_argument('--force',action='store_true',help='cascade away drifted rows and local dependents of imported tasks'); x.set_defaults(fn=migrate_rollback)
x=s.add_parser('brain-inventory'); x.add_argument('--hermes-home',dest='hermes_home',default='',help='Hermes home to inventory (default ~/.hermes; read-only)'); x.add_argument('--claude-home',dest='claude_home',default='',help='Claude home to inventory (default ~/.claude; read-only)'); x.add_argument('--timeout',type=float,default=3.0,help='per-request HTTP timeout in seconds'); x.add_argument('--out',default=None,help='write a sealed mindos-brain-inventory-v1 manifest'); x.set_defaults(fn=brain_inventory)
x=s.add_parser('brain-inventory-check'); x.add_argument('path'); x.set_defaults(fn=brain_inventory_check)
x=s.add_parser('brain-import'); x.add_argument('--inventory',required=True,help='sealed mindos-brain-inventory-v1 manifest'); x.add_argument('--target',required=True,help='explicit absolute new MindOS home; never the inventoried source home'); x.add_argument('--apply',action='store_true',help='without this flag emit a dry-run plan only'); x.add_argument('--redact',action='store_true',help='copy credential-shaped text only as redacted derivatives; otherwise quarantine'); x.add_argument('--out',default='',help='sealed import report path (default <target>/migrations/)'); x.set_defaults(fn=brain_import)
x=s.add_parser('onboard'); x.add_argument('--inventory',required=True,help='sealed autopilot-migration-inventory-v1 manifest from migrate-inventory'); x.add_argument('--source-id',dest='source_id',default='',help='autopilot_sqlite source to import; auto-selected when exactly one is healthy'); x.add_argument('--apply',action='store_true',help='without this flag onboarding plans and verifies but does not import'); x.add_argument('--out',default=None,help='write a sealed autopilot-onboarding-v1 report document'); x.add_argument('--redact',action='store_true'); x.add_argument('--allow-secret',action='store_true'); x.add_argument('--relink-audit',dest='relink_audit',action='store_true',help='merge into a non-empty audit ledger and relink the combined chain (forwarded to migrate-import)'); x.add_argument('--probe',action='store_true',help='run the end-to-end cross-agent handoff/recall/ack/complete probe (requires --apply)'); x.set_defaults(fn=onboard)
if __name__ == '__main__':
    args=p.parse_args(); args.fn(args)
