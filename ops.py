#!/usr/bin/env python3
"""Autopilot v1.1 safe operations: recovery, approvals, reconciliation, reports."""
from __future__ import annotations
import json, os, re, sqlite3, subprocess, sys, tempfile, urllib.request, hashlib
from datetime import datetime, timezone
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

def recover(args=None):
    now=utc(); recovered=[]; failed=[]
    max_retries = getattr(args, 'max_retries', 3) if args is not None else 3
    dry_run = bool(getattr(args, 'dry_run', False))
    plan = []
    with db() as c:
        rows=c.execute("SELECT id,lease_owner,lease_expires_at,status,retry_count FROM tasks WHERE lease_expires_at!='' AND lease_expires_at<=? AND status IN ('claimed','running','waiting_for_agent')",(now,)).fetchall()
        for r in rows:
            new_retry = r['retry_count'] + 1
            if new_retry > max_retries:
                # Retry budget exhausted: fail the task instead of looping forever.
                plan.append(('failed', r, new_retry))
            else:
                plan.append(('recovered', r, new_retry))
        if dry_run:
            # Non-destructive preview: report what the next real pass would do.
            print(json.dumps({'ok':True,'dry_run':True,
                'would_recover':[r['id'] for kind,r,_ in plan if kind=='recovered'],
                'would_fail':[r['id'] for kind,r,_ in plan if kind=='failed'],
                'count':len(plan)}))
            return
        for kind, r, new_retry in plan:
            if kind == 'failed':
                c.execute("UPDATE tasks SET status='failed',lease_owner='',lease_expires_at='',retry_count=?,blocked_reason='max lease retries exceeded',updated_at=? WHERE id=?",(new_retry,now,r['id']))
                autopilot.audit(c, 'task', r['id'], 'lease_failed', {'previous_owner': r['lease_owner'], 'previous_status': r['status'], 'retry_count': new_retry})
                failed.append(r['id'])
            else:
                c.execute("UPDATE tasks SET status='queued',lease_owner='',lease_expires_at='',retry_count=?,blocked_reason='stale lease recovered',updated_at=? WHERE id=?",(new_retry,now,r['id']))
                autopilot.audit(c, 'task', r['id'], 'lease_recovered', {'previous_owner': r['lease_owner'], 'previous_status': r['status'], 'retry_count': new_retry})
                recovered.append(r['id'])
    print(json.dumps({'ok':True,'recovered':recovered,'failed':failed,'count':len(recovered)+len(failed)}))

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

SNAPSHOT_TABLES = ('tasks', 'heartbeats', 'receipts', 'notes', 'task_deps', 'audit_events')

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
x=s.add_parser('recover'); x.add_argument('--max-retries',type=int,default=3); x.add_argument('--dry-run',action='store_true'); x.set_defaults(fn=recover)
x=s.add_parser('snapshot'); x.add_argument('--out',default=None); x.set_defaults(fn=snapshot)
x=s.add_parser('snapshot-check'); x.add_argument('path'); x.set_defaults(fn=snapshot_check)
x=s.add_parser('approval'); x.add_argument('action',choices=['approve','reject','block']); x.add_argument('id'); x.add_argument('--by',default='leo'); x.add_argument('--reason',default=''); x.add_argument('--next-action',default=''); x.set_defaults(fn=approval)
x=s.add_parser('policy'); x.add_argument('project'); x.add_argument('action'); x.set_defaults(fn=policy)
args=p.parse_args(); args.fn(args)
