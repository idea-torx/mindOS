#!/usr/bin/env python3
"""Autopilot v1.1 safe operations: recovery, approvals, reconciliation, reports."""
from __future__ import annotations
import json, os, re, sqlite3, subprocess, sys, urllib.request
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
    now=utc(); recovered=[]
    with db() as c:
        rows=c.execute("SELECT id,lease_owner,lease_expires_at,status FROM tasks WHERE lease_expires_at!='' AND lease_expires_at<? AND status IN ('claimed','running','waiting_for_agent')",(now,)).fetchall()
        for r in rows:
            c.execute("UPDATE tasks SET status='queued',lease_owner='',lease_expires_at='',retry_count=retry_count+1,blocked_reason='stale lease recovered',updated_at=? WHERE id=?",(now,r['id']))
            autopilot.audit(c, 'task', r['id'], 'lease_recovered', {'previous_owner': r['lease_owner'], 'previous_status': r['status']})
            recovered.append(r['id'])
    print(json.dumps({'ok':True,'recovered':recovered,'count':len(recovered)}))

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

def policy(args):
    path=Path.home()/'.hermes/autopilot/policies'/f'{args.project.lower()}.yaml'
    if not path.exists(): print(json.dumps({'allowed':False,'reason':'no project policy'})); return
    text=path.read_text(); key=args.action+'_requires_user: true'
    requires=key in text
    print(json.dumps({'project':args.project,'action':args.action,'allowed':not requires,'requires_user':requires,'policy':str(path)}))

import argparse
p=argparse.ArgumentParser(); s=p.add_subparsers(dest='cmd',required=True)
for name,fn in [('recover',recover),('processes',processes),('github',github),('sentry',sentry),('morning',morning)]:
 x=s.add_parser(name); x.set_defaults(fn=fn)
x=s.add_parser('approval'); x.add_argument('action',choices=['approve','reject','block']); x.add_argument('id'); x.add_argument('--by',default='leo'); x.add_argument('--reason',default=''); x.add_argument('--next-action',default=''); x.set_defaults(fn=approval)
x=s.add_parser('policy'); x.add_argument('project'); x.add_argument('action'); x.set_defaults(fn=policy)
args=p.parse_args(); args.fn(args)
