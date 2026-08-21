#!/usr/bin/env python3
"""Executable smoke verification for the local Autopilot control plane."""
import json, os, subprocess, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).parent
with tempfile.TemporaryDirectory() as td:
    env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
    def run(*args):
        p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
        if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
        return json.loads(p.stdout)
    def run_fail(*args):
        p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
        assert p.returncode != 0, ('expected failure', args, p.stdout, p.stderr)
        return p.stderr
    task = run('create','--project','Verify','--title','smoke','--id','verify-1')
    assert task['status'] == 'queued'
    run('claim','verify-1','--owner','tester','--minutes','5')
    # Lease exclusivity: a second owner must not acquire a live lease.
    err = run_fail('claim','verify-1','--owner','intruder','--minutes','5')
    assert 'lease owned by tester' in err
    # Same-owner reclaim is allowed; renewal via heartbeat keeps the lease live.
    run('claim','verify-1','--owner','tester','--minutes','5')
    beat = run('heartbeat','verify-1','--owner','tester','--note','smoke')
    assert beat['status'] == 'running' and beat['lease_expires_at']
    # A non-holder must not be able to renew someone else's lease.
    run_fail('heartbeat','verify-1','--owner','intruder')
    rec = run('receipt','verify-1','--kind','verification','--payload','{"result":"pass"}')
    assert len(rec['sha256']) == 64 and (Path(td) / 'receipts' / (rec['receipt_id'] + '.json')).stat().st_mode & 0o077 == 0
    rows = run('list'); assert rows[0]['last_receipt'] == rec['receipt_id']
    import sqlite3
    with sqlite3.connect(Path(td) / 'state.db') as db:
        assert db.execute('select count(*) from audit_events').fetchone()[0] >= 4
    # Concurrency: exactly one of N simultaneous claimers wins the lease.
    run('create','--project','Verify','--title','race','--id','race-1')
    procs = [subprocess.Popen(
        [sys.executable, str(ROOT / 'autopilot.py'), 'claim', 'race-1', '--owner', f'worker-{i}', '--minutes', '5'],
        env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for i in range(8)]
    results = [p.communicate() + (p.returncode,) for p in procs]
    assert [r[2] for r in results].count(0) == 1, ('expected exactly one winner', results)
    winner = json.loads(next(r[0] for r in results if r[2] == 0))
    assert winner['lease_owner'].startswith('worker-')
    # Recovery: an expired lease is requeued and its retry budget consumed.
    run('create','--project','Verify','--title','recover smoke','--id','rec-1')
    run('claim','rec-1','--owner','tester','--minutes','0')
    def ops(*a):
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
        assert p.returncode == 0, (a, p.stdout, p.stderr)
        return json.loads(p.stdout)
    out = ops('recover','--max-retries','1')
    assert out['recovered'] == ['rec-1'] and out['failed'] == []
    row = next(r for r in run('list') if r['id'] == 'rec-1')
    assert row['status'] == 'queued' and row['retry_count'] == 1
    # Retry budget exhausted: the next stale lease fails the task permanently.
    run('claim','rec-1','--owner','tester','--minutes','0')
    out = ops('recover','--max-retries','1')
    assert out['failed'] == ['rec-1']
    detail = run('show','rec-1')
    assert detail['status'] == 'failed' and detail['blocked_reason'] == 'max lease retries exceeded'
    assert any(e['action'] == 'lease_failed' for e in detail['audit'])
    # Observability: metrics reflect the exercised state.
    m = run('metrics')
    assert m['tasks_by_status'].get('failed') == 1
    assert m['tasks_by_status'].get('claimed') == 1
    assert m['stale_leases'] == 0 and m['receipts'] >= 1 and m['audit_events'] >= 4
    # Dependencies: dispatch skips blocked tasks; claim enforces dependency order.
    run('create','--project','Verify','--title','blocker','--id','dep-a','--priority','P1')
    run('create','--project','Verify','--title','dependent','--id','dep-b','--priority','P0')
    run('dep','dep-b','dep-a')
    nx = run('next')
    assert nx['task']['id'] == 'dep-a', ('expected unblocked dep-a despite lower priority', nx)
    err = run_fail('claim','dep-b','--owner','tester','--minutes','5')
    assert 'unsatisfied dependencies' in err and 'dep-a(queued)' in err
    m = run('metrics'); assert m['queued_blocked_by_deps'] == 1
    got = run('next','--claim','--owner','tester','--minutes','5')
    assert got['claimed'] is True and got['task']['id'] == 'dep-a' and got['lease_expires_at']
    run('update','dep-a','--status','completed')
    got = run('next','--claim','--owner','tester','--minutes','5')
    assert got['task']['id'] == 'dep-b'
    detail = run('show','dep-b')
    dep = next(d for d in detail['dependencies'] if d['id'] == 'dep-a')
    assert dep['satisfied'] == 1 and dep['status'] == 'completed'
    # Cycle protection: a->b then b->a must be rejected.
    run('create','--project','Verify','--title','cyc1','--id','cyc-1')
    run('create','--project','Verify','--title','cyc2','--id','cyc-2')
    run('dep','cyc-2','cyc-1')
    err = run_fail('dep','cyc-1','cyc-2')
    assert 'cycle' in err
    # Self-dependency rejected.
    run('create','--project','Verify','--title','self','--id','dep-c')
    run_fail('dep','dep-c','dep-c')
    # create --depends-on wires dependencies at creation time.
    run('create','--project','Verify','--title','chained','--id','dep-d','--depends-on','dep-c')
    detail = run('show','dep-d')
    assert [d['id'] for d in detail['dependencies']] == ['dep-c']
    # Dispatch with no eligible work returns a null task.
    empty = run('next','--project','NoSuchProject')
    assert empty['task'] is None
print('autopilot verification: PASS')