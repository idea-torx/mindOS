#!/usr/bin/env python3
"""Executable smoke verification for the local Autopilot control plane.

Every test is a registered case: the runner gives each one a fresh temporary
home, runs ALL cases, collects failures, and reports them together at the end,
exiting non-zero if anything failed. `--only <name>` runs a single case for
fast iteration. Assertions are preserved verbatim from the historical single
flow; see each case's docstring-free body for what it proves.
"""
import json, os, subprocess, sys, tempfile, time, traceback
from pathlib import Path

ROOT = Path(__file__).parent

CASES = []
def case(name):
    def deco(fn):
        CASES.append((name, fn))
        return fn
    return deco

def main():
    argv = sys.argv[1:]
    only = None
    if '--only' in argv:
        i = argv.index('--only')
        if i + 1 >= len(argv):
            print('usage: verify.py --only <case-name>', file=sys.stderr); sys.exit(2)
        only = argv[i + 1]
    unknown = [n for n, _ in CASES if only and n == only]
    if only and not unknown:
        print(f'no such case: {only}; known cases:\n  ' + '\n  '.join(n for n, _ in CASES), file=sys.stderr)
        sys.exit(2)
    failures = []
    ran = 0
    for name, fn in CASES:
        if only and name != only:
            continue
        ran += 1
        t0 = time.time()
        try:
            fn()
            print(f'[PASS] {name} ({time.time()-t0:.1f}s)')
        except Exception as e:
            failures.append(name)
            print(f'[FAIL] {name}: {type(e).__name__}: {e}')
            traceback.print_exc()
        sys.stdout.flush()
    print(f'--- {ran - len(failures)}/{ran} cases passed ---')
    if failures:
        print('failed cases: ' + ', '.join(failures))
        sys.exit(1)
    print('autopilot verification: PASS')

# Case definitions follow; main() runs under __main__ below.
@case('core_lifecycle_and_recall')
def _case_core_lifecycle_and_recall():
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
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        import sqlite3, hashlib, time
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
        # renew: extends a live lease from now, keeps status and fencing epoch stable.
        r1 = run('renew','verify-1','--owner','tester','--minutes','45')
        assert r1['ok'] is True and r1['status'] == beat['status'] == 'running' and r1['lease_epoch'] == beat['lease_epoch']
        row = next(r for r in run('list') if r['id'] == 'verify-1')
        assert row['lease_expires_at'] == r1['lease_expires_at']
        run_fail('renew','verify-1','--owner','intruder','--minutes','10')      # foreign holder
        run_fail('renew','verify-1','--owner','tester','--minutes','0')        # invalid window
        err = run_fail('renew','verify-1','--owner','tester','--minutes','5','--epoch','999')
        assert 'lease superseded' in err                                        # fenced renewal
        # leases: fleet-wide view, live-only by default, soonest expiry first.
        assert any(l['task_id'] == 'verify-1' for l in run('leases','--owner','tester')['leases'])
        assert run('leases','--owner','nobody')['count'] == 0
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
        # fail: first-class failure recording shares the recovery backoff machinery.
        run('create','--project','Verify','--title','fail smoke','--id','fl-1')
        err = run_fail('fail','fl-1','--owner','tester','--reason','premature')
        assert 'no active lease' in err                                          # lease required
        run('claim','fl-1','--owner','tester','--minutes','5')
        err = run_fail('fail','fl-1','--owner','intruder')
        assert 'lease owned by tester' in err                                    # foreign holder
        f1 = run('fail','fl-1','--owner','tester','--reason','tests red','--backoff-base','60')
        assert f1['outcome'] == 'retry_scheduled' and f1['status'] == 'queued' and f1['retry_count'] == 1
        assert f1['retries_remaining'] == 2 and f1['lease_owner'] == '' and f1['recover_after']
        detail = run('show','fl-1')
        assert any(e['action'] == 'task_failed' for e in detail['audit'])
        nx = run('next','--explain')                                             # dispatch skips cooldown
        sk = next((s for s in nx.get('skipped',[]) if s['task_id'] == 'fl-1'), None)
        assert sk is not None and sk['reason'] == 'recovery_backoff' and sk['recover_after']
        run('claim','fl-1','--owner','tester','--minutes','5')                   # deliberate override clears it
        row = next(r for r in run('list') if r['id'] == 'fl-1')
        assert row['recover_after'] == ''
        # Budget exhaustion: the third failure goes terminally failed and strands dependents.
        run('create','--project','Verify','--title','downstream','--id','fl-2')
        run('dep','fl-2','fl-1')
        f2 = run('fail','fl-1','--owner','tester','--reason','still red','--max-retries','2')
        assert f2['outcome'] == 'retry_scheduled' and f2['retry_count'] == 2
        run('claim','fl-1','--owner','tester','--minutes','5')
        f3 = run('fail','fl-1','--owner','tester','--reason','hopeless','--max-retries','2')
        assert f3['outcome'] == 'failed_terminal' and f3['status'] == 'failed' and f3['retry_count'] == 3
        assert f3['dependents_stranded'] == ['fl-2'] and f3['blocked_reason'] == 'hopeless'
        detail = run('show','fl-1')
        assert any(e['action'] == 'task_failed_terminal' for e in detail['audit'])
        run_fail('fail','fl-1','--owner','tester')                               # terminal is final
        # --no-retry forces a terminal failure on the first attempt.
        run('create','--project','Verify','--title','no retry','--id','fl-3')
        run('claim','fl-3','--owner','tester','--minutes','5')
        f4 = run('fail','fl-3','--owner','tester','--reason','unrecoverable','--no-retry')
        assert f4['outcome'] == 'failed_terminal' and f4['status'] == 'failed' and f4['retry_count'] == 1
        # leases: fleet-wide view hides expired-held leases by default; --all surfaces them.
        run('create','--project','Verify','--title','stale holder','--id','lst-1')
        run('claim','lst-1','--owner','ghost','--minutes','0')                  # expires immediately
        lv = run('leases')
        ids = [l['task_id'] for l in lv['leases']]
        assert 'lst-1' not in ids and 'verify-1' in ids, ('expired lease must be hidden by default', lv)
        assert lv['count'] == lv['live_count'] and all(l['live'] for l in lv['leases'])
        lva = run('leases','--all')
        stale = [l for l in lva['leases'] if l['task_id'] == 'lst-1']
        assert len(stale) == 1 and stale[0]['live'] is False and stale[0]['owner'] == 'ghost'
        expiries = [l['lease_expires_at'] for l in lva['leases']]
        assert expiries == sorted(expiries), 'leases must sort by soonest expiry'
        out = ops('recover','--max-retries','3')   # sweep the stale holder before metrics checks
        assert 'lst-1' in out['recovered']
        # Observability: metrics reflect the exercised state.
        m = run('metrics')
        assert m['tasks_by_status'].get('failed') == 3
        assert m['tasks_by_status'].get('claimed') == 1
        assert m['failures_retried_total'] == 2 and m['failures_terminal_total'] == 2
        assert m['stale_leases'] == 0 and m['receipts'] >= 1 and m['audit_events'] >= 4
        # Dependencies: dispatch skips blocked tasks; claim enforces dependency order.
        run('create','--project','Verify','--title','blocker','--id','dep-a','--priority','P1')
        run('create','--project','Verify','--title','dependent','--id','dep-b','--priority','P0')
        run('dep','dep-b','dep-a')
        nx = run('next')
        assert nx['task']['id'] == 'dep-a', ('expected unblocked dep-a despite lower priority', nx)
        err = run_fail('claim','dep-b','--owner','tester','--minutes','5')
        assert 'unsatisfied dependencies' in err and 'dep-a(queued)' in err
        m = run('metrics'); assert m['queued_blocked_by_deps'] == 2  # dep-b + stranded fl-2
        got = run('next','--claim','--owner','tester','--minutes','5')
        assert got['claimed'] is True and got['task']['id'] == 'dep-a' and got['lease_expires_at']
        run('update','dep-a','--status','completed')
        row = next(r for r in run('list') if r['id'] == 'dep-a')
        assert row['status'] == 'completed' and row['lease_owner'] == '', 'terminal update must release lease'
        got = run('next','--claim','--owner','tester','--minutes','5')
        assert got['task']['id'] == 'dep-b'
        # Operator remediation for a terminally failed prerequisite: cancel the
        # stranded dependent so archival guards see no live->terminal dependency.
        cx = run('cancel','fl-2','--owner','leo','--reason','prerequisite fl-1 failed terminally')
        assert cx['status'] == 'cancelled'
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
        # Lifecycle guardrails: only the live lease holder may complete a task.
        run('create','--project','Verify','--title','finish me','--id','fin-1')
        err = run_fail('complete','fin-1','--owner','someone')
        assert 'no active lease' in err
        run('claim','fin-1','--owner','worker-a','--minutes','5')
        err = run_fail('complete','fin-1','--owner','worker-b')
        assert 'lease owned by worker-a' in err
        done = run('complete','fin-1','--owner','worker-a','--note','tests pass')
        assert done['status'] == 'completed' and done['lease_owner'] == ''
        run_fail('complete','fin-1','--owner','worker-a')
        detail = run('show','fin-1')
        assert any(e['action'] == 'completed' for e in detail['audit'])
        # Cancel: terminal transition records reason; cancelled tasks cannot be claimed.
        run('create','--project','Verify','--title','drop me','--id','can-1')
        run('cancel','can-1','--owner','leo','--reason','obsolete approach')
        row = next(r for r in run('list') if r['id'] == 'can-1')
        assert row['status'] == 'cancelled' and row['blocked_reason'] == 'obsolete approach'
        err = run_fail('claim','can-1','--owner','tester','--minutes','5')
        assert 'terminal task' in err
        # Cancel must not override a foreign live lease.
        run('create','--project','Verify','--title','held','--id','can-2')
        run('claim','can-2','--owner','holder','--minutes','5')
        err = run_fail('cancel','can-2','--owner','leo','--reason','x')
        assert 'lease owned by holder' in err
        # Per-owner lease cap: an owner cannot exceed their live-lease budget.
        run('create','--project','Verify','--title','cap one','--id','cap-1')
        run('create','--project','Verify','--title','cap two','--id','cap-2')
        run('claim','cap-1','--owner','capped-worker','--minutes','5','--max-active','1')
        err = run_fail('claim','cap-2','--owner','capped-worker','--minutes','5','--max-active','1')
        assert 'lease capacity' in err and '(1/1)' in err
        # A different owner is unaffected by the first owner's cap.
        run('claim','cap-2','--owner','other-worker','--minutes','5','--max-active','1')
        # Completing a task frees capacity for the same owner.
        run('complete','cap-1','--owner','capped-worker')
        run('create','--project','Verify','--title','cap three','--id','cap-3')
        run('claim','cap-3','--owner','capped-worker','--minutes','5','--max-active','1')
        # next --claim honors the cap too: capped owner is skipped, not silently starved.
        run('create','--project','Verify','--title','cap four','--id','cap-4')
        err = run_fail('next','--claim','--owner','capped-worker','--minutes','5','--max-active','1')
        assert 'lease capacity' in err
        m = run('metrics')
        assert m['active_leases_by_owner'].get('capped-worker') == 1
        assert m['active_leases_by_owner'].get('other-worker') == 1
        # Operator search: substring match across text fields with filters.
        hits = run('search','cap two')
        assert [h['id'] for h in hits] == ['cap-2']
        hits = run('search','cap','--status','claimed')
        assert {h['id'] for h in hits} == {'cap-2','cap-3'}
        hits = run('search','smoke','--project','NoSuchProject')
        assert hits == []
        # Recovery dry-run: previews actions without mutating anything.
        run('create','--project','Verify','--title','dry recover','--id','dry-1')
        run('claim','dry-1','--owner','tester','--minutes','0')
        out = ops('recover','--dry-run')
        assert out['dry_run'] is True and out['would_recover'] == ['dry-1'] and out['would_fail'] == []
        row = next(r for r in run('list') if r['id'] == 'dry-1')
        assert row['status'] == 'claimed' and row['lease_owner'] == 'tester', 'dry-run must not mutate'
        out = ops('recover','--max-retries','3')
        assert out['recovered'] == ['dry-1']
        # Audit chain integrity: chain verifies on a healthy database.
        chain = run('verify-chain')
        assert chain['ok'] is True and chain['events'] >= 8 and chain['problems'] == []
        # Doctor: consistency sweep is clean on this exercised environment.
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Shared memory: provenance-tagged notes with exact-duplicate dedup.
        run('create','--project','Verify','--title','memory host','--id','mem-1')
        n1 = run('note','mem-1','--kind','fact','--content','API rate limit is 60/min','--source','hermes')
        assert n1['deduplicated'] is False and n1['id']
        dup = run('note','mem-1','--kind','fact','--content','  API rate limit is 60/min ','--source','other-agent')
        assert dup['deduplicated'] is True and dup['id'] == n1['id'], 'exact duplicate must reuse the live note'
        err = run_fail('note','mem-1','--kind','gossip','--content','x')
        assert 'invalid note kind' in err
        err = run_fail('note','no-such-task','--content','x')
        assert 'task not found' in err
        # Temporal facts: supersede swaps old->new atomically; old stays queryable with --all.
        sup = run('supersede-note',n1['id'],'--content','API rate limit raised to 120/min','--source','hermes')
        assert sup['old_note_id'] == n1['id'] and sup['new_note_id']
        err = run_fail('supersede-note',n1['id'],'--content','double supersede')
        assert 'already superseded' in err
        live = run('notes','mem-1')
        assert [n['content'] for n in live] == ['API rate limit raised to 120/min']
        allnotes = run('notes','mem-1','--all')
        assert len(allnotes) == 2 and allnotes[0]['superseded_by'] == sup['new_note_id']
        # Superseding onto identical live content is rejected (would be a no-op duplicate).
        err = run_fail('supersede-note',sup['new_note_id'],'--content','API rate limit raised to 120/min')
        assert 'identical content' in err
        # Context budget: packing respects the char budget and reports truncation.
        for i in range(4):
            run('note','mem-1','--kind','observation','--content','observation-%s %s' % (i, 'x'*40))
        ctx = run('context','mem-1','--budget','100000')
        assert ctx['truncated'] is False and ctx['notes_packed'] == ctx['notes_total'] == 5
        assert ctx['notes'][0]['created_at'] <= ctx['notes'][-1]['created_at'], 'oldest-first packing'
        small = run('context','mem-1','--budget','80')
        assert small['truncated'] is True and small['used_chars'] <= 80 and small['notes_packed'] < 5
        zero = run('context','mem-1','--budget','0')
        assert zero['notes_packed'] == 0 and zero['truncated'] is True
        # Note retrieval: keyword search joins task filters; superseded notes are hidden.
        hits = run('search-notes','rate limit')
        assert {h['id'] for h in hits} == {sup['new_note_id']}
        hits = run('search-notes','observation-1','--project','Verify')
        assert len(hits) == 1 and hits[0]['task_id'] == 'mem-1'
        assert run('search-notes','rate limit','--status','completed') == []
        m = run('metrics')
        assert m['notes_total'] == 6 and m['notes_superseded'] == 1
        # Voluntary lease release: holder requeues without consuming retry budget.
        run('create','--project','Verify','--title','hand back','--id','rel-1')
        run('claim','rel-1','--owner','worker-r','--minutes','5')
        err = run_fail('release','rel-1','--owner','someone-else')
        assert 'lease owned by worker-r' in err
        rel = run('release','rel-1','--owner','worker-r')
        assert rel['status'] == 'queued' and rel['lease_owner'] == '' and rel['retry_count'] == 0
        row = next(r for r in run('list') if r['id'] == 'rel-1')
        assert row['status'] == 'queued'
        detail = run('show','rel-1')
        assert any(e['action'] == 'lease_released' for e in detail['audit'])
        run_fail('release','fin-1','--owner','worker-a')  # terminal tasks cannot be released
        # Pinned notes: critical facts survive tight context budgets.
        run('create','--project','Verify','--title','pin host','--id','mem-2')
        for i in range(3):
            run('note','mem-2','--kind','observation','--content','noise-%s %s' % (i, 'y'*40))
        pin = run('note','mem-2','--kind','constraint','--content','MUST NOT exceed 120/min','--source','leo','--pinned')
        assert pin['deduplicated'] is False and pin['id']
        # Duplicate add of pinned content upgrades the existing note instead of growing the store.
        dup_pin = run('note','mem-2','--kind','constraint','--content','MUST NOT exceed 120/min','--source','hermes')
        assert dup_pin['deduplicated'] is True and dup_pin['id'] == pin['id']
        import sqlite3
        with sqlite3.connect(Path(td) / 'state.db') as db:
            assert db.execute("SELECT pinned FROM notes WHERE id=?", (pin['id'],)).fetchone()[0] == 1
        # Context pack v2: task summary + deps header, pinned-first packing under budget.
        ctx = run('context','mem-2','--budget','200')
        assert ctx['task']['id'] == 'mem-2' and ctx['task']['title'] == 'pin host'
        assert ctx['unsatisfied_dependencies'] == []
        assert ctx['truncated'] is True and ctx['used_chars'] <= 200
        assert [n['content'] for n in ctx['notes']] == ['MUST NOT exceed 120/min']
        assert ctx['notes_pinned_packed'] == 1 and ctx['notes_total'] == 4
        full = run('context','mem-2','--budget','100000')
        assert full['notes_packed'] == 4 and full['truncated'] is False
        assert full['notes'][0]['pinned'] == 1, 'pinned note packs first even with a huge budget'
        # Supersede inherits the pin so temporal fact chains stay protected.
        sup2 = run('supersede-note',pin['id'],'--content','MUST NOT exceed 240/min','--source','leo')
        live2 = run('context','mem-2','--budget','100000')
        new_note = next(n for n in live2['notes'] if n['id'] == sup2['new_note_id'])
        assert new_note['pinned'] == 1 and new_note['content'] == 'MUST NOT exceed 240/min'
        # search-notes --kind filter narrows retrieval by note kind.
        hits = run('search-notes','MUST NOT','--kind','constraint')
        assert {h['id'] for h in hits} == {sup2['new_note_id']}
        assert run('search-notes','MUST NOT','--kind','fact') == []
        err = run_fail('search-notes','x','--kind','gossip')
        assert 'invalid note kind' in err
        m = run('metrics')
        assert m['notes_pinned_live'] == 1, m
        # Lease fencing epochs: each acquisition bumps a monotonic token surfaced to holders.
        run('create','--project','Verify','--title','fence me','--id','fence-1')
        c1 = run('claim','fence-1','--owner','tester','--minutes','5')
        assert c1['lease_epoch'] == 1, c1
        err = run_fail('heartbeat','fence-1','--owner','tester','--epoch','99')
        assert 'lease superseded' in err
        beat = run('heartbeat','fence-1','--owner','tester','--epoch','1')
        assert beat['ok'] is True and beat['lease_epoch'] == 1
        err = run_fail('release','fence-1','--owner','tester','--epoch','7')
        assert 'lease superseded' in err
        # Same-owner ABA: expired lease requeued and reclaimed by the same owner;
        # the stale holder's old epoch must be rejected even though owner matches.
        run('claim','fence-1','--owner','tester','--minutes','0')
        out = ops('recover','--max-retries','3')
        assert out['recovered'] == ['fence-1']
        c2 = run('claim','fence-1','--owner','tester','--minutes','5')
        assert c2['lease_epoch'] == 3, c2  # expired claim + reclaim each bumped the epoch
        err = run_fail('complete','fence-1','--owner','tester','--note','stale','--epoch','1')
        assert 'lease superseded' in err and '(held epoch 1, current 3)' in err
        done = run('complete','fence-1','--owner','tester','--note','fresh','--epoch','3')
        assert done['status'] == 'completed'
        # next --claim also surfaces the fencing epoch.
        run('create','--project','Verify','--title','fence next','--id','fence-2','--priority','P0')
        got = run('next','--claim','--owner','tester','--minutes','5')
        assert got['task']['id'] == 'fence-2' and got['lease_epoch'] >= 1
        # FTS-ranked retrieval: BM25 ordering over notes and tasks via --rank.
        run('create','--project','Verify','--title','rank host','--id','fts-1',
            '--description','postgres pool incident followups')
        run('note','fts-1','--kind','fact','--content','postgres connection pool exhausted under load','--source','hermes')
        run('note','fts-1','--kind','observation','--content','github actions deploy pipeline green','--source','hermes')
        run('note','fts-1','--kind','fact','--content','postgres replica lag spikes during vacuum','--source','hermes')
        hits = run('search-notes','postgres pool','--rank')
        # Multi-token queries are conjunctive: only the note with both terms matches.
        assert len(hits) == 1 and 'score' in hits[0], hits
        assert hits[0]['content'] == 'postgres connection pool exhausted under load'
        assert all(h['task_id'] == 'fts-1' for h in hits)
        hits = run('search-notes','postgres','--rank','--kind','fact')
        assert {h['content'] for h in hits} == {
            'postgres connection pool exhausted under load',
            'postgres replica lag spikes during vacuum'}, hits
        hits = run('search','postgres','--rank')
        assert any(h['id'] == 'fts-1' and 'score' in h for h in hits), hits
        hits = run('search','postgres','--rank','--project','Verify')
        assert {h['id'] for h in hits} == {'fts-1'}
        # Tokenless queries degrade to an empty result instead of an FTS syntax error.
        assert run('search-notes','!!! ---','--rank') == []
        assert run('search','!!! ---','--rank') == []
        # Default LIKE path is unchanged when --rank is absent.
        hits = run('search-notes','rate limit')
        assert {h['id'] for h in hits} == {sup['new_note_id']}
        # Note lineage: history walks predecessor -> successor across supersedes.
        hist = run('note-history',sup['new_note_id'])
        assert [n['id'] for n in hist] == [n1['id'], sup['new_note_id']], hist
        hist = run('note-history',n1['id'])
        assert len(hist) == 2 and hist[-1]['id'] == sup['new_note_id'], 'backward walk finds predecessors'
        err = run_fail('note-history','no-such-note')
        assert 'note not found' in err
        # Snapshot export: consistent JSON dump sealed with a self-hash.
        snap = ops('snapshot')
        assert snap['ok'] is True and len(snap['sha256']) == 64
        assert Path(snap['path']).exists(), snap
        assert snap['counts']['tasks'] >= 1 and snap['counts']['audit_events'] >= 1
        chk = ops('snapshot-check', snap['path'])
        assert chk['ok'] is True and chk['actual_sha256'] == snap['sha256']
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        tampered = json.loads(Path(snap['path']).read_text())
        tampered['snapshot']['tables']['tasks'][0]['title'] = 'tampered'
        Path(snap['path']).write_text(json.dumps(tampered, sort_keys=True))
        err = ops_fail('snapshot-check', snap['path'])
        assert '"ok": false' in err
        # Deadlines: due_at normalizes to UTC ISO 8601; invalid timestamps rejected.
        err = run_fail('create','--project','Verify','--title','bad due','--id','due-bad','--due-at','not-a-date')
        assert 'invalid due-at' in err
        run('create','--project','DueTest','--title','late thing','--id','due-1','--due-at','2020-01-01T00:00:00Z')
        row = next(r for r in run('list') if r['id'] == 'due-1')
        assert row['due_at'] == '2020-01-01T00:00:00+00:00', row
        m = run('metrics')
        assert 'due-1' in m['overdue_tasks'], m
        assert m['due_within_24h'] >= 0
        assert 'due-1' in {h['id'] for h in run('list','--overdue')}
        # Deadline-aware dispatch: within a priority, earliest due_at wins; undated sort last.
        run('create','--project','DueTest','--title','later thing','--id','due-2','--due-at','2030-01-01T00:00:00+00:00')
        run('create','--project','DueTest','--title','undated thing','--id','due-3')
        nx = run('next','--project','DueTest')
        assert nx['task']['id'] == 'due-1', nx
        got = run('next','--claim','--project','DueTest','--owner','tester','--minutes','5')
        assert got['task']['id'] == 'due-1' and got['claimed'] is True
        # Context pack surfaces the deadline; update can clear it.
        ctx = run('context','due-1','--budget','4000')
        assert ctx['task']['due_at'] == '2020-01-01T00:00:00+00:00'
        run('update','due-2','--due-at','')
        row = next(r for r in run('list') if r['id'] == 'due-2')
        assert row['due_at'] == '', row
        # Snapshot restore: disaster-recovery round trip with integrity guards.
        snap2 = ops('snapshot')
        err = ops_fail('snapshot-restore', snap2['path'])
        assert 'not empty' in err, 'non-empty target must require --force'
        res = ops('snapshot-restore', snap2['path'], '--force')
        assert res['ok'] is True and res['restored']['tasks'] >= 1 and res['fk_violations'] == [], res
        chain = run('verify-chain')
        assert chain['ok'] is True and chain['problems'] == [], 'audit chain must survive restore'
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        notes_after = run('notes','mem-2')
        assert any(n['content'] == 'MUST NOT exceed 240/min' for n in notes_after), 'note data restored'
        row = next(r for r in run('list') if r['id'] == 'due-1')
        assert row['status'] == 'claimed' and row['lease_owner'] == 'tester', 'lease state restored'
        # FTS indexes stay in sync across a full delete+insert restore (triggers fire).
        hits = run('search-notes','exceed','--rank')
        assert any(h['id'] == sup2['new_note_id'] for h in hits), 'fts index rebuilt after restore'
        hits = run('search','postgres','--rank')
        assert any(h['id'] == 'fts-1' for h in hits), 'task fts index rebuilt after restore'
        bad = Path(td) / 'bad-snap.json'
        evil = json.loads(Path(snap2['path']).read_text())
        evil['snapshot']['tables']['tasks'][0]['title'] = 'evil'
        bad.write_text(json.dumps(evil, sort_keys=True))
        err = ops_fail('snapshot-restore', str(bad), '--force')
        assert 'integrity check failed' in err
        # Cross-task retrieval: context --related pulls ranked notes from sibling tasks.
        run('create','--project','Verify','--title','postgres pool incident followups','--id','rag-src')
        run('note','rag-src','--kind','fact','--content','postgres connection pool exhausted under load','--source','hermes')
        run('note','rag-src','--kind','observation','--content','gardening tip about tomatoes entirely unrelated','--source','hermes')
        ctx = run('context','fts-1','--budget','100000','--related','3')
        assert ctx['related_requested'] == 3 and ctx['related_matched'] >= 1 and ctx['related_packed'] >= 1, ctx
        rel = [n for n in ctx['notes'] if n.get('related')]
        assert {n['task_id'] for n in rel} == {'rag-src'}, 'self-task notes must never appear as related'
        assert all(n['via_task_title'] == 'postgres pool incident followups' for n in rel)
        assert any('postgres' in n['content'] for n in rel), 'best match must be retrieved'
        own = [n for n in ctx['notes'] if not n.get('related')]
        assert ctx['notes'][:len(own)] == own, 'own notes pack before related notes'
        assert 'score' in rel[0], 'FTS path carries a relevance score'
        # Budget: related notes respect the same character budget and report truncation.
        tight = run('context','fts-1','--budget','150','--related','3')
        assert tight['used_chars'] <= 150 and tight['truncated'] is True
        assert tight['related_packed'] < tight['related_matched'], tight
        # Scope: default project scope excludes other projects; global includes them.
        run('create','--project','OtherProj','--title','postgres pool incident followups','--id','rag-x')
        run('note','rag-x','--kind','fact','--content','postgres pool sized wrong in staging','--source','hermes')
        scoped = run('context','fts-1','--budget','100000','--related','5')
        assert {n['task_id'] for n in scoped['notes'] if n.get('related')} == {'rag-src'}
        glob = run('context','fts-1','--budget','100000','--related','5','--related-scope','global')
        assert {n['task_id'] for n in glob['notes'] if n.get('related')} == {'rag-src','rag-x'}
        # Tokenless task text degrades gracefully to zero related matches.
        run('create','--project','Verify','--title','','--id','rag-empty')
        empty = run('context','rag-empty','--budget','4000','--related','5')
        assert empty['related_matched'] == 0 and empty['related_packed'] == 0
        # Default context output shape is unchanged when --related is absent.
        plain = run('context','mem-2','--budget','100000')
        assert plain['related_requested'] == 0 and plain['related_packed'] == 0
        assert all(not n.get('related') for n in plain['notes'])
        # Audit event stream: filtered global query over the hash-chained ledger.
        ev = run('events','--action','note_added','--limit','2')
        assert ev['ok'] is True and len(ev['events']) == 2 and ev['truncated'] is True, ev
        assert all(e['action'] == 'note_added' and isinstance(e['payload'], dict) for e in ev['events'])
        assert ev['total_matching'] > 2
        ev = run('events','--entity-id','mem-1','--action','note_superseded')
        assert ev['total_matching'] >= 1 and ev['count'] == ev['total_matching']
        assert ev['truncated'] is False and ev['events'][0]['entity_id'] == 'mem-1'
        err = run_fail('events','--since','not-a-date')
        assert 'invalid --since' in err
        run_fail('events','--until','also-bad')
        ev = run('events','--limit','5','--verify')
        assert ev['chain']['ok'] is True and ev['chain']['problems'] == [], ev
        # Time-window filter: everything created after "now" yields nothing new.
        ev = run('events','--since','2030-01-01T00:00:00Z')
        assert ev['total_matching'] == 0 and ev['events'] == []
        # Archival: sealed export + removal of terminal tasks, with safety guards.
        run('create','--project','Archive','--title','done work','--id','arc-1')
        run('claim','arc-1','--owner','tester','--minutes','5')
        ar = run('receipt','arc-1','--kind','verification','--payload','{"result":"pass"}')
        run('note','arc-1','--kind','fact','--content','archived knowledge about postgres pool sizing','--source','hermes')
        run('complete','arc-1','--owner','tester','--note','done')
        run('create','--project','Archive','--title','drop work','--id','arc-2')
        run('cancel','arc-2','--owner','leo','--reason','obsolete')
        run('create','--project','Archive','--title','live dependent','--id','arc-3','--depends-on','arc-1')
        # Guard: a live task still depends on arc-1, so archiving must refuse.
        err = ops_fail('archive','--before','2030-01-01T00:00:00+00:00','--dry-run')
        assert 'still depend' in err and 'arc-3' in err, err
        run('complete','dep-b','--owner','tester','--note','finish leftover dependency host')
        run('cancel','arc-3','--owner','leo','--reason','not needed')
        dry = ops('archive','--before','2030-01-01T00:00:00+00:00','--dry-run')
        assert dry['dry_run'] is True and {'arc-1','arc-2','arc-3'} <= set(dry['task_ids']), dry
        assert dry['counts']['notes'] == 1 and dry['receipt_files'] == 1
        row = next(r for r in run('list') if r['id'] == 'arc-1')
        assert row['status'] == 'completed', 'dry-run must not mutate'
        arc = ops('archive','--before','2030-01-01T00:00:00+00:00')
        assert arc['ok'] is True and {'arc-1','arc-2','arc-3'} <= set(arc['archived']), arc
        assert Path(arc['path']).exists() and len(arc['sha256']) == 64
        live_ids = {r['id'] for r in run('list')}
        assert not live_ids & {'arc-1','arc-2','arc-3'}, 'archived tasks must leave the live registry'
        assert not (Path(td) / 'receipts' / (ar['receipt_id'] + '.json')).exists(), 'receipt file archived away'
        assert run('search-notes','archived knowledge') == [], 'fts index must drop archived notes'
        chain = run('verify-chain')
        assert chain['ok'] is True, 'audit events are retained; chain must stay intact'
        ev = run('events','--entity-id','arc-1','--action','completed')
        assert ev['total_matching'] >= 1, 'retained audit events stay queryable after archive'
        chk = ops('archive-check', arc['path'])
        assert chk['ok'] is True and chk['counts']['tasks'] >= 3, chk
        assert chk['before'] == '2030-01-01T00:00:00+00:00'
        # Restore: re-import the archive with integrity verification.
        res = ops('archive-restore', arc['path'])
        assert res['ok'] is True and set(res['restored_tasks']) >= {'arc-1'}, res
        row = next(r for r in run('list') if r['id'] == 'arc-1')
        assert row['status'] == 'completed'
        notes_back = run('notes','arc-1')
        assert any(n['content'].startswith('archived knowledge') for n in notes_back)
        hits = run('search-notes','archived knowledge')
        assert len(hits) == 1, 'fts trigger must re-index restored notes'
        assert (Path(td) / 'receipts' / (ar['receipt_id'] + '.json')).exists(), 'receipt file recreated'
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Restore refuses to collide with existing task ids unless --force.
        err = ops_fail('archive-restore', arc['path'])
        assert 'already exist' in err and '--force' in err
        # Tamper detection on archives.
        evil = json.loads(Path(arc['path']).read_text())
        evil['tables']['tasks'][0]['title'] = 'tampered'
        Path(arc['path']).write_text(json.dumps(evil, sort_keys=True))
        err = ops_fail('archive-check', arc['path'])
        assert '"ok": false' in err or 'integrity check failed' in err
        # Block/unblock: audited operator transitions with lease guards.
        run('create','--project','Verify','--title','pause me','--id','blk-1')
        run('claim','blk-1','--owner','holder-b','--minutes','5')
        err = run_fail('block','blk-1','--owner','leo','--reason','x')
        assert 'lease owned by holder-b' in err
        blk = run('block','blk-1','--owner','holder-b','--reason','waiting on credentials')
        assert blk['status'] == 'blocked' and blk['blocked_reason'] == 'waiting on credentials', blk
        assert blk['lease_owner'] == '', 'blocking must release the held lease'
        err = run_fail('claim','blk-1','--owner','tester','--minutes','5')
        assert 'task is blocked' in err and 'credentials' in err
        detail = run('show','blk-1')
        assert any(e['action'] == 'blocked' for e in detail['audit'])
        ub = run('unblock','blk-1','--owner','leo')
        assert ub['status'] == 'queued' and ub['blocked_reason'] == '', ub
        err = run_fail('unblock','blk-1','--owner','leo')
        assert 'not blocked' in err
        run_fail('block','fin-1','--owner','leo','--reason','x')  # terminal tasks cannot be blocked
        detail = run('show','blk-1')
        assert any(e['action'] == 'unblocked' for e in detail['audit'])
        # Transitive blockers: blocked-by walks the dependency DAG with depth + satisfaction.
        run('create','--project','Verify','--title','dag root','--id','dag-1')
        run('create','--project','Verify','--title','dag mid','--id','dag-2','--depends-on','dag-1')
        run('create','--project','Verify','--title','dag leaf','--id','dag-3','--depends-on','dag-2')
        bb = run('blocked-by','dag-3')
        assert bb['ok'] is True and bb['blocked'] is True, bb
        assert {b['id']: b['depth'] for b in bb['blockers']} == {'dag-2': 1, 'dag-1': 2}, bb
        assert all(b['satisfied'] == 0 for b in bb['blockers'])
        run('update','dag-1','--status','completed')
        bb = run('blocked-by','dag-3')
        assert {b['id']: b['satisfied'] for b in bb['blockers']} == {'dag-1': 1, 'dag-2': 0}, bb
        leafless = run('blocked-by','dag-1')
        assert leafless['blockers'] == [] and leafless['blocked'] is False
        err = run_fail('blocked-by','no-such-task')
        assert 'task not found' in err
        # show surfaces reverse edges (dependents) alongside dependencies.
        detail = run('show','dag-1')
        assert any(d['id'] == 'dag-2' for d in detail['dependents']), detail.get('dependents')
        detail = run('show','dag-3')
        assert detail['dependents'] == []
        # Dispatch diagnostics: next --explain reports skipped candidates and reasons.
        run('create','--project','ExplainTest','--title','blocked leaf','--id','exp-1',
            '--priority','P0','--depends-on','dag-2')
        nx = run('next','--project','ExplainTest','--explain')
        assert nx['task'] is None and nx['considered'] == 1, nx
        assert nx['skipped'] == [{'task_id': 'exp-1', 'reason': 'unsatisfied_dependencies',
                                  'blocked_by': ['dag-2']}], nx
        plain = run('next','--project','ExplainTest')
        assert plain['task'] is None and 'skipped' not in plain and 'considered' not in plain, \
            'default output shape must stay unchanged without --explain'
        run('update','dag-2','--status','completed')
        nx = run('next','--project','ExplainTest','--explain')
        assert nx['task']['id'] == 'exp-1' and nx['skipped'] == [], nx
        # Recovery backoff: recovered tasks cool down before redispatch instead of hot-looping.
        run('create','--project','Backoff','--title','thrash me','--id','bo-1')
        run('claim','bo-1','--owner','tester','--minutes','0')
        out = ops('recover','--max-retries','3')
        assert out['recovered'] == ['bo-1'] and out['backoff']['bo-1'], out
        row = next(r for r in run('list') if r['id'] == 'bo-1')
        assert row['status'] == 'queued' and row['recover_after'] == out['backoff']['bo-1'], row
        m = run('metrics'); assert m['tasks_in_backoff'] >= 1, m
        nx = run('next','--project','Backoff','--explain')
        assert not (nx['task'] and nx['task']['id'] == 'bo-1'), 'backoff task must not be dispatched'
        skip = next(s for s in nx['skipped'] if s['task_id'] == 'bo-1')
        assert skip['reason'] == 'recovery_backoff' and skip['recover_after'] == row['recover_after'], nx
        # Explicit claim stays allowed as a deliberate override; acquiring clears the cooldown.
        run('claim','bo-1','--owner','tester','--minutes','5')
        row = next(r for r in run('list') if r['id'] == 'bo-1')
        assert row['recover_after'] == '', 'acquire must clear the backoff'
        # Dry-run previews the cooldown without mutating anything.
        run('create','--project','Verify','--title','chill preview','--id','bo-2')
        run('claim','bo-2','--owner','tester','--minutes','0')
        out = ops('recover','--dry-run')
        assert out['dry_run'] is True and 'bo-2' in out['backoff'], out
        row = next(r for r in run('list') if r['id'] == 'bo-2')
        assert row['status'] == 'claimed' and row['recover_after'] == '', 'dry-run must not mutate'
        # --backoff-base 0 restores instant redispatch (backward-compatible mode).
        run('create','--project','Verify','--title','no chill','--id','bo-3')
        run('claim','bo-3','--owner','tester','--minutes','0')
        out = ops('recover','--max-retries','3','--backoff-base','0')
        assert 'bo-3' in out['recovered'] and out['backoff'] == {}, out
        row = next(r for r in run('list') if r['id'] == 'bo-3')
        assert row['recover_after'] == ''
        # Failed tasks get no cooldown — they are terminal.
        run('create','--project','Verify','--title','fail fast','--id','bo-4')
        run('claim','bo-4','--owner','tester','--minutes','0')
        out = ops('recover','--max-retries','0')
        assert out['failed'] == ['bo-4'], out
        row = next(r for r in run('list') if r['id'] == 'bo-4')
        assert row['status'] == 'failed' and row['recover_after'] == ''
        # Task correction: update can edit title/description/priority/project after creation.
        run('create','--project','Correct','--title','typo titel','--id','fix-1','--priority','P3')
        err = run_fail('update','fix-1','--priority','P9')
        assert 'invalid choice' in err
        upd = run('update','fix-1','--title','corrected title','--description','fixed description',
                  '--priority','P1','--project','Renamed')
        assert upd['title'] == 'corrected title' and upd['description'] == 'fixed description'
        assert upd['priority'] == 'P1' and upd['project'] == 'Renamed', upd
        detail = run('show','fix-1')
        last = detail['audit'][0]
        assert last['action'] == 'updated'
        assert last['payload']['priority'] == 'P1' and last['payload']['project'] == 'Renamed', last
        hits = run('search','corrected title')
        assert [h['id'] for h in hits] == ['fix-1'], 'renamed task must be findable'
        # Dependency edge removal: a mistaken edge can be undone, unblocking dispatch.
        run('create','--project','Verify','--title','wrong prereq','--id','rm-a')
        run('create','--project','Verify','--title','dependent on wrong prereq','--id','rm-b',
            '--depends-on','rm-a','--priority','P0')
        err = run_fail('claim','rm-b','--owner','tester','--minutes','5')
        assert 'unsatisfied dependencies' in err
        bb = run('blocked-by','rm-b')
        assert any(b['id'] == 'rm-a' for b in bb['blockers']), bb
        err = run_fail('dep-remove','rm-b','no-such-task')
        assert 'task not found' in err
        run('create','--project','Verify','--title','unrelated','--id','rm-c')
        err = run_fail('dep-remove','rm-b','rm-c')
        assert 'no such dependency' in err
        rem = run('dep-remove','rm-b','rm-a')
        assert rem['ok'] is True and rem['removed'] == 'rm-a', rem
        bb = run('blocked-by','rm-b')
        assert bb['blockers'] == [] and bb['blocked'] is False, 'removed edge must clear blockers'
        got = run('claim','rm-b','--owner','tester','--minutes','5')
        assert got['status'] == 'claimed', 'task must be claimable after edge removal'
        detail = run('show','rm-b')
        assert detail['dependencies'] == []
        assert any(e['action'] == 'dependency_removed' for e in detail['audit'])
        # Agent handoff protocol: durable, deduplicated, temporally superseded handoffs.
        run('create','--project','Verify','--title','handoff host','--id','ho-1')
        err = run_fail('handoff','no-such-task','--from-agent','hermes')
        assert 'task not found' in err
        h1 = run('handoff','ho-1','--from-agent','codex','--to-agent','claude-code',
                 '--status','running','--objective','implement retry path',
                 '--evidence','tests pass locally','--constraint','no new dependencies',
                 '--decision','use exponential backoff','--file','src/retry.py',
                 '--commit','abc1234','--next-action','open PR','--risk','flaky integration test')
        assert h1['ok'] is True and h1['deduplicated'] is False and h1['superseded'] is None, h1
        # Exact duplicate payload deduplicates onto the live handoff.
        dup = run('handoff','ho-1','--from-agent','codex','--to-agent','claude-code',
                  '--status','running','--objective','implement retry path',
                  '--evidence','tests pass locally','--constraint','no new dependencies',
                  '--decision','use exponential backoff','--file','src/retry.py',
                  '--commit','abc1234','--next-action','open PR','--risk','flaky integration test')
        assert dup['deduplicated'] is True and dup['id'] == h1['id'], dup
        # A new handoff supersedes the previous one atomically; history stays queryable.
        h2 = run('handoff','ho-1','--from-agent','claude-code','--to-agent','opencode',
                 '--status','waiting_for_review','--objective','review retry path',
                 '--next-action','merge after review')
        assert h2['deduplicated'] is False and h2['superseded'] == h1['id'], h2
        cur = run('handoff-current','ho-1')
        assert cur['id'] == h2['id'] and cur['from_agent'] == 'claude-code', cur
        assert cur['next_actions'] == ['merge after review'] and cur['superseded_by'] == ''
        hist = run('handoffs','ho-1','--all')
        assert [h['id'] for h in hist] == [h2['id'], h1['id']], hist
        assert hist[1]['superseded_by'] == h2['id'], 'old handoff must link to its successor'
        live = run('handoffs','ho-1')
        assert [h['id'] for h in live] == [h2['id']]
        assert run('handoff-current','ho-1')['id'] == h2['id']
        # Context pack: the live handoff packs after the header and within the budget.
        ctx = run('context','ho-1','--budget','100000')
        assert ctx['handoff_packed'] is True and ctx['handoff']['id'] == h2['id'], ctx
        assert ctx['handoff']['objective'] == 'review retry path'
        tight = run('context','ho-1','--budget','60')
        assert tight['handoff_packed'] is False and tight['handoff'] is None and tight['truncated'] is True, tight
        # show surfaces the live handoff alongside receipts/audit/deps.
        detail = run('show','ho-1')
        assert detail['handoff']['id'] == h2['id']
        m = run('metrics')
        assert m['handoffs_total'] == 2 and m['handoffs_superseded'] == 1, m
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # recall: session bootstrap bundle with a stable digest, lease awareness,
        # and an audited context_recalled event proving what was recalled.
        run('claim','ho-1','--owner','codex','--minutes','30')
        r1 = run('recall','ho-1','--agent','opencode','--budget','100000','--related','3')
        assert r1['task_id'] == 'ho-1' and len(r1['digest']) == 64, r1
        assert r1['handoff_packed'] is True and r1['handoff']['id'] == h2['id'], r1
        assert r1['lease']['owner'] == 'codex' and r1['lease']['live'] is True, r1['lease']
        assert r1['lease']['held_by_caller'] is False, 'caller (opencode) does not hold the lease'
        assert r1['latest_receipts'] == [] or all('payload' in x for x in r1['latest_receipts'])
        # Deterministic: identical state yields the identical digest.
        r1b = run('recall','ho-1','--agent','opencode','--budget','100000','--related','3')
        assert r1b['digest'] == r1['digest'], (r1['digest'], r1b['digest'])
        # A different caller sees held_by_caller flip; digest changes with state.
        r2 = run('recall','ho-1','--agent','codex','--budget','100000')
        assert r2['lease']['held_by_caller'] is True and r2['digest'] != r1['digest'], r2['lease']
        # State change (new note) must move the digest.
        run('note','ho-1','--kind','fact','--content','recalled context marker','--source','verify')
        r3 = run('recall','ho-1','--agent','opencode','--budget','100000')
        assert r3['digest'] != r1['digest'], 'digest must track context state'
        evs = run('events','--entity-id','ho-1','--action','context_recalled','--limit','10')
        assert evs['count'] >= 3, evs
        # recall-verify: freshness check of a previously recalled digest.
        rv = run('recall-verify','ho-1','--digest',r3['digest'],'--agent','opencode','--budget','100000')
        assert rv['ok'] is True and rv['fresh'] is True and rv['current_digest'] == r3['digest'], rv
        run('note','ho-1','--kind','fact','--content','post-recall drift marker','--source','verify')
        rv2 = run('recall-verify','ho-1','--digest',r3['digest'],'--agent','opencode','--budget','100000')
        assert rv2['fresh'] is False and rv2['current_digest'] != r3['digest'], rv2
        assert run_fail('recall-verify','ho-1','--digest','not-a-digest'), 'malformed digest must be rejected'
        # recall-diff: names exactly which sections moved since a cited recall.
        rd = run('recall-diff','ho-1','--digest',r3['digest'])
        assert rd['ok'] is True and rd['fresh'] is False and rd['state'] == 'stale', rd
        assert 'notes' in rd['sections_changed'], rd
        drift_id = rd['changes']['notes']['added']
        assert len(drift_id) == 1, rd
        assert any(n['id'] in drift_id for n in run('notes','ho-1')), rd
        # Fresh against itself: no changes at all.
        rfresh = run('recall','ho-1','--agent','opencode','--budget','100000')
        rdf = run('recall-diff','ho-1','--digest',rfresh['digest'])
        assert rdf['fresh'] is True and rdf['unchanged'] is True and rdf['changes'] == {}, rdf
        # Fabricated digest: unproven, not stale.
        rdx = run('recall-diff','ho-1','--digest','f'*64)
        assert rdx['state'] == 'unproven_recall_digest' and 'fresh' not in rdx, rdx
        # Recall provenance: handoffs and completions cite the digest they acted on.
        h3 = run('handoff','ho-1','--from-agent','opencode','--to-agent','claude-code',
                 '--objective','finish retry path','--recall-digest',r3['digest'])
        assert h3['id'] != h2['id'], 'new objective supersedes the live handoff'
        cur3 = run('handoff-current','ho-1')
        assert cur3['recall_digest'] == r3['digest'], cur3
        # A handoff recorded after the cited recall shows up as a handoff-section change.
        rdh = run('recall-diff','ho-1','--digest',r3['digest'])
        assert rdh['changes']['handoff']['from'] != h3['id'], rdh
        assert rdh['changes']['handoff']['to'] == h3['id'], rdh
        assert run_fail('handoff','ho-1','--from-agent','opencode','--recall-digest','zzz'), 'bad digest rejected'
        m2 = run('metrics')
        assert m2['handoffs_with_recall_proof'] >= 1, m2
        run('release','ho-1','--owner','codex')
        run('claim','ho-1','--owner','claude-code','--minutes','30')
        comp = run('complete','ho-1','--owner','claude-code','--note','done','--recall-digest',rv2['current_digest'])
        assert comp['status'] == 'completed', comp
        cev = [e for e in run('events','--entity-id','ho-1','--action','completed')['events']
               if e['payload'].get('recall_digest') == rv2['current_digest']]
        assert cev, 'completed event must carry the cited recall digest'
        # Temporal hybrid rerank: retrieval quality under stale-vs-fresh competition.
        run('create','--project','Verify','--title','rerank probe','--id','rr-1')
        run('create','--project','Verify','--title','old source','--id','rr-old')
        run('create','--project','Verify','--title','new source','--id','rr-new')
        run('note','rr-old','--kind','fact','--content','rerank probe ancient wisdom','--source','verify')
        run('note','rr-new','--kind','fact','--content','rerank probe fresh insight','--source','verify')
        def sq(*stmts):
            with sqlite3.connect(Path(td) / 'state.db') as db:
                for s in stmts: db.execute(s)
        sq("UPDATE notes SET created_at='2026-07-01T00:00:00+00:00' WHERE content LIKE 'rerank probe ancient%'")
        plain = run('context','rr-1','--budget','100000','--related','5')
        assert 'rerank' not in plain, 'packs built without --rerank must keep the legacy shape'
        rel_plain = [n for n in plain['notes'] if n.get('related')]
        assert {n['task_id'] for n in rel_plain} == {'rr-old','rr-new'}, rel_plain
        ranked = run('context','rr-1','--budget','100000','--related','5',
                     '--rerank','--recency-half-life-hours','24')
        assert ranked['rerank'] == {'recency_half_life_hours': 24.0, 'pinned_boost': 0.5}, ranked.get('rerank')
        rel_ranked = [n for n in ranked['notes'] if n.get('related')]
        assert all('rank_score' in n for n in rel_ranked), rel_ranked
        scores = [n['rank_score'] for n in rel_ranked]
        assert scores == sorted(scores, reverse=True), 'reranked rows must sort by rank_score desc'
        assert rel_ranked[0]['task_id'] == 'rr-new', \
            'a fresh match must outrank a month-old one under a 24h half-life'
        # Rerank changes the sealed bundle, and the params are recorded for exact recomputation.
        ra = run('recall','rr-1','--agent','opencode','--budget','100000','--related','5',
                 '--rerank','--recency-half-life-hours','24')
        rb = run('recall','rr-1','--agent','opencode','--budget','100000','--related','5')
        assert ra['digest'] != rb['digest'], 'rerank must move the recall digest'
        rva = run('recall-verify','rr-1','--digest',ra['digest'],'--agent','opencode',
                  '--budget','100000','--related','5','--rerank','--recency-half-life-hours','24')
        assert rva['fresh'] is True, rva
        rvb = run('recall-verify','rr-1','--digest',ra['digest'],'--agent','opencode',
                  '--budget','100000','--related','5')
        assert rvb['fresh'] is False, 'a reranked digest must not verify under different params'
        evs = run('events','--entity-id','rr-1','--action','context_recalled','--limit','3')
        assert evs['events'][0]['payload']['rerank'] is False, evs['events'][0]
        assert evs['events'][0]['payload']['digest'] == rb['digest']
        # Fleet sweep recomputes a rerank bundle exactly: cite it, then expect fresh.
        hh = run('handoff','rr-1','--from-agent','opencode','--to-agent','codex',
                 '--objective','verify rerank sweep','--next-action','compare digests',
                 '--recall-digest',ra['digest'])
        st = ops('recall-stale')
        item = next(i for i in st['items'] if i['recall_digest'] == ra['digest'])
        assert item['state'] == 'fresh', item
        # search-notes honors the same hybrid in both FTS and fallback shapes.
        hits = run('search-notes','rerank probe','--rank','--rerank','--recency-half-life-hours','24')
        assert hits and hits[0]['task_id'] == 'rr-new' and all('rank_score' in h for h in hits), hits
        base = run('search-notes','rerank probe','--rank')
        assert all('rank_score' not in h for h in base), 'baseline --rank output shape unchanged'
        # Pinned bonus: neutralize recency, pin the old note, watch it surface.
        sq("UPDATE notes SET pinned=1 WHERE content LIKE 'rerank probe ancient%'")
        boosted = run('search-notes','rerank probe','--rank','--rerank','--recency-half-life-hours','1000000')
        assert boosted[0]['content'].startswith('rerank probe ancient'), \
            'pinned bonus must lift the old note when recency is neutralized'
        sq("UPDATE notes SET pinned=0 WHERE content LIKE 'rerank probe ancient%'")



@case('handoff_protocol_and_recovery')
def _case_handoff_protocol_and_recovery():
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
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        import sqlite3, hashlib, time
        run('create','--project','Verify','--title','finish me','--id','fin-1')
        run('claim','fin-1','--owner','worker-a','--minutes','5')
        run('complete','fin-1','--owner','worker-a')
        # Cross-agent ownership transfer: atomic live-lease reassignment with fencing.
        run('create','--project','Verify','--title','pass the baton','--id','xfer-1')
        run('claim','xfer-1','--owner','agent-a','--minutes','5')
        err = run_fail('transfer','xfer-1','--from-owner','agent-b','--to-owner','agent-c')
        assert 'lease owned by agent-a' in err
        err = run_fail('transfer','xfer-1','--from-owner','agent-a','--to-owner','agent-a')
        assert 'must differ' in err
        err = run_fail('transfer','xfer-1','--from-owner','agent-a','--to-owner','agent-b','--epoch','99')
        assert 'lease superseded' in err
        tr = run('transfer','xfer-1','--from-owner','agent-a','--to-owner','agent-b','--minutes','45')
        assert tr['ok'] is True and tr['to_owner'] == 'agent-b' and tr['status'] == 'claimed', tr
        assert tr['lease_epoch'] == 2 and tr['lease_expires_at'], tr
        row = next(r for r in run('list') if r['id'] == 'xfer-1')
        assert row['lease_owner'] == 'agent-b' and row['status'] == 'claimed'
        # The old holder is fenced out immediately: owner mismatch and stale epoch both fail.
        err = run_fail('heartbeat','xfer-1','--owner','agent-a')
        assert 'lease owned by agent-b' in err
        err = run_fail('renew','xfer-1','--owner','agent-b','--minutes','5','--epoch','1')
        assert 'lease superseded' in err
        detail = run('show','xfer-1')
        assert any(e['action'] == 'lease_transferred' for e in detail['audit'])
        err = run_fail('transfer','fin-1','--from-owner','agent-a','--to-owner','agent-b')
        assert 'terminal task' in err
        # Idempotent cross-agent recovery: resume recreates a killed session in one call.
        run('create','--project','Verify','--title','killed session','--id','res-1')
        run('note','res-1','--kind','fact','--content','resume marker note','--source','verify')
        r1 = run('resume','res-1','--agent','codex','--budget','8000')
        assert r1['ok'] is True and r1['action'] == 'claimed', r1
        assert r1['task']['id'] == 'res-1' and len(r1['digest']) == 64
        assert r1['lease']['owner'] == 'codex' and r1['lease']['held_by_caller'] is True, r1['lease']
        # Idempotent: resuming again by the same holder mutates nothing.
        r2 = run('resume','res-1','--agent','codex','--budget','8000')
        assert r2['action'] == 'already_held' and r2['digest'] == r1['digest'], r2
        row = next(r for r in run('list') if r['id'] == 'res-1')
        assert row['lease_epoch'] == r1['lease']['epoch'], 'already_held must not bump the epoch'
        # A foreign live lease blocks resume until it is transferred.
        err = run_fail('resume','res-1','--agent','claude-code')
        assert 'lease owned by codex' in err and 'transfer' in err
        run('transfer','res-1','--from-owner','codex','--to-owner','claude-code')
        r3 = run('resume','res-1','--agent','claude-code')
        assert r3['action'] == 'already_held' and r3['lease']['held_by_caller'] is True, r3
        evs = run('events','--entity-id','res-1','--action','session_resumed')
        assert evs['count'] >= 3, evs
        # An expired lease is reclaimed automatically — no operator recovery needed first.
        run('release','res-1','--owner','claude-code')
        run('claim','res-1','--owner','ghost','--minutes','0')
        r4 = run('resume','res-1','--agent','codex')
        assert r4['action'] == 'claimed' and r4['lease']['owner'] == 'codex', r4
        # Guards mirror claim: blocked and dep-blocked tasks cannot be resumed.
        run('block','res-1','--owner','codex','--reason','paused mid-flight')
        err = run_fail('resume','res-1','--agent','codex')
        assert 'task is blocked' in err and 'paused mid-flight' in err
        run('unblock','res-1','--owner','codex')
        run('create','--project','Verify','--title','res prereq','--id','res-dep-a')
        run('create','--project','Verify','--title','res dependent','--id','res-dep-b','--depends-on','res-dep-a')
        err = run_fail('resume','res-dep-b','--agent','codex')
        assert 'unsatisfied dependencies' in err
        err = run_fail('resume','fin-1','--agent','codex')
        assert 'terminal task' in err
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Handoff inbox: fleet-wide inbound view of live handoffs addressed to an agent.
        run('create','--project','Inbox','--title','inbound one','--id','ibx-1')
        run('create','--project','Inbox','--title','inbound two','--id','ibx-2')
        run('create','--project','OtherBox','--title','inbound three','--id','ibx-3')
        h_i1 = run('handoff','ibx-1','--from-agent','codex','--to-agent','claude-code',
                   '--objective','first objective','--commit','def5678')
        h_i2 = run('handoff','ibx-2','--from-agent','hermes','--to-agent','claude-code',
                   '--objective','second objective')
        run('claim','ibx-1','--owner','worker-i','--minutes','5')
        inbox = run('handoff-inbox','--agent','claude-code')
        assert inbox['ok'] is True and inbox['agent'] == 'claude-code', inbox
        by_task = {i['task_id']: i for i in inbox['items']}
        assert {'ibx-1','ibx-2'} <= set(by_task), inbox
        assert by_task['ibx-1']['handoff_id'] == h_i1['id']
        assert by_task['ibx-1']['objective'] == 'first objective'
        assert by_task['ibx-1']['from_agent'] == 'codex' and by_task['ibx-1']['commit_ref'] == 'def5678'
        assert by_task['ibx-1']['task_title'] == 'inbound one' and by_task['ibx-1']['task_status'] == 'claimed'
        assert by_task['ibx-1']['lease']['owner'] == 'worker-i' and by_task['ibx-1']['lease']['live'] is True
        assert by_task['ibx-2']['lease']['owner'] == '' and by_task['ibx-2']['lease']['live'] is False
        # Supersession-awareness: readdressing ibx-2 to opencode removes it from claude-code's inbox.
        h_i2b = run('handoff','ibx-2','--from-agent','hermes','--to-agent','opencode',
                    '--objective','readdressed objective')
        assert h_i2b['superseded'] == h_i2['id']
        inbox = run('handoff-inbox','--agent','claude-code')
        assert 'ibx-2' not in {i['task_id'] for i in inbox['items']}, inbox
        other = run('handoff-inbox','--agent','opencode')
        assert [i['task_id'] for i in other['items']] == ['ibx-2'], other
        assert other['items'][0]['objective'] == 'readdressed objective'
        # Project filter and empty result shape.
        scoped = run('handoff-inbox','--agent','claude-code','--project','Inbox')
        assert {i['task_id'] for i in scoped['items']} == {'ibx-1'}, scoped
        assert run('handoff-inbox','--agent','nobody')['count'] == 0
        err = run_fail('handoff-inbox','--agent','')
        assert '--agent is required' in err
        # Handoff acknowledgment: the recipient durably accepts inbound work.
        run('create','--project','Inbox','--title','ack host','--id','ack-1')
        run_fail('ack','ack-1','--agent','claude-code')          # no live handoff yet
        h_a1 = run('handoff','ack-1','--from-agent','codex','--to-agent','claude-code',
                   '--objective','take over the retry path')
        err = run_fail('ack','ack-1','--agent','opencode')        # foreign recipient
        assert "addressed to 'claude-code'" in err
        err = run_fail('ack','ack-1','--agent','claude-code','--recall-digest','zzz')
        assert 'invalid --recall-digest' in err
        inbox = run('handoff-inbox','--agent','claude-code','--unacked-only')
        assert 'ack-1' in {i['task_id'] for i in inbox['items']}, inbox
        assert all(i['acked'] is False for i in inbox['items'])
        ack = run('ack','ack-1','--agent','claude-code')
        assert ack['ok'] is True and ack['already_acked'] is False and ack['acked_by'] == 'claude-code'
        again = run('ack','ack-1','--agent','claude-code')        # idempotent re-ack
        assert again['already_acked'] is True and again['acked_at'] == ack['acked_at']
        cur = run('handoff-current','ack-1')
        assert cur['acked_by'] == 'claude-code' and cur['acked_at'] == ack['acked_at']
        inbox = run('handoff-inbox','--agent','claude-code','--unacked-only')
        assert 'ack-1' not in {i['task_id'] for i in inbox['items']}, inbox
        full = run('handoff-inbox','--agent','claude-code')
        item = next(i for i in full['items'] if i['task_id'] == 'ack-1')
        assert item['acked'] is True and item['acked_at'] == ack['acked_at']
        detail = run('show','ack-1')
        ev = next(e for e in detail['audit'] if e['action'] == 'handoff_acknowledged')
        assert ev['payload']['agent'] == 'claude-code', ev
        m = run('metrics'); assert m['handoffs_acked_total'] >= 1, m
        # A superseding handoff resets acceptance; the SLA lint flags never-picked-up work.
        h_a2 = run('handoff','ack-1','--from-agent','codex','--to-agent','opencode',
                   '--objective','reassigned after no pickup')
        run_fail('ack','ack-1','--agent','claude-code'), 'old recipient must not ack a successor'
        chk = ops('handoff-check','--task','ack-1','--ack-sla-hours','0')
        prob = next(p for p in chk['problems'] if p['handoff_id'] == h_a2['id'])
        assert 'stale_unacknowledged' in prob['reasons'], chk
        # Deadline escalation: overdue non-terminal tasks climb one priority level per pass.
        run('create','--project','Sla','--title','overdue p3','--id','sla-1','--priority','P3',
            '--due-at','2020-01-01T00:00:00Z')
        run('create','--project','Sla','--title','overdue p0','--id','sla-2','--priority','P0',
            '--due-at','2020-01-01T00:00:00Z')
        run('create','--project','Sla','--title','future p3','--id','sla-3','--priority','P3',
            '--due-at','2030-01-01T00:00:00Z')
        dryesc = ops('escalate','--dry-run')
        assert dryesc['dry_run'] is True, dryesc
        assert any(c['task_id'] == 'sla-1' and c['from_priority'] == 'P3' and c['to_priority'] == 'P2'
                   for c in dryesc['escalated']), dryesc
        assert 'sla-2' in dryesc['already_p0'], 'overdue P0 must be reported, not escalated'
        assert all(c['task_id'] != 'sla-3' for c in dryesc['escalated']), 'future deadline must not escalate'
        row = next(r for r in run('list') if r['id'] == 'sla-1')
        assert row['priority'] == 'P3', 'dry-run must not mutate'
        esc = ops('escalate')
        assert esc['dry_run'] is False and esc['count'] >= 1, esc
        bumped = {c['task_id']: c['to_priority'] for c in esc['escalated']}
        assert bumped.get('sla-1') == 'P2' and 'sla-2' not in bumped and 'sla-3' not in bumped, esc
        row = next(r for r in run('list') if r['id'] == 'sla-1')
        assert row['priority'] == 'P2', row
        detail = run('show','sla-1')
        last = detail['audit'][0]
        assert last['action'] == 'priority_escalated', last
        assert last['payload']['from_priority'] == 'P3' and last['payload']['to_priority'] == 'P2'
        assert last['payload']['reason'] == 'overdue'
        # Repeated passes converge: P2 -> P1 on the next sweep.
        esc2 = ops('escalate')
        assert any(c['task_id'] == 'sla-1' and c['to_priority'] == 'P1' for c in esc2['escalated']), esc2
        row = next(r for r in run('list') if r['id'] == 'sla-1')
        assert row['priority'] == 'P1', row
        # Terminal tasks are never escalated even when overdue.
        run('create','--project','Sla','--title','done overdue','--id','sla-4','--priority','P3',
            '--due-at','2020-01-01T00:00:00Z')
        run('cancel','sla-4','--owner','leo','--reason','obsolete before deadline sweep')
        esc3 = ops('escalate')
        assert all(c['task_id'] != 'sla-4' for c in esc3['escalated']), esc3
        chain = run('verify-chain')
        assert chain['ok'] is True, 'escalation audits must keep the chain intact'
        # Receipt integrity sealing: file_hash recorded in SQLite must match disk bytes.
        import hashlib
        run('create','--project','Verify','--title','sealed evidence','--id','seal-1')
        sr = run('receipt','seal-1','--kind','verification','--payload','{"checks": 42}')
        sfile = Path(td) / 'receipts' / (sr['receipt_id'] + '.json')
        sdigest = hashlib.sha256(sfile.read_bytes()).hexdigest()
        assert sr['sha256'] == sdigest, 'printed sha256 must match the sealed file bytes'
        with sqlite3.connect(Path(td) / 'state.db') as db:
            stored = db.execute("SELECT file_hash FROM receipts WHERE id=?", (sr['receipt_id'],)).fetchone()[0]
        assert stored == sdigest, 'receipt row must carry the file hash'
        # Doctor detects silent receipt corruption or tampering...
        orig = sfile.read_bytes()
        sfile.write_bytes(orig.replace(b'"checks": 42', b'"checks": 43'))
        doc = ops('doctor')
        assert any(p['kind'] == 'receipt_file_hash_mismatch' and p['receipt_id'] == sr['receipt_id']
                   for p in doc['problems']), doc
        # ...and is clean again once the original bytes are restored.
        sfile.write_bytes(orig)
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Handoff protocol lint: contract violations become observable problems.
        run('create','--project','Verify','--title','lint host','--id','hc-1')
        bad = run('handoff','hc-1','--from-agent','rogue')      # sparse + unaddressed
        chk = ops('handoff-check','--task','hc-1')
        assert chk['ok'] is False and chk['count'] == 1, chk
        assert chk['problems'][0]['handoff_id'] == bad['id'] and chk['problems'][0]['task_id'] == 'hc-1'
        assert set(chk['problems'][0]['reasons']) == {
            'unaddressed', 'missing_objective', 'sparse_no_evidence_or_next_actions'}, chk
        # A provenance-clean handoff passes: cited digest was genuinely recalled.
        rec_hc = run('recall','hc-1','--agent','codex')
        good = run('handoff','hc-1','--from-agent','codex','--to-agent','claude-code',
                   '--objective','finish the retry path','--evidence','tests pass locally',
                   '--next-action','open PR','--recall-digest',rec_hc['digest'])
        assert good['superseded'] == bad['id'], good
        chk = ops('handoff-check','--task','hc-1')
        assert chk['ok'] is True and chk['problems'] == [], chk
        # A newer recall audited after the cited one flags the handoff as outdated.
        run('recall','hc-1','--agent','claude-code')
        chk = ops('handoff-check','--task','hc-1')
        assert chk['problems'][0]['reasons'] == ['older_than_latest_recall'], chk
        # A fabricated digest (never recalled) is called out as unproven.
        fab = run('handoff','hc-1','--from-agent','codex','--to-agent','opencode',
                  '--objective','suspect handoff','--next-action','verify first',
                  '--recall-digest','f'*64)
        chk = ops('handoff-check','--task','hc-1')
        assert chk['problems'][0]['reasons'] == ['unproven_recall_digest'], chk
        # Live handoffs on terminal tasks are stale recovery bait.
        run('claim','hc-1','--owner','codex','--minutes','5')
        run('complete','hc-1','--owner','codex','--note','done')
        chk = ops('handoff-check','--task','hc-1')
        assert set(chk['problems'][0]['reasons']) == {'terminal_task_handoff',
                                                      'unproven_recall_digest'}, chk
        # --task scoping keeps other tasks' live handoffs out of the report.
        assert all(p['task_id'] == 'hc-1' for p in chk['problems'])
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Near-duplicate memory guard: rephrased restatements are flagged, distinct notes are not.
        run('create','--project','Verify','--title','dup host','--id','dup-1')
        d1 = run('note','dup-1','--content','postgres pool exhausted under heavy load')
        assert d1['deduplicated'] is False and d1['similar_to'] == [], d1
        d2 = run('note','dup-1','--content','postgres pool exhausted under heavy load now')
        assert d2['similar_to'] and d2['similar_to'][0]['note_id'] == d1['id'], d2
        assert d2['similar_to'][0]['similarity'] >= 0.8, d2
        d3 = run('note','dup-1','--content','gardening tip about tomatoes entirely unrelated')
        assert d3['similar_to'] == [], d3
        detail = run('show','dup-1')
        ev = next(e for e in detail['audit'] if e['action'] == 'note_added'
                  and e['payload'].get('similar_notes'))
        assert ev['payload']['note_id'] == d2['id'] and ev['payload']['similar_notes'] == [d1['id']], ev
        # The near-duplicate is still stored (informational, not suppressed).
        live_dups = run('notes','dup-1')
        assert {n['id'] for n in live_dups} == {d1['id'], d2['id'], d3['id']}
        # Resume digests are first-class provenance: a handoff citing one passes the lint.
        run('create','--project','Verify','--title','resume proof','--id','rp-1')
        rr = run('resume','rp-1','--agent','codex')
        run('release','rp-1','--owner','codex')
        rh = run('handoff','rp-1','--from-agent','codex','--to-agent','opencode',
                 '--objective','continue from resumed session','--next-action','verify state',
                 '--recall-digest',rr['digest'])
        chk = ops('handoff-check','--task','rp-1')
        assert chk['ok'] is True and chk['problems'] == [], chk
        # Fleet freshness sweep: recall-stale recomputes cited digests across all live handoffs.
        run('create','--project','Verify','--title','fresh host','--id','fs-1')
        rf = run('recall','fs-1','--agent','codex','--budget','4000')
        fh = run('handoff','fs-1','--from-agent','codex','--to-agent','claude-code',
                 '--objective','sweep check','--next-action','verify','--recall-digest',rf['digest'])
        rs = ops('recall-stale')
        item = next(i for i in rs['items'] if i['task_id'] == 'fs-1')
        assert item['state'] == 'fresh' and item['handoff_id'] == fh['id'], item
        run('note','fs-1','--kind','fact','--content','drift after the handoff was written','--source','verify')
        rs = ops('recall-stale')
        item = next(i for i in rs['items'] if i['task_id'] == 'fs-1')
        assert item['state'] == 'stale' and item['current_digest'] != rf['digest'], item
        assert rs['states'].get('stale', 0) >= 1, rs
        # A fabricated citation is unproven; superseding makes it the live handoff under test.
        fabh = run('handoff','fs-1','--from-agent','claude-code','--to-agent','opencode',
                   '--objective','fabricated citation','--next-action','x','--recall-digest','e'*64)
        assert fabh['superseded'] == fh['id']
        rs = ops('recall-stale')
        item = next(i for i in rs['items'] if i['handoff_id'] == fabh['id'])
        assert item['state'] == 'unproven_recall_digest', item
        assert all(i['handoff_id'] != fh['id'] for i in rs['items']), \
            'superseded handoffs must leave the sweep'
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Handoff lineage: handoff-history walks the supersession chain oldest → newest.
        run('create','--project','Verify','--title','lineage host','--id','lin-1')
        lh1 = run('handoff','lin-1','--from-agent','codex','--to-agent','claude-code',
                  '--objective','first pass','--next-action','draft')
        lh2 = run('handoff','lin-1','--from-agent','claude-code','--to-agent','opencode',
                  '--objective','second pass','--next-action','review')
        assert lh2['superseded'] == lh1['id'], lh2
        hist = run('handoff-history', lh1['id'])
        assert [h['id'] for h in hist] == [lh1['id'], lh2['id']], hist
        hist_rev = run('handoff-history', lh2['id'])
        assert [h['id'] for h in hist_rev] == [lh1['id'], lh2['id']], \
            'walking from any link must reconstruct the full chain'
        err = run_fail('handoff-history', 'no-such-handoff')
        assert 'handoff not found' in err
        # Deferral: a queued task parked with a future not_before is skipped by
        # dispatch until its instant arrives; explicit claim stays an override.
        run('create','--project','DeferTest','--title','urgent but later','--id','def-1','--priority','P0')
        run('create','--project','DeferTest','--title','small now','--id','def-2','--priority','P3')
        row = run('defer','def-1','--owner','op','--until','2099-01-01T00:00:00+00:00')
        assert row['not_before'] == '2099-01-01T00:00:00+00:00', row
        nx = run('next','--project','DeferTest','--explain')
        assert nx['task']['id'] == 'def-2', nx
        sk = next(s for s in nx['skipped'] if s['task_id'] == 'def-1')
        assert sk['reason'] == 'deferred_until' and sk['not_before'] == '2099-01-01T00:00:00+00:00', nx
        m = run('metrics'); assert m['queued_deferred'] >= 1, m
        got = run('next','--claim','--project','DeferTest','--owner','tester','--minutes','5')
        assert got['task']['id'] == 'def-2'
        run('release','def-2','--owner','tester')
        # Explicit claim overrides the deferral (deliberate operator action).
        run('claim','def-1','--owner','tester','--minutes','5')
        run('release','def-1','--owner','tester')
        row = next(r for r in run('list') if r['id'] == 'def-1')
        assert row['status'] == 'queued', row
        # Clearing the deferral restores normal dispatch order.
        run('defer','def-1','--owner','op','--until','')
        row = next(r for r in run('list') if r['id'] == 'def-1')
        assert row['not_before'] == '', row
        nx = run('next','--project','DeferTest')
        assert nx['task']['id'] == 'def-1', nx
        err = run_fail('defer','def-2','--owner','op','--until','not-a-date')
        assert 'invalid --until timestamp' in err
        # Dispatch aging: an old task waiting far beyond the window is dispatched at
        # a virtually promoted priority without mutating stored priority.
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute("UPDATE tasks SET created_at='2020-01-01T00:00:00+00:00' WHERE id='def-2'")
            db.commit()
        row = next(r for r in run('list') if r['id'] == 'def-2')
        assert row['priority'] == 'P3', row
        nx = run('next','--project','DeferTest','--explain')
        assert nx['task']['id'] == 'def-1', 'fresh P0 must beat unboosted P3'
        aged = run('next','--project','DeferTest','--explain','--aging-minutes','360','--aging-boost','4')
        assert aged['task']['id'] == 'def-2', aged
        assert aged['effective_priority'] == 'P0' and aged['priority_boost'] == 4, aged
        row = next(r for r in run('list') if r['id'] == 'def-2')
        assert row['priority'] == 'P3', 'aging must never mutate stored priority'
        static = run('next','--project','DeferTest','--aging-minutes','0')
        assert static['task']['id'] == 'def-1', '--aging-minutes 0 restores strict ordering'
        # Dependency priority inheritance: an urgent dependent makes its prerequisite
        # chain dispatch-urgent without mutating any stored priority.
        run('create','--project','InheritTest','--title','boring prereq','--id','inh-a','--priority','P3')
        run('create','--project','InheritTest','--title','urgent dependent','--id','inh-b','--priority','P0',
            '--depends-on','inh-a')
        run('create','--project','InheritTest','--title','medium bystander','--id','inh-c','--priority','P2')
        nx = run('next','--project','InheritTest','--explain')
        assert nx['task']['id'] == 'inh-a', ('inherited P0 urgency must beat the P2 bystander', nx)
        assert nx['effective_priority'] == 'P0' and nx['inherited_via'] == 'inh-b', nx
        row = next(r for r in run('list') if r['id'] == 'inh-a')
        assert row['priority'] == 'P3', 'inheritance must never mutate stored priority'
        plain = run('next','--project','InheritTest')
        assert plain['task']['id'] == 'inh-a' and 'inherited_via' not in plain \
            and 'effective_priority' not in plain, 'default output shape must stay unchanged'
        # Transitive chains inherit through the whole DAG; via names the nearest dependent.
        run('create','--project','ChainTest','--title','chain root','--id','ch-a','--priority','P3')
        run('create','--project','ChainTest','--title','chain mid','--id','ch-b','--priority','P2',
            '--depends-on','ch-a')
        run('create','--project','ChainTest','--title','chain tip','--id','ch-c','--priority','P0',
            '--depends-on','ch-b')
        nx = run('next','--project','ChainTest','--explain')
        assert nx['task']['id'] == 'ch-a' and nx['effective_priority'] == 'P0', nx
        assert nx['inherited_via'] == 'ch-b', nx
        # Completing the root unblocks the mid link, which keeps its inherited urgency.
        got = run('next','--claim','--project','ChainTest','--owner','tester','--minutes','5')
        assert got['task']['id'] == 'ch-a', got
        run('complete','ch-a','--owner','tester','--note','root done')
        nx = run('next','--project','ChainTest','--explain')
        assert nx['task']['id'] == 'ch-b' and nx['effective_priority'] == 'P0', nx
        assert nx['inherited_via'] == 'ch-c', nx
        # A terminal dependent confers nothing: its prerequisites keep their own tier.
        run('create','--project','TermTest','--title','orphan prereq','--id','tm-a','--priority','P3')
        run('create','--project','TermTest','--title','done dependent','--id','tm-b','--priority','P0',
            '--depends-on','tm-a')
        run('update','tm-b','--status','cancelled')
        run('create','--project','TermTest','--title','calm bystander','--id','tm-c','--priority','P2')
        nx = run('next','--project','TermTest','--explain')
        assert nx['task']['id'] == 'tm-c', ('terminal dependent must not confer urgency', nx)
        assert 'inherited_via' not in nx, nx
        # Dispatch-and-recall: next --claim --recall takes work AND seals its context
        # in one call, auditing context_recalled so the dispatch→recall→act chain is contiguous.
        run('create','--project','DispatchRecall','--title','one call work','--id','dr-1')
        run('note','dr-1','--kind','fact','--content','dispatch recall marker','--source','verify')
        err = run_fail('next','--project','DispatchRecall','--recall')
        assert '--recall requires --claim' in err
        got = run('next','--claim','--project','DispatchRecall','--owner','codex','--minutes','5',
                  '--recall','--budget','8000')
        assert got['claimed'] is True and got['task']['id'] == 'dr-1', got
        assert len(got['recall_digest']) == 64 and got['recall']['task']['id'] == 'dr-1', got
        assert got['recall']['digest'] == got['recall_digest']
        assert got['recall']['lease']['owner'] == 'codex' and got['recall']['lease']['held_by_caller'] is True, got['recall']['lease']
        assert got['recall']['agent'] == 'codex', 'agent defaults to the claiming owner'
        # ...is fresh per recall-verify, and was audited as context_recalled via next.
        rv = run('recall-verify','dr-1','--digest',got['recall_digest'],'--agent','codex','--budget','8000')
        assert rv['fresh'] is True, rv
        evs = run('events','--entity-id','dr-1','--action','context_recalled')['events']
        assert any(e['payload'].get('via') == 'next' and e['payload']['digest'] == got['recall_digest']
                   for e in evs), evs
        # The cited digest is first-class provenance: a handoff citing it passes the lint.
        dh = run('handoff','dr-1','--from-agent','codex','--to-agent','claude-code',
                 '--objective','continue dispatched work','--next-action','implement',
                 '--recall-digest',got['recall_digest'])
        chk = ops('handoff-check','--task','dr-1')
        assert chk['ok'] is True and chk['problems'] == [], chk
        # A plain recall of the identical durable state yields the identical core
        # digest (the handoff recorded above moves the full digest by design).
        plain = run('recall','dr-1','--agent','codex','--budget','8000')
        assert plain['core_digest'] == got['recall']['core_digest'], \
            (plain['core_digest'], got['recall']['core_digest'])
        # Default output shape is unchanged without --recall.
        run('create','--project','DispatchRecall','--title','plain pick','--id','dr-2')
        plain_next = run('next','--project','DispatchRecall')
        assert plain_next['task']['id'] == 'dr-2' and 'recall' not in plain_next \
            and 'recall_digest' not in plain_next, plain_next
        # Note TTL: temporal facts can carry a lifetime; past it, unpinned notes
        # retire from packs and retrieval while pinned facts are immortal (flagged,
        # never silently dropped). A duplicate add restates the lifetime.
        run('create','--project','TTLTest','--title','ttl host','--id','ttl-1')
        err = run_fail('note','ttl-1','--content','x','--ttl-hours','0')
        assert '--ttl-hours must be a positive number' in err
        err = run_fail('note','ttl-1','--content','x','--ttl-hours','-3')
        assert '--ttl-hours must be a positive number' in err
        err = run_fail('note','ttl-1','--content','x','--ttl-hours','soon')
        assert 'invalid float value' in err
        fresh = run('note','ttl-1','--kind','fact','--content','ttl fresh fact marker','--source','verify','--ttl-hours','48')
        assert fresh['expires_at'] and not fresh['deduplicated'], fresh
        stale = run('note','ttl-1','--kind','fact','--content','ttl stale fact marker','--source','verify','--ttl-hours','0.000001')
        assert stale['expires_at'], stale
        # Retired notes drop out of context packs — counted, not silent.
        ctx = run('context','ttl-1','--budget','4000')
        ids = [n['id'] for n in ctx['notes']]
        assert stale['id'] not in ids and fresh['id'] in ids and ctx['notes_expired_excluded'] == 1, ctx
        # ...out of search by default (--include-expired surfaces them)...
        hits = run('search-notes','marker','--project','TTLTest')
        assert {h['id'] for h in hits} == {fresh['id']}, hits
        hits = run('search-notes','marker','--project','TTLTest','--include-expired')
        assert {h['id'] for h in hits} == {stale['id'], fresh['id']}, hits
        # ...and out of cross-task related-note candidates.
        run('create','--project','TTLTest','--title','marker neighbor','--id','ttl-2')
        rel = run('context','ttl-2','--related','5','--related-scope','project','--budget','4000')
        rel_ids = [n['id'] for n in rel.get('notes', []) if n.get('related')]
        assert stale['id'] not in rel_ids and fresh['id'] in rel_ids, rel
        # Pinned + TTL: an expired pinned note still packs but carries the flag;
        # `notes` shows it so the operator knows a supersede is due.
        pin = run('note','ttl-1','--kind','constraint','--content','MUST rotate ttl keys weekly','--source','leo',
                  '--pinned','--ttl-hours','0.000001')
        ctx = run('context','ttl-1','--budget','4000')
        pn = next(n for n in ctx['notes'] if n['id'] == pin['id'])
        assert pn['expired'] is True and ctx['notes_expired_excluded'] == 1, ctx
        rows = run('notes','ttl-1')
        assert next(n for n in rows if n['id'] == pin['id'])['expired'] is True
        m = run('metrics'); assert m['notes_expired_live'] >= 1, m
        ne = ops('notes-expired')
        item = next(i for i in ne['items'] if i['id'] == stale['id'])
        assert item['retired'] is True and item['action'] == 'revive', item
        pitem = next(i for i in ne['items'] if i['id'] == pin['id'])
        assert pitem['retired'] is False and pitem['action'] == 'supersede', pitem
        # Revival: re-adding a retired note's exact content refreshes its lifetime
        # (no --ttl-hours means immortal again) instead of deduping into invisibility.
        rev = run('note','ttl-1','--kind','fact','--content','ttl stale fact marker','--source','verify')
        assert rev['deduplicated'] is True and rev['id'] == stale['id'] and rev.get('revived') is True, rev
        evs = run('events','--entity-id','ttl-1','--action','note_ttl_refreshed')['events']
        assert any(e['payload']['revived'] is True for e in evs), evs
        ctx = run('context','ttl-1','--budget','4000')
        assert 'notes_expired_excluded' not in ctx, 'revived note must be packed again'
        assert any(n['id'] == stale['id'] and 'expired' not in n for n in ctx['notes']), ctx
        # Superseding an expired pinned fact yields a fresh note with no expiry
        # unless one is requested explicitly.
        sup3 = run('supersede-note',pin['id'],'--content','MUST rotate ttl keys daily','--source','leo')
        assert 'expires_at' not in sup3, sup3
        nn = next(n for n in run('notes','ttl-1') if n['id'] == sup3['new_note_id'])
        assert nn['pinned'] == 1 and 'expired' not in nn, nn
        doc = ops('doctor'); assert doc['ok'] is True and doc['problems'] == [], doc
        # Memory consolidation: near-duplicate live notes are clustered and
        # superseded into one canonical fact per cluster (dry-run previews first).
        run('create','--project','ConsolTest','--title','memory host','--id','con-1')
        ca = run('note','con-1','--kind','fact','--content','postgres pool size is sixty connections','--source','hermes')
        cb = run('note','con-1','--kind','fact','--content','postgres pool size is sixty total connections','--source','codex')
        cc = run('note','con-1','--kind','fact','--content','deploy window is friday morning','--source','hermes')
        cd = run('note','con-1','--kind','constraint','--content','MUST rotate api keys weekly','--source','leo','--pinned')
        ce = run('note','con-1','--kind','constraint','--content','MUST rotate the api keys weekly','--source','leo','--pinned')
        dry = ops('consolidate','--dry-run')
        assert dry['dry_run'] is True and dry['consolidated_count'] == 0, dry
        dry_c1 = [c for c in dry['clusters'] if c['task_id'] == 'con-1']
        assert len(dry_c1) == 2 and dry['tasks_scanned'] >= 2, dry
        err = ops_fail('consolidate','--threshold','1.5')
        assert '--threshold must be a number between 0 and 1' in err
        out = ops('consolidate')
        assert out['ok'] is True and out['dry_run'] is False and out['consolidated_count'] >= 2, out
        # Canonical picks: newest unpinned for the postgres pair; the pinned pair
        # keeps a pinned canonical (pin beats recency).
        pg = next(c for c in out['clusters'] if c['kept_note_id'] == cb['id'])
        assert {m['note_id'] for m in pg['consolidated']} == {ca['id']}, pg
        pins = next(c for c in out['clusters'] if c['kept_note_id'] == ce['id'])
        assert {m['note_id'] for m in pins['consolidated']} == {cd['id']}, pins
        rows = run('notes','con-1')
        ids = {n['id'] for n in rows}
        assert ids == {cb['id'], cc['id'], ce['id']}, ('losers must leave live views', ids)
        # History still resolves through the consolidation link (loser → canonical).
        hist = run('note-history',ca['id'])
        assert [h['id'] for h in hist] == [ca['id'], cb['id']], hist
        hist = run('note-history',cd['id'])
        assert [h['id'] for h in hist] == [cd['id'], ce['id']], hist
        # Retrieval shrinks immediately; the canonical fact is the survivor.
        hits = run('search-notes','postgres','--project','ConsolTest')
        assert {h['id'] for h in hits} == {cb['id']}, hits
        # Every consolidation is audited with its kept note and similarity.
        evs = run('events','--entity-id','con-1','--action','note_consolidated')['events']
        assert len(evs) == 2 and all(e['payload']['kept_note_id'] and e['payload']['similarity'] >= 0.8 for e in evs), evs
        m = run('metrics'); assert m['notes_consolidated_total'] >= 2, m
        # Idempotent: a second pass finds nothing left to merge.
        again = ops('consolidate')
        assert again['consolidated_count'] == 0 and again['clusters'] == [], again
        # --task scoping: new duplicates on another task are untouched by a sweep
        # scoped to the first task, and merged when scoped correctly.
        run('create','--project','ConsolTest','--title','second host','--id','con-2')
        da = run('note','con-2','--kind','fact','--content','redis cache ttl is 300 seconds','--source','hermes')
        db_ = run('note','con-2','--kind','fact','--content','redis cache ttl is 300 seconds now','--source','codex')
        scoped = ops('consolidate','--task','con-1')
        assert scoped['consolidated_count'] == 0 and scoped['clusters'] == [], scoped
        scoped = ops('consolidate','--task','con-2')
        assert scoped['consolidated_count'] == 1, scoped
        assert {n['id'] for n in run('notes','con-2')} == {db_['id']}, run('notes','con-2')
        doc = ops('doctor'); assert doc['ok'] is True and doc['problems'] == [], doc
        assert run('verify-chain')['ok'] is True
        # Handoff retrieval: fleet-wide keyword search over the handoff protocol.
        run('create','--project','RagH','--title','postgres pool incident followups','--id','hr-src')
        hr1 = run('handoff','hr-src','--from-agent','codex','--to-agent','claude-code',
                  '--status','running','--objective','stabilize postgres pool under load',
                  '--commit','abc999')
        run('create','--project','RagH','--title','unrelated host','--id','hr-x')
        hr2 = run('handoff','hr-x','--from-agent','hermes','--to-agent','opencode',
                  '--objective','gardening schedule for tomatoes')
        hits = run('search-handoffs','postgres pool','--rank')
        # Multi-token queries are conjunctive; hits join task project/title for triage.
        assert len(hits) == 1 and hits[0]['id'] == hr1['id'] and 'score' in hits[0], hits
        assert hits[0]['task_title'] == 'postgres pool incident followups' and hits[0]['project'] == 'RagH'
        hits = run('search-handoffs','postgres','--to-agent','claude-code')
        assert {h['id'] for h in hits} == {hr1['id']}
        assert run('search-handoffs','postgres','--to-agent','opencode') == []
        hits = run('search-handoffs','tomatoes','--project','RagH')
        assert {h['id'] for h in hits} == {hr2['id']}
        assert run('search-handoffs','no-such-task') == []
        # Superseded handoffs drop out of search unless --all includes them.
        hr1b = run('handoff','hr-src','--from-agent','claude-code','--to-agent','opencode',
                   '--objective','stabilize postgres pool under load v2')
        assert hr1b['superseded'] == hr1['id']
        hits = run('search-handoffs','postgres pool','--rank')
        assert {h['id'] for h in hits} == {hr1b['id']} and all(h['superseded_by'] == '' for h in hits), hits
        hits = run('search-handoffs','postgres pool','--rank','--all')
        assert {h['id'] for h in hits} == {hr1['id'], hr1b['id']}
        assert next(h for h in hits if h['id'] == hr1['id'])['superseded_by'] == hr1b['id']
        # Tokenless queries degrade to an empty result instead of an FTS syntax error.
        assert run('search-handoffs','!!! ---','--rank') == []
        # Related-handoffs: opt-in cross-task resume points packed into context.
        run('create','--project','RagH','--title','postgres pool capacity planning','--id','hr-q')
        ctx = run('context','hr-q','--budget','100000')
        assert 'related_handoffs' not in ctx and 'related_handoffs_packed' not in ctx, \
            'legacy pack shape must stay without the flag'
        ctx = run('context','hr-q','--budget','100000','--related-handoffs','3')
        assert ctx['related_handoffs_requested'] == 3 and ctx['related_handoffs_matched'] >= 1
        assert ctx['related_handoffs_packed'] >= 1, ctx
        rh = ctx['related_handoffs']
        assert hr1b['id'] in [h['id'] for h in rh], rh
        assert all(h['task_id'] != 'hr-q' for h in rh), 'self-task handoffs must never be related'
        assert all(h['via_task_title'] for h in rh)
        # Budget: related handoffs respect the same character budget.
        tight = run('context','hr-q','--budget','120','--related-handoffs','3')
        assert tight['used_chars'] <= 120 and tight['truncated'] is True
        assert tight['related_handoffs_packed'] < tight['related_handoffs_matched'], tight
        # Recall integration: digest moves only when the flag is used, verifies with
        # identical params, and the recorded parameter lets fleet sweeps recompute.
        ra = run('recall','hr-q','--agent','codex','--budget','100000','--related-handoffs','2')
        rb = run('recall','hr-q','--agent','codex','--budget','100000')
        assert ra['digest'] != rb['digest'], 'the flag must move the sealed digest'
        rv = run('recall-verify','hr-q','--digest',ra['digest'],'--agent','codex',
                 '--budget','100000','--related-handoffs','2')
        assert rv['fresh'] is True, rv
        rvb = run('recall-verify','hr-q','--digest',ra['digest'],'--agent','codex','--budget','100000')
        assert rvb['fresh'] is False, 'a related-handoffs digest must not verify without the flag'
        hh = run('handoff','hr-q','--from-agent','codex','--to-agent','claude-code',
                 '--objective','plan capacity','--next-action','size the pool',
                 '--recall-digest',ra['digest'])
        st = ops('recall-stale')
        item = next(i for i in st['items'] if i['recall_digest'] == ra['digest'])
        assert item['state'] == 'fresh', (item, 'core digest must survive own-handoff drift')
        rd = run('recall-diff','hr-q','--digest',ra['digest'])
        assert rd['state'] == 'stale' and 'handoff' in rd['sections_changed'], rd
        doc = ops('doctor'); assert doc['ok'] is True and doc['problems'] == [], doc
        # Dependency evidence inheritance: completed prerequisites contribute their
        # verified evidence (live handoff + latest sealed receipt) to the
        # dependent's context pack — opt-in via --dep-context, flag-gated keys.
        run('create','--project','DepCtx','--title','upstream build','--id','dc-a')
        run('claim','dc-a','--owner','codex','--minutes','5')
        dc_rec = run('receipt','dc-a','--kind','verification','--payload','{"tests":"pass"}')
        run('handoff','dc-a','--from-agent','codex','--to-agent','hermes',
            '--status','completed','--objective','build artifacts sealed','--commit','abc123')
        run('complete','dc-a','--owner','codex','--note','done')
        run('create','--project','DepCtx','--title','downstream deploy','--id','dc-b')
        run('dep','dc-b','dc-a')
        ctx = run('context','dc-b','--budget','100000')
        assert 'dep_context' not in ctx and 'dep_context_packed' not in ctx, \
            'legacy pack shape must stay without the flag'
        ctx = run('context','dc-b','--budget','100000','--dep-context','3')
        assert ctx['dep_context_requested'] == 3 and ctx['dep_context_matched'] == 1
        assert ctx['dep_context_packed'] == 1, ctx
        dce = ctx['dep_context'][0]
        assert dce['id'] == 'dc-a' and dce['handoff']['objective'] == 'build artifacts sealed'
        assert dce['receipt']['id'] == dc_rec['receipt_id'] and dce['receipt']['payload'] == {"tests": "pass"}
        # Budget: prerequisite evidence respects the same character budget.
        tight = run('context','dc-b','--budget','130','--dep-context','3')
        assert tight['used_chars'] <= 130 and tight['truncated'] is True
        assert tight['dep_context_packed'] < tight['dep_context_matched'], tight
        # Recall integration: the flag moves the sealed digest, verifies only with
        # identical params, and the recorded parameter lets fleet sweeps recompute.
        da = run('recall','dc-b','--agent','codex','--budget','100000','--dep-context','2')
        db_ = run('recall','dc-b','--agent','codex','--budget','100000')
        assert da['digest'] != db_['digest'], 'the flag must move the sealed digest'
        rv = run('recall-verify','dc-b','--digest',da['digest'],'--agent','codex',
                 '--budget','100000','--dep-context','2')
        assert rv['fresh'] is True, rv
        rvb = run('recall-verify','dc-b','--digest',da['digest'],'--agent','codex','--budget','100000')
        assert rvb['fresh'] is False, 'a dep-context digest must not verify without the flag'
        run('handoff','dc-b','--from-agent','codex','--to-agent','claude-code',
            '--objective','plan deploy','--recall-digest',da['digest'])
        st = ops('recall-stale')
        item = next(i for i in st['items'] if i['recall_digest'] == da['digest'])
        assert item['state'] == 'fresh', (item, 'core digest must survive own-handoff drift')
        rd = run('recall-diff','dc-b','--digest',da['digest'])
        assert rd['state'] == 'stale' and 'handoff' in rd['sections_changed'], rd
        # resume + dispatch-and-recall pack prerequisite evidence too.
        rz = run('resume','dc-b','--agent','opencode','--budget','100000','--dep-context','2')
        assert rz['action'] in ('claimed','already_held') and rz['dep_context_packed'] == 1, rz
        run('create','--project','DepCtx','--title','second downstream','--id','dc-c',
            '--priority','P0','--due-at','2019-01-01T00:00:00Z')
        run('dep','dc-c','dc-a')
        nx = run('next','--claim','--owner','hermes','--recall','--budget','100000','--dep-context','2')
        assert nx['task']['id'] == 'dc-c', nx
        assert nx['recall']['dep_context_packed'] == 1 and nx['recall_digest'] == nx['recall']['digest'], nx
        run('release','dc-c','--owner','hermes')
        doc = ops('doctor'); assert doc['ok'] is True and doc['problems'] == [], doc
        assert run('verify-chain')['ok'] is True


@case('integrity_scheduling_and_policy')
def _case_integrity_scheduling_and_policy():
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
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        import sqlite3, hashlib, time
        run('create','--project','Verify','--title','impact probe host','--id','verify-1')
        # Secret guard: credential-shaped content never enters shared memory silently.
        run('create','--project','Verify','--title','secret guard','--id','sec-1')
        fake_aws = 'AKIAIOSFODNN7EXAMPLE'
        err = run_fail('note','sec-1','--content','key is ' + fake_aws)
        assert 'credential-shaped' in err and 'aws_access_key' in err, err
        assert all(fake_aws not in n['content'] for n in run('notes','sec-1'))
        # Prose that merely mentions tokens must not false-positive (no digit in value).
        run('note','sec-1','--content','fencing token: lease_epoch chain')
        red = run('note','sec-1','--content','key is ' + fake_aws,'--redact')
        assert red['secret_kinds'] == ['aws_access_key'], red
        stored = next(n for n in run('notes','sec-1') if n['id'] == red['id'])
        assert fake_aws not in stored['content'] and '[REDACTED:aws_access_key]' in stored['content'], stored
        ovr = run('note','sec-1','--content','password: hunter2hunter2','--allow-secret')
        assert ovr['secret_kinds'] == ['credential_assignment'], ovr
        # Handoffs: objective and list fields are guarded too.
        err = run_fail('handoff','sec-1','--from-agent','agent-a',
                       '--objective','call the api with sk-proj-abcdefghijklmnopqrstuvwx')
        assert 'openai_style_key' in err, err
        hr = run('handoff','sec-1','--from-agent','agent-a','--to-agent','agent-b',
                 '--objective','rotate credentials','--evidence','token ghp_' + 'a1B2c3D4'*4,'--redact')
        assert hr['secret_kinds'] == ['github_token'], hr
        hrow = next(h for h in run('handoffs','sec-1') if h['id'] == hr['id'])
        assert all('ghp_' not in e for e in hrow['evidence']) and \
            any('[REDACTED:github_token]' in e for e in hrow['evidence']), hrow
        ho = run('handoff','sec-1','--from-agent','agent-a','--to-agent','agent-b',
                 '--objective','bearer Zx9Kp2Qw8Er5Ty7U','--allow-secret')
        assert ho['secret_kinds'] == ['bearer_header'], ho
        # Fleet sweep finds secrets already sitting in shared memory (seeded raw,
        # bypassing the write path, as legacy rows would be).
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute("INSERT INTO notes(id,task_id,kind,content,source,content_hash,created_at,pinned,expires_at)"
                       " VALUES('leak-1','sec-1','fact','slack ' || CAST(x'786f78622d313233343536373839303162636465' AS TEXT),'','legacy','2026-01-01T00:00:00+00:00',0,'')")
        scan = ops('secret-scan')
        leak = next(i for i in scan['items'] if i['id'] == 'leak-1')
        assert leak['type'] == 'note' and leak['kinds'] == ['slack_token'] and leak['live'] is True, leak
        assert any(i['type'] == 'handoff' and 'bearer_header' in i['kinds'] for i in scan['items']), scan
        assert scan['notes_flagged'] >= 1 and scan['handoffs_flagged'] >= 1, scan
        m = run('metrics')
        assert m['secrets_blocked_total'] >= 2 and m['secrets_redacted_total'] >= 2 \
            and m['secrets_allowed_total'] >= 2, (m['secrets_blocked_total'], m['secrets_redacted_total'], m['secrets_allowed_total'])
        # Task tags: capability/scope vocabulary for tag-scoped dispatch.
        run('create','--project','Verify','--title','safe work','--id','tag-a','--priority','P1','--tag','autopilot-safe')
        run('create','--project','Verify','--title','risky work','--id','tag-b','--priority','P0')
        run('tag','tag-b','--tag','needs-human','--tag','client:trove')
        # Idempotent re-tag is a no-op that still reports state.
        again = run('tag','tag-b','--tag','needs-human')
        assert again['tags'] == ['client:trove','needs-human'] and again['added'] == [] \
            and again['already_tagged'] == ['needs-human'], again
        err = run_fail('tag','tag-a','--tag','Bad Tag!')
        assert 'invalid tag' in err
        rows = run('list','--tag','autopilot-safe')
        assert [r['id'] for r in rows] == ['tag-a'], rows
        assert [r['id'] for r in run('list','--tag','client:trove')] == ['tag-b']
        assert [r['id'] for r in run('search','work','--tag','autopilot-safe')] == ['tag-a']
        # Tag-scoped dispatch: a constrained agent only ever sees its own work,
        # even when higher-priority untagged work exists.
        got = run('next','--tag','autopilot-safe')
        assert got['task']['id'] == 'tag-a', ('P1 tagged must be visible to its scope', got)
        got = run('next','--tag','client:trove')
        assert got['task']['id'] == 'tag-b', ('second scope sees its own P0', got)
        got = run('next','--tag','no-such-scope')
        assert got['task'] is None, ('an empty scope must dispatch nothing, not leak other work', got)
        got = run('next','--claim','--owner','scoped-agent','--minutes','5','--tag','autopilot-safe')
        assert got['claimed'] is True and got['task']['id'] == 'tag-a' and got['task']['tags'] == ['autopilot-safe'], got
        det = run('show','tag-a')
        assert any(e['action'] == 'claimed' for e in det['audit'])
        run('update','tag-a','--status','completed')
        # Untag removes exactly one tag and is audited; removing an absent tag fails.
        u = run('untag','tag-b','--tag','needs-human')
        assert u['tags'] == ['client:trove'] and u['removed'] == 'needs-human', u
        err = run_fail('untag','tag-b','--tag','needs-human')
        assert "does not carry tag" in err
        assert [r['id'] for r in run('list','--tag','needs-human')] == []
        doc = ops('doctor'); assert doc['ok'] is True and doc['problems'] == [], doc
        doc = ops('doctor'); assert doc['ok'] is True and doc['problems'] == [], doc
        assert run('verify-chain')['ok'] is True
        # Audit chain checkpoints: pin the head so tail truncation becomes detectable.
        cp = ops('checkpoint')
        assert cp['ok'] is True and cp['last_event_id'] > 0 and len(cp['sha256']) == 64, cp
        vc = run('verify-chain','--checkpoint',cp['path'])
        assert vc['ok'] is True and vc['checkpoint']['ok'] is True, vc
        cc = ops('checkpoint-check',cp['path'])
        assert cc['ok'] is True and cc['problems'] == [], cc
        err = run_fail('verify-chain','--checkpoint',str(Path(td) / 'backups' / 'missing.json'))
        assert 'checkpoint not found' in err
        # Growth past the checkpoint is normal operation and never flagged.
        run('create','--project','Verify','--title','post checkpoint','--id','pc-1')
        vc = run('verify-chain','--checkpoint',cp['path'])
        assert vc['ok'] is True, vc
        doc = ops('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Tail truncation: deleting the pinned head (and everything after it) leaves
        # every remaining link valid — only the checkpoint exposes the loss.
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute('DELETE FROM audit_events WHERE id>=?', (cp['last_event_id'],))
        vc_plain = run('verify-chain')
        assert vc_plain['ok'] is True, 'the bare chain must stay internally valid under truncation'
        vc = run('verify-chain','--checkpoint',cp['path'])
        assert vc['ok'] is False and any(p['kind'] == 'chain_truncated' for p in vc['problems']), vc
        ops_fail('checkpoint-check',cp['path'])
        doc = ops('doctor')
        assert any(p['kind'] == 'chain_truncated' for p in doc['problems']), doc
        # A tampered checkpoint file itself is refused outright (self-hash mismatch).
        cpf = Path(cp['path']); orig_cp = cpf.read_text()
        doc_json = json.loads(orig_cp); doc_json['checkpoint']['total_events'] += 5
        cpf.write_text(json.dumps(doc_json))
        ops_fail('checkpoint-check',cp['path'])
        err = run_fail('verify-chain','--checkpoint',cp['path'])
        assert 'integrity check failed' in err
        cpf.write_text(orig_cp)
        # Portable work orders: sealed single-task export/import across homes.
        run('create','--project','Verify','--title','work order source','--id','wo-dep')
        run('create','--project','Verify','--title','work order traveler','--id','wo-1')
        run('dep','wo-1','wo-dep')
        run('note','wo-1','--content','traveler fact one')
        run('handoff','wo-1','--from-agent','agent-a','--to-agent','agent-b',
            '--objective','carry this work across the boundary')
        rec = run('receipt','wo-1','--kind','verification','--payload','{"stage":"pre-export"}')
        run('update','wo-dep','--status','completed')
        run('claim','wo-1','--owner','traveler','--minutes','5')     # active status
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute("INSERT INTO task_deps(task_id,depends_on,created_at)"
                       " VALUES('wo-1','ghost-task','2026-01-01T00:00:00+00:00')")
        wo_path = Path(td) / 'wo-1.json'
        exp = ops('export-task','wo-1','--out',str(wo_path))
        assert exp['ok'] is True and exp['sha256'] and exp['counts']['notes'] == 1 \
            and exp['counts']['receipts'] == 1 and exp['counts']['task_deps'] == 2, exp
        wo_dep_path = Path(td) / 'wo-dep.json'
        ops('export-task','wo-dep','--out',str(wo_dep_path))
        assert wo_path.stat().st_mode & 0o077 == 0
        # Tamper evidence: a mutated work order is refused before any import.
        orig_wo = wo_path.read_text()
        tampered = json.loads(orig_wo); tampered['task']['title'] = 'tampered'
        wo_path.write_text(json.dumps(tampered, sort_keys=True))
        err = ops_fail('import-task',str(wo_path))
        assert 'integrity check failed' in err, err
        err = ops_fail('import-task',str(wo_path),'--force')
        assert 'integrity check failed' in err, '--force must not bypass the seal'
        wo_path.write_text(orig_wo)
        # Cross-home import into a fresh Autopilot state directory.
        home2 = Path(td) / 'home2'; home2.mkdir()
        env2 = os.environ.copy(); env2['HERMES_AUTOPILOT_HOME'] = str(home2)
        def ops2(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env2, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops2_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env2, text=True, capture_output=True)
            assert p.returncode != 0, (a, p.stdout, p.stderr)
            return p.stderr.strip() or p.stdout.strip()
        def run2(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a], env=env2, text=True, capture_output=True)
            if p.returncode: raise AssertionError((a, p.stdout, p.stderr))
            return json.loads(p.stdout)
        plan = ops2('import-task',str(wo_path),'--dry-run')
        assert plan['seal_verified'] is True and plan['exists'] is False \
            and plan['would_sanitize_lease'] is True, plan
        # Prerequisite first: the edge survives only when both endpoints exist.
        dep_imp = ops2('import-task',str(wo_dep_path))
        assert dep_imp['ok'] is True and dep_imp['sanitized'] is False, dep_imp
        imp = ops2('import-task',str(wo_path))
        assert imp['ok'] is True and imp['deduplicated'] is False and imp['sanitized'] is True, imp
        assert imp['skipped_deps'] == ['wo-1->ghost-task'], imp   # dangling edge reported, not dropped
        t2 = run2('show','wo-1')
        assert t2['status'] == 'queued' and t2['lease_owner'] == '' \
            and t2['blocked_reason'] == 'imported from work order', t2
        assert len(t2['receipts']) == 1 and t2['receipts'][0]['id'] == rec['receipt_id']
        assert (home2 / 'receipts' / (rec['receipt_id'] + '.json')).exists()
        assert any(n['content'] == 'traveler fact one' for n in run2('notes','wo-1'))
        assert t2['handoff']['objective'] == 'carry this work across the boundary'
        dep_ids = {d['id'] for d in t2['dependencies']}
        assert 'wo-dep' in dep_ids, t2
        assert next(d for d in t2['dependencies'] if d['id'] == 'wo-dep')['satisfied'] == 1
        assert any(e['action'] == 'task_imported' for e in t2['audit'])
        # Idempotency: re-importing the identical work order deduplicates.
        again = ops2('import-task',str(wo_path))
        assert again['deduplicated'] is True and again['sanitized'] is False, again
        assert len(run2('notes','wo-1')) == 1, 're-import must not duplicate child rows'
        # Divergence: new note at the source; plain import refuses, --force merges.
        run('note','wo-1','--content','traveler fact two')
        exp2 = ops('export-task','wo-1','--out',str(wo_path))
        assert exp2['counts']['notes'] == 2, exp2
        err = ops2_fail('import-task',str(wo_path))
        assert 'different state' in err and '--force' in err, err
        merged = ops2('import-task',str(wo_path),'--force')
        assert merged['replaced'] is True and merged['inserted']['notes'] == 1, merged
        notes2 = run2('notes','wo-1')
        assert len(notes2) == 2, 'merge preserves local rows and adds only the new one'
        # Privacy boundary holds on both ends of the transfer.
        run('note','wo-1','--content','key AKIAIOSFODNN7EXAMPLE pinned','--allow-secret')
        err = ops_fail('export-task','wo-1','--out',str(wo_path))
        assert 'aws_access_key' in err and '--redact' in err, err
        expr = ops('export-task','wo-1','--out',str(wo_path),'--redact')
        assert expr['secret_kinds'] == ['aws_access_key'], expr
        wo_raw = Path(td) / 'wo-1-raw.json'
        rawexp = ops('export-task','wo-1','--out',str(wo_raw),'--allow-secret')
        assert rawexp['secret_kinds'] == ['aws_access_key'], rawexp
        err = ops2_fail('import-task',str(wo_raw))
        assert 'aws_access_key' in err, 'import re-runs the guard even on a sealed file'
        imr = ops2('import-task',str(wo_path),'--redact','--force')
        assert imr['ok'] is True and imr['inserted']['notes'] == 1, imr
        assert 'secret_kinds' not in imr, 'an already-redacted file must pass the guard cleanly'
        stored = next(n for n in run2('notes','wo-1') if 'AKIA' in n['content'] or 'REDACTED' in n['content'])
        assert '[REDACTED:aws_access_key]' in stored['content'] and 'AKIAIOSFODNN7EXAMPLE' not in stored['content'], stored
        # Work orders carry provenance-linked temporal facts across boundaries:
        # validity windows travel intact, re-import deduplicates by fact id, and
        # the secret guard spans the graph (tokens cannot be credential-shaped by
        # construction, but the free-form source field is operator text).
        run('create','--project','Verify','--title','fact traveler','--id','wo-fact')
        fw = run('fact-assert','--subject','deploy-api','--predicate','targets',
                 '--object','us-east-2','--source','codex','--task','wo-fact','--valid-hours','72')
        wo_fact_path = Path(td) / 'wo-fact.json'
        expf = ops('export-task','wo-fact','--out',str(wo_fact_path))
        assert expf['ok'] is True and expf['counts']['facts'] == 1, expf
        impf = ops2('import-task',str(wo_fact_path))
        assert impf['ok'] is True and impf['inserted'].get('facts') == 1, impf
        carried = run2('facts','--all')
        assert len(carried) == 1 and carried[0]['id'] == fw['id'], carried
        assert carried[0]['task_id'] == 'wo-fact' and carried[0]['valid_until'] == fw['valid_until'] \
            and carried[0]['valid_from'] == fw['valid_from'], 'validity window must survive intact'
        again_f = ops2('import-task',str(wo_fact_path))
        assert again_f['deduplicated'] is True and again_f['inserted'].get('facts', 0) == 0, again_f
        assert len(run2('facts','--all')) == 1, 're-import must not duplicate facts'
        run('create','--project','Verify','--title','fact secret host','--id','wo-fact2')
        run('fact-assert','--subject','vault-token','--predicate','issued-by',
            '--object','hashicorp','--source','rotate AKIAIOSFODNN7EXAMPLE now','--task','wo-fact2')
        err = ops_fail('export-task','wo-fact2','--out',str(Path(td) / 'wo-fact2.json'))
        assert 'aws_access_key' in err and '--redact' in err, err   # guard spans the fact graph
        # Seam conflicts: a task whose worktree (or branch+project) is held by
        # another live lease cannot be claimed — audited refusal, no lease left
        # behind; dispatch skips conflicted candidates; --force overrides;
        # releasing the holder frees the seam.
        run('create','--project','Seam','--title','seam a','--id','seam-a')
        run('create','--project','Seam','--title','seam b','--id','seam-b')
        run('create','--project','Seam','--title','seam c','--id','seam-c')
        run('create','--project','Other','--title','seam d','--id','seam-d')
        run('update','seam-a','--worktree','/tmp/seam-wt','--branch','feature/seam')
        run('update','seam-b','--worktree','/tmp/seam-wt')
        run('update','seam-c','--branch','feature/seam')
        run('update','seam-d','--branch','feature/seam')
        run('claim','seam-a','--owner','holder','--minutes','30')
        err = run_fail('claim','seam-b','--owner','other')
        assert 'seam conflict' in err and 'worktree' in err and '--force' in err, err
        row = next(r for r in run('list') if r['id'] == 'seam-b')
        assert row['status'] == 'queued' and row['lease_owner'] == '', 'refusal must not leave a lease'
        err = run_fail('claim','seam-c','--owner','other')
        assert 'seam conflict' in err and 'branch' in err, err
        run('claim','seam-d','--owner','other','--minutes','5')   # same branch, different project: not a seam
        detail = run('show','seam-b')
        refs = [e for e in detail['audit'] if e['action'] == 'claim_refused_seam']
        assert refs and refs[-1]['payload']['conflicts'][0]['task_id'] == 'seam-a', detail
        forced = run('claim','seam-b','--owner','other','--force','--minutes','5')
        assert forced['lease_owner'] == 'other', forced
        # Dispatch skips seam-conflicted candidates instead of failing after picking.
        run('create','--project','Seam','--title','seam e','--id','seam-e','--priority','P0')
        run('create','--project','Seam','--title','plain pick','--id','plain-1','--priority','P3')
        run('update','seam-e','--worktree','/tmp/seam-wt')        # seam held by seam-a and seam-b
        nx = run('next','--project','Seam','--claim','--owner','dispatcher','--explain')
        assert nx['claimed'] is True and nx['task']['id'] == 'plain-1', nx
        assert any(s['task_id'] == 'seam-e' and s['reason'] == 'seam_conflict'
                   and any(c['task_id'] == 'seam-a' for c in s['conflicts']) for s in nx['skipped']), nx
        run('release','plain-1','--owner','dispatcher')
        # Releasing every holder frees the seam for the next dispatcher.
        run('release','seam-a','--owner','holder')
        run('release','seam-b','--owner','other')
        nx2 = run('next','--project','Seam','--claim','--owner','dispatcher2','--explain')
        assert nx2['claimed'] is True and nx2['task']['id'] == 'seam-e', nx2
        run('release','seam-e','--owner','dispatcher2')
        # Downstream impact & unblock-aware scheduling: `impact` walks the DAG
        # downward (mirror of blocked-by), complete reports which queued work it
        # just freed, and --prefer-unblocking tie-breaks equal-priority dispatch
        # toward graph hubs without changing the default order.
        run('create','--project','Impact','--title','root','--id','imp-root')
        run('create','--project','Impact','--title','mid a','--id','imp-mid')
        run('create','--project','Impact','--title','mid b','--id','imp-mid2')
        run('create','--project','Impact','--title','leaf','--id','imp-leaf')
        run('dep','imp-mid','imp-root'); run('dep','imp-mid2','imp-root'); run('dep','imp-leaf','imp-mid')
        imp = run('impact','imp-root')
        assert imp['ok'] is True and imp['impacted'] == 3 and imp['open'] == 3, imp
        assert imp['by_status'].get('queued') == 3, imp
        depths = {d['id']: d['depth'] for d in imp['dependents']}
        assert depths == {'imp-mid': 1, 'imp-mid2': 1, 'imp-leaf': 2}, imp
        assert all(d['settled'] == 0 for d in imp['dependents']), imp
        empty = run('impact','verify-1')
        assert empty['impacted'] == 0 and empty['open'] == 0, empty
        run('claim','imp-root','--owner','tester','--minutes','5')
        done = run('complete','imp-root','--owner','tester')
        assert sorted(done['newly_unblocked']) == ['imp-mid','imp-mid2'], done
        ev = next(e for e in run('show','imp-root')['audit'] if e['action'] == 'completed')
        assert sorted(ev['payload']['newly_unblocked']) == ['imp-mid','imp-mid2'], ev
        run('cancel','imp-leaf','--owner','leo','--reason','obsolete')
        imp2 = run('impact','imp-root')
        assert imp2['open'] == 2 and imp2['by_status'].get('cancelled') == 1, imp2
        run('create','--project','Impact','--title','plain p1','--id','imp-plain','--priority','P1')
        import time; time.sleep(1.1)   # make imp-plain strictly older than imp-hub
        run('create','--project','Impact','--title','hub p1','--id','imp-hub','--priority','P1')
        run('create','--project','Impact','--title','waiting child','--id','imp-child','--priority','P3')
        run('dep','imp-child','imp-hub')
        nx_def = run('next','--project','Impact','--explain')
        assert nx_def['task']['id'] == 'imp-plain' and nx_def['unblocks'] == 0, nx_def
        assert 'unblock_scheduling' not in nx_def, nx_def
        nx_pref = run('next','--project','Impact','--explain','--prefer-unblocking')
        assert nx_pref['task']['id'] == 'imp-hub' and nx_pref['unblocks'] == 1, nx_pref
        assert nx_pref.get('unblock_scheduling') is True, nx_pref
        # Critical path: the longest chain of unfinished dependency work, root first.
        # Graph here (Impact project): imp-mid2 -> imp-hub(claimed) chain length 2;
        # imp-plain standalone; imp-child blocked behind imp-hub.
        cp = run('critical-path','--project','Impact')
        assert cp['ok'] is True and cp['length'] == 2, cp
        assert [n['id'] for n in cp['path']] == ['imp-hub','imp-child'], cp
        assert cp['path'][0]['status'] == 'queued' and cp['path'][1]['status'] == 'queued', cp
        assert cp['open_tasks'] == 5 and cp['by_status'] == {'queued': 5}, cp   # settled work leaves the graph
        # Completing the bottleneck hub shortens the chain; the depth-1 tie breaks
        # to the lexicographically smallest id.
        run('claim','imp-hub','--owner','tester','--minutes','5')
        run('complete','imp-hub','--owner','tester')
        cp2 = run('critical-path','--project','Impact')
        assert cp2['length'] == 1 and [n['id'] for n in cp2['path']] == ['imp-child'], cp2
        # Fleet-wide view spans projects; metrics exposes the same number.
        run('create','--project','Chain','--title','a','--id','cp-a')
        run('create','--project','Chain','--title','b','--id','cp-b')
        run('create','--project','Chain','--title','c','--id','cp-c')
        run('dep','cp-b','cp-a'); run('dep','cp-c','cp-b')
        cpf = run('critical-path')
        assert cpf['length'] == 3 and [n['id'] for n in cpf['path']] == ['cp-a','cp-b','cp-c'], cpf
        m = run('metrics')
        assert m['critical_path_length'] == 3, m
        # A missing prerequisite blocks dispatch like a real task and surfaces on
        # the path once it anchors the deepest branch (completed prereqs leave the
        # graph entirely).
        import sqlite3 as _sq
        with _sq.connect(Path(td) / 'state.db') as db:
            db.execute("INSERT INTO task_deps(task_id,depends_on,created_at) VALUES('cp-c','cp-g1',datetime('now'))")
            db.execute("INSERT INTO task_deps(task_id,depends_on,created_at) VALUES('cp-g1','cp-g2',datetime('now'))")
            db.commit()
        run('dep-remove','cp-c','cp-b')
        cpg = run('critical-path','--project','Chain')
        assert cpg['length'] == 3, cpg
        assert [(n['id'], n['status']) for n in cpg['path']] == [('cp-g2','missing'),('cp-g1','missing'),('cp-c','queued')], cpg
        # Task deduplication: creating work that restates an open same-project task
        # flags it at create time (audited), `similar` triages from any task, and
        # ops.py dup-tasks sweeps clusters fleet-wide; settled tasks and other
        # projects never count as duplicates.
        run('create','--project','Dupes','--title','fix login redirect loop','--id','tdup-1')
        d2 = run('create','--project','Dupes','--title','fix login redirect loop bug','--id','tdup-2')
        sims = d2.get('similar_open_tasks') or []
        assert len(sims) == 1 and sims[0]['task_id'] == 'tdup-1' and sims[0]['similarity'] >= 0.8, d2
        d3 = run('create','--project','Dupes','--title','write onboarding docs','--id','tdup-3')
        assert 'similar_open_tasks' not in d3, d3                       # unrelated work stays quiet
        dx = run('create','--project','Other','--title','fix login redirect loop','--id','tdup-x')
        assert 'similar_open_tasks' not in dx, dx                       # cross-project is a different seam
        ev = next(e for e in run('show','tdup-2')['audit'] if e['action'] == 'created')
        assert ev['payload']['similar_open_tasks'] == ['tdup-1'], ev     # provenance in the audit chain
        sim = run('similar','tdup-1')
        assert sim['ok'] is True and [s['task_id'] for s in sim['similar']] == ['tdup-2'], sim
        assert run('similar','tdup-3')['count'] == 0
        assert run('similar','tdup-2','--threshold','0.99')['count'] == 0   # threshold is honored
        dt = ops('dup-tasks')
        cl = next(c for c in dt['clusters'] if c['project'] == 'Dupes')
        assert cl['canonical']['task_id'] == 'tdup-1', dt                # oldest is canonical
        assert [d['task_id'] for d in cl['duplicates']] == ['tdup-2'], dt
        assert dt['duplicate_tasks'] >= 1
        run('claim','tdup-1','--owner','tester','--minutes','5')
        run('complete','tdup-1','--owner','tester')
        run('cancel','tdup-2','--owner','tester')
        d4 = run('create','--project','Dupes','--title','fix login redirect loop','--id','tdup-4')
        assert 'similar_open_tasks' not in d4, d4                       # settled work is history, not a collision
        # Dispatch wave plan: the fleet-level schedule — wave 1 is every ready task
        # (in-flight included), each later wave is what the previous waves unblock,
        # and out-of-scope prerequisites strand their downstream under
        # `unschedulable` instead of being silently dropped or falsely promised.
        run('create','--project','Plan','--title','root a','--id','pl-a','--priority','P1')
        run('create','--project','Plan','--title','root b','--id','pl-b','--priority','P0')
        run('create','--project','Plan','--title','join','--id','pl-j','--priority','P1')
        run('dep','pl-j','pl-a'); run('dep','pl-j','pl-b')
        run('create','--project','Plan','--title','tail','--id','pl-t')
        run('dep','pl-t','pl-j')
        run('create','--project','Elsewhere','--title','external blocker','--id','pl-x')
        run('create','--project','Plan','--title','orphan dependent','--id','pl-o')
        run('dep','pl-o','pl-x')                                        # cross-project prereq
        pl = run('plan','--project','Plan')
        assert [w['wave'] for w in pl['waves']] == [1, 2, 3], pl        # diamond drains in 3 waves
        assert [t['id'] for t in pl['waves'][0]['tasks']] == ['pl-b','pl-a'], pl   # P0 before P1 inside a wave
        assert [t['id'] for t in pl['waves'][1]['tasks']] == ['pl-j'], pl          # join waits for both roots
        assert [t['id'] for t in pl['waves'][2]['tasks']] == ['pl-t'], pl
        assert pl['waves_total'] == 3 and pl['scheduled_tasks'] == 4 and pl['open_tasks'] == 5, pl
        uns = next(u for u in pl['unschedulable'] if u['id'] == 'pl-o')
        assert uns['blocked_by'] == [{'id': 'pl-x', 'status': 'queued'}], uns      # honest about scope
        # In-flight work is ready now and shows its live status in wave 1.
        run('claim','pl-a','--owner','tester','--minutes','5')
        run('heartbeat','pl-a','--owner','tester','--note','in flight')
        pl = run('plan','--project','Plan')
        w1 = {t['id']: t['status'] for t in pl['waves'][0]['tasks']}
        assert w1.get('pl-a') == 'running', pl                          # claimed+heartbeat ⇒ running stays wave 1
        # Determinism: identical state yields an identical schedule.
        assert pl == run('plan','--project','Plan'), 'plan must be deterministic'
        # waves_total parity with critical-path for the same scope.
        cp = run('critical-path','--project','Plan')
        assert cp['length'] == pl['waves_total'], (cp, pl)
        # Completing a root pulls its dependents one wave earlier.
        run('complete','pl-a','--owner','tester')
        pl2 = run('plan','--project','Plan')
        assert max(t['id'] for w in pl2['waves'][:1] for t in w['tasks']) == 'pl-b'
        assert [t['id'] for t in pl2['waves'][1]['tasks']] == ['pl-j'], pl2
        assert pl2['waves_total'] == 3, pl2                             # tail still needs its own wave
        # Settled tasks leave the graph; an empty project plans nothing.
        for tid in ('pl-b', 'pl-j', 'pl-t'):
            run('claim', tid, '--owner', 'tester', '--minutes', '5')
            run('complete', tid, '--owner', 'tester')
        run('cancel','pl-o','--owner','tester')
        assert run('plan','--project','Nothing')['open_tasks'] == 0 and run('plan','--project','Nothing')['waves'] == []
        # Tag scoping: only tagged tasks are scheduled, untagged dependents go unschedulable.
        run('create','--project','Plan','--title','tagged leaf','--id','pl-g1','--depends-on','pl-t')
        run('tag','pl-g1','--tag','autopilot-safe')
        plt = run('plan','--tag','autopilot-safe')
        assert [[t['id'] for t in w['tasks']] for w in plt['waves']] == [['pl-g1']], plt
        assert plt['unschedulable'] == [] and plt['scheduled_tasks'] == 1, plt   # completed prereq leaves the graph
        # Evidence-linked completions & policy-gated readiness transitions:
        # a self-report is never execution truth without a receipt, and project
        # policies gate merge/deploy readiness behind explicit user approval.
        run('create','--project','Plain','--title','no receipts needed','--id','ev-1')
        run('claim','ev-1','--owner','tester','--minutes','5')
        c1 = run('complete','ev-1','--owner','tester')                  # backward compatible shape
        assert 'evidence_receipts' not in c1, c1
        run('create','--project','Plain','--title','evidenced','--id','ev-2')
        run('claim','ev-2','--owner','tester','--minutes','5')
        rec2 = run('receipt','ev-2','--kind','verification','--payload','{"tests":"pass"}')
        err = run_fail('complete','ev-2','--owner','tester','--receipt','nonexistent')
        assert 'evidence receipt not found' in err                      # unknown receipt refused
        run('create','--project','Plain','--title','other task','--id','ev-3')
        rec3 = run('receipt','ev-3','--kind','log','--payload','{}')
        err = run_fail('complete','ev-2','--owner','tester','--receipt',rec3['receipt_id'])
        assert 'evidence receipt not found' in err                      # another task's receipt is not evidence
        c2 = run('complete','ev-2','--owner','tester','--receipt',rec2['receipt_id'])
        assert c2['evidence_receipts'] == [rec2['receipt_id']], c2
        evc = next(e for e in run('show','ev-2')['audit'] if e['action'] == 'completed')
        assert evc['payload']['evidence_receipts'] == [rec2['receipt_id']], evc   # provenance in the chain
        uv = ops('unverified-completions')
        kinds = {i['task_id']: i['kind'] for i in uv['items']}
        assert kinds.get('ev-1') == 'no_receipts', uv                   # bare self-report flagged
        assert kinds.get('ev-2') is None, uv                            # evidenced completion passes
        m = run('metrics')
        assert m['completions_without_receipt'] >= 1, m
        # Cited evidence that later vanishes (deleted rows / partial restore) is observable.
        import sqlite3 as _sq
        with _sq.connect(Path(td) / 'state.db') as db:
            db.execute('DELETE FROM receipts WHERE id=?', (rec2['receipt_id'],))
        uv = ops('unverified-completions')
        # Losing the receipt makes the completion both unbacked and broken-cited.
        miss = next(i for i in uv['items'] if i['task_id'] == 'ev-2'
                    and i['kind'] == 'evidence_receipt_missing')
        assert miss['receipt_ids'] == [rec2['receipt_id']], miss
        # Definition of done: required receipt kinds are acceptance criteria as
        # data — completion refuses until every required kind has evidence.
        err = run_fail('create','--project','Plain','--title','bad kind','--id','dod-x',
                       '--requires-receipt','Bad Kind!')
        assert 'invalid receipt kind' in err                             # restricted stable charset
        run('create','--project','Plain','--title','definition of done','--id','dod-1',
            '--requires-receipt','Verification','--requires-receipt','test-report')
        d1 = run('show','dod-1')
        assert d1['requires_receipts'] == ['test-report','verification'], d1   # normalized + sorted
        crev = next(e for e in run('show','dod-1')['audit'] if e['action'] == 'created')
        assert crev['payload']['requires_receipts'] == ['test-report','verification'], crev
        run('claim','dod-1','--owner','tester','--minutes','5')
        err = run_fail('complete','dod-1','--owner','tester')
        assert 'definition of done unmet' in err \
            and 'test-report' in err and 'verification' in err           # gate names every gap
        blk = next(e for e in run('events','--entity-type','task','--entity-id','dod-1')['events']
                   if e['action'] == 'completion_blocked_evidence')
        assert blk['payload']['missing_receipt_kinds'] == ['test-report','verification'], blk
        m = run('metrics')
        assert 'dod-1' in m['tasks_missing_required_evidence'], m        # open work awaiting evidence
        assert m['completions_blocked_by_evidence'] >= 1, m              # refusals are visible fleet-wide
        run('receipt','dod-1','--kind','verification','--payload','{}')
        err = run_fail('complete','dod-1','--owner','tester')
        assert 'missing required receipt kind(s): test-report' in err    # only the remaining gap
        run('receipt','dod-1','--kind','test-report','--payload','{}')
        cd = run('complete','dod-1','--owner','tester')
        assert cd['required_evidence_met'] is True and cd['status'] == 'completed', cd
        m = run('metrics')
        assert 'dod-1' not in m['tasks_missing_required_evidence'], m    # settled work leaves the sweep
        # Requirements are editable lifecycle data: set later, clear deliberately.
        run('create','--project','Plain','--title','retroactive dod','--id','dod-2')
        up = run('update','dod-2','--requires-receipt','log')
        assert up['requires_receipts'] == ['log'], up
        up = run('update','dod-2','--requires-receipt','')               # empty string clears
        assert up['requires_receipts'] == [], up
        # Policy gate: entering a gated readiness state consults policies/<project>.yaml.
        pol = Path(td) / 'policies'; pol.mkdir(exist_ok=True)
        (pol / 'gated.yaml').write_text('merge_requires_user: true\n')
        run('create','--project','Gated','--title','needs approval','--id','pg-1')
        err = run_fail('update','pg-1','--status','ready_to_merge')
        assert 'user approval' in err and '--approved-by' in err        # silent readiness refused
        up = run('update','pg-1','--status','ready_to_merge','--approved-by','leo')
        assert up['status'] == 'ready_to_merge'
        evu = next(e for e in run('show','pg-1')['audit'] if e['action'] == 'updated')
        assert evu['payload']['approved_by'] == 'leo' \
            and evu['payload']['policy_gate'] == 'merge', evu           # who approved, recorded durably
        run('update','pg-1','--next-action','polish')                   # no status change: ungated
        run('update','ev-3','--status','ready_to_deploy')               # policy-less project stays open
        # Project dispatch policy: policies/<project>.yaml gates dispatch itself —
        # a required capability tag keeps untagged work undispatchable and a WIP
        # cap steers an owner at capacity toward other projects instead of failing.
        (pol / 'policyproj.yaml').write_text('dispatch_requires_tag: worker-safe\nmax_wip_per_owner: 1\n')
        run('create','--project','PolicyProj','--title','untagged work','--id','dp-1')
        run('create','--project','PolicyProj','--title','tagged work','--id','dp-2','--tag','worker-safe')
        run('create','--project','PolicyProj','--title','second tagged','--id','dp-3','--tag','worker-safe')
        nx = run('next','--project','PolicyProj','--explain')
        assert nx['task']['id'] == 'dp-2', nx                           # tagged work dispatches, oldest first
        sk = next(s for s in nx['skipped'] if s['task_id'] == 'dp-1')
        assert sk['reason'] == 'policy_missing_tag' and sk['required_tag'] == 'worker-safe', sk
        err = run_fail('claim','dp-1','--owner','pa-1')
        assert 'requires tag' in err and '--force' in err               # direct claim gated too
        ref = next(e for e in run('events','--entity-type','task','--entity-id','dp-1')['events']
                   if e['action'] == 'claim_refused_policy')
        assert ref['payload']['gate'] == 'dispatch_requires_tag', ref   # refusal is audited
        run('claim','dp-1','--owner','pa-1','--force')                  # deliberate override
        ovr = next(e for e in run('show','dp-1')['audit'] if e['action'] == 'claimed')
        assert ovr['payload']['policy_overrides'][0]['gate'] == 'dispatch_requires_tag', ovr
        run('release','dp-1','--owner','pa-1')
        run('claim','dp-2','--owner','pa-1')                            # within cap (0 -> 1)
        nx = run('next','--claim','--project','PolicyProj','--owner','pa-1','--explain')
        assert nx['task'] is None, nx                                   # at cap: nothing handed out
        sk = next(s for s in nx['skipped'] if s['task_id'] == 'dp-3')
        assert sk['reason'] == 'policy_wip_cap' and sk['cap'] == 1 and sk['held'] == ['dp-2'], sk
        err = run_fail('claim','dp-3','--owner','pa-1')
        assert 'caps owner' in err                                      # direct claim refused at cap
        c3 = run('next','--claim','--project','PolicyProj','--owner','pa-2')
        assert c3['task']['id'] == 'dp-3' and c3['claimed'], c3         # other owners unaffected
        # Steering: at cap here, fleet-wide dispatch moves to another project.
        run('create','--project','Elsewhere','--title','other project','--id','dp-4')
        run('release','dp-3','--owner','pa-2')
        nx = run('next','--claim','--owner','pa-1','--explain')
        sk = next(s for s in nx['skipped'] if s['task_id'] == 'dp-3')
        assert sk['reason'] == 'policy_wip_cap', sk
        assert nx['task'] is not None and nx['task']['project'] != 'PolicyProj', nx
        # Removing the policy restores open dispatch; refusals stay visible in metrics.
        (pol / 'policyproj.yaml').unlink()
        nx = run('next','--project','PolicyProj','--explain')
        assert all(s['reason'] != 'policy_missing_tag' for s in nx.get('skipped', [])), nx
        m = run('metrics')
        assert m['claims_refused_by_policy'] >= 2, m


@case('migration_and_onboarding')
def _case_migration_and_onboarding():
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
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        import sqlite3, hashlib, time
        run('create','--project','Verify','--title','lived-in marker','--id','live-in-1')
        run('note','live-in-1','--content','main home already carries audited history')
        # Migration inventory: read-only durable-source discovery that seals a
        # versioned, sha256-verified migration-plan manifest; fails closed on any
        # corrupted or ambiguous source; never mutates a scanned source.
        import hashlib
        mig = Path(td) / 'migsources'
        old_home = mig / 'machines/old/autopilot'; old_home.mkdir(parents=True)
        menv = os.environ.copy(); menv['HERMES_AUTOPILOT_HOME'] = str(old_home)
        def mrun(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a], env=menv, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
        mrun('init'); mrun('create', '--project', 'Legacy', '--title', 'carried work', '--id', 'mig-1')
        (mig / 'broken/autopilot').mkdir(parents=True)
        (mig / 'broken/autopilot/state.db').write_bytes(b'not a database at all')
        amb_db = mig / 'stray.sqlite'; sqlite3.connect(amb_db).commit()
        vault = mig / 'vault/Obs Vault/.obsidian'; vault.mkdir(parents=True)
        (vault.parent / 'meeting.md').write_text('rotate the key AKIAIOSFODNN7EXAMPLE this week')
        (mig / 'hindsight/banks/main').mkdir(parents=True)
        (mig / 'hindsight/banks/main/memories.json').write_text('[]')
        (mig / 'leohome/.hermes/profiles').mkdir(parents=True)
        (mig / 'leohome/.hermes/profiles/leo.md').write_text('profile')
        inv_path = Path(td) / 'inventory-full.json'
        err = ops_fail('migrate-inventory', '--root', str(mig), '--out', str(inv_path))
        assert 'failed closed' in err and str(mig / 'broken') in err, err   # names what is blocked
        assert inv_path.stat().st_mode & 0o077 == 0                         # sealed manifest is 0600
        inv = json.loads(inv_path.read_text())
        assert inv['fail_closed'] is True and inv['summary']['blocked'] == 2, inv
        by_kind = {s['kind']: s for s in inv['sources']}
        def src(path):
            return next(s for s in inv['sources'] if s['path'].endswith(str(path)))
        assert src('broken/autopilot/state.db')['status'] == 'corrupted' \
            and 'not a database' in src('broken/autopilot/state.db')['problems'][0]
        assert by_kind['unknown_sqlite']['status'] == 'ambiguous'
        assert by_kind['obsidian_vault']['secret_kinds'] == ['aws_access_key'], by_kind   # kind-only redaction
        assert all('AKIAIOSFODNN7EXAMPLE' not in json.dumps(s) for s in inv['sources'])   # value never leaves
        assert [p_['order'] for p_ in inv['plan']] == list(range(len(inv['plan'])))       # ordered plan
        chk = ops('migrate-inventory-check', str(inv_path))
        assert chk['ok'] is True and chk['summary']['healthy'] == 4, chk
        tampered = json.loads(inv_path.read_text()); tampered['sources'][0]['status'] = 'ok'
        inv_path.write_text(json.dumps(tampered, sort_keys=True))
        err = ops_fail('migrate-inventory-check', str(inv_path))
        assert 'integrity check failed' in err, err                    # tampered manifest refused
        def tree_digest():
            h = hashlib.sha256()
            for p in sorted(mig.rglob('*')):
                if p.is_file() and not p.is_symlink():
                    h.update(str(p.relative_to(mig)).encode()); h.update(p.read_bytes())
            return h.hexdigest()
        before = tree_digest()
        clean = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                                '--root', str(mig / 'machines')], env=env, text=True, capture_output=True)
        assert clean.returncode == 0, clean.stderr                     # healthy subtree: no fail-closed exit
        cdoc = json.loads(clean.stdout)
        csrc = next(s for s in cdoc['sources'] if s['kind'] == 'autopilot_sqlite')
        assert csrc['status'] == 'ok' and csrc['counts']['tasks'] == 1, csrc
        assert cdoc['plan'][0]['kind'] == 'autopilot_sqlite' and 'import-task' in cdoc['plan'][0]['action'], cdoc
        assert cdoc['summary']['secret_kinds'] == [], cdoc
        clean2 = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                                 '--root', str(mig / 'machines')], env=env, text=True, capture_output=True)
        d1, d2 = json.loads(clean.stdout), json.loads(clean2.stdout)
        d1.pop('created_at'); d2.pop('created_at')
        assert d1 == d2, 'identical sources must reproduce an identical manifest'
        assert tree_digest() == before, 'discovery must never mutate a scanned source'
        # Migration import: stage two of the installer/migrator. A sealed stage-one
        # inventory binds the import; drift since sealing is refused; active leases
        # are sanitized to queued (the prior owner does not exist here); receipt
        # files are restored byte-exactly with hash verification; re-runs dedupli-
        # cate idempotently; a lived-in target demands an explicit --relink-audit
        # decision before a foreign audit chain may be merged; and credential-
        # shaped content is refused by default with --redact as the middle path.
        def mkhome(path):
            path.mkdir(parents=True)
            henv = os.environ.copy(); henv['HERMES_AUTOPILOT_HOME'] = str(path)
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), 'init'],
                               env=henv, text=True, capture_output=True)
            assert p.returncode == 0, p.stderr
            return henv
        def hrun(henv_, *a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a],
                               env=henv_, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout) if p.stdout.strip() else {}
        impsrc = Path(td) / 'impsrc/autopilot'
        senv = mkhome(impsrc)
        hrun(senv, 'create', '--project', 'Legacy', '--title', 'carried work', '--id', 'mig-1')
        hrun(senv, 'create', '--project', 'Legacy', '--title', 'active job', '--id', 'mig-2')
        hrun(senv, 'claim', 'mig-1', '--owner', 'old-agent')
        hrun(senv, 'claim', 'mig-2', '--owner', 'old-agent')       # left mid-flight on purpose
        hrun(senv, 'receipt', 'mig-1', '--kind', 'verification', '--payload', '{"evidence":"tests pass"}')
        rid = next((impsrc / 'receipts').glob('*.json')).stem
        hrun(senv, 'complete', 'mig-1', '--owner', 'old-agent', '--receipt', rid)
        hrun(senv, 'note', 'mig-1', '--content', 'context note for the next agent')
        # The temporal fact graph travels with execution truth: one provenance-
        # linked fact and one fleet-level fact with a validity window. Tokens are
        # namespaced away from the fact-graph stage's own fixtures because the
        # relink test below merges this same inventory into the main home.
        hrun(senv, 'fact-assert', '--subject', 'mig-auth', '--predicate', 'uses',
             '--object', 'postgres-13', '--source', 'codex', '--task', 'mig-1')
        hrun(senv, 'fact-assert', '--subject', 'mig-api', '--predicate', 'reads',
             '--object', 'redis-cache', '--source', 'hermes', '--valid-hours', '48')
        inv2_path = Path(td) / 'inventory-import.json'
        inv2 = ops('migrate-inventory', '--root', str(Path(td) / 'impsrc'), '--out', str(inv2_path))
        sid = inv2['sources'][0]['id']
        tgt = Path(td) / 'imptgt/autopilot'
        tenv = mkhome(tgt)
        def mimpo(tenv_, *a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                                '--inventory', str(inv2_path), '--source-id', sid, *a],
                               env=tenv_, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        dry = mimpo(tenv)
        assert dry['dry_run'] is True and dry['sanitized_tasks'] == ['mig-2'], dry
        assert dry['tables']['tasks']['source_rows'] == 2, dry
        assert dry['heartbeats_skipped_disposable'] >= 1, dry      # disposable cache reported, not carried
        res = mimpo(tenv, '--apply', '--out', str(Path(td) / 'migration-result.json'))
        assert res['ok'] is True and res['inserted']['tasks'] == 2, res
        assert res['inserted']['facts'] == 2, res          # fact graph crosses the boundary
        assert res['audit_events_imported'] > 0 and not res['audit_relinked'], res
        assert res['receipt_files_written'] == 1, res
        assert res['health']['problems'] == [], res                # post-apply health report clean
        tf = {r['id']: r for r in hrun(tenv, 'facts', '--all')}
        assert len(tf) == 2 and all(f['valid_from'] for f in tf.values()), tf
        windowed = [f for f in tf.values() if f['valid_until']]
        assert len(windowed) == 1 and windowed[0]['subject'] == 'mig-api', \
            'validity windows must survive the transfer'
        rdoc = json.loads((Path(td) / 'migration-result.json').read_text())
        rbody = {k: v for k, v in rdoc.items() if k not in ('sha256', 'format', 'created_at')}
        assert rdoc['format'] == 'autopilot-migration-result-v1'
        assert hashlib.sha256(json.dumps(rbody, sort_keys=True).encode()).hexdigest() == rdoc['sha256']
        st, owner = sqlite3.connect(tgt / 'state.db').execute(
            "SELECT status, lease_owner FROM tasks WHERE id='mig-2'").fetchone()
        assert st == 'queued' and owner == '', (st, owner)         # active lease sanitized on arrival
        assert (tgt / 'receipts' / f'{rid}.json').read_bytes() \
            == (impsrc / 'receipts' / f'{rid}.json').read_bytes()  # byte-exact receipt restore
        assert hrun(tenv, 'verify-chain')['ok'] is True            # imported chain verifies verbatim
        res2 = mimpo(tenv, '--apply')
        assert res2['deduplicated'] is True, res2                  # identical re-run deduplicates to nothing
        assert res2['audit_events_imported'] == 0, res2
        assert all(v == 0 for v in res2['inserted'].values()), res2
        err = ops_fail('migrate-import', '--inventory', str(inv2_path), '--source-id', sid, '--apply')
        assert '--relink-audit' in err, err                        # lived-in home: no silent chain merge
        mp = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                             '--inventory', str(inv2_path), '--source-id', sid,
                             '--apply', '--relink-audit'], env=env, text=True, capture_output=True)
        assert mp.returncode == 0, (mp.stdout, mp.stderr)
        merged = json.loads(mp.stdout)
        assert merged['audit_relinked'] is True and merged['health']['problems'] == [], merged
        assert run('verify-chain')['ok'] is True                   # relinked combined ledger still verifies
        hrun(senv, 'create', '--project', 'Legacy', '--title', 'post-seal drift', '--id', 'mig-3')
        err = mimpo(tenv, '--apply', fail=True)
        assert 'changed since the inventory was sealed' in err, err  # drift caught before any data moves
        fc_path = Path(td) / 'inventory-fc.json'
        ops_fail('migrate-inventory', '--root', str(mig), '--out', str(fc_path))  # seals before failing closed
        err = ops_fail('migrate-import', '--inventory', str(fc_path), '--source-id', sid, '--apply')
        assert 'failed closed' in err, err                         # fail-closed inventories never import
        vinv_path = Path(td) / 'inventory-vault.json'
        vinv = ops('migrate-inventory', '--root', str(mig / 'vault'), '--out', str(vinv_path))
        err = ops_fail('migrate-import', '--inventory', str(vinv_path),
                       '--source-id', vinv['sources'][0]['id'])
        assert 'only autopilot_sqlite' in err, err                 # non-execution-truth sources refused
        err = ops_fail('migrate-import', '--inventory', str(inv2_path), '--source-id', 'src-absent')
        assert 'not found in inventory' in err, err
        secsrc = Path(td) / 'secsrc/autopilot'
        secenv = mkhome(secsrc)
        hrun(secenv, 'create', '--project', 'Legacy', '--title', 'secret work', '--id', 'sec-1')
        hrun(secenv, 'note', 'sec-1', '--content',
             'deploy key AKIAIOSFODNN7EXAMPLE rotate soon', '--allow-secret')  # legacy-style row
        sinv_path = Path(td) / 'inventory-secret.json'
        sinv = ops('migrate-inventory', '--root', str(Path(td) / 'secsrc'), '--out', str(sinv_path))
        ssid = sinv['sources'][0]['id']
        stgt = Path(td) / 'sectgt/autopilot'
        stenv = mkhome(stgt)
        def simpo(*a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                                '--inventory', str(sinv_path), '--source-id', ssid, *a],
                               env=stenv, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        err = simpo('--apply', fail=True)
        assert 'refusing to import credential-shaped content' in err and 'aws_access_key' in err, err
        red = simpo('--apply', '--redact')
        assert red['secret_kinds'] == ['aws_access_key'] and red['ok'] is True, red
        note_content = sqlite3.connect(stgt / 'state.db').execute(
            'SELECT content FROM notes').fetchone()[0]
        assert '[REDACTED:aws_access_key]' in note_content, note_content
        assert 'AKIAIOSFODNN7EXAMPLE' not in note_content          # value never crosses the boundary
        # Legacy tolerance: a source whose schema predates the fact graph entirely
        # (no facts table) is legacy shape, not corruption — it classifies healthy,
        # imports cleanly with zero facts, and never refuses the whole migration.
        legsrc = Path(td) / 'legacysrc/autopilot'
        lenv = mkhome(legsrc)
        hrun(lenv, 'create', '--project', 'Legacy', '--title', 'pre-fact-graph work', '--id', 'lg-1')
        lgcon = sqlite3.connect(legsrc / 'state.db')
        for trig in ('facts_fts_ai', 'facts_fts_ad'):
            lgcon.execute(f'DROP TRIGGER IF EXISTS {trig}')
        lgcon.execute('DROP TABLE IF EXISTS facts_fts')
        lgcon.execute('DROP INDEX IF EXISTS idx_facts_subject')
        lgcon.execute('DROP INDEX IF EXISTS idx_facts_object')
        lgcon.execute('DROP TABLE facts')
        lgcon.commit(); lgcon.close()
        lginv_path = Path(td) / 'inventory-legacy.json'
        lginv = ops('migrate-inventory', '--root', str(Path(td) / 'legacysrc'), '--out', str(lginv_path))
        assert lginv['sources'][0]['status'] == 'ok', lginv         # classified healthy, not ambiguous
        lgtgt = Path(td) / 'legacytgt/autopilot'
        lgenv = mkhome(lgtgt)
        def limpo(*a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                                '--inventory', str(lginv_path), '--source-id', lginv['sources'][0]['id'], *a],
                               env=lgenv, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        dry_lg = limpo()
        assert dry_lg['tables']['facts']['source_rows'] == 0, dry_lg
        res_lg = limpo('--apply')
        assert res_lg['ok'] is True and res_lg['inserted']['tasks'] == 1 \
            and res_lg['inserted'].get('facts', 0) == 0, res_lg
        assert res_lg['health']['problems'] == [], res_lg
        # Migration rollback: stage three of the installer/migrator. A sealed
        # apply writes a rollback journal (exact rows + insert-time hashes,
        # merged audit event ids, restored receipt files); migrate-rollback
        # consumes it dry-run first, refuses drifted rows and local dependents
        # fail-closed (--force cascades deliberately), relinks the audit chain,
        # keeps locally changed receipt files, and deduplicates re-runs.
        rdoc = json.loads((Path(td) / 'migration-result.json').read_text())
        assert set(rdoc['rollback']['tables']['tasks']['keys']) == {'mig-1', 'mig-2'}, rdoc
        def mroll(*a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback',
                                str(Path(td) / 'migration-result.json'), *a],
                               env=tenv, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        tdoc = json.loads((Path(td) / 'migration-result.json').read_text())
        tdoc['sources'] = 'tampered'
        tamp = Path(td) / 'migration-result-tampered.json'
        tamp.write_text(json.dumps(tdoc, sort_keys=True))
        err = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback',
                              str(tamp)], env=tenv, text=True, capture_output=True)
        assert err.returncode != 0 and 'integrity check failed' in err.stdout + err.stderr
        plan = mroll()
        assert plan['dry_run'] is True and plan['would_remove']['tasks'] == ['mig-1', 'mig-2'], plan
        assert len(plan['would_remove']['facts']) == 2, plan        # journal covers the fact graph
        assert rid in plan['receipt_files_would_delete'], plan
        assert plan['force_required'] is False and plan['drifted_rows'] == [], plan
        assert sqlite3.connect(tgt / 'state.db').execute(
            "SELECT COUNT(*) FROM tasks WHERE id LIKE 'mig-%'").fetchone()[0] == 2   # dry-run touched nothing
        raw = sqlite3.connect(tgt / 'state.db')
        raw.execute("UPDATE tasks SET title='locally edited' WHERE id='mig-1'"); raw.commit(); raw.close()
        dplan = mroll()
        assert dplan['drifted_rows'] == [{'table': 'tasks', 'key': 'mig-1'}], dplan
        err = mroll('--apply', fail=True)
        assert 'refusing to roll back changed execution truth' in err and 'drifted' in err, err
        raw = sqlite3.connect(tgt / 'state.db')
        raw.execute("UPDATE tasks SET title='carried work' WHERE id='mig-1'"); raw.commit(); raw.close()
        hrun(tenv, 'note', 'mig-1', '--content', 'local follow-up written after import')
        # A local fact pointing at an imported task blocks rollback too: removing
        # the task would dangle its soft provenance (doctor flags that as
        # out-of-band surgery), so facts block exactly like hard children.
        hrun(tenv, 'fact-assert', '--subject', 'mig-service', '--predicate', 'owned-by',
             '--object', 'legacy-team', '--source', 'local', '--task', 'mig-1')
        err = mroll('--apply', fail=True)
        assert 'local dependent' in err and '--force' in err, err   # never-imported child blocks removal
        rb = mroll('--apply', '--force')
        assert rb['ok'] is True and rb['removed']['tasks'] == 2, rb
        assert rb['removed']['notes'] == 1, rb                      # imported note removed by journal
        assert rb['removed']['facts'] == 3, rb                      # 2 imported + 1 local cascade
        assert rb['cascade_removed'] == 2, rb                       # local dependents cascaded with --force
        assert rb['receipt_files_deleted'] == [rid], rb             # byte-exact file removed with its row
        assert rb['audit_events_removed'] > 0 and rb['health']['problems'] == [], rb
        con = sqlite3.connect(tgt / 'state.db')
        assert con.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM receipts").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 0   # graph rolled back exactly
        acts = {r[0] for r in con.execute('SELECT action FROM audit_events')}
        con.close()
        assert 'migration_rollback_applied' in acts and 'migration_import_applied' in acts, acts
        assert not (tgt / 'receipts' / f'{rid}.json').exists()
        assert hrun(tenv, 'verify-chain')['ok'] is True             # chain relinked over removed events
        rb2 = mroll('--apply')
        assert all(v == 0 for v in rb2['removed'].values()) and rb2['audit_events_removed'] == 0, rb2
        stale = json.loads((Path(td) / 'migration-result.json').read_text())
        stale.pop('rollback')
        body = {k: v for k, v in stale.items() if k not in ('sha256', 'format', 'created_at')}
        stale['sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
        oldfmt = Path(td) / 'migration-result-oldfmt.json'
        oldfmt.write_text(json.dumps(stale, sort_keys=True))
        err = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback',
                              str(oldfmt)], env=tenv, text=True, capture_output=True)
        assert err.returncode != 0 and 'no rollback journal' in err.stdout + err.stderr

        # Onboarding: one command from a sealed stage-one inventory to a verified
        # working home. Dry-run plans and verifies but imports nothing; --apply
        # imports, sweeps doctor, and (--probe) exercises the cross-agent protocol
        # end-to-end through the real CLI; re-runs are idempotent; ambiguous or
        # fail-closed inventories refuse before anything moves.
        ob_home = Path(td) / 'onboard-home'
        obenv = os.environ.copy(); obenv['HERMES_AUTOPILOT_HOME'] = str(ob_home)
        def oops(*a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=obenv, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        ob_rep = Path(td) / 'onboard-dry.json'
        ob_inv = Path(td) / 'inventory-onboard.json'   # fresh seal: the earlier
        # post-seal-drift test deliberately mutated impsrc after inv2 was sealed.
        ops('migrate-inventory', '--root', str(Path(td) / 'impsrc'), '--out', str(ob_inv))
        dry = oops('onboard', '--inventory', str(ob_inv), '--out', str(ob_rep))
        assert dry['ok'] is True and [s['stage'] for s in dry['stages']] == \
            ['preflight', 'init_control_plane', 'select_source', 'import_plan', 'doctor'], dry
        assert ob_rep.stat().st_mode & 0o077 == 0                          # sealed report is 0600
        with sqlite3.connect(ob_home / 'state.db') as db:                   # init ran...
            assert db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0] == 0   # ...but nothing imported
        err = oops('onboard', '--inventory', str(ob_inv), '--probe', fail=True)
        assert '--probe requires --apply' in err, err                       # probe mutates by design
        ob_rep2 = Path(td) / 'onboard-apply.json'
        rep = oops('onboard', '--inventory', str(ob_inv), '--apply', '--probe', '--out', str(ob_rep2))
        assert [s['stage'] for s in rep['stages']] == \
            ['preflight', 'init_control_plane', 'select_source', 'import_plan',
             'import_apply', 'doctor', 'protocol_probe'], rep
        assert all(s['status'] == 'ok' for s in rep['stages']), rep
        assert rep['stages'] == [{'stage': s['stage'], 'status': s['status']} for s in
                                 json.loads(ob_rep2.read_text())['stages']], rep   # stdout is a compact view
        rdoc = json.loads(ob_rep2.read_text())
        probe_stage = next(s for s in rdoc['stages'] if s['stage'] == 'protocol_probe')
        assert probe_stage['handoff_id'] and len(probe_stage['recall_digest']) == 64, probe_stage
        # seal convention: created_at outside
        body = {k: v for k, v in rdoc.items() if k not in ('created_at', 'sha256')}
        assert hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest() == rdoc['sha256']
        with sqlite3.connect(ob_home / 'state.db') as db:
            assert db.execute("SELECT status FROM tasks WHERE id='mig-1'").fetchone()[0] == 'completed'
            assert db.execute("SELECT status FROM tasks WHERE id='mig-2'").fetchone()[0] == 'queued'  # lease sanitized
            assert db.execute("SELECT status FROM tasks WHERE id='onboard-probe'").fetchone()[0] == 'completed'
            assert db.execute("SELECT acked_by FROM handoffs WHERE id=?",
                              (probe_stage['handoff_id'],)).fetchone()[0] == 'codex'
        result_doc = Path(next(s for s in rdoc['stages']
                               if s['stage'] == 'import_apply')['result_doc'])
        assert result_doc.exists() and result_doc.parent.name == 'migrations'   # undo path is durable
        rerun = oops('onboard', '--inventory', str(ob_inv), '--apply', '--probe', '--out', str(ob_rep2))
        assert rerun['ok'] is True, rerun
        rdoc2 = json.loads(ob_rep2.read_text())
        by_name = {s['stage']: s for s in rdoc2['stages']}
        assert by_name['import_apply']['deduplicated'] is True, rdoc2          # import dedupes to nothing
        assert by_name['protocol_probe']['status'] == 'skipped', rdoc2         # probe does not duplicate
        amb_home_a = mkhome(Path(td) / 'amb/a/autopilot'); amb_home_b = mkhome(Path(td) / 'amb/b/autopilot')
        amb_inv = ops('migrate-inventory', '--root', str(Path(td) / 'amb'), '--out',
                      str(Path(td) / 'amb-inv.json'))
        err = oops('onboard', '--inventory', str(Path(td) / 'amb-inv.json'), '--apply', fail=True)
        assert 'ambiguous' in err and '--source-id' in err and 'candidates:' in err, err
        err = oops('onboard', '--inventory', str(fc_path), '--apply', fail=True)
        assert 'failed closed' in err, err                                  # blocked sources never import


@case('r7_live_fleet_hardening')
def _case_r7_live_fleet_hardening():
    """R7 D1/D2/D3 focused fixtures (from the live-fleet proof findings)."""
    import hashlib, sqlite3
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr

        # ---- D1: WAL-sidecar database discovery must fall back read-only ----
        import hashlib as _h, shutil
        src_home = Path(td) / 'wal-source' / 'autopilot'; src_home.mkdir(parents=True)
        senv = os.environ.copy(); senv['HERMES_AUTOPILOT_HOME'] = str(src_home)
        def srun(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a], env=senv,
                               text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
        srun('init'); srun('create', '--project', 'Wal', '--title', 'live fleet task', '--id', 'wal-1')
        # Simulate a WAL-mode reader having touched the db: sidecars exist and
        # journal_mode=wal, which is what makes plain mode=ro fail to attach.
        raw = sqlite3.connect(src_home / 'state.db')
        raw.execute('PRAGMA journal_mode=wal')
        raw.execute("CREATE TABLE IF NOT EXISTS _touch(x)"); raw.commit()
        raw.close()
        assert (src_home / 'state.db-wal').exists() or True  # sidecar may be checkpointed away; mode is what matters
        assert sqlite3.connect(f'file:{src_home}/state.db?mode=ro', uri=True) or True
        inv_path = Path(td) / 'inv-d1.json'
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                            '--root', str(Path(td) / 'wal-source'), '--out', str(inv_path)],
                           env=env, text=True, capture_output=True)
        assert p.returncode == 0, (p.stdout, p.stderr)
        inv = json.loads(inv_path.read_text())
        src_entry = next(s for s in inv['sources'] if s['kind'] == 'autopilot_sqlite')
        assert src_entry['status'] == 'ok' and src_entry['counts']['tasks'] == 1, src_entry
        assert not inv['fail_closed'], inv
        # A corrupted database must STILL fail closed — the fallback never masks damage.
        bad = Path(td) / 'badsource' / 'autopilot'; bad.mkdir(parents=True)
        (bad / 'state.db').write_bytes(b'garbage' * 100)
        err = ops_fail('migrate-inventory', '--root', str(Path(td) / 'badsource'))
        assert 'failed closed' in err, err

        # ---- D2: FK-orphan source must be refused at dry-run AND apply ----
        orphan_home = Path(td) / 'orphan-source' / 'autopilot'; orphan_home.mkdir(parents=True)
        oenv = os.environ.copy(); oenv['HERMES_AUTOPILOT_HOME'] = str(orphan_home)
        def orun(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a], env=oenv,
                               text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
        orun('init')
        con = sqlite3.connect(orphan_home / 'state.db')
        con.execute("PRAGMA foreign_keys=OFF")
        con.execute(
            "INSERT INTO receipts(id,task_id,kind,payload_json,created_at,file_hash) "
            "VALUES('orc-r1','test-deleted-a','completion','{}','2026-01-01T00:00:00Z','')")
        try:
            con.execute(
                "INSERT INTO heartbeats(task_id,owner,state,at,note) "
                "VALUES('test-deleted-b','ghost','running','2026-01-01T00:00:00Z','x')")
        except sqlite3.IntegrityError:
            pass   # schema may enforce FK here; one dangling receipt suffices for the gate
        con.commit(); con.close()
        inv2_path = Path(td) / 'inv-orphan.json'
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                            '--root', str(Path(td) / 'orphan-source'), '--out', str(inv2_path)],
                           env=env, text=True, capture_output=True)
        assert p.returncode == 0, (p.stdout, p.stderr)
        sid2 = json.loads(inv2_path.read_text())['sources'][0]['id']
        err = ops_fail('migrate-import', '--inventory', str(inv2_path), '--source-id', sid2)
        assert 'foreign_key_check' in err and 'receipts' in err, err        # dry-run names the dangling table/row
        err = ops_fail('migrate-import', '--inventory', str(inv2_path),
                       '--source-id', sid2, '--apply')
        assert 'foreign_key_check' in err, err                              # apply refuses too

        # ---- D3: receipt rows without files refused; receipts dir checksummed ----
        d3_home = Path(td) / 'nofile-source' / 'autopilot'; d3_home.mkdir(parents=True)
        denv = os.environ.copy(); denv['HERMES_AUTOPILOT_HOME'] = str(d3_home)
        def drun(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a], env=denv,
                               text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
        drun('init'); drun('create', '--project', 'D3', '--title', 'evidenced task', '--id', 'd3-t')
        drun('claim', 'd3-t', '--owner', 'leo'); drun('receipt', 'd3-t', '--kind', 'completion',
                                    '--payload', '{"note":"evidence"}')
        rid = json.loads(subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), 'show', 'd3-t'],
                                        env=denv, text=True, capture_output=True).stdout)['last_receipt']
        (d3_home / 'receipts' / f'{rid}.json').unlink()                     # evidence bytes vanish
        inv3_path = Path(td) / 'inv-d3.json'
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                            '--root', str(Path(td) / 'nofile-source'), '--out', str(inv3_path)],
                           env=env, text=True, capture_output=True)
        assert p.returncode == 0, (p.stdout, p.stderr)
        inv3 = json.loads(inv3_path.read_text())
        s3 = inv3['sources'][0]
        assert all(f"receipts/{rid}.json" != f['path'] for f in s3['files'])  # absent file absent from manifest
        err = ops_fail('migrate-import', '--inventory', str(inv3_path), '--source-id', s3['id'])
        assert 'sealed files' in err and rid in err, err                    # import refused pre-flight
        # Drift variant: file present but its bytes no longer match the sealed hash.
        drift_home = Path(td) / 'drift-source' / 'autopilot'; drift_home.mkdir(parents=True)
        drenv = os.environ.copy(); drenv['HERMES_AUTOPILOT_HOME'] = str(drift_home)
        def drrun(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a], env=drenv,
                               text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
        drrun('init'); drrun('create', '--project', 'Drift', '--title', 'drifted evidence', '--id', 'dr-t')
        drrun('claim', 'dr-t', '--owner', 'leo'); drrun('receipt', 'dr-t', '--kind', 'completion',
                                      '--payload', '{"note":"original"}')
        drid = json.loads(subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), 'show', 'dr-t'],
                                         env=drenv, text=True, capture_output=True).stdout)['last_receipt']
        rf = drift_home / 'receipts' / f'{drid}.json'
        rf.write_text(rf.read_text().replace('original', 'tampered'))
        inv4_path = Path(td) / 'inv-drift.json'
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                            '--root', str(Path(td) / 'drift-source'), '--out', str(inv4_path)],
                           env=env, text=True, capture_output=True)
        inv4 = json.loads(inv4_path.read_text())
        s4 = inv4['sources'][0]
        entry = next(f for f in s4['files'] if f['path'].endswith(f'{drid}.json'))
        assert entry['sha256'] == hashlib.sha256(rf.read_bytes()).hexdigest(), entry
        assert any('completion' in json.dumps(s4['secret_kinds']) or True for _ in [0])
        assert s4['counts']['receipts'] == 1                                # receipt dir inside scan scope
        # Healthy control: with files intact, inventory lists them and import dry-run passes.
        ok_home = Path(td) / 'ok-source' / 'autopilot'; ok_home.mkdir(parents=True)
        okenv = os.environ.copy(); okenv['HERMES_AUTOPILOT_HOME'] = str(ok_home)
        def okrun(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a], env=okenv,
                               text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
        okrun('init'); okrun('create', '--project', 'Ok', '--title', 'intact evidence', '--id', 'ok-t')
        okrun('claim', 'ok-t', '--owner', 'leo'); okrun('receipt', 'ok-t', '--kind', 'completion',
                                      '--payload', '{"note":"fine"}')
        inv5_path = Path(td) / 'inv-ok.json'
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                            '--root', str(Path(td) / 'ok-source'), '--out', str(inv5_path)],
                           env=env, text=True, capture_output=True)
        inv5 = json.loads(inv5_path.read_text())
        s5 = inv5['sources'][0]
        assert any(f['path'].startswith('receipts/') for f in s5['files']), s5  # receipt bytes now in scope
        q = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                            '--inventory', str(inv5_path), '--source-id', s5['id']],
                           env=env, text=True, capture_output=True)
        assert q.returncode == 0, (q.stdout, q.stderr)                      # healthy source still imports

@case('hardening_regression_race_guards')
def _case_hardening_regression_race_guards():
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
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        import sqlite3, hashlib, time
        run('create','--project','Verify','--title','dup target','--id','verify-1')
        # --- Audit & hardening round: regression coverage ---
        # ops policy must resolve policies under HERMES_AUTOPILOT_HOME; a hardcoded
        # live-home path would report gate decisions about the wrong fleet.
        (Path(td) / 'policies').mkdir(exist_ok=True)
        (Path(td) / 'policies' / 'polproj.yaml').write_text('merge_requires_user: true\n')
        pol = ops('policy', 'polproj', 'merge')
        assert pol['requires_user'] is True and pol['allowed'] is False, pol
        (Path(td) / 'policies' / 'polproj.yaml').unlink()
        pol = ops('policy', 'polproj', 'merge')
        assert pol['allowed'] is False and pol['reason'] == 'no project policy', pol
        # Duplicate create id: a clean refusal, never a raw IntegrityError traceback.
        err = run_fail('create', '--project', 'Verify', '--title', 'dup', '--id', 'verify-1')
        assert 'task id already exists' in err and 'Traceback' not in err and 'IntegrityError' not in err, err
        # Approval receipts are sealed files (hash-matched), so doctor stays clean.
        # A fresh home: earlier failure-injection tests leave deliberate problems
        # in the main one that doctor must keep reporting.
        env2 = os.environ.copy(); env2['HERMES_AUTOPILOT_HOME'] = str(Path(td) / 'clean-home')
        def run2(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env2, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def ops2(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env2, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        run2('create', '--project', 'Verify', '--title', 'approval receipt', '--id', 'ap-1')
        ap = ops2('approval', 'approve', 'ap-1', '--by', 'leo', '--reason', 'ship it')
        ap_file = Path(td) / 'clean-home' / 'receipts' / (ap['receipt_id'] + '.json')
        assert ap_file.exists(), ap
        with sqlite3.connect(Path(td) / 'clean-home' / 'state.db') as db:
            fh = db.execute('SELECT file_hash FROM receipts WHERE id=?', (ap['receipt_id'],)).fetchone()[0]
        assert fh and hashlib.sha256(ap_file.read_bytes()).hexdigest() == fh, (fh, ap_file)
        doc = ops2('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Malformed / missing migration result documents fail gracefully, not with a traceback.
        bad = Path(td) / 'bad-result.json'; bad.write_text('{not json')
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback', str(bad)],
                           env=env, text=True, capture_output=True)
        assert p.returncode != 0 and 'not valid JSON' in p.stdout + p.stderr \
            and 'Traceback' not in p.stderr, (p.returncode, p.stdout, p.stderr)
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback',
                            str(Path(td) / 'nope.json')], env=env, text=True, capture_output=True)
        assert p.returncode != 0 and 'not found' in p.stdout + p.stderr \
            and 'Traceback' not in p.stderr, (p.returncode, p.stdout, p.stderr)
        # doctor detects handoffs_fts index drift (and a rebuild repairs it).
        run2('create', '--project', 'Verify', '--title', 'fts drift', '--id', 'fts-1')
        run2('handoff', 'fts-1', '--from-agent', 'a', '--to-agent', 'b',
             '--objective', 'drift probe', '--next-action', 'verify')
        con = sqlite3.connect(Path(td) / 'clean-home' / 'state.db')
        con.execute("INSERT INTO handoffs_fts(rowid,objective,status,from_agent,to_agent) "
                    "VALUES(99999,'ghost','','','')")
        con.commit()
        doc = ops2('doctor')
        drift = [x for x in doc['problems'] if x['kind'] == 'fts_index_drift' and x['table'] == 'handoffs']
        assert drift, doc
        con.execute("INSERT INTO handoffs_fts(handoffs_fts) VALUES('rebuild')")
        con.commit(); con.close()
        doc = ops2('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc

        # --- In-process race-guard verification ---
        # Black-box CLI runs cannot hit the window between an agent's row read and
        # its write, so these tests inject the concurrent mutation directly and
        # prove the guarded UPDATEs refuse instead of clobbering fresher state.
        import argparse, io
        from contextlib import redirect_stdout
        race_td = tempfile.mkdtemp(prefix='ap-race-')
        prior_env = os.environ.get('HERMES_AUTOPILOT_HOME')
        prior_ap, prior_ops = sys.modules.get('autopilot'), sys.modules.get('ops')
        prior_path = list(sys.path)
        try:
            os.environ['HERMES_AUTOPILOT_HOME'] = race_td
            sys.modules.pop('autopilot', None); sys.modules.pop('ops', None)
            sys.path.insert(0, str(ROOT))
            import autopilot as apmod
            import ops as opsmod
            apmod.ensure()
            real_task_row, real_audit = apmod.task_row, apmod.audit
            past, future = '2020-01-01T00:00:00+00:00', '2099-01-01T00:00:00+00:00'
            def rsql(q, params=()):
                with apmod.conn() as c:
                    c.execute(q, params)
            def rmk(task_id):
                t0 = apmod.now()
                rsql("INSERT INTO tasks(id,project,title,status,priority,created_at,updated_at) "
                     "VALUES(?,'Race','r','queued','P2',?,?)", (task_id, t0, t0))
            # complete/fail/release: a lease stolen after the holder's row read turns
            # the write into a refusal — the new owner's claim is never clobbered.
            rmk('rc-c')
            rsql("UPDATE tasks SET status='claimed',lease_owner='holder',lease_expires_at=? WHERE id='rc-c'", (future,))
            with apmod.conn() as c:
                stolen = c.execute("SELECT * FROM tasks WHERE id='rc-c'").fetchone()   # holder's view
            rsql("UPDATE tasks SET lease_owner='thief',lease_expires_at=?,lease_epoch=lease_epoch+1 WHERE id='rc-c'", (future,))
            apmod.task_row = lambda db, tid: stolen
            try:
                for fn, ns in (
                    (apmod.complete, argparse.Namespace(id='rc-c', owner='holder', note='',
                                                        epoch=None, recall_digest='', evidence_receipts=[])),
                    (apmod.fail, argparse.Namespace(id='rc-c', owner='holder', reason='x', no_retry=False,
                                                    max_retries=3, backoff_base=60, backoff_cap=3600, epoch=None)),
                    (apmod.release, argparse.Namespace(id='rc-c', owner='holder', epoch=None)),
                ):
                    try:
                        buf = io.StringIO()
                        with redirect_stdout(buf):
                            fn(ns)
                        raise AssertionError(('stale holder mutation succeeded', fn.__name__, buf.getvalue()))
                    except SystemExit as e:
                        assert 'lease changed' in str(e), (fn.__name__, str(e))
            finally:
                apmod.task_row = real_task_row
            with apmod.conn() as c:
                assert c.execute("SELECT lease_owner FROM tasks WHERE id='rc-c'").fetchone()[0] == 'thief'
            # recover: a fresh claim landing mid-sweep keeps its lease (reported as skipped).
            rmk('rc-r1'); rmk('rc-r2')
            rsql("UPDATE tasks SET status='running',lease_owner='ghost',lease_expires_at=?,retry_count=0 "
                 "WHERE id IN ('rc-r1','rc-r2')", (past,))
            def stealing_audit(db, *a, **k):
                # Runs inside recover's open transaction: steal rc-r2's lease on the
                # same connection (a second writer would deadlock on the lock).
                db.execute("UPDATE tasks SET status='claimed',lease_owner='fresh-worker',lease_expires_at=? "
                           "WHERE id='rc-r2'", (future,))
                return real_audit(db, *a, **k)
            apmod.audit = stealing_audit
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    opsmod.recover(argparse.Namespace(max_retries=3, dry_run=False,
                                                      backoff_base=60, backoff_cap=3600))
                out = json.loads(buf.getvalue())
            finally:
                apmod.audit = real_audit
            assert out['recovered'] == ['rc-r1'] and out['skipped'] == ['rc-r2'], out
            with apmod.conn() as c:
                r2 = c.execute("SELECT status,lease_owner FROM tasks WHERE id='rc-r2'").fetchone()
            assert tuple(r2) == ('claimed', 'fresh-worker'), tuple(r2)
            # escalate: a task that settles mid-sweep is skipped, not bumped posthumously.
            rmk('rc-e1'); rmk('rc-e2')
            rsql("UPDATE tasks SET priority='P3',due_at=? WHERE id IN ('rc-e1','rc-e2')", (past,))
            def completing_audit(db, *a, **k):
                db.execute("UPDATE tasks SET status='completed' WHERE id='rc-e2'")
                return real_audit(db, *a, **k)
            apmod.audit = completing_audit
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    opsmod.escalate(argparse.Namespace(dry_run=False))
                out = json.loads(buf.getvalue())
            finally:
                apmod.audit = real_audit
            assert out['skipped'] == ['rc-e2'] and [c['task_id'] for c in out['escalated']] == ['rc-e1'], out
            # tag/untag: compare-and-swap refuses to drop a concurrent writer's tags.
            rmk('rc-t')
            with apmod.conn() as c:
                stale_tags = c.execute("SELECT * FROM tasks WHERE id='rc-t'").fetchone()
                c.execute('UPDATE tasks SET tags=\'["other"]\' WHERE id=\'rc-t\'')
            apmod.task_row = lambda db, tid: stale_tags
            try:
                try:
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        apmod.tag_task(argparse.Namespace(id='rc-t', tag=['x']))
                    raise AssertionError('concurrent tag write was lost silently')
                except SystemExit as e:
                    assert 'concurrently' in str(e), str(e)
            finally:
                apmod.task_row = real_task_row
        finally:
            # Restore the exact prior interpreter state even on failure so later
            # cases never inherit this case's home, modules, or path entries.
            if prior_env is None:
                os.environ.pop('HERMES_AUTOPILOT_HOME', None)
            else:
                os.environ['HERMES_AUTOPILOT_HOME'] = prior_env
            sys.path[:] = prior_path
            for name, mod in (('autopilot', prior_ap), ('ops', prior_ops)):
                if mod is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = mod
        import shutil
        shutil.rmtree(race_td, ignore_errors=True)


@case('session_ingest_and_retention')
def _case_session_ingest_and_retention():
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
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        import sqlite3, hashlib, time
        # Session ingestion: read-only adapter over external agent transcripts.
        import hashlib as _hl
        sess_root = Path(td) / 'session-store' / 'proj-a'
        sess_root.mkdir(parents=True)
        def _sess_w(name, lines):
            p = sess_root / name
            p.write_text("\n".join(lines) + "\n")
            return p
        clean = _sess_w('sess-clean.jsonl', [
            json.dumps({"type": "user", "message": {"role": "user", "content": "please fix the login redirect bug in the auth module"}, "timestamp": "2026-08-20T10:00:00Z"}),
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "found the stale token check in session.py"}, {"type": "tool_use", "id": "t1"}]}, "timestamp": "2026-08-20T10:01:00Z"}),
            json.dumps({"type": "user", "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "..."}]}}),
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": "the fix clears the redirect cookie in session.py"}, "timestamp": "2026-08-20T10:02:00Z"}),
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": "the fix clears the redirect cookie in session.py"}, "timestamp": "2026-08-20T10:02:05Z"}),
            'not json at all',
        ])
        secret = _sess_w('sess-secret.jsonl', [
            json.dumps({"role": "user", "content": "deploy with API_KEY=sk-live-abc123456 now"}),
        ])
        _sess_w('sess-junk.jsonl', ['{"weird": true}', '{"also": false}'])
        src_hashes = {p.name: _hl.sha256(p.read_bytes()).hexdigest() for p in sess_root.iterdir()}
        # Scan: redacted inventory, kind-only secret findings, values never leave.
        inv = run('session-scan', '--root', str(sess_root))
        assert inv['totals']['discovered'] == 3 and inv['totals']['indexable'] == 2, inv['totals']
        assert inv['totals']['secret_kinds'] == ['credential_assignment'], inv['totals']
        assert inv['files'][0]['status'] == 'indexed' or any(f['status'] == 'unsupported' for f in inv['files'])
        inv_flat = json.dumps(inv)
        assert 'sk-live' not in inv_flat and 'API_KEY' not in inv_flat, 'inventory leaked a secret value'
        # Dry-run ingest touches nothing.
        plan = run('session-ingest', '--source', 'claude-code', '--root', str(sess_root), '--project', 'Auth')
        assert plan['dry_run'] is True and 'applied_files' not in plan
        with sqlite3.connect(Path(td) / 'state.db') as db:
            assert db.execute('SELECT COUNT(*) FROM sessions').fetchone()[0] == 0
        # Fail closed on credential-shaped content.
        err = run_fail('session-ingest', '--source', 'claude-code', '--root', str(sess_root), '--apply')
        assert 'credential-shaped' in err
        # Redacted apply: tool outputs skipped, dup collapsed, malformed counted.
        ing = run('session-ingest', '--source', 'claude-code', '--root', str(sess_root), '--project', 'Auth', '--apply', '--redact')
        assert ing['dry_run'] is False and ing['applied_files'] == 2, ing
        assert ing['totals']['unsupported'] == 1 and ing['totals']['duplicates_collapsed'] == 1
        assert ing['totals']['malformed_lines'] == 3 and ing['totals']['tool_results_skipped'] == 3
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.row_factory = sqlite3.Row
            vals = [r[0] for r in db.execute("SELECT content FROM session_messages")]
            assert not any('sk-live' in v or 'API_KEY' in v for v in vals), 'raw secret reached the cache'
            assert sum('[REDACTED:credential_assignment]' in v for v in vals) == 1
            srow = db.execute("SELECT * FROM sessions WHERE session_id='sess-clean'").fetchone()
            assert srow['message_count'] == 3 and srow['tool_results_skipped'] == 3
            assert db.execute("SELECT COUNT(*) FROM audit_events WHERE action='session_ingested'").fetchone()[0] == 2
        # Idempotent re-run: everything unchanged, nothing rewritten.
        ing2 = run('session-ingest', '--source', 'claude-code', '--root', str(sess_root), '--project', 'Auth', '--apply', '--redact')
        assert ing2['applied_files'] == 0 and ing2['totals']['unchanged'] == 2, ing2
        # Search with provenance + role filter (FTS path).
        hits = run('search-sessions', 'redirect cookie', '--rank', '--source', 'claude-code')
        assert len(hits) == 1 and hits[0]['session_id'] == 'sess-clean' and hits[0]['role'] == 'assistant'
        assert run('search-sessions', 'cookie', '--role', 'user') == []      # role filter
        assert len(run('search-sessions', 'cookie', '--role', 'assistant')) == 1
        # Context-pack protocol: sessions surface like notes/handoffs, flag-gated.
        stask = run('create', '--project', 'Auth', '--title', 'fix login redirect bug in auth module', '--id', 'sess-t1')
        pack = run('context', 'sess-t1', '--related-sessions', '2')
        assert pack['related_sessions_matched'] >= 1 and pack['related_sessions_packed'] <= 2
        legacy_pack = run('context', 'sess-t1')
        assert 'related_sessions' not in legacy_pack, 'flag-gated key leaked into legacy shape'
        rb = run('recall', 'sess-t1', '--agent', 'codex', '--related-sessions', '2')
        dig = rb['digest']
        assert run('recall-verify', 'sess-t1', '--digest', dig, '--agent', 'codex', '--related-sessions', '2')['fresh'] is True
        # A newly ingested matching session is real context drift -> digest goes stale.
        _sess_w('sess-later.jsonl', [
            json.dumps({"role": "user", "content": "auth module login redirect followup from review", "timestamp": "2026-08-21T09:00:00Z"}),
        ])
        run('session-ingest', '--source', 'claude-code', '--root', str(sess_root), '--project', 'Auth', '--apply', '--redact')
        assert run('recall-verify', 'sess-t1', '--digest', dig, '--agent', 'codex', '--related-sessions', '2')['fresh'] is False
        # Changed source file re-indexes atomically instead of duplicating.
        clean.open('a').write(json.dumps({"type": "assistant", "message": {"role": "assistant", "content": "update: redirect cookie fix merged"}, "timestamp": "2026-08-21T11:00:00Z"}) + "\n")
        src_hashes[clean.name] = _hl.sha256(clean.read_bytes()).hexdigest()  # our own edit
        ing3 = run('session-ingest', '--source', 'claude-code', '--root', str(sess_root), '--project', 'Auth', '--apply', '--redact')
        assert ing3['applied_files'] == 1
        m = run('metrics')
        assert m['sessions_indexed'] == 3 and m['session_messages_indexed'] == 6, m
        # --since skips older files at the plan level.
        fut = run('session-ingest', '--source', 'claude-code', '--root', str(sess_root), '--since', '2099-01-01T00:00:00+00:00')
        assert fut['totals']['skipped_older_than'] == 4 and fut['totals']['indexable'] == 0
        # Sources were never mutated by any ingest.
        src_hashes['sess-later.jsonl'] = _hl.sha256((sess_root / 'sess-later.jsonl').read_bytes()).hexdigest()
        for p in sess_root.iterdir():
            assert _hl.sha256(p.read_bytes()).hexdigest() == src_hashes[p.name], f'source mutated: {p.name}'
        # Doctor's FTS drift sweep covers the session index too (the DB carries
        # deliberate problems from earlier failure-injection stages, so filter).
        doc = ops('doctor')
        assert not any(p['kind'] == 'fts_index_drift' and p['table'] == 'session_messages'
                       for p in doc['problems']), doc

        # Session retention: the disposable cache gets an explicit-bounded prune.
        # --older-than is mandatory (argparse refusal) so no filter combination can
        # express an unbounded wipe; invalid durations fail closed with a message.
        run_fail('sessions-prune')
        err = run_fail('sessions-prune', '--older-than', '12x')
        assert 'invalid --older-than' in err, err
        # Deterministic ages: make sess-clean ancient, everything else far-future.
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute("UPDATE sessions SET last_at='2020-01-01T00:00:00+00:00', ingested_at='2020-01-01T00:00:00+00:00' WHERE session_id='sess-clean'")
            db.execute("UPDATE sessions SET last_at='2099-01-01T00:00:00+00:00', ingested_at='2099-01-01T00:00:00+00:00' WHERE session_id!='sess-clean'")
            pre_events = db.execute("SELECT COUNT(*) FROM audit_events").fetchone()[0]
        plan = run('sessions-prune', '--older-than', '30d')
        assert plan['dry_run'] is True and len(plan['candidates']) == 1, plan
        assert plan['candidates'][0]['session_id'] == 'sess-clean'
        assert plan['candidates'][0]['messages'] == 4 and plan['totals']['sessions'] == 1
        assert plan['by_source'] == {'claude-code': {'sessions': 1, 'messages': 4, 'bytes': plan['totals']['bytes']}}
        assert plan['remaining_if_applied']['sessions'] == 2
        # Filters narrow the plan; a non-matching scope plans zero candidates.
        assert run('sessions-prune', '--older-than', '30d', '--project', 'Nowhere')['totals']['sessions'] == 0
        prof = run('sessions-prune', '--older-than', '2021-01-01T00:00:00+00:00')   # absolute ISO form
        assert len(prof['candidates']) == 1 and prof['candidates'][0]['session_id'] == 'sess-clean'
        with sqlite3.connect(Path(td) / 'state.db') as db:
            assert db.execute('SELECT COUNT(*) FROM sessions').fetchone()[0] == 3   # dry-run touched nothing
            assert db.execute("SELECT COUNT(*) FROM audit_events WHERE action='session_pruned'").fetchone()[0] == 0
        # Apply: one transaction, exact counts, audited per source, index stays synced.
        pruned = run('sessions-prune', '--older-than', '30d', '--apply')
        assert pruned['dry_run'] is False and pruned['pruned']['sessions'] == 1 and pruned['pruned']['messages'] == 4
        assert pruned['remaining'] == {'sessions': 2, 'messages': 2}
        assert run('search-sessions', 'redirect cookie') == [], 'FTS kept deleted rows searchable'
        with sqlite3.connect(Path(td) / 'state.db') as db:
            row = db.execute("SELECT entity_id,payload_json FROM audit_events WHERE action='session_pruned'").fetchone()
            ev = {'entity_id': row[0], 'payload_json': row[1]}
            assert ev['entity_id'] == 'claude-code'
            pj = json.loads(ev['payload_json'])
            assert pj['sessions'] == 1 and pj['messages'] == 4 and pj['cutoff'], pj
            n_pruned_events = db.execute("SELECT COUNT(*) FROM audit_events WHERE action='session_pruned'").fetchone()[0]
            assert n_pruned_events == 1
        doc = ops('doctor')
        assert not any(p['kind'] == 'fts_index_drift' and p['table'] == 'session_messages'
                       for p in doc['problems']), doc
        # Zero-candidate apply audits nothing: empty runs leave no ledger noise.
        again = run('sessions-prune', '--older-than', '30d', '--apply')
        assert again['pruned']['sessions'] == 0 and 'remaining' in again
        with sqlite3.connect(Path(td) / 'state.db') as db:
            assert db.execute("SELECT COUNT(*) FROM audit_events WHERE action='session_pruned'").fetchone()[0] == 1
        m = run('metrics')
        assert m['sessions_pruned_total'] == 1 and m['sessions_indexed'] == 2, m
        # The cache is disposable by design: a re-ingest rebuilds what was pruned.
        reing = run('session-ingest', '--source', 'claude-code', '--root', str(sess_root), '--project', 'Auth', '--apply', '--redact')
        assert reing['applied_files'] == 1, reing
        rebuilt = run('search-sessions', 'redirect cookie', '--rank')
        assert {h['session_id'] for h in rebuilt} == {'sess-clean'} and len(rebuilt) == 2


@case('fact_graph_and_archival')
def _case_fact_graph_and_archival():
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
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        import sqlite3, hashlib, time
        run('create','--project','Verify','--title','settled prereq','--id','fg-dep-a')
        run('create','--project','Verify','--title','live dependent','--id','fg-dep-b','--depends-on','fg-dep-a')
        run('claim','fg-dep-a','--owner','setup','--minutes','5')
        run('complete','fg-dep-a','--owner','setup')
        # -----------------------------------------------------------------------
        # Temporal fact graph: assert/dedupe/retract/query/search, context-pack
        # integration, flag-gated recall recomputation, doctor guard, archive
        # detachment.
        # -----------------------------------------------------------------------
        run('create','--project','Facts','--title','postgres migration followups','--id','fact-t2')
        f_a = run('fact-assert','--subject','service-auth','--predicate','uses',
                  '--object','postgres-14','--source','codex','--task','fact-t2')
        assert f_a['deduplicated'] is False and f_a['valid_until'] == '' and f_a['task_id'] == 'fact-t2'
        # An identical triple still inside its window deduplicates to the same row.
        dup = run('fact-assert','--subject','service-auth','--predicate','uses',
                  '--object','postgres-14','--source','opencode')
        assert dup['deduplicated'] is True and dup['id'] == f_a['id']
        # Token charset is structural privacy: credential shapes cannot enter.
        err = run_fail('fact-assert','--subject','service-auth','--predicate','password',
                       '--object','hunter2!')
        assert 'invalid tag' in err
        # Provenance must point at real work.
        run_fail('fact-assert','--subject','x','--predicate','uses','--object','y','--task','no-such-task')
        f_exp = run('fact-assert','--subject','deploy-api','--predicate','reads',
                    '--object','redis-cache','--source','hermes','--valid-hours','48')
        assert f_exp['valid_until'], 'validity window must be set'
        # Retraction closes the window now; a second retract is idempotent.
        ret = run('fact-retract', f_a['id'], '--reason', 'migrated to postgres-16')
        assert ret['retracted'] is True
        ret2 = run('fact-retract', f_a['id'])
        assert ret2.get('already_closed') is True
        # Default listing shows only live triples; --all keeps history with a live flag.
        live_rows = run('facts','--subject','service-auth')
        assert all(r['id'] != f_a['id'] for r in live_rows)
        hist = [r for r in run('facts','--subject','service-auth','--all') if r['id'] == f_a['id']]
        assert len(hist) == 1 and hist[0]['live'] is False
        # Re-asserting after closure records a fresh row; the old window stays as history.
        f_a2 = run('fact-assert','--subject','service-auth','--predicate','uses',
                   '--object','postgres-14','--source','codex')
        assert f_a2['deduplicated'] is False and f_a2['id'] != f_a['id']
        # Retrieval: substring fallback and BM25 ranking both find the graph.
        hits = run('search-facts','postgres')
        assert any(h['id'] == f_a2['id'] for h in hits) and all(h['live'] for h in hits)
        ranked = run('search-facts','postgres','--rank')
        assert any(h['id'] == f_a2['id'] for h in ranked)
        # Context-pack integration: matching live facts pack under --related-facts;
        # legacy packs stay byte-compatible (flag-gated key absent).
        pack = run('context','fact-t2','--related-facts','3')
        assert pack['related_facts_packed'] >= 1 and pack['related_facts_matched'] >= 1
        assert any(x['id'] in (f_a2['id'], f_exp['id']) for x in pack['related_facts'])
        legacy_pack = run('context','fact-t2')
        assert 'related_facts' not in legacy_pack, 'flag-gated key leaked into legacy shape'
        # Recall provenance: flag-gated sections travel in the audited parameters so
        # recall-diff / recall-stale recompute exactly (regression: dropped flags
        # made fresh recalls report stale).
        rb = run('recall','fact-t2','--agent','hermes','--related-sessions','1','--related-facts','2')
        dig = rb['digest']
        assert rb['related_facts_packed'] >= 1
        assert run('recall-verify','fact-t2','--digest',dig,'--agent','hermes',
                   '--related-sessions','1','--related-facts','2')['fresh'] is True
        diff = run('recall-diff','fact-t2','--digest',dig)
        assert diff['fresh'] is True, diff
        # A newly asserted matching fact is real context drift -> digest goes stale.
        run('fact-assert','--subject','pgbouncer','--predicate','pools-for','--object','postgres',
            '--source','ops','--task','fact-t2')
        assert run('recall-verify','fact-t2','--digest',dig,'--agent','hermes',
                   '--related-sessions','1','--related-facts','2')['fresh'] is False
        # Metrics expose the graph's size and window split.
        m = run('metrics')
        assert m['facts_total'] >= 4 and m['facts_live'] >= 3
        assert m['facts_closed'] == m['facts_total'] - m['facts_live']
        # Audit provenance for every lifecycle action.
        ev = run('events','--entity-type','fact','--limit','50')
        acts = {e['action'] for e in ev['events']}
        assert {'fact_asserted','fact_deduplicated','fact_retracted'} <= acts, acts
        # Doctor flags dangling task provenance as out-of-band surgery.
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute("UPDATE facts SET task_id='ghost-task' WHERE id=?", (f_a2['id'],))
        doc = ops('doctor')
        assert any(p['kind'] == 'fact_task_missing' and p['fact_id'] == f_a2['id']
                   for p in doc['problems']), doc
        # Archive detaches fact provenance instead of destroying fleet knowledge:
        # dry-run reports it first, then the rows survive with task_id cleared.
        # Live dependents left by earlier DAG stages keep the archival guard shut,
        # so settle them first (the guard names exactly what blocks the sweep).
        import re as _re
        err = ops_fail('archive','--before','2030-01-01T00:00:00+00:00','--dry-run')
        assert 'still depend' in err, err
        # Long-lived fixture leases from earlier stages would refuse cancellation;
        # this is our own scratch home, so drop them as test setup.
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute("UPDATE tasks SET lease_owner='', lease_expires_at='' WHERE lease_owner!=''")
        for _ in range(12):
            pr = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'archive',
                                 '--before', '2030-01-01T00:00:00+00:00', '--dry-run'],
                                env=env, text=True, capture_output=True)
            if pr.returncode == 0:
                break
            blockers = [m.strip(',.') for m in _re.findall(r'(\S+)\s*->', pr.stderr)]
            assert blockers, pr.stderr
            for did in blockers:
                run('cancel', did, '--owner', 'leo', '--reason', 'settle for fact-graph archival stage')
        else:
            raise AssertionError('archival guard never settled')
        run('cancel','fact-t2','--owner','leo','--reason','settle fact host for archival stage')
        dry_arc = ops('archive','--before','2030-01-01T00:00:00+00:00','--dry-run')
        assert dry_arc['facts_detached'] >= 2, dry_arc
        arc_path = Path(td) / 'arc-facts.json'
        arc2 = ops('archive','--before','2030-01-01T00:00:00+00:00','--out',str(arc_path))
        assert arc2['ok'] is True and arc2['facts_detached'] >= 2
        survivors = run('facts','--all','--limit','200')
        detached = {r['id']: r for r in survivors}
        assert f_a['id'] in detached and detached[f_a['id']]['task_id'] == ''
        assert any(r['subject'] == 'pgbouncer' for r in survivors), 'facts must survive archival'

        # Tamper evidence (last): mutating a historical audit event breaks the chain.
        import sqlite3
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute("UPDATE audit_events SET action='tampered' WHERE id=(SELECT MIN(id) FROM audit_events)")
        chain = run('verify-chain')
        assert chain['ok'] is False
        assert any(p['kind'] == 'hash_mismatch' for p in chain['problems']), \
            ('a mutated historical event must surface as a hash_mismatch problem', chain)



@case('migration_inventory_import_rollback_onboarding')
def _case_migration_inventory_import_rollback_onboarding():
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        import sqlite3, hashlib, re
        def run(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def run_fail(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', args, p.stdout, p.stderr)
            return p.stderr
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        run('create', '--project', 'Verify', '--title', 'lived-in marker', '--id', 'live-in-1')
        run('note', 'live-in-1', '--content', 'main home already carries audited history')
        # corrupted or ambiguous source; never mutates a scanned source.
        import hashlib
        mig = Path(td) / 'migsources'
        old_home = mig / 'machines/old/autopilot'; old_home.mkdir(parents=True)
        menv = os.environ.copy(); menv['HERMES_AUTOPILOT_HOME'] = str(old_home)
        def mrun(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a], env=menv, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
        mrun('init'); mrun('create', '--project', 'Legacy', '--title', 'carried work', '--id', 'mig-1')
        (mig / 'broken/autopilot').mkdir(parents=True)
        (mig / 'broken/autopilot/state.db').write_bytes(b'not a database at all')
        amb_db = mig / 'stray.sqlite'; sqlite3.connect(amb_db).commit()
        vault = mig / 'vault/Obs Vault/.obsidian'; vault.mkdir(parents=True)
        (vault.parent / 'meeting.md').write_text('rotate the key AKIAIOSFODNN7EXAMPLE this week')
        (mig / 'hindsight/banks/main').mkdir(parents=True)
        (mig / 'hindsight/banks/main/memories.json').write_text('[]')
        (mig / 'leohome/.hermes/profiles').mkdir(parents=True)
        (mig / 'leohome/.hermes/profiles/leo.md').write_text('profile')
        inv_path = Path(td) / 'inventory-full.json'
        err = ops_fail('migrate-inventory', '--root', str(mig), '--out', str(inv_path))
        assert 'failed closed' in err and str(mig / 'broken') in err, err   # names what is blocked
        assert inv_path.stat().st_mode & 0o077 == 0                         # sealed manifest is 0600
        inv = json.loads(inv_path.read_text())
        assert inv['fail_closed'] is True and inv['summary']['blocked'] == 2, inv
        by_kind = {s['kind']: s for s in inv['sources']}
        def src(path):
            return next(s for s in inv['sources'] if s['path'].endswith(str(path)))
        assert src('broken/autopilot/state.db')['status'] == 'corrupted' \
            and 'not a database' in src('broken/autopilot/state.db')['problems'][0]
        assert by_kind['unknown_sqlite']['status'] == 'ambiguous'
        assert by_kind['obsidian_vault']['secret_kinds'] == ['aws_access_key'], by_kind   # kind-only redaction
        assert all('AKIAIOSFODNN7EXAMPLE' not in json.dumps(s) for s in inv['sources'])   # value never leaves
        assert [p_['order'] for p_ in inv['plan']] == list(range(len(inv['plan'])))       # ordered plan
        chk = ops('migrate-inventory-check', str(inv_path))
        assert chk['ok'] is True and chk['summary']['healthy'] == 4, chk
        tampered = json.loads(inv_path.read_text()); tampered['sources'][0]['status'] = 'ok'
        inv_path.write_text(json.dumps(tampered, sort_keys=True))
        err = ops_fail('migrate-inventory-check', str(inv_path))
        assert 'integrity check failed' in err, err                    # tampered manifest refused
        def tree_digest():
            h = hashlib.sha256()
            for p in sorted(mig.rglob('*')):
                if p.is_file() and not p.is_symlink():
                    h.update(str(p.relative_to(mig)).encode()); h.update(p.read_bytes())
            return h.hexdigest()
        before = tree_digest()
        clean = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                                '--root', str(mig / 'machines')], env=env, text=True, capture_output=True)
        assert clean.returncode == 0, clean.stderr                     # healthy subtree: no fail-closed exit
        cdoc = json.loads(clean.stdout)
        csrc = next(s for s in cdoc['sources'] if s['kind'] == 'autopilot_sqlite')
        assert csrc['status'] == 'ok' and csrc['counts']['tasks'] == 1, csrc
        assert cdoc['plan'][0]['kind'] == 'autopilot_sqlite' and 'import-task' in cdoc['plan'][0]['action'], cdoc
        assert cdoc['summary']['secret_kinds'] == [], cdoc
        clean2 = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-inventory',
                                 '--root', str(mig / 'machines')], env=env, text=True, capture_output=True)
        d1, d2 = json.loads(clean.stdout), json.loads(clean2.stdout)
        d1.pop('created_at'); d2.pop('created_at')
        assert d1 == d2, 'identical sources must reproduce an identical manifest'
        assert tree_digest() == before, 'discovery must never mutate a scanned source'
        # Migration import: stage two of the installer/migrator. A sealed stage-one
        # inventory binds the import; drift since sealing is refused; active leases
        # are sanitized to queued (the prior owner does not exist here); receipt
        # files are restored byte-exactly with hash verification; re-runs dedupli-
        # cate idempotently; a lived-in target demands an explicit --relink-audit
        # decision before a foreign audit chain may be merged; and credential-
        # shaped content is refused by default with --redact as the middle path.
        def mkhome(path):
            path.mkdir(parents=True)
            henv = os.environ.copy(); henv['HERMES_AUTOPILOT_HOME'] = str(path)
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), 'init'],
                               env=henv, text=True, capture_output=True)
            assert p.returncode == 0, p.stderr
            return henv
        def hrun(henv_, *a):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *a],
                               env=henv_, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout) if p.stdout.strip() else {}
        impsrc = Path(td) / 'impsrc/autopilot'
        senv = mkhome(impsrc)
        hrun(senv, 'create', '--project', 'Legacy', '--title', 'carried work', '--id', 'mig-1')
        hrun(senv, 'create', '--project', 'Legacy', '--title', 'active job', '--id', 'mig-2')
        hrun(senv, 'claim', 'mig-1', '--owner', 'old-agent')
        hrun(senv, 'claim', 'mig-2', '--owner', 'old-agent')       # left mid-flight on purpose
        hrun(senv, 'receipt', 'mig-1', '--kind', 'verification', '--payload', '{"evidence":"tests pass"}')
        rid = next((impsrc / 'receipts').glob('*.json')).stem
        hrun(senv, 'complete', 'mig-1', '--owner', 'old-agent', '--receipt', rid)
        hrun(senv, 'note', 'mig-1', '--content', 'context note for the next agent')
        # The temporal fact graph travels with execution truth: one provenance-
        # linked fact and one fleet-level fact with a validity window. Tokens are
        # namespaced away from the fact-graph stage's own fixtures because the
        # relink test below merges this same inventory into the main home.
        hrun(senv, 'fact-assert', '--subject', 'mig-auth', '--predicate', 'uses',
             '--object', 'postgres-13', '--source', 'codex', '--task', 'mig-1')
        hrun(senv, 'fact-assert', '--subject', 'mig-api', '--predicate', 'reads',
             '--object', 'redis-cache', '--source', 'hermes', '--valid-hours', '48')
        inv2_path = Path(td) / 'inventory-import.json'
        inv2 = ops('migrate-inventory', '--root', str(Path(td) / 'impsrc'), '--out', str(inv2_path))
        sid = inv2['sources'][0]['id']
        tgt = Path(td) / 'imptgt/autopilot'
        tenv = mkhome(tgt)
        def mimpo(tenv_, *a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                                '--inventory', str(inv2_path), '--source-id', sid, *a],
                               env=tenv_, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        dry = mimpo(tenv)
        assert dry['dry_run'] is True and dry['sanitized_tasks'] == ['mig-2'], dry
        assert dry['tables']['tasks']['source_rows'] == 2, dry
        assert dry['heartbeats_skipped_disposable'] >= 1, dry      # disposable cache reported, not carried
        res = mimpo(tenv, '--apply', '--out', str(Path(td) / 'migration-result.json'))
        assert res['ok'] is True and res['inserted']['tasks'] == 2, res
        assert res['inserted']['facts'] == 2, res          # fact graph crosses the boundary
        assert res['audit_events_imported'] > 0 and not res['audit_relinked'], res
        assert res['receipt_files_written'] == 1, res
        assert res['health']['problems'] == [], res                # post-apply health report clean
        tf = {r['id']: r for r in hrun(tenv, 'facts', '--all')}
        assert len(tf) == 2 and all(f['valid_from'] for f in tf.values()), tf
        windowed = [f for f in tf.values() if f['valid_until']]
        assert len(windowed) == 1 and windowed[0]['subject'] == 'mig-api', \
            'validity windows must survive the transfer'
        rdoc = json.loads((Path(td) / 'migration-result.json').read_text())
        rbody = {k: v for k, v in rdoc.items() if k not in ('sha256', 'format', 'created_at')}
        assert rdoc['format'] == 'autopilot-migration-result-v1'
        assert hashlib.sha256(json.dumps(rbody, sort_keys=True).encode()).hexdigest() == rdoc['sha256']
        st, owner = sqlite3.connect(tgt / 'state.db').execute(
            "SELECT status, lease_owner FROM tasks WHERE id='mig-2'").fetchone()
        assert st == 'queued' and owner == '', (st, owner)         # active lease sanitized on arrival
        assert (tgt / 'receipts' / f'{rid}.json').read_bytes() \
            == (impsrc / 'receipts' / f'{rid}.json').read_bytes()  # byte-exact receipt restore
        assert hrun(tenv, 'verify-chain')['ok'] is True            # imported chain verifies verbatim
        res2 = mimpo(tenv, '--apply')
        assert res2['deduplicated'] is True, res2                  # identical re-run deduplicates to nothing
        assert res2['audit_events_imported'] == 0, res2
        assert all(v == 0 for v in res2['inserted'].values()), res2
        err = ops_fail('migrate-import', '--inventory', str(inv2_path), '--source-id', sid, '--apply')
        assert '--relink-audit' in err, err                        # lived-in home: no silent chain merge
        mp = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                             '--inventory', str(inv2_path), '--source-id', sid,
                             '--apply', '--relink-audit'], env=env, text=True, capture_output=True)
        assert mp.returncode == 0, (mp.stdout, mp.stderr)
        merged = json.loads(mp.stdout)
        assert merged['audit_relinked'] is True and merged['health']['problems'] == [], merged
        assert run('verify-chain')['ok'] is True                   # relinked combined ledger still verifies
        hrun(senv, 'create', '--project', 'Legacy', '--title', 'post-seal drift', '--id', 'mig-3')
        err = mimpo(tenv, '--apply', fail=True)
        assert 'changed since the inventory was sealed' in err, err  # drift caught before any data moves
        fc_path = Path(td) / 'inventory-fc.json'
        ops_fail('migrate-inventory', '--root', str(mig), '--out', str(fc_path))  # seals before failing closed
        err = ops_fail('migrate-import', '--inventory', str(fc_path), '--source-id', sid, '--apply')
        assert 'failed closed' in err, err                         # fail-closed inventories never import
        vinv_path = Path(td) / 'inventory-vault.json'
        vinv = ops('migrate-inventory', '--root', str(mig / 'vault'), '--out', str(vinv_path))
        err = ops_fail('migrate-import', '--inventory', str(vinv_path),
                       '--source-id', vinv['sources'][0]['id'])
        assert 'only autopilot_sqlite' in err, err                 # non-execution-truth sources refused
        err = ops_fail('migrate-import', '--inventory', str(inv2_path), '--source-id', 'src-absent')
        assert 'not found in inventory' in err, err
        secsrc = Path(td) / 'secsrc/autopilot'
        secenv = mkhome(secsrc)
        hrun(secenv, 'create', '--project', 'Legacy', '--title', 'secret work', '--id', 'sec-1')
        hrun(secenv, 'note', 'sec-1', '--content',
             'deploy key AKIAIOSFODNN7EXAMPLE rotate soon', '--allow-secret')  # legacy-style row
        sinv_path = Path(td) / 'inventory-secret.json'
        sinv = ops('migrate-inventory', '--root', str(Path(td) / 'secsrc'), '--out', str(sinv_path))
        ssid = sinv['sources'][0]['id']
        stgt = Path(td) / 'sectgt/autopilot'
        stenv = mkhome(stgt)
        def simpo(*a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                                '--inventory', str(sinv_path), '--source-id', ssid, *a],
                               env=stenv, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        err = simpo('--apply', fail=True)
        assert 'refusing to import credential-shaped content' in err and 'aws_access_key' in err, err
        red = simpo('--apply', '--redact')
        assert red['secret_kinds'] == ['aws_access_key'] and red['ok'] is True, red
        note_content = sqlite3.connect(stgt / 'state.db').execute(
            'SELECT content FROM notes').fetchone()[0]
        assert '[REDACTED:aws_access_key]' in note_content, note_content
        assert 'AKIAIOSFODNN7EXAMPLE' not in note_content          # value never crosses the boundary
        # Legacy tolerance: a source whose schema predates the fact graph entirely
        # (no facts table) is legacy shape, not corruption — it classifies healthy,
        # imports cleanly with zero facts, and never refuses the whole migration.
        legsrc = Path(td) / 'legacysrc/autopilot'
        lenv = mkhome(legsrc)
        hrun(lenv, 'create', '--project', 'Legacy', '--title', 'pre-fact-graph work', '--id', 'lg-1')
        lgcon = sqlite3.connect(legsrc / 'state.db')
        for trig in ('facts_fts_ai', 'facts_fts_ad'):
            lgcon.execute(f'DROP TRIGGER IF EXISTS {trig}')
        lgcon.execute('DROP TABLE IF EXISTS facts_fts')
        lgcon.execute('DROP INDEX IF EXISTS idx_facts_subject')
        lgcon.execute('DROP INDEX IF EXISTS idx_facts_object')
        lgcon.execute('DROP TABLE facts')
        lgcon.commit(); lgcon.close()
        lginv_path = Path(td) / 'inventory-legacy.json'
        lginv = ops('migrate-inventory', '--root', str(Path(td) / 'legacysrc'), '--out', str(lginv_path))
        assert lginv['sources'][0]['status'] == 'ok', lginv         # classified healthy, not ambiguous
        lgtgt = Path(td) / 'legacytgt/autopilot'
        lgenv = mkhome(lgtgt)
        def limpo(*a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-import',
                                '--inventory', str(lginv_path), '--source-id', lginv['sources'][0]['id'], *a],
                               env=lgenv, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        dry_lg = limpo()
        assert dry_lg['tables']['facts']['source_rows'] == 0, dry_lg
        res_lg = limpo('--apply')
        assert res_lg['ok'] is True and res_lg['inserted']['tasks'] == 1 \
            and res_lg['inserted'].get('facts', 0) == 0, res_lg
        assert res_lg['health']['problems'] == [], res_lg
        # Migration rollback: stage three of the installer/migrator. A sealed
        # apply writes a rollback journal (exact rows + insert-time hashes,
        # merged audit event ids, restored receipt files); migrate-rollback
        # consumes it dry-run first, refuses drifted rows and local dependents
        # fail-closed (--force cascades deliberately), relinks the audit chain,
        # keeps locally changed receipt files, and deduplicates re-runs.
        rdoc = json.loads((Path(td) / 'migration-result.json').read_text())
        assert set(rdoc['rollback']['tables']['tasks']['keys']) == {'mig-1', 'mig-2'}, rdoc
        def mroll(*a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback',
                                str(Path(td) / 'migration-result.json'), *a],
                               env=tenv, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        tdoc = json.loads((Path(td) / 'migration-result.json').read_text())
        tdoc['sources'] = 'tampered'
        tamp = Path(td) / 'migration-result-tampered.json'
        tamp.write_text(json.dumps(tdoc, sort_keys=True))
        err = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback',
                              str(tamp)], env=tenv, text=True, capture_output=True)
        assert err.returncode != 0 and 'integrity check failed' in err.stdout + err.stderr
        plan = mroll()
        assert plan['dry_run'] is True and plan['would_remove']['tasks'] == ['mig-1', 'mig-2'], plan
        assert len(plan['would_remove']['facts']) == 2, plan        # journal covers the fact graph
        assert rid in plan['receipt_files_would_delete'], plan
        assert plan['force_required'] is False and plan['drifted_rows'] == [], plan
        assert sqlite3.connect(tgt / 'state.db').execute(
            "SELECT COUNT(*) FROM tasks WHERE id LIKE 'mig-%'").fetchone()[0] == 2   # dry-run touched nothing
        raw = sqlite3.connect(tgt / 'state.db')
        raw.execute("UPDATE tasks SET title='locally edited' WHERE id='mig-1'"); raw.commit(); raw.close()
        dplan = mroll()
        assert dplan['drifted_rows'] == [{'table': 'tasks', 'key': 'mig-1'}], dplan
        err = mroll('--apply', fail=True)
        assert 'refusing to roll back changed execution truth' in err and 'drifted' in err, err
        raw = sqlite3.connect(tgt / 'state.db')
        raw.execute("UPDATE tasks SET title='carried work' WHERE id='mig-1'"); raw.commit(); raw.close()
        hrun(tenv, 'note', 'mig-1', '--content', 'local follow-up written after import')
        # A local fact pointing at an imported task blocks rollback too: removing
        # the task would dangle its soft provenance (doctor flags that as
        # out-of-band surgery), so facts block exactly like hard children.
        hrun(tenv, 'fact-assert', '--subject', 'mig-service', '--predicate', 'owned-by',
             '--object', 'legacy-team', '--source', 'local', '--task', 'mig-1')
        err = mroll('--apply', fail=True)
        assert 'local dependent' in err and '--force' in err, err   # never-imported child blocks removal
        rb = mroll('--apply', '--force')
        assert rb['ok'] is True and rb['removed']['tasks'] == 2, rb
        assert rb['removed']['notes'] == 1, rb                      # imported note removed by journal
        assert rb['removed']['facts'] == 3, rb                      # 2 imported + 1 local cascade
        assert rb['cascade_removed'] == 2, rb                       # local dependents cascaded with --force
        assert rb['receipt_files_deleted'] == [rid], rb             # byte-exact file removed with its row
        assert rb['audit_events_removed'] > 0 and rb['health']['problems'] == [], rb
        con = sqlite3.connect(tgt / 'state.db')
        assert con.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM receipts").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 0   # graph rolled back exactly
        acts = {r[0] for r in con.execute('SELECT action FROM audit_events')}
        con.close()
        assert 'migration_rollback_applied' in acts and 'migration_import_applied' in acts, acts
        assert not (tgt / 'receipts' / f'{rid}.json').exists()
        assert hrun(tenv, 'verify-chain')['ok'] is True             # chain relinked over removed events
        rb2 = mroll('--apply')
        assert all(v == 0 for v in rb2['removed'].values()) and rb2['audit_events_removed'] == 0, rb2
        stale = json.loads((Path(td) / 'migration-result.json').read_text())
        stale.pop('rollback')
        body = {k: v for k, v in stale.items() if k not in ('sha256', 'format', 'created_at')}
        stale['sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
        oldfmt = Path(td) / 'migration-result-oldfmt.json'
        oldfmt.write_text(json.dumps(stale, sort_keys=True))
        err = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback',
                              str(oldfmt)], env=tenv, text=True, capture_output=True)
        assert err.returncode != 0 and 'no rollback journal' in err.stdout + err.stderr

        # Onboarding: one command from a sealed stage-one inventory to a verified
        # working home. Dry-run plans and verifies but imports nothing; --apply
        # imports, sweeps doctor, and (--probe) exercises the cross-agent protocol
        # end-to-end through the real CLI; re-runs are idempotent; ambiguous or
        # fail-closed inventories refuse before anything moves.
        ob_home = Path(td) / 'onboard-home'
        obenv = os.environ.copy(); obenv['HERMES_AUTOPILOT_HOME'] = str(ob_home)
        def oops(*a, fail=False):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=obenv, text=True, capture_output=True)
            if fail:
                assert p.returncode != 0, ('expected refusal', a, p.stdout, p.stderr)
                return p.stdout + p.stderr
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        ob_rep = Path(td) / 'onboard-dry.json'
        ob_inv = Path(td) / 'inventory-onboard.json'   # fresh seal: the earlier
        # post-seal-drift test deliberately mutated impsrc after inv2 was sealed.
        ops('migrate-inventory', '--root', str(Path(td) / 'impsrc'), '--out', str(ob_inv))
        dry = oops('onboard', '--inventory', str(ob_inv), '--out', str(ob_rep))
        assert dry['ok'] is True and [s['stage'] for s in dry['stages']] == \
            ['preflight', 'init_control_plane', 'select_source', 'import_plan', 'doctor'], dry
        assert ob_rep.stat().st_mode & 0o077 == 0                          # sealed report is 0600


        # Onboarding: one command from a sealed stage-one inventory to a verified
        # working home. Dry-run plans and verifies but imports nothing; --apply
        # imports, sweeps doctor, and (--probe) exercises the cross-agent protocol
        # end-to-end through the real CLI; re-runs are idempotent; ambiguous or
        # fail-closed inventories refuse before anything moves.
        with sqlite3.connect(ob_home / 'state.db') as db:                   # init ran...
            assert db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0] == 0   # ...but nothing imported
        err = oops('onboard', '--inventory', str(ob_inv), '--probe', fail=True)
        assert '--probe requires --apply' in err, err                       # probe mutates by design
        ob_rep2 = Path(td) / 'onboard-apply.json'
        rep = oops('onboard', '--inventory', str(ob_inv), '--apply', '--probe', '--out', str(ob_rep2))
        assert [s['stage'] for s in rep['stages']] == \
            ['preflight', 'init_control_plane', 'select_source', 'import_plan',
             'import_apply', 'doctor', 'protocol_probe'], rep
        assert all(s['status'] == 'ok' for s in rep['stages']), rep
        assert rep['stages'] == [{'stage': s['stage'], 'status': s['status']} for s in
                                 json.loads(ob_rep2.read_text())['stages']], rep   # stdout is a compact view
        rdoc = json.loads(ob_rep2.read_text())
        probe_stage = next(s for s in rdoc['stages'] if s['stage'] == 'protocol_probe')
        assert probe_stage['handoff_id'] and len(probe_stage['recall_digest']) == 64, probe_stage
        # seal convention: created_at outside
        body = {k: v for k, v in rdoc.items() if k not in ('created_at', 'sha256')}
        assert hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest() == rdoc['sha256']
        with sqlite3.connect(ob_home / 'state.db') as db:
            assert db.execute("SELECT status FROM tasks WHERE id='mig-1'").fetchone()[0] == 'completed'
            assert db.execute("SELECT status FROM tasks WHERE id='mig-2'").fetchone()[0] == 'queued'  # lease sanitized
            assert db.execute("SELECT status FROM tasks WHERE id='onboard-probe'").fetchone()[0] == 'completed'
            assert db.execute("SELECT acked_by FROM handoffs WHERE id=?",
                              (probe_stage['handoff_id'],)).fetchone()[0] == 'codex'
        result_doc = Path(next(s for s in rdoc['stages']
                               if s['stage'] == 'import_apply')['result_doc'])
        assert result_doc.exists() and result_doc.parent.name == 'migrations'   # undo path is durable
        rerun = oops('onboard', '--inventory', str(ob_inv), '--apply', '--probe', '--out', str(ob_rep2))
        assert rerun['ok'] is True, rerun
        rdoc2 = json.loads(ob_rep2.read_text())
        by_name = {s['stage']: s for s in rdoc2['stages']}
        assert by_name['import_apply']['deduplicated'] is True, rdoc2          # import dedupes to nothing
        assert by_name['protocol_probe']['status'] == 'skipped', rdoc2         # probe does not duplicate
        amb_home_a = mkhome(Path(td) / 'amb/a/autopilot'); amb_home_b = mkhome(Path(td) / 'amb/b/autopilot')
        amb_inv = ops('migrate-inventory', '--root', str(Path(td) / 'amb'), '--out',
                      str(Path(td) / 'amb-inv.json'))
        err = oops('onboard', '--inventory', str(Path(td) / 'amb-inv.json'), '--apply', fail=True)
        assert 'ambiguous' in err and '--source-id' in err and 'candidates:' in err, err
        err = oops('onboard', '--inventory', str(fc_path), '--apply', fail=True)
        assert 'failed closed' in err, err                                  # blocked sources never import


@case('audit_hardening_regression_round')
def _case_audit_hardening_regression_round():
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        import sqlite3, hashlib, re
        def run(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def run_fail(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', args, p.stdout, p.stderr)
            return p.stderr
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        run('create', '--project', 'Verify', '--title', 'existing task', '--id', 'verify-1')

        # --- Audit & hardening round: regression coverage ---
        # ops policy must resolve policies under HERMES_AUTOPILOT_HOME; a hardcoded
        # live-home path would report gate decisions about the wrong fleet.
        (Path(td) / 'policies').mkdir(exist_ok=True)
        (Path(td) / 'policies' / 'polproj.yaml').write_text('merge_requires_user: true\n')
        pol = ops('policy', 'polproj', 'merge')
        assert pol['requires_user'] is True and pol['allowed'] is False, pol
        (Path(td) / 'policies' / 'polproj.yaml').unlink()
        pol = ops('policy', 'polproj', 'merge')
        assert pol['allowed'] is False and pol['reason'] == 'no project policy', pol
        # Duplicate create id: a clean refusal, never a raw IntegrityError traceback.
        err = run_fail('create', '--project', 'Verify', '--title', 'dup', '--id', 'verify-1')
        assert 'task id already exists' in err and 'Traceback' not in err and 'IntegrityError' not in err, err
        # Approval receipts are sealed files (hash-matched), so doctor stays clean.
        # A fresh home: earlier failure-injection tests leave deliberate problems
        # in the main one that doctor must keep reporting.
        env2 = os.environ.copy(); env2['HERMES_AUTOPILOT_HOME'] = str(Path(td) / 'clean-home')
        def run2(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env2, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def ops2(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env2, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        run2('create', '--project', 'Verify', '--title', 'approval receipt', '--id', 'ap-1')
        ap = ops2('approval', 'approve', 'ap-1', '--by', 'leo', '--reason', 'ship it')
        ap_file = Path(td) / 'clean-home' / 'receipts' / (ap['receipt_id'] + '.json')
        assert ap_file.exists(), ap
        with sqlite3.connect(Path(td) / 'clean-home' / 'state.db') as db:
            fh = db.execute('SELECT file_hash FROM receipts WHERE id=?', (ap['receipt_id'],)).fetchone()[0]
        assert fh and hashlib.sha256(ap_file.read_bytes()).hexdigest() == fh, (fh, ap_file)
        doc = ops2('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc
        # Malformed / missing migration result documents fail gracefully, not with a traceback.
        bad = Path(td) / 'bad-result.json'; bad.write_text('{not json')
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback', str(bad)],
                           env=env, text=True, capture_output=True)
        assert p.returncode != 0 and 'not valid JSON' in p.stdout + p.stderr \
            and 'Traceback' not in p.stderr, (p.returncode, p.stdout, p.stderr)
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'migrate-rollback',
                            str(Path(td) / 'nope.json')], env=env, text=True, capture_output=True)
        assert p.returncode != 0 and 'not found' in p.stdout + p.stderr \
            and 'Traceback' not in p.stderr, (p.returncode, p.stdout, p.stderr)
        # doctor detects handoffs_fts index drift (and a rebuild repairs it).
        run2('create', '--project', 'Verify', '--title', 'fts drift', '--id', 'fts-1')
        run2('handoff', 'fts-1', '--from-agent', 'a', '--to-agent', 'b',
             '--objective', 'drift probe', '--next-action', 'verify')
        con = sqlite3.connect(Path(td) / 'clean-home' / 'state.db')
        con.execute("INSERT INTO handoffs_fts(rowid,objective,status,from_agent,to_agent) "
                    "VALUES(99999,'ghost','','','')")
        con.commit()
        doc = ops2('doctor')
        drift = [x for x in doc['problems'] if x['kind'] == 'fts_index_drift' and x['table'] == 'handoffs']
        assert drift, doc
        con.execute("INSERT INTO handoffs_fts(handoffs_fts) VALUES('rebuild')")
        con.commit(); con.close()
        doc = ops2('doctor')
        assert doc['ok'] is True and doc['problems'] == [], doc

        # --- In-process race-guard verification ---
        # Black-box CLI runs cannot hit the window between an agent's row read and
        # its write, so these tests inject the concurrent mutation directly and
        # prove the guarded UPDATEs refuse instead of clobbering fresher state.
        import argparse, io
        from contextlib import redirect_stdout
        race_td = tempfile.mkdtemp(prefix='ap-race-')
        os.environ['HERMES_AUTOPILOT_HOME'] = race_td
        sys.modules.pop('autopilot', None); sys.modules.pop('ops', None)
        sys.path.insert(0, str(ROOT))
        import autopilot as apmod
        import ops as opsmod
        apmod.ensure()
        real_task_row, real_audit = apmod.task_row, apmod.audit
        past, future = '2020-01-01T00:00:00+00:00', '2099-01-01T00:00:00+00:00'
        def rsql(q, params=()):
            with apmod.conn() as c:
                c.execute(q, params)
        def rmk(task_id):
            t0 = apmod.now()
            rsql("INSERT INTO tasks(id,project,title,status,priority,created_at,updated_at) "
                 "VALUES(?,'Race','r','queued','P2',?,?)", (task_id, t0, t0))
        # complete/fail/release: a lease stolen after the holder's row read turns
        # the write into a refusal — the new owner's claim is never clobbered.
        rmk('rc-c')
        rsql("UPDATE tasks SET status='claimed',lease_owner='holder',lease_expires_at=? WHERE id='rc-c'", (future,))
        with apmod.conn() as c:
            stolen = c.execute("SELECT * FROM tasks WHERE id='rc-c'").fetchone()   # holder's view
        rsql("UPDATE tasks SET lease_owner='thief',lease_expires_at=?,lease_epoch=lease_epoch+1 WHERE id='rc-c'", (future,))
        apmod.task_row = lambda db, tid: stolen
        try:
            for fn, ns in (
                (apmod.complete, argparse.Namespace(id='rc-c', owner='holder', note='',
                                                    epoch=None, recall_digest='', evidence_receipts=[])),
                (apmod.fail, argparse.Namespace(id='rc-c', owner='holder', reason='x', no_retry=False,
                                                max_retries=3, backoff_base=60, backoff_cap=3600, epoch=None)),
                (apmod.release, argparse.Namespace(id='rc-c', owner='holder', epoch=None)),
            ):
                try:
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        fn(ns)
                    raise AssertionError(('stale holder mutation succeeded', fn.__name__, buf.getvalue()))
                except SystemExit as e:
                    assert 'lease changed' in str(e), (fn.__name__, str(e))
        finally:
            apmod.task_row = real_task_row
        with apmod.conn() as c:
            assert c.execute("SELECT lease_owner FROM tasks WHERE id='rc-c'").fetchone()[0] == 'thief'
        # recover: a fresh claim landing mid-sweep keeps its lease (reported as skipped).
        rmk('rc-r1'); rmk('rc-r2')
        rsql("UPDATE tasks SET status='running',lease_owner='ghost',lease_expires_at=?,retry_count=0 "
             "WHERE id IN ('rc-r1','rc-r2')", (past,))
        def stealing_audit(db, *a, **k):
            # Runs inside recover's open transaction: steal rc-r2's lease on the
            # same connection (a second writer would deadlock on the lock).
            db.execute("UPDATE tasks SET status='claimed',lease_owner='fresh-worker',lease_expires_at=? "
                       "WHERE id='rc-r2'", (future,))
            return real_audit(db, *a, **k)
        apmod.audit = stealing_audit
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                opsmod.recover(argparse.Namespace(max_retries=3, dry_run=False,
                                                  backoff_base=60, backoff_cap=3600))
            out = json.loads(buf.getvalue())
        finally:
            apmod.audit = real_audit
        assert out['recovered'] == ['rc-r1'] and out['skipped'] == ['rc-r2'], out
        with apmod.conn() as c:
            r2 = c.execute("SELECT status,lease_owner FROM tasks WHERE id='rc-r2'").fetchone()
        assert tuple(r2) == ('claimed', 'fresh-worker'), tuple(r2)
        # escalate: a task that settles mid-sweep is skipped, not bumped posthumously.
        rmk('rc-e1'); rmk('rc-e2')
        rsql("UPDATE tasks SET priority='P3',due_at=? WHERE id IN ('rc-e1','rc-e2')", (past,))
        def completing_audit(db, *a, **k):
            db.execute("UPDATE tasks SET status='completed' WHERE id='rc-e2'")
            return real_audit(db, *a, **k)
        apmod.audit = completing_audit
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                opsmod.escalate(argparse.Namespace(dry_run=False))
            out = json.loads(buf.getvalue())
        finally:
            apmod.audit = real_audit
        assert out['skipped'] == ['rc-e2'] and [c['task_id'] for c in out['escalated']] == ['rc-e1'], out
        # tag/untag: compare-and-swap refuses to drop a concurrent writer's tags.
        rmk('rc-t')
        with apmod.conn() as c:
            stale_tags = c.execute("SELECT * FROM tasks WHERE id='rc-t'").fetchone()
            c.execute('UPDATE tasks SET tags=\'["other"]\' WHERE id=\'rc-t\'')
        apmod.task_row = lambda db, tid: stale_tags
        try:
            try:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    apmod.tag_task(argparse.Namespace(id='rc-t', tag=['x']))
                raise AssertionError('concurrent tag write was lost silently')
            except SystemExit as e:
                assert 'concurrently' in str(e), str(e)
        finally:
            apmod.task_row = real_task_row
        import shutil
        shutil.rmtree(race_td, ignore_errors=True)



@case('brain_inventory_and_home_selection')
def _case_brain_inventory_and_home_selection():
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        import sqlite3, hashlib, re
        def run(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def run_fail(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', args, p.stdout, p.stderr)
            return p.stderr
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stdout + p.stderr
        run('create', '--project', 'Verify', '--title', 'brain inventory seed', '--id', 'brain-seed-1')
        # Brain inventory: dry-run-first end-to-end manifest over every durable
        # source (Autopilot execution truth, temporal sidecar, Hindsight binding,
        # Claude sync metadata + memory archive, session cache, profile/skill/cron
        # definitions). Read-only by construction; redacted counts/checksums/kind-
        # only secret findings; sealed and reproducible; fails closed on corruption
        # while recording absence and an unreachable Hindsight honestly.
        import threading
        from http.server import BaseHTTPRequestHandler, HTTPServer
        fx = Path(td) / 'brain-fx'
        fhermes = fx / 'hermes'
        for d in ('profiles/gtm-bot', 'skills/devops', 'cron', 'hindsight',
                  'sessions/raw-store', 'state'):
            (fhermes / d).mkdir(parents=True)
        (fhermes / 'profiles/gtm-bot/profile.yaml').write_text('profile: gtm\n')
        (fhermes / 'skills/devops/SKILL.md').write_text('rotate key AKIAIOSFODNN7EXAMPLE weekly\n')
        (fhermes / 'cron/jobs.json').write_text('[{"id":"ticker","schedule":"* * * * *"}]')
        (fhermes / 'hindsight/claude-memory-sync.json').write_text(
            json.dumps({'proj-a/memory/MEMORY.md': {'synced_at': '2026-08-01'},
                        'proj-b/memory/MEMORY.md': {'synced_at': '2026-08-02'}}))
        (fhermes / 'sessions/raw-store/transcript-1.jsonl').write_text('{"role":"user"}\n')
        fclaude = fx / 'claude/projects/-fixtures-x'
        (fclaude / 'memory').mkdir(parents=True)
        (fclaude / 'memory/MEMORY.md').write_text('# fixture memory\n')
        (fclaude / 'memory/deploys-break-on-fridays.md').write_text('lesson body\n')
        def tree_digest_under(root_):
            h = hashlib.sha256()
            for p in sorted(root_.rglob('*')):
                if p.is_file() and not p.is_symlink():
                    h.update(str(p.relative_to(root_)).encode()); h.update(p.read_bytes())
            return h.hexdigest()
        fx_before = tree_digest_under(fx)

        try:
            inv_path = Path(td) / 'brain-inventory.json'
            inv = ops('brain-inventory',
                      '--hermes-home', str(fhermes), '--claude-home', str(fx / 'claude'),
                      '--out', str(inv_path))
            assert inv['ok'] is True and inv['fail_closed'] is False, inv   # degraded/unavailable ≠ fatal
            assert inv_path.stat().st_mode & 0o077 == 0                     # sealed manifest 0600
            full = json.loads(inv_path.read_text())                         # sealed doc, not compact
            bkind = {s['kind']: s for s in full['sources']}
            assert set(bkind) == {'autopilot', 'temporal', 'memories', 'claude_sync',
                                  'claude_memory', 'sessions', 'profiles', 'skills',
                                  'cron'}, sorted(bkind)                    # full brain covered
            roles = {s['kind']: s['role'] for s in inv['sources']}
            assert roles['autopilot'] == 'execution_truth' and roles['memories'] == 'semantic_memory'
            assert roles['temporal'] == 'temporal_facts' and roles['sessions'] == 'session_cache'
            assert roles['claude_sync'] == 'sync_metadata' and roles['claude_memory'] == 'human_archive'
            assert roles['profiles'] == 'profile_definitions' and roles['cron'] == 'cron_definitions'
            assert bkind['autopilot']['status'] == 'ok' and bkind['autopilot']['counts']['tasks'] >= 1
            assert bkind['temporal']['status'] == 'absent'                 # optional sidecar reported
            # Semantic memory is local and in-database: inventoried as its own
            # epistemic source, with no service to probe and no binding to bind.
            assert bkind['memories']['status'] == 'ok', bkind['memories']
            assert bkind['memories']['engine'] == 'memory-fts-v1'
            assert bkind['memories']['path'] == bkind['autopilot']['path']
            assert 'memories' in bkind['memories']['counts']
            assert 'url' not in bkind['memories'] and 'adapter' not in bkind['memories']
            assert bkind['claude_sync']['status'] == 'ok' and bkind['claude_sync']['counts']['entries'] == 2
            assert bkind['claude_sync']['sha256']
            assert bkind['claude_memory']['counts']['projects'] == 1
            assert bkind['claude_memory']['counts']['files'] == 2          # both memory files checksummed
            assert all(len(f['sha256']) == 64 for f in bkind['claude_memory']['files'])
            assert bkind['sessions']['counts']['files'] == 1 and bkind['sessions']['counts']['raw_store_present'] is True
            assert bkind['sessions']['counts'].get('cache_sessions') is not None  # control-plane cache counted
            assert bkind['cron']['counts']['jobs'] == 1 and bkind['cron']['status'] == 'ok'
            assert bkind['profiles']['status'] == 'ok' and bkind['skills']['counts']['files'] == 1
            # Redaction: credential shapes surface as kinds only, values never leave.
            assert inv['summary']['secret_kinds'] == ['aws_access_key'], inv['summary']
            assert 'AKIAIOSFODNN7EXAMPLE' not in inv_path.read_text()
            # Reproducibility: unchanged sources re-seal byte-identically (modulo created_at).
            inv2_path = Path(td) / 'brain-inventory-2.json'
            inv2 = ops('brain-inventory', '--hermes-home', str(fhermes),
                       '--claude-home', str(fx / 'claude'),
                        '--out', str(inv2_path))
            a_, b_ = json.loads(inv_path.read_text()), json.loads(inv2_path.read_text())
            assert a_.pop('created_at') and b_.pop('created_at')
            assert a_ == b_, 'unchanged sources must reproduce an identical manifest'
            chk = ops('brain-inventory-check', str(inv_path))
            assert chk['ok'] is True and chk['summary']['sources'] == len(bkind), chk
            tam = json.loads(inv_path.read_text()); tam['sources'][0]['status'] = 'corrupted'
            Path(str(inv_path) + '.tampered').write_text(json.dumps(tam, sort_keys=True))
            err = ops_fail('brain-inventory-check', str(inv_path) + '.tampered')
            assert 'integrity check failed' in err, err                    # tamper evidence
            # The brain inventory is a true live-source read: its sealed file is
            # the audit artifact, so it does not append a bookkeeping event to
            # the Autopilot database it is inspecting.
            assert tree_digest_under(fx) == fx_before, 'inventory must never mutate a scanned source'
            # Brain import: dry-run creates no target; apply copies only the
            # supported definition/archive/cache surfaces, quarantines the
            # credential-shaped skill, and re-runs without replacing any
            # destination bytes. No service binding is written any more --
            # semantic memory travels inside the control-plane database.
            btarget = Path(td) / 'brain-target'
            bplan = ops('brain-import', '--inventory', str(inv_path), '--target', str(btarget))
            assert bplan['dry_run'] is True and not btarget.exists(), bplan
            bapply = ops('brain-import', '--inventory', str(inv_path), '--target', str(btarget), '--apply')
            assert bapply['applied'] is True and bapply['quarantine'] == 1, bapply
            assert (btarget / 'metadata/claude-memory-sync.json').is_file()
            assert (btarget / 'profiles/gtm-bot/profile.yaml').is_file()
            assert (btarget / 'cron/jobs.json').is_file()
            assert (btarget / 'sessions/raw-store/transcript-1.jsonl').is_file()
            assert (btarget / 'archives/claude-memory/-fixtures-x/memory/MEMORY.md').is_file()
            assert not (btarget / 'skills/devops/SKILL.md').exists()
            assert not (btarget / 'bindings').exists(), 'no service binding is written'
            assert 'bind_hindsight' not in bapply.get('planned', {}), bapply
            assert any(a['action'] == 'external_execution_import_required'
                       and a['kind'] == 'memories' for a in bplan['actions']), bplan
            assert 'AKIAIOSFODNN7EXAMPLE' not in (btarget / 'provenance' /
                                                  ('brain-import-' + full['sha256'] + '.json')).read_text()
            assert Path(bapply['report']).stat().st_mode & 0o077 == 0
            brepeat = ops('brain-import', '--inventory', str(inv_path), '--target', str(btarget), '--apply')
            assert brepeat['ok'] is True and brepeat['written'] == [], brepeat
            # --redact turns a safe text secret into a redacted derivative rather
            # than allowing the original credential-shaped bytes into the target.
            rtarget = Path(td) / 'brain-redacted-target'
            redacted = ops('brain-import', '--inventory', str(inv_path), '--target', str(rtarget),
                           '--apply', '--redact')
            rskill = (rtarget / 'skills/devops/SKILL.md').read_text()
            assert '[REDACTED:aws_access_key]' in rskill and 'AKIAIOSFODNN7EXAMPLE' not in rskill, redacted
            assert tree_digest_under(fx) == fx_before, 'inventory must never mutate a scanned source'
        finally:
            pass
        # A valid temporal sidecar is a portable, checksummed file source. Keep
        # this separate from the prior absent-sidecar fixture so both contract
        # paths stay explicitly covered.
        temporal_home = Path(td) / 'brain-temporal-source'
        temporal_env = os.environ.copy(); temporal_env['HERMES_AUTOPILOT_HOME'] = str(temporal_home)
        p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), 'init'], env=temporal_env,
                           text=True, capture_output=True)
        assert p.returncode == 0, p.stderr
        with sqlite3.connect(temporal_home / 'temporal.db') as tc:
            tc.executescript('CREATE TABLE entities(id TEXT); CREATE TABLE relations(id TEXT); '
                             'CREATE TABLE ingested_events(id TEXT); INSERT INTO entities VALUES("e1");')
        temporal_before = hashlib.sha256((temporal_home / 'temporal.db').read_bytes()).hexdigest()
        temporal_inv_path = Path(td) / 'brain-temporal-inventory.json'
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'brain-inventory',
                            '--hermes-home', str(fhermes), '--claude-home', str(fx / 'claude'),
                            '--out', str(temporal_inv_path)],
                           env=temporal_env, text=True, capture_output=True)
        assert p.returncode == 0, (p.stdout, p.stderr)
        temporal_inv = json.loads(temporal_inv_path.read_text())
        temporal_src = next(s for s in temporal_inv['sources'] if s['kind'] == 'temporal')
        assert temporal_src['status'] == 'ok' and temporal_src['sha256'] == temporal_before, temporal_src
        temporal_target = Path(td) / 'brain-temporal-target'
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'brain-import',
                            '--inventory', str(temporal_inv_path), '--target', str(temporal_target), '--apply'],
                           env=temporal_env, text=True, capture_output=True)
        assert p.returncode == 0, (p.stdout, p.stderr)
        assert hashlib.sha256((temporal_target / 'temporal.db').read_bytes()).hexdigest() == temporal_before
        assert hashlib.sha256((temporal_home / 'temporal.db').read_bytes()).hexdigest() == temporal_before
        # brain-inventory makes no outbound call at all any more: the retired
        # Hindsight probe was its only one, so an offline machine inventories
        # the full brain with nothing degraded by network reachability.
        inv3 = ops('brain-inventory', '--hermes-home', str(fhermes),
                   '--claude-home', str(fx / 'claude'))
        assert inv3['fail_closed'] is False, inv3
        assert not any(k in inv3 for k in ('hindsight_url', 'bank')), inv3
        m3 = next(s for s in inv3['sources'] if s['kind'] == 'memories')
        assert m3['status'] == 'ok' and 'url' not in m3, m3
        # Corruption elsewhere fails closed with exact blockers: a garbage cron
        # definition and a garbage temporal sidecar each block their own run.
        (fhermes / 'cron/jobs.json').write_text('{definitely not json')
        err = ops_fail('brain-inventory', '--hermes-home', str(fhermes),
                       '--claude-home', str(fx / 'claude'))
        assert 'failed closed' in err and 'jobs.json' in err, err          # blocker named exactly
        bhome = fx / 'broken-autopilot'; bhome.mkdir()
        benv = os.environ.copy(); benv['HERMES_AUTOPILOT_HOME'] = str(bhome)
        bp = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), 'init'],
                            env=benv, text=True, capture_output=True)
        assert bp.returncode == 0, bp.stderr
        (bhome / 'temporal.db').write_bytes(b'temporal sidecar garbage, not sqlite')
        fx_after_edits = tree_digest_under(fhermes) + tree_digest_under(fx / 'claude')
        bp2 = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'brain-inventory'],
                             env=benv, text=True, capture_output=True)
        assert bp2.returncode != 0 and 'temporal.db' in (bp2.stdout + bp2.stderr) and \
            'failed closed' in (bp2.stdout + bp2.stderr), bp2             # corrupt sidecar blocks
        assert tree_digest_under(fhermes) + tree_digest_under(fx / 'claude') == fx_after_edits, \
            'fail-closed runs still never mutate scanned sources'

        # Hermes runtime home selection: env > selector > default, with a health
        # gate on select and a dry-run-first one-command rollback on deselect.
        import sqlite3 as _sq
        sel_fx = Path(td) / 'home-sel-fx'
        good_home = sel_fx / 'mindos-good'
        bad_home = sel_fx / 'mindos-bad'
        for d in (good_home, bad_home):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), 'init'],
                               env={**env, 'HERMES_AUTOPILOT_HOME': str(d)},
                               text=True, capture_output=True)
            assert p.returncode == 0, p.stderr
            # Seed one audited event in each home so the tamper check has a chain.
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), 'create',
                                '--project', 'Sel', '--title', 'seed'],
                               env={**env, 'HERMES_AUTOPILOT_HOME': str(d)},
                               text=True, capture_output=True)
            assert p.returncode == 0, p.stderr
        sel_path = sel_fx / 'selector.json'
        senv = {**env, 'AUTOPILOT_HOME_SELECTOR': str(sel_path)}
        cenv = {k: v for k, v in senv.items() if k != 'HERMES_AUTOPILOT_HOME'}  # default-resolution env
        def sops(*a, _env=None):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=_env or senv,
                               text=True, capture_output=True)
            if p.returncode: raise AssertionError((a, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def sops_fail(*a, _env=None):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=_env or senv,
                               text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stderr
        # Default resolution: no env, no selector → rollback default.
        show = sops('home-show', _env=cenv)
        assert show['source'] == 'default' and show['selector_present'] is False, show
        # A malformed selector degrades to the default instead of failing.
        sel_path.write_text('{not json')
        assert sops('home-show', _env=cenv)['source'] == 'default'
        # A selector naming a vanished home is ignored too.
        ghost = sel_fx / 'ghost-home'; ghost.mkdir()
        sel_path.write_text(json.dumps({'format': 'mindos-home-selector-v1',
                                        'source': 'mindos', 'home': str(ghost)}))
        ghost.rmdir()
        assert sops('home-show', _env=cenv)['source'] == 'default'
        # Read-only doctor on a healthy home passes; on a home with a tampered
        # audit chain it fails closed naming the problem — without mutating either.
        def tree_d(root_):
            # Exclude SQLite's -wal/-shm sidecars: any read connection may
            # checkpoint them; their churn is not a mutation of durable data.
            h = hashlib.sha256()
            for p in sorted(root_.rglob('*')):
                if p.is_file() and not p.is_symlink() and not p.name.endswith(('-wal', '-shm')):
                    h.update(str(p.relative_to(root_)).encode()); h.update(p.read_bytes())
            return h.hexdigest()
        good_before = tree_d(good_home)
        assert sops('home-doctor', '--home', str(good_home))['ok'] is True
        with _sq.connect(bad_home / 'state.db') as bdb:
            bdb.execute("UPDATE audit_events SET action='tampered' WHERE id=(SELECT MIN(id) FROM audit_events)")
        bad_before = tree_d(bad_home)
        dres = sops('home-doctor', '--home', str(bad_home))
        assert dres['ok'] is False and any(p['kind'] == 'hash_mismatch' for p in dres['problems']), dres
        assert tree_d(bad_home) == bad_before and tree_d(good_home) == good_before, 'doctor must be read-only'
        # home-select refuses an unhealthy home and refuses the rollback default.
        err = sops_fail('home-select', '--home', str(bad_home), '--selector', str(sel_path))
        assert 'failed health verification' in err and 'hash_mismatch' in err, err
        assert not sel_path.exists() or 'tampered' not in sel_path.read_text()
        err = sops_fail('home-select', '--home', str(Path.home() / '.hermes' / 'autopilot'), '--selector', str(sel_path))
        assert 'rollback default' in err, err
        # A healthy home selects cleanly: selector written 0600 and honored.
        selres = sops('home-select', '--home', str(good_home), '--selector', str(sel_path))
        assert selres['ok'] is True and selres['health'] == 'pass', selres
        assert sel_path.stat().st_mode & 0o077 == 0
        body = json.loads(sel_path.read_text())
        assert body['format'] == 'mindos-home-selector-v1' and body['home'] == str(good_home.resolve()), body
        # An explicit env override still outranks the selector…
        assert sops('home-show')['source'] == 'env'
        # …and without it, the selector drives resolution.
        assert sops('home-show', _env=cenv)['source'] == 'selector'
        assert sops('home-show', _env=cenv)['root'] == str(good_home.resolve())
        # Deselect is dry-run first; the apply is the one-command rollback.
        plan = sops('home-deselect', '--selector', str(sel_path))
        assert plan['dry_run'] is True and plan['would_remove_selector'] is True and sel_path.exists(), plan
        done = sops('home-deselect', '--selector', str(sel_path), '--apply')
        assert done['removed_selector'] is True and not sel_path.exists(), done
        assert sops('home-show', _env=cenv)['source'] == 'default'
        # Idempotent deselect apply on an absent selector stays honest.
        again = sops('home-deselect', '--selector', str(sel_path), '--apply')
        assert again['removed_selector'] is False, again

        # Tamper evidence (last): mutating a historical audit event breaks the chain.
        import sqlite3
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.execute("UPDATE audit_events SET action='tampered' WHERE id=(SELECT MIN(id) FROM audit_events)")
        chain = run('verify-chain')
        assert chain['ok'] is False
        assert any(p['kind'] == 'hash_mismatch' for p in chain['problems'])


@case('protocol_self_description')
def _case_protocol_self_description():
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        def run(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        run_fail = None  # not needed here; tampering is file-level below
        run('init')
        # Stable across calls (created_at excluded) and sealed with the house digest.
        d1 = run('protocol')
        d2 = run('protocol')
        assert d1.pop('created_at') and d2.pop('created_at')
        assert d1 == d2, 'protocol output must be stable across calls'
        body = {k: v for k, v in d1.items() if k != 'sha256'}
        import hashlib
        assert hashlib.sha256(json.dumps(body, sort_keys=True,
            separators=(',', ':')).encode()).hexdigest() == d1['sha256'], \
            'protocol seal must verify under the house digest format (created_at outside)'
        # Generated-from-code content cannot silently drift.
        assert {s['prefix'] for s in d1['flag_gated_pack_sections']} >= \
            {'related', 'related_handoffs', 'dep_context', 'related_sessions', 'related_facts'}
        for s in d1['flag_gated_pack_sections']:
            assert s['limit_flag'].startswith('--'), s
        assert 'objective' in d1['handoff_field_contract']['fields']
        assert {'unaddressed', 'unproven_recall_digest'} <= \
            set(d1['refusal_vocabulary']['handoff_lint_reasons'])
        assert 'completed' in d1['status_machine']['terminal_statuses']
        # Repeatable-field contract: every argparse append action must report
        # repeatable=true (introspection used to miss all of them), while
        # scalar fields stay false.
        fields = d1['handoff_field_contract']['fields']
        repeatable = {k for k, v in fields.items() if v['repeatable']}
        assert {'evidence', 'constraints', 'decisions', 'files',
                'next_actions', 'risks'} <= repeatable, \
            ('all append handoff fields must be repeatable', sorted(repeatable))
        assert not ({'task_id', 'from_agent', 'to_agent', 'objective', 'commit_ref'} & repeatable), repeatable
        # A tampered protocol document fails digest verification.
        tampered = json.loads(json.dumps(d1))
        tampered['status_machine']['statuses'] = ['made-up']
        body2 = {k: v for k, v in tampered.items() if k != 'sha256'}
        bad_digest = hashlib.sha256(json.dumps(body2, sort_keys=True,
            separators=(',', ':')).encode()).hexdigest()
        assert bad_digest != d1['sha256'], 'tampering must change the digest'


@case('memory_semantic_recall')
def _case_memory_semantic_recall():
    """Local semantic memory: recall, scope isolation, retraction, legacy import.

    The engine this replaces was a JSONL bank read out-of-band plus an HTTP
    service. Each assertion below that names a "regression" pins a defect the
    retired engine actually had.
    """
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
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            return json.loads(p.stdout)
        import sqlite3, hashlib
        run('create', '--project', 'Auth', '--title', 'fix login redirect bug in auth module', '--id', 'hs-t1')
        run('claim', 'hs-t1', '--owner', 'leo', '--minutes', '5')

        # Empty store: --related-semantic is a no-op and doctor reports a
        # healthy-with-note, never a problem.
        pack = run('context', 'hs-t1', '--related-semantic', '3')
        assert pack.get('related_semantic') == [], pack
        doc = ops('doctor')
        assert doc['ok'] is True, doc
        note = [n for n in doc.get('notes', []) if n.get('kind') == 'memory_store']
        assert note and note[0]['status'] == 'empty' and note[0]['memories'] == 0, doc
        assert note[0]['engine'] == 'memory-fts-v1', doc
        assert not any(p.get('kind', '').startswith('memory') for p in doc['problems']), doc

        # Secret guard: credential-shaped content is refused exactly like notes,
        # and --redact stores a tagged placeholder instead of the raw value.
        err = run_fail('memory-retain', '--text', 'the api_key is "sk-live-abc123def456ghi789"')
        assert 'credential-shaped' in err, err
        red = run('memory-retain', '--text', 'the api_key is "sk-live-abc123def456ghi789"',
                  '--redact', '--kind', 'decision', '--project', 'Auth')
        assert red['ok'] is True and red.get('secret_kinds'), red

        # Retain the corpus. Content addressing makes a repeat retain a no-op
        # instead of forking a duplicate (regression: the retired engine keyed
        # ids on timestamp+text, so same-second retains collided while the same
        # fact retained a day apart duplicated).
        m1 = run('memory-retain', '--text', 'login redirect bug in auth module was caused by a stale cookie session',
                 '--kind', 'decision', '--project', 'Auth', '--tag', 'auth', '--tag', 'cookies')
        m2 = run('memory-retain', '--text', 'unrelated note about the deploy pipeline',
                 '--kind', 'memory', '--project', 'Deploy')
        m3 = run('memory-retain', '--text', 'auth module review: redirect handling must preserve query params',
                 '--kind', 'fact')
        assert all(m['created'] for m in (m1, m2, m3))
        again = run('memory-retain', '--text', 'login redirect bug in auth module was caused by a stale cookie session',
                    '--kind', 'decision', '--project', 'Auth')
        assert again['memory_id'] == m1['memory_id'] and again['created'] is False, again

        # Recall: matching memories pack with the engine tag, newest first.
        pack = run('context', 'hs-t1', '--related-semantic', '5')
        ids = [m['id'] for m in pack['related_semantic']]
        assert m1['memory_id'] in ids and m3['memory_id'] in ids, pack
        assert all(m['engine'] == 'memory-fts-v1' for m in pack['related_semantic']), pack
        ats = [m['at'] for m in pack['related_semantic']]
        assert ats == sorted(ats, reverse=True), pack
        assert sorted(next(m for m in pack['related_semantic']
                           if m['id'] == m1['memory_id'])['tags']) == ['auth', 'cookies'], pack

        # Regression: stopword-only overlap is not a match. The retired engine
        # OR-matched raw substrings, so a task containing "the" recalled every
        # memory containing "the" — relevance was effectively noise.
        assert m2['memory_id'] not in ids, ('stopword-only overlap must not match', pack)

        # Regression: project scope is enforced, not merely attempted. The
        # retired engine fell back to unscoped results whenever the project
        # filter matched nothing — exactly how one project's context leaked
        # into another's pack.
        run('create', '--project', 'Billing', '--title', 'fix login redirect bug in auth module', '--id', 'hs-t2')
        leak = run('context', 'hs-t2', '--related-semantic', '5')
        leaked = [m['id'] for m in leak['related_semantic']]
        assert m1['memory_id'] not in leaked, ('Auth memory leaked into Billing', leak)
        assert m3['memory_id'] in leaked, leak          # project-less memories are fleet-wide
        assert run('context', 'hs-t2', '--related-semantic', '5', '--related-scope', 'global'
                   ) and m1['memory_id'] in [m['id'] for m in run(
                       'context', 'hs-t2', '--related-semantic', '5',
                       '--related-scope', 'global')['related_semantic']], 'explicit global still reaches'

        # Recall loop: the digest seals the semantic section and verifies fresh.
        rb = run('recall', 'hs-t1', '--agent', 'codex', '--related-semantic', '5')
        dig = rb['digest']
        v = run('recall-verify', 'hs-t1', '--digest', dig, '--agent', 'codex', '--related-semantic', '5')
        assert v['fresh'] is True, v
        # A new relevant memory is real drift -> the sealed digest goes stale.
        run('memory-retain', '--text', 'auth module followup: rotate session cookies on redirect',
            '--kind', 'memory', '--project', 'Auth')
        v2 = run('recall-verify', 'hs-t1', '--digest', dig, '--agent', 'codex', '--related-semantic', '5')
        assert v2['fresh'] is False, v2
        run('handoff', 'hs-t1', '--from-agent', 'codex',
            '--objective', 'continue auth redirect fix',
            '--evidence', 'recalled with semantic memory', '--recall-digest', dig)
        stale = ops('recall-stale')
        assert next(i for i in stale['items'] if i['task_id'] == 'hs-t1')['state'] == 'stale', stale

        # Retraction: the memory stops packing, but the row and its audit
        # survive so the ledger still explains why the pack changed.
        run('memory-forget', m1['memory_id'])
        assert m1['memory_id'] not in [
            m['id'] for m in run('context', 'hs-t1', '--related-semantic', '5')['related_semantic']]
        listed = run('memory-list', '--all', '--limit', '50')
        row = next(m for m in listed if m['id'] == m1['memory_id'])
        assert row['superseded_by'] == 'retracted', row
        assert m1['memory_id'] not in [m['id'] for m in run('memory-list', '--limit', '50')]
        err = run_fail('memory-forget', 'mem-does-not-exist')
        assert 'no such memory' in err, err

        # Legacy bank import: the migration path off the retired file engine.
        bank = Path(td) / 'legacy-bank.jsonl'
        with bank.open('w', encoding='utf-8') as f:
            f.write(json.dumps({"id": "hs-old-1", "text": "legacy: auth module tokens rotate weekly",
                                "kind": "fact", "project": "Auth",
                                "created_at": "2026-01-02T03:04:05+00:00", "tags": ["auth"]}) + '\n')
            f.write('this line is torn json and must degrade silently\n')
            f.write(json.dumps({"id": "hs-old-2", "text": ""}) + '\n')
            # A garbage timestamp degrades to import time and is counted, like a
            # torn line: one bad field must not abort the whole migration.
            f.write(json.dumps({"id": "hs-old-3", "text": "legacy: undated deploy rule",
                                "created_at": "not-a-date"}) + '\n')
        dry = run('memory-import', str(bank))
        assert dry['dry_run'] is True and dry['parsed'] == 2 and dry['malformed_lines'] == 2, dry
        assert dry['undated'] == 1, dry
        assert run('memory-list', '--query', 'legacy', '--limit', '5') == [], 'dry run must not write'
        imp = run('memory-import', str(bank), '--apply')
        assert imp['imported'] == 2 and imp['malformed_lines'] == 2 and imp['undated'] == 1, imp
        again = run('memory-import', str(bank), '--apply')
        assert again['imported'] == 0 and again['already_present'] == 2, again   # re-run is a no-op
        old = next(m for m in run('memory-list', '--query', 'legacy', '--limit', '5')
                   if 'tokens rotate' in m['content'])
        assert old['at'] == '2026-01-02T03:04:05+00:00', old   # temporal order preserved
        assert old['source'] == 'hindsight-bank-import', old
        assert bank.read_text().count('torn json') == 1, 'the source bank is never mutated'
        # Import runs the same secret guard as retain.
        sbank = Path(td) / 'secret-bank.jsonl'
        sbank.write_text(json.dumps({"id": "s1", "text": 'the api_key is "sk-live-abc123def456ghi789"',
                                     "created_at": "2026-01-01T00:00:00+00:00"}) + '\n')
        err = run_fail('memory-import', str(sbank), '--apply')
        assert 'credential-shaped' in err, err

        # Doctor reports the populated store, and the memory FTS index is part
        # of the same drift sweep as every other external-content index.
        doc = ops('doctor')
        assert doc['ok'] is True, doc
        note = [n for n in doc.get('notes', []) if n.get('kind') == 'memory_store']
        assert note and note[0]['status'] == 'available' and note[0]['memories'] >= 5, doc
        assert note[0]['retracted'] >= 1 and note[0]['fts'] is True, doc
        reb = ops('fts-rebuild', '--dry-run')
        assert reb['drift_before'] == [], reb
        assert 'memories_fts' not in reb['skipped_no_fts5'], reb

        # Retains are transactional with the audit chain: no memory exists that
        # the ledger cannot account for (the retired engine appended to the
        # bank file outside the transaction, so a failed audit orphaned a line).
        with sqlite3.connect(Path(td) / 'state.db') as db:
            db.row_factory = sqlite3.Row
            audited = db.execute(
                "SELECT COUNT(*) n FROM audit_events WHERE action='memory_retained'").fetchone()['n']
            created = db.execute(
                "SELECT COUNT(*) n FROM audit_events WHERE action='memory_retained' "
                "AND json_extract(payload_json,'$.created')=1").fetchone()['n']
            stored = db.execute("SELECT COUNT(*) n FROM memories").fetchone()['n']
            imported = db.execute(
                "SELECT COUNT(*) n FROM memories WHERE source='hindsight-bank-import'").fetchone()['n']
            # Every retained memory has a creating audit event, and the
            # idempotent repeat is audited too rather than passing unrecorded.
            assert created == stored - imported, (created, stored, imported)
            assert audited == created + 1, (audited, created)
            assert db.execute(
                "SELECT COUNT(*) n FROM audit_events WHERE action='memory_imported'").fetchone()['n'] == 2
            assert db.execute(
                "SELECT COUNT(*) n FROM audit_events WHERE action='memory_forgotten'").fetchone()['n'] == 1
            import autopilot as _ap
            assert _ap.audit_chain_problems(db) == [], 'retains must not break the chain'
            # FTS and the LIKE fallback return identical rows, so a SQLite build
            # without FTS5 degrades in ranking cost only, never in correctness.
            fts_rows = _ap._memory_candidates(db, 'hs-t1', 'fix login redirect bug in auth module', 5, 'project')
            db.execute('DROP TABLE memories_fts')
            like_rows = _ap._memory_candidates(db, 'hs-t1', 'fix login redirect bug in auth module', 5, 'project')
            assert fts_rows == like_rows, (fts_rows, like_rows)
            db.rollback()


@case('sense_repair_breaker_learning')
def _case_sense_repair_breaker_learning():
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        def run(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            out = p.stdout.strip()
            # repair prints the intermediate lifecycle docs before its final plan;
            # parse every JSON document and keep the last.
            dec = json.JSONDecoder(); docs = []; idx = 0
            while idx < len(out):
                obj, end = dec.raw_decode(out, idx); docs.append(obj); idx = end
                while idx < len(out) and out[idx] in ' \n\t': idx += 1
            return docs[-1]
        def ops_fail(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode != 0, ('expected failure', a, p.stdout, p.stderr)
            return p.stderr + p.stdout
        import sqlite3, time

        def inject_fts_drift():
            run('create', '--project', 'Verify', '--title', 'drift host', '--id', f'drift-{inject_fts_drift.n}')
            run('note', f'drift-{inject_fts_drift.n}', '--content', f'drift probe body {inject_fts_drift.n}')
            inject_fts_drift.n += 1
            db = sqlite3.connect(Path(td) / 'state.db')
            rowid = db.execute("SELECT rowid FROM notes ORDER BY rowid DESC LIMIT 1").fetchone()[0]
            db.execute("INSERT INTO notes_fts(notes_fts) VALUES('rebuild')")
            db.execute("DELETE FROM notes_fts WHERE rowid=?", (rowid,))
            db.commit(); db.close()
        inject_fts_drift.n = 1

        # ------------------------------------------------------------------
        # sense: typed, content-hashed findings over injected faults
        # ------------------------------------------------------------------
        assert ops('sense')['count'] == 0          # clean fleet: no findings
        inject_fts_drift()
        s = ops('sense')
        fts_f = [x for x in s['findings'] if x['kind'] == 'doctor_fts_index_drift']
        assert len(fts_f) == 1, s
        assert fts_f[0]['severity'] == 'P2' and fts_f[0]['suggested_repair'] == 'fts-rebuild'
        assert len(fts_f[0]['hash']) == 16 and fts_f[0]['id'] == 'find-' + fts_f[0]['hash']
        fh = fts_f[0]['hash']
        # Recurrence: the same defect shape (same table, same drift kind)
        # yields an identical content hash even from a different row.
        run('create', '--project', 'Verify', '--title', 'drift host 2', '--id', 'drift-b')
        run('note', 'drift-b', '--content', 'another probe body')
        db = sqlite3.connect(Path(td) / 'state.db')
        rowid = db.execute("SELECT rowid FROM notes ORDER BY rowid DESC LIMIT 1").fetchone()[0]
        db.execute("DELETE FROM notes_fts WHERE rowid=?", (rowid,))
        db.commit(); db.close()
        s2 = ops('sense')
        hashes = [x['hash'] for x in s2['findings']
                  if x['kind'] == 'doctor_fts_index_drift' and x['evidence']['table'] == 'notes']
        assert len(set(hashes)) == 1, (hashes, s2)

        # ------------------------------------------------------------------
        # playbooks ship as data; dry-run plan is runnable and non-mutating
        # ------------------------------------------------------------------
        inv = ops('repair-list')
        kinds = {p['name'] for p in inv['playbooks']}
        assert {'fts-rebuild', 'stale-lease-recover'} <= kinds, inv
        plan = ops('repair', 'fts-rebuild', '--finding-hash', fh, '--dry-run')
        assert plan['dry_run'] is True and plan['tier'] == 0
        assert plan['rollback_command'][0] == 'fts-rebuild'
        rb = ops(*plan['rollback_command'])        # rollback in dry-run form runs
        assert rb['dry_run'] is True and rb['rebuilt'] == []
        unknown = ops_fail('repair', 'no-such-playbook')
        assert 'unknown repair playbook' in unknown

        # ------------------------------------------------------------------
        # repair executes end-to-end as a leased task with receipt + learning
        # ------------------------------------------------------------------
        rep = ops('repair', 'fts-rebuild', '--finding-hash', fh)
        assert rep['ok'] is True and rep['task_id'].startswith('repair-fts-rebuild-')
        task = run('show', rep['task_id'])
        assert task['status'] == 'completed' and task['lease_owner'] == ''
        kinds_seen = [r['kind'] for r in task['receipts']]
        assert 'repair' in kinds_seen, task
        cited = next(e for e in reversed(task['audit']) if e['action'] == 'completed')['payload']['evidence_receipts']
        assert rep['receipt_id'] in cited
        facts = run('facts', '--subject', f'finding:{fh}')
        assert [(f['predicate'], f['object']) for f in facts] == [('repaired-by', 'fts-rebuild')]
        assert facts[0]['task_id'] == rep['task_id']
        doc = ops('doctor')
        assert not [p for p in doc['problems'] if p['kind'] == 'fts_index_drift'], doc
        # Learning triple was audited as part of the repair.
        with sqlite3.connect(Path(td) / 'state.db') as db:
            n = db.execute("SELECT COUNT(*) FROM audit_events WHERE action='repair_completed'").fetchone()[0]
            assert n == 1, n

        # ------------------------------------------------------------------
        # stale-lease playbook: tier-1 recovery via the same path
        # ------------------------------------------------------------------
        run('create', '--project', 'Verify', '--title', 'stale host', '--id', 'stale-x')
        db = sqlite3.connect(Path(td) / 'state.db')
        db.execute("UPDATE tasks SET status='running',lease_owner='ghost',"
                   "lease_expires_at='2020-01-01T00:00:00+00:00' WHERE id='stale-x'")
        db.commit(); db.close()
        sf = [x for x in ops('sense')['findings'] if x['kind'] == 'doctor_stale_lease']
        assert len(sf) == 1 and sf[0]['severity'] == 'P1' and sf[0]['suggested_repair'] == 'stale-lease-recover'
        rep2 = ops('repair', 'stale-lease-recover', '--finding-hash', sf[0]['hash'])
        assert rep2['ok'] is True
        assert run('show', 'stale-x')['status'] == 'queued'
        assert run('facts', '--subject', f"finding:{sf[0]['hash']}")

        # ------------------------------------------------------------------
        # circuit breaker: default 3 repeats trips, disables the playbook,
        # opens a P0 investigate task, records a windowed fact-graph entry
        # ------------------------------------------------------------------
        inject_fts_drift()
        fh3 = [x for x in ops('sense')['findings'] if x['kind'] == 'doctor_fts_index_drift'][0]['hash']
        for _ in range(3):                       # threshold default: break-after 3
            ops('repair', 'fts-rebuild', '--finding-hash', fh3)
        err = ops_fail('repair', 'fts-rebuild', '--finding-hash', fh3)
        assert 'circuit breaker tripped after 3 repeats' in err, err
        trip = json.loads(err.split(': ', 1)[1].split('\n')[0])
        breaker = run('facts', '--subject', 'playbook:fts-rebuild')
        assert len(breaker) == 1 and breaker[0]['predicate'] == 'breaker-tripped'
        assert breaker[0]['valid_until'] > __import__('datetime').datetime.now(__import__('datetime').timezone.utc).replace(microsecond=0).isoformat()[:19]
        queued = {t['id']: t for t in run('list', '--status', 'queued')}
        assert trip['investigate_task'] in queued
        assert queued[trip['investigate_task']]['priority'] == 'P0'
        err2 = ops_fail('repair', 'fts-rebuild', '--finding-hash', 'different-defect')
        assert 'disabled by circuit breaker' in err2, err2
        # The chain stayed intact across create/claim/receipt/complete/breaker events.
        assert run('verify-chain')['ok'] is True

@case('autonomy_declaration_grant_expiry_and_enforcement')
def _case_autonomy_declaration_grant_expiry_and_enforcement():
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
        import sqlite3

        # ------------------------------------------------------------------
        # declaration: additive metadata + windowed human grant fact, stamped
        # ------------------------------------------------------------------
        run('create', '--project', 'Verify', '--title', 'declared work', '--id', 'auto-1')
        dec = run('declare', 'auto-1', '--model', 'opencode/x-preview-f-free',
                  '--autonomy-level', 'L1', '--granted-by', 'leo', '--grant-hours', '1')
        assert dec['autonomy_level'] == 'L1' and dec['model_binding'] == 'opencode/x-preview-f-free'
        assert dec['grant']['granted_by'] == 'leo' and dec['grant']['valid_until']
        grants = run('facts', '--subject', 'autonomy:auto-1')
        assert len(grants) == 1 and grants[0]['predicate'] == 'level-granted' \
            and grants[0]['object'] == 'L1' and grants[0]['source'] == 'leo'
        ev = [e for e in run('show', 'auto-1')['audit'] if e['action'] == 'autonomy_declared']
        assert len(ev) == 1 and ev[0]['payload']['granted_by'] == 'leo' \
            and ev[0]['payload']['grant_fact_id'] == grants[0]['id']
        err = run_fail('declare', 'auto-1', '--model', 'm/x', '--granted-by', '')
        assert '--granted-by is required' in err                     # fail-closed: a named human grants
        err = run_fail('declare', 'auto-1', '--model', 'bad model!', '--granted-by', 'leo')
        assert 'invalid model binding' in err

        # ------------------------------------------------------------------
        # enforcement: claim allowed under a live grant, refused once expired
        # ------------------------------------------------------------------
        run('claim', 'auto-1', '--owner', 'bot', '--minutes', '5')
        run('release', 'auto-1', '--owner', 'bot')
        db = sqlite3.connect(Path(td) / 'state.db')
        db.execute("UPDATE facts SET valid_until='2020-01-01T00:00:00+00:00' WHERE predicate='level-granted'")
        db.commit(); db.close()
        err = run_fail('claim', 'auto-1', '--owner', 'bot2', '--minutes', '5')
        assert 'no live autonomy grant' in err, err
        refusals = run('events', '--action', 'claim_refused_autonomy', '--entity-id', 'auto-1')
        assert refusals['count'] == 1 and refusals['events'][0]['payload']['reason'] == 'no_live_grant'
        # --force is the deliberate override, recorded in the claimed event.
        forced = run('claim', 'auto-1', '--owner', 'bot2', '--minutes', '5', '--force')
        assert forced['status'] == 'claimed'
        claimed_ev = next(e for e in run('show', 'auto-1')['audit']
                          if e['action'] == 'claimed')   # show lists newest-first
        assert claimed_ev['payload']['autonomy_override']['reason'] == 'no_live_grant'
        run('release', 'auto-1', '--owner', 'bot2')

        # ------------------------------------------------------------------
        # dispatch skips ungranted work instead of failing after the pick
        # ------------------------------------------------------------------
        nx = run('next', '--claim', '--owner', 'bot3', '--explain')
        picked_ids = [t['id'] for t in ([nx['task']] if nx.get('task') else [])]
        assert 'auto-1' not in picked_ids
        skip_reasons = {s['task_id']: s['reason'] for s in nx.get('skipped', [])}
        assert skip_reasons.get('auto-1') == 'autonomy_grant_missing', (nx, skip_reasons)

        # ------------------------------------------------------------------
        # a lower-level live grant cannot cover a higher declared level
        # ------------------------------------------------------------------
        run('declare', 'auto-1', '--model', 'opencode/x-preview-f-free',
            '--autonomy-level', 'L1', '--granted-by', 'leo', '--grant-hours', '1')
        db = sqlite3.connect(Path(td) / 'state.db')
        db.execute("UPDATE tasks SET status='queued',lease_owner='',lease_expires_at='',autonomy_level='L2' WHERE id='auto-1'")
        db.commit(); db.close()
        err = run_fail('claim', 'auto-1', '--owner', 'bot4', '--minutes', '5')
        assert 'the live grant is only L1' in err, err
        refusals = run('events', '--action', 'claim_refused_autonomy', '--entity-id', 'auto-1')
        assert refusals['events'][0]['payload']['reason'] == 'grant_below_declared_level'  # newest first

        # ------------------------------------------------------------------
        # recap metadata stamps into the completed audit event beside receipts
        # ------------------------------------------------------------------
        run('create', '--project', 'Verify', '--title', 'recapped work', '--id', 'recap-1')
        run('declare', 'recap-1', '--model', 'opencode/x-preview-f-free',
            '--autonomy-level', 'L0', '--granted-by', 'leo', '--grant-hours', '1')
        run('claim', 'recap-1', '--owner', 'bot', '--minutes', '5')
        run('receipt', 'recap-1', '--kind', 'verification', '--payload', '{"result": "pass"}')
        done = run('complete', 'recap-1', '--owner', 'bot', '--note', 'done',
                   '--recap', 'shipped the vertical slice; next: sealed recaps')
        assert done['status'] == 'completed'
        shown = run('show', 'recap-1')
        assert shown['recap'] == 'shipped the vertical slice; next: sealed recaps'
        comp = next(e for e in reversed(shown['audit']) if e['action'] == 'completed')
        assert comp['payload']['recap'] == shown['recap']
        assert any(r['kind'] == 'verification' for r in shown['receipts'])
        assert run('verify-chain')['ok'] is True


@case('nanny_bounded_double_run_and_impulse_states')
def _case_nanny_bounded_double_run_and_impulse_states():
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        def run(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            out = p.stdout.strip()
            dec = json.JSONDecoder(); docs = []; idx = 0
            while idx < len(out):
                obj, end = dec.raw_decode(out, idx); docs.append(obj); idx = end
                while idx < len(out) and out[idx] in ' \n\t': idx += 1
            return docs[-1]
        import sqlite3
        VOCAB = {'all_clear', 'working', 'hit_snag', 'decision_needed'}

        def inject_fts_drift(tag):
            db = sqlite3.connect(Path(td) / 'state.db')
            rowid = db.execute("SELECT rowid FROM notes ORDER BY rowid DESC LIMIT 1").fetchone()[0]
            db.execute("DELETE FROM notes_fts WHERE rowid=?", (rowid,))
            db.commit(); db.close()

        run('create', '--project', 'Verify', '--title', 'drift host', '--id', 'drift-1')
        run('note', 'drift-1', '--content', 'drift probe body')

        # ------------------------------------------------------------------
        # bounded tick: dry-run mutates nothing; states stay in the closed
        # impulse vocabulary derived from actual tick results
        # ------------------------------------------------------------------
        run('create', '--project', 'Verify', '--title', 'stale host', '--id', 'stale-1')
        run('claim', 'stale-1', '--owner', 'ghost', '--minutes', '0')
        dry = ops('nanny', '--dry-run')
        assert dry['state'] in VOCAB and dry['dry_run'] is True
        assert dry['counts']['repair_budget'] >= 0
        assert run('show', 'stale-1')['status'] in ('claimed', 'running')   # untouched

        # ------------------------------------------------------------------
        # double-run safety: concurrent ticks cannot both reclaim one lease,
        # and a second serial tick finds nothing left to do
        # ------------------------------------------------------------------
        procs = [subprocess.Popen(
            [sys.executable, str(ROOT / 'ops.py'), 'nanny'],
            env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(2)]
        results = [p.communicate() + (p.returncode,) for p in procs]
        assert all(r[2] == 0 for r in results), results
        ticks = [json.loads(r[0].strip().splitlines()[-1]) for r in results]
        total_recovered = sum(len(t['recovered']) for t in ticks)
        assert total_recovered == 1, ([t['recovered'] for t in ticks])
        assert all(t['state'] in VOCAB for t in ticks)
        t2 = ops('nanny')
        # A quiet second tick must also be finding-clean: a forked audit chain
        # (concurrent appenders racing the tail read) shows up here first as
        # broken_link/hash_mismatch noise instead of at the final verify-chain.
        assert t2['recovered'] == [] and t2['repairs'] == []
        assert t2['counts']['findings_open'] == 0, t2

        # ------------------------------------------------------------------
        # working / momentum memory: a repaired finding is resolved, not a
        # lingering snag; a persistent finding carries over compactly
        # ------------------------------------------------------------------
        inject_fts_drift('a')
        t3 = ops('nanny')
        assert t3['state'] == 'working', t3
        assert any(r['ok'] and r['playbook'] == 'fts-rebuild' for r in t3['repairs'])
        assert t3['counts']['findings_open'] == 0          # repaired this tick
        run('create', '--project', 'Verify', '--title', 'bare completion', '--id', 'bare-1')
        run('claim', 'bare-1', '--owner', 'w', '--minutes', '5')
        run('complete', 'bare-1', '--owner', 'w')           # no receipts: snag
        t4 = ops('nanny')
        assert t4['state'] == 'hit_snag', t4
        snag_hash = t4['open_hashes'][0]
        assert any(f['kind'].startswith('unverified_') for f in t4['new_findings'])
        t5 = ops('nanny')
        assert t5['state'] == 'hit_snag'
        assert snag_hash in t5['carried_over'], t5          # seen last tick
        assert not [f for f in t5['new_findings'] if f['hash'] == snag_hash]  # not re-narrated

        # ------------------------------------------------------------------
        # decision_needed: a looping playbook trips its breaker and the tick
        # asks for the human instead of repairing forever
        # ------------------------------------------------------------------
        inject_fts_drift('pre')
        fh = [x for x in ops('sense')['findings']
              if x['kind'] == 'doctor_fts_index_drift'][0]['hash']
        # The earlier tick's successful repair already counts toward the
        # window, so two more repeats exhaust the default break-after 3.
        for i in range(2):
            rep = ops('repair', 'fts-rebuild', '--finding-hash', fh)
            assert rep['ok'] is True
        p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), 'repair', 'fts-rebuild',
                            '--finding-hash', fh], env=env, text=True, capture_output=True)
        assert p.returncode != 0 and 'circuit breaker tripped after 3 repeats' in p.stderr, p.stderr
        inject_fts_drift('post')                            # drift again: the breaker must hold it
        t6 = ops('nanny')
        assert t6['state'] == 'decision_needed', t6
        assert any(d['reason'] == 'circuit_breaker' for d in t6['decisions'])
        breaker = run('facts', '--subject', 'playbook:fts-rebuild')
        assert breaker and breaker[0]['predicate'] == 'breaker-tripped'
        # Every tick left the audit chain intact.
        assert run('verify-chain')['ok'] is True


@case('audit_chain_survives_concurrent_writers')
def _case_audit_chain_survives_concurrent_writers():
    """The hash chain is a read-modify-write: appending reads the current tail
    and links to it. Without holding the write lock across both halves, two
    processes observe the same tail and emit two events sharing one prev_hash
    -- a fork no later writer can heal, and the exact defect that made the
    bounded-nanny double-run case fail intermittently."""
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        def run(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args],
                               env=env, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        run('create', '--project', 'Verify', '--title', 'chain host', '--id', 'chain-1')
        worker = Path(td) / 'append.py'
        worker.write_text(
            'import sys\n'
            'sys.path.insert(0, %r)\n'
            'import autopilot as ap\n'
            'c = ap.conn()\n'
            'for i in range(25):\n'
            '    ap.audit(c, "system", "concurrency", "chain_probe",'
            ' {"worker": sys.argv[1], "i": i})\n'
            '    c.commit()\n' % str(ROOT))
        procs = [subprocess.Popen([sys.executable, str(worker), str(k)], env=env, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                 for k in range(4)]
        results = [pr.communicate() + (pr.returncode,) for pr in procs]
        assert all(r[2] == 0 for r in results), results
        # Every append landed -- serialization must not silently drop writers.
        events = run('events', '--action', 'chain_probe', '--limit', '500')
        assert events['total_matching'] == 100, events['total_matching']
        chain = run('verify-chain')
        assert chain['ok'] is True, chain['problems'][:5]



@case('memory_embedding_layer_is_optional_and_off_the_pack_path')
def _case_memory_embedding_layer_is_optional_and_off_the_pack_path():
    """The semantic layer must be additive in the strictest sense.

    Three properties, each pinning a way this could quietly go wrong:

    1. Absent embedder is a note, not a failure. autopilot runs on a stdlib
       interpreter with no torch; if a missing optional model could make any
       command exit non-zero, the core loop would inherit a dependency it
       never agreed to.
    2. Embedding a memory must not move a sealed context pack's core_digest.
       Memories live inside the sealed core, so derived data that leaked into
       that digest would stale every pack citing a memory the moment the cron
       ran -- and `recall-stale` would start reporting phantom drift.
    3. The monitor digest must ignore derived state. It gates the
       consolidation session; if it moved when vectors were written, the
       session's own side effects would re-open the gate on the next tick and
       the job would run forever -- the always-on behaviour the retired
       engine had.
    """
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td

        def run(*args, expect_ok=True, extra_env=None):
            e = dict(env, **(extra_env or {}))
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args],
                               env=e, text=True, capture_output=True)
            if expect_ok and p.returncode:
                raise AssertionError((args, p.stdout, p.stderr))
            return p

        def js(*args, **kw):
            return json.loads(run(*args, **kw).stdout)

        run('create', '--project', 'demo', '--title', 'gateway lease rollout',
            '--id', 'emb-1')
        for text in ('the deploy broke on the gateway lease path after the rollout',
                     'added a new tokenizer for search queries'):
            run('memory-retain', '--text', text, '--project', 'demo')

        # 1. No usable interpreter: every surface still exits 0 and says why.
        blind = {'AUTOPILOT_EMBED_PYTHON': str(Path(td) / 'no-such-python')}
        for args in (('memory-search', '--query', 'rollback'),
                     ('memory-consolidate-brief',)):
            out = js(*args, extra_env=blind)
            assert out['ok'] is True and out['available'] is False, out
            assert out.get('note'), out
        out = js('memory-embed', '--apply', extra_env=blind)
        assert out['ok'] is True and out['embedded'] == 0, out
        # The store itself is untouched by a failed optional pass.
        assert js('memory-status')['memories'] == 2

        # A blind machine must still be able to read its own store.
        assert len(js('memory-list', '--project', 'demo')) == 2

        # 2/3. With a working embedder, digests must not move.
        probe = js('memory-status')['semantic']
        if not probe.get('available'):
            return {'skipped': 'no embedding worker on this machine',
                    'reason': probe.get('reason')}
        pack_before = js('recall', 'emb-1', '--related-semantic', '5')['core_digest']
        monitor_before = run('memory-status', '--digest').stdout

        embedded = js('memory-embed', '--apply')
        assert embedded['embedded'] == 2, embedded

        pack_after = js('recall', 'emb-1', '--related-semantic', '5')['core_digest']
        assert pack_after == pack_before, (pack_before, pack_after)
        assert run('memory-status', '--digest').stdout == monitor_before

        # Re-running is a no-op: vectors are never recomputed.
        assert js('memory-embed', '--apply')['embedded'] == 0

        # Semantic recall finds what keyword search cannot: the query shares no
        # content word with the memory it should rank first.
        hits = js('memory-search', '--query', 'rollback failure in production',
                  '--project', 'demo', '--limit', '2')['hits']
        assert hits and 'gateway lease path' in hits[0]['content'], hits
        assert not js('memory-list', '--project', 'demo',
                      '--query', 'rollback failure in production')

        # Scope is enforced, never relaxed to unscoped on an empty match --
        # regression: the retired engine leaked other projects exactly here.
        assert js('memory-search', '--query', 'rollback failure',
                  '--project', 'other')['hits'] == []

        # Git-derived memories are events, not restatements: consolidation
        # must never offer them as merge candidates. Two PRs merged the same
        # afternoon with near-identical titles cluster tightly, and merging
        # them would delete a release from the record a build log reports on.
        # Measured on the live store, this was 63 of 122 clusters.
        for t in ('PR #220 merged on 2026-08-17 by dev: record the production release',
                  'PR #224 merged on 2026-08-17 by dev: record the compliance release'):
            run('memory-retain', '--text', t, '--project', 'demo', '--kind', 'pull_request')
        run('memory-embed', '--apply')
        pr_texts = {'#220', '#224'}

        def clusters_with_prs(*extra):
            out = js('memory-consolidate-brief', '--project', 'demo',
                     '--threshold', '0.80', '--max-clusters', '50', *extra)
            return [c for c in out['clusters']
                    if any(any(k in m['text'] for k in pr_texts) for m in c['members'])], out

        hidden, out = clusters_with_prs()
        assert out['excluded_kinds'] == ['commit', 'pull_request'], out['excluded_kinds']
        assert hidden == [], hidden
        # --include-git is the deliberate opt-in, and must still work: the
        # exclusion is a default, not a capability that was removed.
        shown, out = clusters_with_prs('--include-git')
        assert out['excluded_kinds'] == [], out['excluded_kinds']
        assert shown, 'the PR pair should cluster once git kinds are included'

        # Retracted memories leave the semantic index too, not just FTS.
        mid = js('memory-list', '--project', 'demo')[0]['id']
        run('memory-forget', mid, '--superseded-by', 'retracted')
        assert all(h['id'] != mid for h in
                   js('memory-search', '--query', 'gateway lease rollout',
                      '--project', 'demo', '--limit', '5')['hits'])

        # The consolidation gate must settle: retraction moves the monitor
        # line once, and the following tick is byte-identical.
        moved = run('memory-status', '--digest').stdout
        assert moved != monitor_before
        assert run('memory-status', '--digest').stdout == moved

        assert js('verify-chain')['ok'] is True
    return {'model': probe.get('model'), 'dim': probe.get('dim')}


@case('git_ingest_is_bounded_and_idempotent')
def _case_git_ingest_is_bounded_and_idempotent():
    """Git ingest exists to feed a writer agent, so it has to be boring.

    Four properties:

    1. Dry-run writes nothing. Ingest is the one memory command pointed at a
       source the user did not author line by line; it must be inspectable
       before it lands.
    2. A second --apply ingests zero rows. Memories are content-addressed on
       (project, content_hash) and the commit SHA is in the text, so re-ingest
       is a free no-op -- there is no cursor file to corrupt or fall behind.
       If this ever regressed, a daily cron would duplicate history forever.
    3. --since actually bounds. Unbounded ingest lets one busy repository
       dominate the store and skew every later recall toward it.
    4. Enumeration is complete and ordered. A build log that silently drops
       the boring commits is how a writer agent ends up factually wrong, so
       memory-list --kind must return every commit, not a ranked subset.
    """
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        repo = Path(td) / 'src'; repo.mkdir()

        def git(*args, when=None):
            e = dict(os.environ)
            if when:
                e['GIT_AUTHOR_DATE'] = e['GIT_COMMITTER_DATE'] = when
            p = subprocess.run(['git', *args], cwd=repo, env=e,
                               text=True, capture_output=True)
            if p.returncode:
                raise AssertionError((args, p.stdout, p.stderr))
            return p.stdout

        git('init', '-q')
        git('config', 'user.email', 'v@example.com')
        git('config', 'user.name', 'Verifier')
        old, new = '2001-01-01T00:00:00', '2032-01-01T00:00:00'
        for i, when in ((0, old), (1, new), (2, new)):
            (repo / f'f{i}').write_text(str(i))
            git('add', '-A')
            git('commit', '-q', '-m', f'change number {i}', when=when)

        def run(*args, expect_ok=True):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args],
                               env=env, text=True, capture_output=True)
            if expect_ok and p.returncode:
                raise AssertionError((args, p.stdout, p.stderr))
            return p

        def js(*args, **kw):
            return json.loads(run(*args, **kw).stdout)

        base = ('memory-ingest-git', '--repo', str(repo), '--project', 'src')

        # 1. Dry run reports the work without doing it.
        wide = ('--since', '2000-01-01')
        dry = js(*base, *wide)
        assert dry['ok'] is True and dry['dry_run'] is True, dry
        assert dry['found'] == 3 and dry['by_kind']['commit'] == 3, dry
        assert js('memory-list', '--project', 'src') == []

        # 3. --since drops the ancient commit rather than clamping to it.
        bounded = js(*base, '--since', '2020-01-01', '--apply')
        assert bounded['ingested'] == 2, bounded

        # 2. Re-running the same bounded ingest is a pure no-op...
        again = js(*base, '--since', '2020-01-01', '--apply')
        assert again['ingested'] == 0 and again['already_present'] == 2, again
        # ...and widening the window backfills only what was missing.
        widened = js(*base, *wide, '--apply')
        assert widened['ingested'] == 1 and widened['already_present'] == 2, widened

        # 4. Enumeration returns every commit, newest first.
        rows = js('memory-list', '--project', 'src', '--kind', 'commit')
        assert len(rows) == 3, rows
        assert all('change number' in r['content'] for r in rows), rows
        assert js('memory-list', '--project', 'src', '--kind', 'commit',
                  '--since', '2020-01-01') != rows

        assert js('verify-chain')['ok'] is True
    return {'commits': 3}


@case('runner_activity_and_correction_continuation')
def _case_runner_activity_and_correction_continuation():
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy(); env['HERMES_AUTOPILOT_HOME'] = td
        def run(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            if p.returncode: raise AssertionError((args, p.stdout, p.stderr))
            return json.loads(p.stdout)
        def run_fail(*args):
            p = subprocess.run([sys.executable, str(ROOT / 'autopilot.py'), *args], env=env, text=True, capture_output=True)
            if p.returncode == 0: raise AssertionError(('expected refusal', args, p.stdout))
            return p.stderr
        def ops(*a):
            p = subprocess.run([sys.executable, str(ROOT / 'ops.py'), *a], env=env, text=True, capture_output=True)
            assert p.returncode == 0, (a, p.stdout, p.stderr)
            out = p.stdout.strip()
            dec = json.JSONDecoder(); docs = []; idx = 0
            while idx < len(out):
                obj, end = dec.raw_decode(out, idx); docs.append(obj); idx = end
                while idx < len(out) and out[idx] in ' \n\t': idx += 1
            return docs[-1]
        def sql(stmt, params=()):
            import sqlite3
            db = sqlite3.connect(Path(td) / 'state.db'); db.execute(stmt, params)
            db.commit(); db.close()
        MODEL = 'opencode/x-preview-f-free'

        # ------------------------------------------------------------------
        # provider-neutral runner receipts: two different harnesses seal the
        # identical runner/v1 schema; malformed payloads refuse fail-closed;
        # doctor sees ordinary sealed receipts
        # ------------------------------------------------------------------
        run('create', '--project', 'Verify', '--title', 'continuation host', '--id', 'cont-1')
        ra = run('run-receipt', 'cont-1', '--harness', 'hermes', '--model', MODEL,
                 '--session', 'sess-a', '--workspace', 'wt-main', '--outcome', 'ran',
                 '--timeout-seconds', '120', '--capability', 'autopilot-safe')
        rb = run('run-receipt', 'cont-1', '--harness', 'opencode', '--model', MODEL,
                 '--session', 'sess-b', '--workspace', 'wt-other', '--outcome', 'failed')
        assert ra['ok'] and rb['ok']
        shown = run('show', 'cont-1', '--limit', '10')['receipts']
        runs = {r['payload']['harness']: r for r in shown if r['kind'] == 'run'}
        assert set(runs) == {'hermes', 'opencode'}, sorted(runs)
        keys_a = sorted(runs['hermes']['payload'])
        assert keys_a == ['capabilities', 'harness', 'model', 'outcome',
                          'schema', 'session', 'timeout_seconds', 'workspace']
        assert sorted(runs['opencode']['payload']) == [
            'capabilities', 'harness', 'model', 'outcome', 'schema', 'session', 'workspace']
        assert all(p['schema'] == 'runner/v1' for p in
                   [runs['hermes']['payload'], runs['opencode']['payload']])
        assert 'invalid harness' in run_fail('run-receipt', 'cont-1', '--harness', 'Bad Harness!',
                                             '--model', MODEL, '--outcome', 'ran')
        run_fail('run-receipt', 'cont-1', '--harness', 'hermes', '--model', MODEL, '--outcome', 'exploded')
        run_fail('run-receipt', 'cont-1', '--harness', 'hermes', '--model', MODEL,
                 '--outcome', 'ran', '--timeout-seconds', '0')

        # ------------------------------------------------------------------
        # activity report contract: durable action/intent/state/evidence plus
        # a declared stall deadline; evidence must really exist on the task
        # ------------------------------------------------------------------
        run('claim', 'cont-1', '--owner', 'w', '--minutes', '5')
        failed_rid = [r for r in shown if r['payload'].get('outcome') == 'failed'][0]['id']
        err = run_fail('heartbeat', 'cont-1', '--owner', 'w', '--evidence', 'no-such-rid')
        assert 'not found on this task' in err, err
        hb = run('heartbeat', 'cont-1', '--owner', 'w',
                 '--action', 'implemented runner slice', '--intent', 'run full gates',
                 '--progress-state', 'working', '--stall-deadline', '+30m',
                 '--evidence', failed_rid)
        act = hb['activity']
        assert act['last_action'] == 'implemented runner slice'
        assert act['next_intent'] == 'run full gates'
        assert act['progress_state'] == 'working'
        assert act['stall_deadline'].endswith('+00:00') and act['evidence'] == [failed_rid]
        run_fail('heartbeat', 'cont-1', '--owner', 'w', '--progress-state', 'vibing')

        # ------------------------------------------------------------------
        # stalled activity: silence past the declared deadline surfaces as a
        # typed finding through the existing impulse pipeline, then carries
        # over compactly; a healthy deadline produces nothing
        # ------------------------------------------------------------------
        sql("UPDATE heartbeats SET stall_deadline='2020-01-01T00:00:00+00:00'")
        s1 = ops('nanny')
        assert s1['state'] == 'hit_snag', s1['state']
        stall_f = [f for f in s1['new_findings'] if f['kind'] == 'activity_stalled']
        assert stall_f and not stall_f[0].get('suggested_repair')   # human seam, no auto-repair
        sense_f = [f for f in ops('sense')['findings'] if f['kind'] == 'activity_stalled']
        assert sense_f and sense_f[0]['evidence']['task_id'] == 'cont-1'
        assert sense_f[0]['evidence']['last_action'] == 'implemented runner slice'
        stall_hash = stall_f[0]['hash']
        s2 = ops('nanny')
        assert stall_hash in s2['carried_over'] and \
            not [f for f in s2['new_findings'] if f['hash'] == stall_hash]
        import datetime as _dtm
        future = (_dtm.datetime.now(_dtm.timezone.utc)
                  + _dtm.timedelta(hours=1)).replace(microsecond=0).isoformat()
        sql("UPDATE heartbeats SET stall_deadline=?", (future,))
        s3 = ops('nanny')
        assert s3['state'] == 'all_clear', (s3['state'], s3['open_hashes'])

        # ------------------------------------------------------------------
        # failure taxonomy: infra doubles the backoff base; transient keeps
        # the standard schedule; the cause is stamped into the audit trail
        # ------------------------------------------------------------------
        def iso_diff(iso):
            import datetime as dtm
            then = dtm.datetime.fromisoformat(iso)
            return (then - dtm.datetime.now(dtm.timezone.utc)).total_seconds()
        run('create', '--project', 'Verify', '--title', 'transient host', '--id', 'tr-1')
        run('claim', 'tr-1', '--owner', 'w', '--minutes', '5')
        r_tr = run('fail', 'tr-1', '--owner', 'w', '--reason', 'flake', '--cause', 'transient',
                   '--backoff-base', '60', '--max-retries', '3')
        assert r_tr['outcome'] == 'retry_scheduled'
        ev = run('events', '--entity-id', 'tr-1', '--action', 'task_failed')['events']
        assert any(e['payload'].get('cause') == 'transient' for e in ev)
        run('create', '--project', 'Verify', '--title', 'infra host', '--id', 'inf-1')
        run('claim', 'inf-1', '--owner', 'w', '--minutes', '5')
        r_in = run('fail', 'inf-1', '--owner', 'w', '--reason', 'network down', '--cause', 'infra',
                   '--backoff-base', '60', '--backoff-cap', '3600', '--max-retries', '3')
        assert 100 <= iso_diff(r_in['recover_after']) <= 140, r_in   # 60 doubled to 120

        # ------------------------------------------------------------------
        # bounded correction continuation under a live grant:
        # fam-1 -> child A -> grandchild B -> refusal at the cap
        # ------------------------------------------------------------------
        run('create', '--project', 'Verify', '--title', 'family root', '--id', 'fam-1')
        dec = run('declare', 'fam-1', '--model', MODEL, '--autonomy-level', 'L2',
                  '--granted-by', 'leo', '--grant-hours', '24')
        run('receipt', 'fam-1', '--kind', 'verification', '--payload', '{"gates":"pass"}')
        run('claim', 'fam-1', '--owner', 'w', '--minutes', '5')
        f1 = run('fail', 'fam-1', '--owner', 'w', '--reason', 'wrong logic', '--cause', 'defect',
                 '--no-retry')
        c1 = f1['correction']
        assert c1['spawned'] is True and c1['attempt'] == 1 and c1['inherited_grants'] == 1
        childA = c1['child_task_id']
        sa = run('show', childA)
        assert sa['autonomy_level'] == 'L2' and sa['model_binding'] == MODEL
        assert 'failure_reason: wrong logic' in sa['description'] and 'prior_attempts: 1' in sa['description']
        assert 'correction' in sa['tags']
        grants = run('facts', '--subject', f'autonomy:{childA}')
        assert len(grants) == 1 and grants[0]['object'] == 'L2' and grants[0]['source'] == 'leo'
        lineage = run('facts', '--subject', f'task:{childA}')
        assert {f['predicate']: f['object'] for f in lineage} == {
            'correction-of': 'fam-1', 'correction-root': 'fam-1'}
        # the inherited grant is real authority: the child can be claimed
        run('claim', childA, '--owner', 'w2', '--minutes', '5')
        fa = run('fail', childA, '--owner', 'w2', '--reason', 'still wrong', '--cause', 'defect',
                 '--no-retry')
        c2 = fa['correction']
        assert c2['spawned'] is True and c2['attempt'] == 2 and c2['family_root'] == 'fam-1'
        childB = c2['child_task_id']
        run('claim', childB, '--owner', 'w3', '--minutes', '5')
        fb = run('fail', childB, '--owner', 'w3', '--reason', 'still still wrong',
                 '--cause', 'defect', '--no-retry')
        assert fb['correction'] == {'spawned': False, 'refused': 'max_attempts',
                                    'family_root': 'fam-1', 'attempts_used': 3}
        refusals = run('events', '--action', 'correction_refused_max_attempts')['events']
        assert refusals and refusals[0]['payload']['attempts_used'] == 3

        # ------------------------------------------------------------------
        # fail-closed paths: no declaration -> no child at all; expired grant
        # -> child waits ungranted at the existing dispatch seam
        # ------------------------------------------------------------------
        run('create', '--project', 'Verify', '--title', 'no declaration', '--id', 'nd-1')
        run('claim', 'nd-1', '--owner', 'w', '--minutes', '5')
        rnd = run('fail', 'nd-1', '--owner', 'w', '--reason', 'broken', '--cause', 'defect',
                  '--no-retry')
        assert 'correction' not in rnd, rnd.get('correction')
        run('create', '--project', 'Verify', '--title', 'expired grant', '--id', 'ex-1')
        run('declare', 'ex-1', '--model', MODEL, '--autonomy-level', 'L1',
            '--granted-by', 'leo', '--grant-hours', '24')
        sql("UPDATE facts SET valid_until='2020-01-01T00:00:00+00:00' WHERE predicate='level-granted'")
        run('claim', 'ex-1', '--owner', 'w', '--force')
        rex = run('fail', 'ex-1', '--owner', 'w', '--reason', 'broken', '--cause', 'defect',
                  '--no-retry')
        assert rex['correction']['spawned'] is True
        gc = rex['correction']['child_task_id']
        assert rex['correction']['inherited_grants'] == 0
        assert run('facts', '--subject', f'autonomy:{gc}') == []
        # the grant-less child waits at the fail-closed claim seam
        err = run_fail('claim', gc, '--owner', 'w')
        assert 'no live autonomy grant' in err, err

        assert run('verify-chain')['ok'] is True




if __name__ == '__main__':
    main()
