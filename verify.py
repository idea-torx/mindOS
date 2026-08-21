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
    row = next(r for r in run('list') if r['id'] == 'dep-a')
    assert row['status'] == 'completed' and row['lease_owner'] == '', 'terminal update must release lease'
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
    # Tamper evidence (last): mutating a historical audit event breaks the chain.
    import sqlite3
    with sqlite3.connect(Path(td) / 'state.db') as db:
        db.execute("UPDATE audit_events SET action='tampered' WHERE id=(SELECT MIN(id) FROM audit_events)")
    chain = run('verify-chain')
    assert chain['ok'] is False
    assert any(p['kind'] == 'hash_mismatch' for p in chain['problems'])
print('autopilot verification: PASS')