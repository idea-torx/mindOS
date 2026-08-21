#!/usr/bin/env python3
"""Seed/update the current known IdeatorX queue without executing work."""
import sqlite3, sys
sys.path.insert(0, '/Users/leofelix/.hermes/autopilot')
import autopilot

items = [
    ('ourpower-visual-audit', 'OurPower', 'Visual audit of first-pass theme work', 'Leo review required before follow-up revision.', 'momentum-manager', 'waiting_for_user', 'P1', 'Review isolated worktree and approve/request revision.'),
    ('gitvoice-production-review', 'Gitvoice', 'Production handoff review', 'Worker deployed and healthy; maintenance/archive review scheduled.', 'momentum-manager', 'waiting_for_review', 'P2', 'Review custom domain/DNS and archive if clean.'),
    ('resumeweaver-visible-ats', 'ResumeWeaver', 'Visible ATS verification', 'Tests pass; live visible verification needs approval.', 'momentum-manager', 'waiting_for_user', 'P1', 'Approve visible --hold verification run.'),
    ('airtight-production-qa', 'Airtight', 'Production readiness and visual QA', 'Untracked worker documentation needs decision.', 'momentum-manager', 'waiting_for_user', 'P1', 'Decide commit/discard and run QA.'),
    ('blyssminds-weekly-block', 'BlyssMinds', 'Define next weekly work block', 'Remaining weekly hours are not recorded.', 'client-manager', 'blocked', 'P1', 'Record available hours before dispatch.'),
    ('byfelix-next-milestone', 'byfelix', 'Next Sonia-approved milestone', 'No deployment without Sonia approval.', 'momentum-manager', 'blocked', 'P1', 'Obtain/define Sonia-approved milestone.'),
]
with autopilot.conn() as db:
    for task_id, project, title, desc, owner, status, priority, next_action in items:
        existing = db.execute('SELECT id FROM tasks WHERE id=?', (task_id,)).fetchone()
        if existing:
            db.execute('UPDATE tasks SET project=?,title=?,description=?,owner=?,status=?,priority=?,next_action=?,updated_at=? WHERE id=?', (project,title,desc,owner,status,priority,next_action,autopilot.now(),task_id))
        else:
            t=autopilot.now()
            db.execute('INSERT INTO tasks(id,project,title,description,owner,status,priority,next_action,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)', (task_id,project,title,desc,owner,status,priority,next_action,t,t))
print('seeded', len(items), 'current tasks')
