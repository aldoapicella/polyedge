#!/usr/bin/env bash
set -euo pipefail
python3 -I - "$(dirname "$0")/../bin/polyedge-funded-stopped-recovery" <<'PY'
import copy
import datetime as dt
import runpy
import sys

module = runpy.run_path(sys.argv[1])
now = dt.datetime(2026, 9, 17, 7, tzinfo=dt.timezone.utc)
saved = {
    'createdAtUtc': now.isoformat(),
    'targetSigner': {'image': 'target', 'revision': 'revision'},
    'installedQuadlet': {'sha256': 'quadlet'},
    'lastRuntime': {'invocationId': 'invocation'},
    'startupJournal': {'sha256': 'journal'},
    'capitalControls': {'cashBaseUnits': '41778267', 'documents': ['etag']},
    'queue': {'before': 0, 'after': 0},
    'approvedRedemptionPreflight': {'collector': {'sha256': 'collector'}, 'proof': {
        'started_ts': 'old-start', 'finished_ts': 'old-end', 'block': {'hash': 'old'},
        'payout_base_units': '5000000', 'condition_id': 'condition',
        'position_inventory': {'sha256': 'positions'}, 'redemption_control': {'etag': 'control'},
        'unresolved_reservation_count': 0
    }}
}
current = copy.deepcopy(saved)
current['approvedRedemptionPreflight']['proof'].update(started_ts='fresh-start', finished_ts='fresh-end', block={'hash': 'fresh'})
assert module['verify'](saved, current, now) == current['approvedRedemptionPreflight']['proof']
for key in ('targetSigner', 'installedQuadlet', 'lastRuntime', 'startupJournal', 'capitalControls', 'queue'):
    changed = copy.deepcopy(current)
    changed[key] = {'different': True}
    try: module['verify'](saved, changed, now)
    except AssertionError: pass
    else: raise AssertionError(key)
for key in ('payout_base_units', 'condition_id', 'position_inventory', 'redemption_control', 'unresolved_reservation_count'):
    changed = copy.deepcopy(current)
    changed['approvedRedemptionPreflight']['proof'][key] = 'changed'
    try: module['verify'](saved, changed, now)
    except AssertionError: pass
    else: raise AssertionError(key)
for offset in (-1, 1801):
    try: module['verify'](saved, current, now + dt.timedelta(seconds=offset))
    except AssertionError: pass
    else: raise AssertionError('stale or future certificate accepted')
print('stopped recovery evidence binding checks passed')
PY
