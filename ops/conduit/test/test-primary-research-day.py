#!/usr/bin/env python3
"""The fixed driver never reads or repairs unverified local selection evidence."""
import hashlib
import json
import runpy
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import patch


loaded = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'bin/polyedge-primary-research-day'))
run = loaded['main']
git_sha = 'a' * 40
protocol = {'training': {'start_inclusive': '2026-09-24T00:00:00Z'},
            'executables': {'git_sha': git_sha},
            'validation': {'end_exclusive': '2026-10-23T00:00:00Z'}}


def digest(path):
    return 'sha256:' + hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_day(work, day, gate_day=None):
    normalized = work / f'data/research/daily/{day}/normalized'
    gate_path = work / f'reports/research/staging/daily-{day}-fixture/data_quality_gate.json'
    normalized.mkdir(parents=True, exist_ok=True)
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    inventory = 'sha256:' + 'b' * 64
    manifest = normalized / 'events_manifest.json'
    manifest.write_text(json.dumps({'raw_source_inventory': {'canonical_sha256': inventory}}))
    audit, execution = gate_path.parent / 'data_audit.json', gate_path.parent / 'execution_quality.json'
    audit.write_text('{}')
    execution.write_text('{}')
    gate = dict(schema='polyedge.primary_data_quality_gate.v1', status='passed', date=gate_day or day,
                git_sha=git_sha, normalized_manifest_sha256=digest(manifest),
                source_inventory_sha256=inventory, data_audit_sha256=digest(audit),
                execution_quality_sha256=digest(execution),
                runtime_identity={'identities': [{'git_sha': git_sha}]})
    gate_path.write_text(json.dumps(gate))
    marker = dict(schema_version=1, date=day, git_sha=git_sha,
                  events_manifest_sha256=digest(manifest), raw_source_inventory_sha256=inventory,
                  quality_gate_path=str(gate_path.relative_to(work)), quality_gate_sha256=digest(gate_path))
    (normalized / '.polyedge-daily-complete.json').write_text(json.dumps(marker))
    return manifest, gate_path


with tempfile.TemporaryDirectory() as directory, patch('subprocess.run') as command:
    root = Path(directory)
    output = root / 'frozen'
    config = root / 'config.json'
    config.write_text(json.dumps(dict(pilot_date='2026-09-22', start_date='2026-09-24',
                                      git_sha=git_sha, oci_python='python',
                                      experiment_directory=str(output))))
    run.__globals__['verify_frozen'] = lambda out, start: protocol

    run(config, root, date(2026, 9, 22))
    command.assert_not_called()
    run(config, root, date(2026, 10, 25))
    command.assert_not_called()

    def blocked_before_commands(label):
        command.reset_mock()
        try:
            run(config, root, date(2026, 9, 26))
        except ValueError:
            pass
        else:
            raise AssertionError(label + ' was accepted')
        command.assert_not_called()

    write_day(root, '2026-09-24', gate_day='2026-09-23')
    blocked_before_commands('copied wrong-date gate')

    write_day(root, '2026-09-24')
    proof = root / 'reports/research/source-bindings/2026-09-24.json'
    proof.parent.mkdir(parents=True, exist_ok=True)
    proof.write_text('{}')
    blocked_before_commands('mismatched previous source proof')

    manifest, _ = write_day(root, '2026-09-24')
    manifest.write_text('{"changed":true}')
    blocked_before_commands('changed native manifest')

    _, gate = write_day(root, '2026-09-24')
    gate.write_text(gate.read_text() + '\n')
    blocked_before_commands('changed gate')

print('primary fixed-window driver checks passed')
