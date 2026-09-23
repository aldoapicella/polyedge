#!/usr/bin/env python3
"""Offline regression: failed, mismatched or already-started pilots cannot freeze."""
import copy
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from preregister_primary import digest, prepare, publish, verify_frozen


with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)

    def write(name, value):
        path = root / name
        path.write_text(json.dumps(value))
        return str(path)

    revision = 'a' * 40
    execution = write('execution.json', {'result': {}})
    audit = write('audit.json', {'result': {'runtime_provenance': {
        'distinct_identity_count': 1, 'identities': [{'git_sha': revision,
            'runtime_role': 'primary', 'execution_mode': 'paper', 'allow_live': False,
            'shadow_only': False}]}}})
    gate = dict(date='2026-09-22', status='passed', git_sha=revision,
                normalized_manifest_sha256='sha256:' + 'b' * 64,
                source_inventory_sha256='sha256:' + 'c' * 64, data_audit_sha256=digest(audit),
                execution_quality_sha256=digest(execution))
    proof = dict(schema='polyedge.primary_oci_day_proof.v1', verified=True,
                 segments_verified=144, segments=[{}] * 144, listed_objects=['object'] * 288,
                 head_receipts=[{}] * 288, date=gate['date'], status='passed', exhaustive_listing=True,
                 normalized_manifest_sha256=gate['normalized_manifest_sha256'],
                 source_inventory_sha256=gate['source_inventory_sha256'], source=dict(
                     provider='oci_object_storage', namespace='axl4ryaas895', region='sa-bogota-1',
                     bucket='bot-events', prefix='events-oci-hot7-v1/', runtime_role='primary',
                     execution_mode='paper', shadow_only=False))
    wallet, registry, binary = [write(name, {}) for name in ('wallet', 'registry', 'binary')]
    prior = dict(maximum_total_selection_replays=500, profile_replays=20,
                 sweep_max_experiments=96, sweep_replays=480, candidate_evaluation_started=False,
                 wallet_config_sha256=digest(wallet), profile_registry_sha256=digest(registry))
    args = SimpleNamespace(pilot_gate=write('gate.json', gate),
                           source_proof=write('proof.json', proof), audit=audit, execution_quality=execution,
                           candidate_plan=write('plan.json', prior), wallet=wallet,
                           registry=registry, binary=binary, git_sha=revision, start='2026-09-24')
    now = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
    holdout, plan, protocol = prepare(args, now)
    assert holdout['start_inclusive'] == '2026-10-24T00:00:00Z'
    assert holdout['end_exclusive'] == '2026-11-21T00:00:00Z'
    assert protocol['final_closure_not_before'] == '2026-11-22T00:00:00Z'
    assert plan['maximum_total_selection_replays'] == 500
    assert protocol['schema'] == 'polyedge.primary_prospective.v2'
    assert protocol['markout_observation_policy']['executable_completion_minimum'] == 0.95
    assert protocol['markout_observation_policy']['application'] == 'future capture only; never readmit a failed pilot'
    for document, field, bad in ((gate, 'status', 'failed'),
                                 (proof, 'exhaustive_listing', False),
                                 (proof, 'schema', 'unknown'),
                                 (proof, 'normalized_manifest_sha256', 'different'),
                                 (gate, 'data_audit_sha256', 'different'),
                                 (gate, 'execution_quality_sha256', 'different'),
                                 (gate, 'git_sha', 'b' * 40)):
        changed = copy.deepcopy(document)
        changed[field] = bad
        path = args.pilot_gate if document is gate else args.source_proof
        Path(path).write_text(json.dumps(changed))
        try:
            prepare(args, now)
        except ValueError:
            pass
        else:
            raise AssertionError('invalid pilot accepted: ' + field)
        Path(path).write_text(json.dumps(document))
    try:
        prepare(args, datetime(2026, 9, 24, tzinfo=timezone.utc))
    except ValueError:
        pass
    else:
        raise AssertionError('past experiment accepted')

    class Client:
        def __init__(self):
            self.objects = {}
            self.modified = 'Wed, 23 Sep 2026 12:00:00 GMT'

        def put_object(self, namespace, bucket, key, body, **kwargs):
            assert kwargs['if_none_match'] == '*'
            if key in self.objects:
                raise ValueError('already exists')
            self.objects[key] = body

        def get_object(self, namespace, bucket, key, **kwargs):
            if 'if_match' in kwargs:
                assert kwargs['if_match'] == 'fixed'
            return SimpleNamespace(data=SimpleNamespace(content=self.objects[key]),
                                   headers={'etag': 'fixed', 'last-modified': self.modified})

    files = ('holdout-contract.json', 'candidate-plan.json', 'protocol.json', 'polyedge-rs',
             'wallet.json', 'frozen_candidates.yaml', 'pilot-gate.json', 'pilot-source-proof.json',
             'pilot-audit.json', 'pilot-execution-quality.json')
    for name in files:
        write(name, protocol if name == 'protocol.json' else {'initial': True})
    write('manifest.json', {'files': {name: digest(root / name) for name in files}})
    client = Client()
    remote = publish(client, root, protocol, {'namespace': 'n', 'bucket': 'b'}, lambda: now)
    assert remote['anchor']['name'] == 'research-contracts/primary-20260924/manifest.json'
    write('remote-preregistration.json', remote)
    assert verify_frozen(root, args.start, client)['id'] == protocol['id']
    # Even a correctly anchored contract must carry this evaluator's exact method.
    original_manifest = (root / 'manifest.json').read_bytes()
    for field, bad in [('schema', 'polyedge.primary_prospective.v1'),
                       ('markout_observation_policy', {})]:
        changed = copy.deepcopy(protocol)
        changed[field] = bad
        write('protocol.json', changed)
        write('manifest.json', {'files': {name: digest(root / name) for name in files}})
        client.objects[remote['anchor']['name']] = (root / 'manifest.json').read_bytes()
        receipt = copy.deepcopy(remote)
        receipt['anchor']['sha256'] = digest(root / 'manifest.json')
        write('remote-preregistration.json', receipt)
        try:
            verify_frozen(root, args.start, client)
        except ValueError as error:
            assert 'observation policy' in str(error)
        else:
            raise AssertionError('different frozen method accepted')
    write('protocol.json', protocol)
    (root / 'manifest.json').write_bytes(original_manifest)
    client.objects[remote['anchor']['name']] = original_manifest
    write('remote-preregistration.json', remote)
    # Updating both a contract and its local manifest cannot replace the OCI anchor.
    write('candidate-plan.json', {'changed': True})
    write('manifest.json', {'files': {name: digest(root / name) for name in files}})
    try:
        verify_frozen(root, args.start, client)
    except ValueError:
        pass
    else:
        raise AssertionError('coordinated local contract/manifest mutation accepted')
    for name in ('holdout-contract.json', 'candidate-plan.json', 'protocol.json', 'manifest.json'):
        write(name, {'changed': True})
    try:
        publish(client, root, protocol, {'namespace': 'n', 'bucket': 'b'}, lambda: now)
    except ValueError:
        pass
    else:
        raise AssertionError('second contract for same experiment identity accepted')
    client = Client()
    client.modified = 'Thu, 24 Sep 2026 00:00:00 GMT'
    try:
        publish(client, root, protocol, {'namespace': 'n', 'bucket': 'b'}, lambda: now)
    except ValueError:
        pass
    else:
        raise AssertionError('late server timestamp accepted')
print('primary preregistration checks passed')
