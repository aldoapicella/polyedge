#!/usr/bin/env python3
"""Freeze one future primary experiment after a verified, separate pilot day."""
import argparse
import base64
import copy
import hashlib
import json
import re
import shutil
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from verify_primary_oci_day import BUCKET, NAMESPACE, PREFIX, REGION

MARKOUT_OBSERVATION_POLICY = dict(
    schema='polyedge.markout_observation_policy.v2',
    horizons_seconds=[1, 5, 30], maximum_observation_delay_ms=2000,
    executable_completion_minimum=0.95,
    denominator='all eligible fill lifecycles, including observed illiquidity',
    observed_unexecutable='only after the inclusive deadline; exact same-token timely durable REST '
        'snapshot with the required exit side empty, no price or PnL imputation',
    missing_late_malformed_or_unjoined='blocking; historical missing events stay unchanged',
    statistics='priced rows only, explicitly incomplete; no positive markout profitability bound '
        'when the evidence window includes observed illiquidity',
    application='future capture only; never readmit a failed pilot')


def digest(path):
    with Path(path).open('rb') as stream:
        return 'sha256:' + hashlib.file_digest(stream, 'sha256').hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_source_proof(proof, gate):
    require(proof.get('schema') == 'polyedge.primary_oci_day_proof.v1'
            and proof.get('status') == 'passed' and proof.get('verified') is True
            and proof.get('exhaustive_listing') is True and proof.get('segments_verified') == 144
            and len(proof.get('segments', [])) == 144 and len(proof.get('listed_objects', [])) == 288
            and len(proof.get('head_receipts', [])) == 288,
            'complete authenticated OCI day proof required')
    binding = dict(provider='oci_object_storage', namespace=NAMESPACE, region=REGION,
                   bucket=BUCKET, prefix=PREFIX + '/', runtime_role='primary',
                   execution_mode='paper', shadow_only=False)
    require(all(proof.get('source', {}).get(k) == v for k, v in binding.items()),
            'OCI primary source binding differs')
    require(proof['date'] == gate['date'], 'pilot dates differ')
    for field in ('normalized_manifest_sha256', 'source_inventory_sha256'):
        require(proof[field] == gate[field], 'day proof differs: ' + field)


def verify_frozen(out, expected_start, client=None):
    out = Path(out)
    start = datetime.combine(date.fromisoformat(expected_start), time(), timezone.utc)
    identity = 'primary-' + start.strftime('%Y%m%d')
    key = f'research-contracts/{identity}/manifest.json'
    receipt = json.loads((out / 'remote-preregistration.json').read_text())
    anchor = receipt['anchor']
    require(receipt['status'] == 'verified' and anchor['anchor'] is True
            and anchor['name'] == key, 'fixed preregistration anchor required')
    if client is None:
        import oci
        client = oci.object_storage.ObjectStorageClient(
            {'region': REGION}, timeout=(10, 30),
            signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner())
    response = client.get_object(NAMESPACE, BUCKET, key, if_match=anchor['etag'])
    body = response.data.content
    require(response.headers['etag'] == anchor['etag']
            and 'sha256:' + hashlib.sha256(body).hexdigest() == anchor['sha256']
            and parsedate_to_datetime(response.headers['last-modified']) < start,
            'authenticated preregistration anchor differs')
    require(body == (out / 'manifest.json').read_bytes(), 'local manifest differs from OCI anchor')
    manifest = json.loads(body)
    required = {'protocol.json', 'candidate-plan.json', 'holdout-contract.json', 'polyedge-rs',
                'wallet.json', 'frozen_candidates.yaml', 'pilot-gate.json', 'pilot-source-proof.json',
                'pilot-audit.json', 'pilot-execution-quality.json'}
    require(set(manifest['files']) == required, 'frozen artifact inventory differs')
    for name, expected in manifest['files'].items():
        require(name not in ('.', '..') and Path(name).name == name,
                'unsafe frozen artifact name')
        path = out / name
        require(path.is_file() and not path.is_symlink() and digest(path) == expected,
                'frozen artifact changed: ' + name)
    protocol = json.loads((out / 'protocol.json').read_text())
    require(protocol.get('schema') == 'polyedge.primary_prospective.v2'
            and protocol.get('markout_observation_policy') == MARKOUT_OBSERVATION_POLICY,
            'frozen observation policy differs from the evaluator')
    require(protocol['id'] == identity and protocol['training']['start_inclusive'][:10] == expected_start,
            'frozen experiment identity differs')
    return protocol


def prepare(args, now):
    gate, source, audit, prior = [json.loads(Path(p).read_text()) for p in
                                 (args.pilot_gate, args.source_proof, args.audit, args.candidate_plan)]
    start = date.fromisoformat(args.start)
    pilot = date.fromisoformat(gate['date'])
    require(start.isoformat() == args.start, 'start must be a canonical UTC date')
    require(datetime.combine(start, time(), timezone.utc) > now, 'experiment must start in the future')
    require(pilot < now.date() and start > pilot, 'pilot must be a separate closed UTC day')
    require(gate['status'] == 'passed', 'pilot data-quality gate failed')
    validate_source_proof(source, gate)
    require(digest(args.audit) == gate['data_audit_sha256'], 'pilot audit hash mismatch')
    require(digest(args.execution_quality) == gate['execution_quality_sha256'],
            'pilot execution-quality hash mismatch')
    require(re.fullmatch('[0-9a-f]{40}', args.git_sha) is not None, 'invalid source revision')
    require(gate['git_sha'] == args.git_sha, 'pilot evaluator revision differs')
    provenance = audit['result']['runtime_provenance']
    identities = provenance['identities']
    require(provenance['distinct_identity_count'] == 1 and len(identities) == 1,
            'pilot needs exactly one runtime identity')
    identity = identities[0]
    require(identity['git_sha'] == args.git_sha and identity['runtime_role'] == 'primary'
            and identity['execution_mode'] == 'paper' and identity['allow_live'] is False
            and identity['shadow_only'] is False, 'pilot capture binding differs')
    require(prior['maximum_total_selection_replays'] == 500 and prior['profile_replays'] == 20
            and prior['sweep_max_experiments'] == 96 and prior['sweep_replays'] == 480
            and prior['candidate_evaluation_started'] is False, 'frozen candidate budget differs')
    require(digest(args.wallet) == prior['wallet_config_sha256'], 'wallet contract changed')
    require(digest(args.registry) == prior['profile_registry_sha256'], 'candidate registry changed')

    def stamp(offset):
        return (start + timedelta(days=offset)).isoformat() + 'T00:00:00Z'

    frozen = now.isoformat()
    experiment = 'primary-' + start.strftime('%Y%m%d')
    selection = dict(start_inclusive=stamp(0), end_exclusive=stamp(29),
                     initial_training_day=start.isoformat(), validation_days=28)
    holdout = dict(schema='polyedge.primary_holdout.v1', id=experiment,
                   frozen_at=frozen, start_inclusive=stamp(30), end_exclusive=stamp(58),
                   test_carry=dict(start_inclusive=stamp(58), end_exclusive=stamp(59)),
                   complete_utc_days=28, selection_window=selection,
                   sources={key: value for key, value in source['source'].items()
                            if key not in ('listing_prefix', 'market_payloads_read')},
                   maximum_selection_replays=500,
                   open_policy='exactly_once_after_fixed_winner_and_positive_validation_bounds',
                   holdout_inspection_before_winner='metadata_and_integrity_only',
                   minimum_block_days=7, minimum_blocks=4, bootstrap_resamples=10000,
                   no_retuning_after_holdout=True, capital_promotion_authorized_by_research=False)
    plan = copy.deepcopy(prior)
    plan.update(frozen_at=frozen, git_sha=args.git_sha,
                binary_sha256=digest(args.binary).removeprefix('sha256:'),
                selection_period=selection, protocol='protocol.json',
                predecessor_candidate_plan=dict(sha256=digest(args.candidate_plan)))
    protocol = dict(schema='polyedge.primary_prospective.v2', id=experiment, frozen_at=frozen,
                    status='preregistered_waiting_for_future_data', training=dict(
                        start_inclusive=stamp(0), end_exclusive=stamp(1), models_fitted=False),
                    validation=dict(start_inclusive=stamp(1), end_exclusive=stamp(29)),
                    selection_carry=dict(start_inclusive=stamp(29), end_exclusive=stamp(30)),
                    final_closure_not_before=stamp(59),
                    market_population='marketstart >= partition start and marketend < partition end',
                    carry_admission='settlement only for included lifecycles; no new decisions, '
                        'opportunities or bootstrap days; all exposure must close by carry end',
                    daily_admission='source-pinned native primary audit and execution-quality gates, '
                        'authenticated exhaustive source bindings, one runtime identity, '
                        'recorder continuity and no blocking or unclassified warnings',
                    markout_observation_policy=copy.deepcopy(MARKOUT_OBSERVATION_POLICY),
                    failure_policy='terminal blocker; never shift, extend or automatically replace this window',
                    predecessor_disposition='failed experiments and original evidence remain immutable',
                    candidate_evaluations=0, winner=None, holdout_opened=False,
                    funded_accounting='separate authenticated cash-flow-adjusted equity and lifecycle '
                        'reconciliation; research cannot authorize funded execution',
                    executables=dict(git_sha=args.git_sha, sha256=digest(args.binary)),
                    pilot=dict(date=pilot.isoformat(), gate_sha256=digest(args.pilot_gate),
                               source_proof_sha256=digest(args.source_proof),
                               audit_sha256=digest(args.audit),
                               execution_quality_sha256=digest(args.execution_quality)))
    return holdout, plan, protocol


def publish(client, out, protocol, source, now=lambda: datetime.now(timezone.utc)):
    start = datetime.fromisoformat(protocol['training']['start_inclusive'].replace('Z', '+00:00'))
    rows = []
    names = ('holdout-contract.json', 'candidate-plan.json', 'protocol.json', 'manifest.json')
    for name in (*names, 'manifest.json'):
        require(now() < start, 'preregistration deadline elapsed during publication')
        body = (out / name).read_bytes()
        checksum = hashlib.sha256(body).digest()
        # The final, fixed-name anchor permits only one contract for this identity.
        anchor = len(rows) == len(names)
        suffix = name if anchor else f'{checksum.hex()}/{name}'
        key = f"research-contracts/{protocol['id']}/{suffix}"
        client.put_object(source['namespace'], source['bucket'], key, body,
                          if_none_match='*', opc_content_sha256=base64.b64encode(checksum).decode())
        response = client.get_object(source['namespace'], source['bucket'], key)
        require(response.data.content == body, 'OCI preregistration readback mismatch')
        modified = parsedate_to_datetime(response.headers['last-modified'])
        require(modified < start and now() < start, 'OCI freeze was not before the experiment')
        rows.append(dict(name=key, sha256='sha256:' + checksum.hex(), anchor=anchor,
                         etag=response.headers['etag'], last_modified=response.headers['last-modified']))
    return dict(status='verified', objects=rows, anchor=rows[-1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('pilot-gate', 'source-proof', 'audit', 'execution-quality', 'candidate-plan', 'wallet',
                 'registry', 'binary', 'git-sha', 'start', 'out'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--publish', action='store_true', help='Publish immutable OCI preregistration')
    args = parser.parse_args()
    holdout, plan, protocol = prepare(args, datetime.now(timezone.utc))
    out = Path(args.out)
    out.mkdir(mode=0o700, parents=True, exist_ok=False)

    def write(name, value):
        with (out / name).open('x') as stream:
            json.dump(value, stream, indent=2)
            stream.write('\n')
        return digest(out / name)

    holdout_hash = write('holdout-contract.json', holdout)
    plan['holdout_contract_sha256'] = holdout_hash
    plan_hash = write('candidate-plan.json', plan)
    protocol.update(candidate_plan_sha256=plan_hash, holdout_contract_sha256=holdout_hash)
    write('protocol.json', protocol)
    for original, name in ((args.binary, 'polyedge-rs'), (args.wallet, 'wallet.json'),
                           (args.registry, 'frozen_candidates.yaml'),
                           (args.pilot_gate, 'pilot-gate.json'),
                           (args.source_proof, 'pilot-source-proof.json'), (args.audit, 'pilot-audit.json'),
                           (args.execution_quality, 'pilot-execution-quality.json')):
        shutil.copyfile(original, out / name)
    (out / 'polyedge-rs').chmod(0o500)
    hashes = {p.name: digest(p) for p in sorted(out.iterdir())}
    require(hashes['polyedge-rs'] == protocol['executables']['sha256'], 'binary changed during freeze')
    require(hashes['wallet.json'] == plan['wallet_config_sha256'], 'wallet changed during freeze')
    require(hashes['frozen_candidates.yaml'] == plan['profile_registry_sha256'],
            'registry changed during freeze')
    for name, key in (('pilot-gate.json', 'gate_sha256'), ('pilot-source-proof.json', 'source_proof_sha256'),
                      ('pilot-audit.json', 'audit_sha256'), ('pilot-execution-quality.json', 'execution_quality_sha256')):
        require(hashes[name] == protocol['pilot'][key], 'pilot evidence changed during freeze')
    write('manifest.json', dict(files=hashes, remote_preregistration_required=True,
                               candidate_evaluations=0, holdout_opened=False))
    if args.publish:
        import oci
        source = holdout['sources']
        require(source['provider'] == 'oci_object_storage', 'OCI source required')
        client = oci.object_storage.ObjectStorageClient(
            {'region': source['region']}, timeout=(10, 30),
            signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner())
        write('remote-preregistration.json', publish(client, out, protocol, source))
    for path in out.iterdir():
        path.chmod(0o500 if path.name == 'polyedge-rs' else 0o400)
    print(json.dumps(dict(status='frozen_locally', directory=str(out),
                          remote_preregistration_required=not args.publish, start=args.start)))


if __name__ == '__main__':
    main()
