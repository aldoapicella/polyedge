# qset-v4 OCI operations runbook

This is an additive, isolated bundle for `campaign-2026-08-24-qset-v4`.
It does not replace or alter qset-v1/v2/v3. The writer is paper-only and starts
on the pointer-only preflight prefix; the runtime configuration switches its
write prefix exactly at `2026-08-24T00:00:00Z` without a restart.

## Hard gates

Do not install or enable the bundle until all of these are true:

- A final (not draft) freeze manifest supplies the full source SHA-256,
  immutable manifest location, immutable ARM64 writer/sealer image digests,
  and their matching full OCI revision labels. Set
  `SHADOW_CODE_FREEZE_FINALIZED=true` only after that artifact is final.
- The qset-v4 image implements `seal-qset-v4-day`; the current repository
  bundle deliberately does not substitute an older seal command.
- The existing qset-v3 writer and processor UAMIs are reused sequentially only after the v3 writer is stopped. Keep their existing `spiffe://polyedge.local/conduit/shadow-qset-v3-writer` federated credential and token lane; do not create a v4 UAMI, FIC, or SPIFFE subject. Before applying v4 scopes, remove their old v3 raw, research, control, and table assignments. The v4 scopes cover only the new raw, research, and control containers plus `ShadowQsetV4EventIndex`, `ShadowQsetV4ChartSeries`, and `ShadowQsetV4MarketCatalog`.
- The Azure data plane has these new containers before startup:
  `polyedge-shadow-qset-v4-events`, `polyedge-research-qset-v4`, and
  `polyedge-qset-v4-control`. No v1/v2/v3 container or table is in the v4
  role assignment. The writer UAMI is custom no-delete on v4 raw only, read-only on v4 control, contributor only on the three v4 tables, and has no v4 research access.
- The exact conservative prior exists in the v4 research container at the
  hash-named reports/research/venue-probe/models path. Its source and
  destination readback both hash to
  sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.
- `/srv/polyedge-ring` is its intended mount with at least 15 GiB free and the
  boot-disk guard passes. The writer is capped at 0.5 CPU/1 GiB and journals
  through the existing capped persistent journal.

## Installation and cutover

Run this only after the gates above, with the existing SPIRE template and
Podman network already installed. Use the final values in the installed files;
the repository examples leave only campaign freeze, image, client, and account
bindings blank. The conservative-prior URI and hash are exact.

```sh
sudo install -m 0755 ops/conduit/bin/polyedge-federated-token-refresh /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-qset-v4-seal-days /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-qset-v4-boundary-guard /usr/local/libexec/
sudo install -m 0644 ops/conduit/quadlets/polyedge-shadow-qset-v4.container /etc/containers/systemd/
sudo install -m 0644 ops/conduit/systemd/polyedge-qset-v4-seal-days.service ops/conduit/systemd/polyedge-qset-v4-first-seal.timer /etc/systemd/system/
sudo install -m 0644 ops/conduit/systemd/polyedge-qset-v4-boundary@.service ops/conduit/systemd/polyedge-qset-v4-boundary-pre.timer ops/conduit/systemd/polyedge-qset-v4-boundary-post.timer /etc/systemd/system/
sudo install -m 0600 ops/conduit/env/shadow-qset-v4.env.example /etc/polyedge/shadow-qset-v4.env
sudo install -m 0640 ops/conduit/env/qset-v4-sealer.env.example /etc/polyedge/qset-v4-sealer.env
sudo chown root:root /etc/polyedge/qset-v4-sealer.env
sudo systemctl daemon-reload
```

Reuse the existing qset-v3 SPIRE workload entry and Azure federated credential only after the v3 writer is stopped, then confirm the decoded token claims locally without printing the token. They must be RS256, the configured HTTPS issuer, subject `spiffe://polyedge.local/conduit/shadow-qset-v3-writer`, the sole Azure Token Exchange audience, and at most six minutes long. Do not create a v4 FIC or SPIFFE subject, and do not reuse `shadow-qset`'s token, account, FIC, or role assignment.

Edit the two installed environment files with the final freeze bindings and
the same immutable writer image/revision values. Set `EXECUTION_FREEZE_ARTIFACT_PATH`
to the reviewed relative blob path, set `EXECUTION_FREEZE_SHA256` equal to
`SHADOW_CODE_FREEZE_SHA256`, and set `SHADOW_CODE_FREEZE_MANIFEST` exactly to
`azure://ACCOUNT/polyedge-qset-v4-control/RELATIVE_PATH`. The sealer passes only
`RELATIVE_PATH` and its hash to `seal-qset-v4-day`; it never passes the Azure URI. Replace only the installed
v4 Quadlet's zero image digest, pull that exact digest after the boot-disk pull
gate, and verify `linux/arm64` and `org.opencontainers.image.revision`. The
v4 bundle intentionally does not use the frozen shared digest-deploy helper. Set `POLYEDGE_QSET_V4_WRITER_IMAGE` to that exact immutable digest and `POLYEDGE_QSET_V4_WRITER_GIT_SHA` to its matching full OCI revision; the boundary guard rejects a container whose image, revision, campaign resources, or final freeze binding differs.

Before enabling the writer, copy the existing immutable conservative prior into
the exact v4 research path. Use only a temporary source-container reader and
destination-container custom no-delete writer for the signed-in operator, then
remove those temporary assignments. Do not use account keys or SAS.

    source_container=polyedge-research
    destination_container=polyedge-research-qset-v4
    model_blob=reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json
    expected_model_sha=91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4
    model_tmp=$(mktemp -d)
    az storage blob download --auth-mode login --account-name stpolyedge6urdjr5nmwx7w --container-name "$source_container" --name "$model_blob" --file "$model_tmp/source.json"
    test "$(sha256sum "$model_tmp/source.json" | cut -d' ' -f1)" = "$expected_model_sha"
    az storage blob upload --auth-mode login --account-name stpolyedge6urdjr5nmwx7w --container-name "$destination_container" --name "$model_blob" --file "$model_tmp/source.json" --overwrite false
    az storage blob download --auth-mode login --account-name stpolyedge6urdjr5nmwx7w --container-name "$destination_container" --name "$model_blob" --file "$model_tmp/readback.json"
    test "$(sha256sum "$model_tmp/readback.json" | cut -d' ' -f1)" = "$expected_model_sha"

Retain the source/destination ETags and hashes in the campaign control proof.
The writer must remain stopped if the copy or readback proof fails.

Before the boundary, enable the existing v3 token timer and start the v4 writer.
Keep qset-v3 stopped; the v4 service never reads or writes its containers. Its preflight remains paper-only and writes only
`shadow-events/preflight/campaign-2026-08-24-qset-v4`.

```sh
sudo systemctl enable --now polyedge-federated-token@shadow-qset-v3-writer.timer
sudo systemctl start polyedge-shadow-qset-v4.service
sudo podman healthcheck run polyedge-shadow-qset-v4
sudo systemctl show -p MainPID -p ActiveEnterTimestamp polyedge-shadow-qset-v4.service
sudo systemctl disable --now polyedge-shadow-qset-v3.service polyedge-qset-v3-boundary-pre.timer polyedge-qset-v3-boundary-post.timer polyedge-qset-v3-first-seal.timer
sudo systemctl enable --now polyedge-qset-v4-boundary-pre.timer polyedge-qset-v4-boundary-post.timer
```

Immediately before and after `2026-08-24T00:00:00Z`, verify the service remains
active with the same `MainPID`. Do not restart it at the boundary. The runtime
switches from the preflight prefix to
`shadow-events/campaign-2026-08-24-qset-v4` from the configured UTC clock.
The pre/post timers invoke the local-only guard at `2026-08-23 23:59:30 UTC` and `2026-08-24 00:01:30 UTC`. It fails closed unless the v4 recorder is clean and its intent publisher is exactly configured, prepared, and pointer-only preflight; the v3 writer and all v3 boundary/first-seal timers are stopped and disabled; and v2 remains healthy. It writes root-owned, no-overwrite receipts under `/srv/polyedge-ring/migration/qset-v4/boundary/` without mutating Azure evidence. After committing the post receipt, it disables only v2's unsafe first-seal timer. Check its journal and retain both receipts.

Before any authorized rollout, use `ops/conduit/bin/polyedge-qset-v4-source-freeze build OUTPUT` from a clean committed checkout with `FREEZE_RESEARCH_IMAGE` set to the reviewed immutable digest. Only an explicitly authorized operator may run `lock-and-upload` to lock the v4 control-container policy, upload with overwrite disabled, and hash-readback the exact manifest. Then run `ops/conduit/bin/polyedge-qset-v4-rbac-handoff check` to prove the eight exact v3 writer/processor assignments and all retired initiators. Its `apply` mode is the only handoff path: it deletes those captured assignments, verifies zero old scopes, deploys v4-only scopes, proves old v3 containers deny the reused writer identity, and never starts v4. Do not run either mutating mode during review.

The first seal is one-shot and disabled until the two complete UTC days exist.
At `2026-08-26 02:15 UTC`, it validates exactly August 24 and August 25 while
the writer is healthy, then takes the existing `/run/polyedge/research.lock`,
fences the v4 writer, seals both days, writes deterministic receipts under
`/srv/polyedge-ring/migration/qset-v4-seal/`, and restarts/health-checks only
the v4 writer. It fails closed on any mismatch or a conflicting receipt.

```sh
sudo systemctl enable --now polyedge-qset-v4-first-seal.timer
sudo systemctl list-timers polyedge-qset-v4-first-seal.timer
```

No recurring daily qset-v4 job is enabled by this bundle. After both seal
receipts are accepted:

1. Run an isolated Bicep what-if with deployProcessorJob=true, the exact
   multi-architecture digest, its 40-character revision, both receipt inventory
   hashes, and the final freeze path/hash. Require no delete and no unrelated
   modify.
2. Deploy the manual processor job, read back its processor UAMI and three
   scoped no-delete/read grants, and prove its negative access to funded,
   qset-v1/v2/v3, Key Vault, Service Bus, and unrelated storage.
3. Start exactly one execution. Require a successful terminal replica, hash
   every v4 campaign output, and validate the atomic report bundle before
   enabling any recurring schedule or considering Azure retirement.
