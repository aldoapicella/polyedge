# qset-v3 OCI operations runbook

This is an additive, isolated bundle for `campaign-2026-08-23-qset-v3`.
It does not replace or alter qset-v1/v2. The writer is paper-only and starts
on the pointer-only preflight prefix; the runtime configuration switches its
write prefix exactly at `2026-08-23T00:00:00Z` without a restart.

## Hard gates

Do not install or enable the bundle until all of these are true:

- A final (not draft) freeze manifest supplies the full source SHA-256,
  immutable manifest location, immutable ARM64 writer/sealer image digests,
  and their matching full OCI revision labels. Set
  `SHADOW_CODE_FREEZE_FINALIZED=true` only after that artifact is final.
- The qset-v3 image implements `seal-qset-v3-day`; the current repository
  bundle deliberately does not substitute an older seal command.
- The qset-v3 UAMI is independent from `shadow-qset`, has one federated
  credential for subject `spiffe://polyedge.local/conduit/shadow-qset-v3-writer`, and
  its scoped data-plane grants cover only the new raw, research, and control
  containers plus `ShadowQsetV3EventIndex`, `ShadowQsetV3ChartSeries`, and
  `ShadowQsetV3MarketCatalog`.
- The Azure data plane has these new containers before startup:
  `polyedge-shadow-qset-v3-events`, `polyedge-research-qset-v3`, and
  `polyedge-qset-v3-control`. No v1/v2 container or table is in the v3
  role assignment. The writer UAMI is custom no-delete on v3 raw only, read-only on v3 control, contributor only on the three v3 tables, and has no v3 research access.
- `/srv/polyedge-ring` is its intended mount with at least 15 GiB free and the
  boot-disk guard passes. The writer is capped at 0.5 CPU/1 GiB and journals
  through the existing capped persistent journal.

## Installation and cutover

Run this only after the gates above, with the existing SPIRE template and
Podman network already installed. Use the final values in the installed files;
the repository examples intentionally remain blank/zero-digest.

```sh
sudo groupadd --system --gid 979 polyedge-identity-shadow-qset-v3-writer
sudo useradd --system --uid 983 --no-create-home --gid polyedge-identity-shadow-qset-v3-writer --shell /usr/sbin/nologin polyedge-identity-shadow-qset-v3-writer
sudo install -d -m 0700 -o polyedge-identity-shadow-qset-v3-writer -g polyedge-identity-shadow-qset-v3-writer /run/polyedge-federated-shadow-qset-v3-writer
sudo install -m 0755 ops/conduit/bin/polyedge-federated-token-refresh /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-qset-v3-seal-days /usr/local/libexec/
sudo install -m 0644 ops/conduit/quadlets/polyedge-shadow-qset-v3.container /etc/containers/systemd/
sudo install -D -m 0644 ops/conduit/systemd/polyedge-federated-token@shadow-qset-v3-writer.service.d/override.conf /etc/systemd/system/polyedge-federated-token@shadow-qset-v3-writer.service.d/override.conf
sudo install -m 0644 ops/conduit/systemd/polyedge-qset-v3-seal-days.service ops/conduit/systemd/polyedge-qset-v3-first-seal.timer /etc/systemd/system/
sudo install -m 0600 ops/conduit/env/shadow-qset-v3.env.example /etc/polyedge/shadow-qset-v3.env
sudo install -m 0640 ops/conduit/env/qset-v3-sealer.env.example /etc/polyedge/qset-v3-sealer.env
sudo chown root:root /etc/polyedge/qset-v3-sealer.env
sudo systemctl daemon-reload
```

Register the new SPIRE workload entry and Azure federated credential for the
new UAMI through the reviewed identity process, then confirm the decoded token
claims locally without printing the token. They must be RS256, the configured
HTTPS issuer, the exact v3 subject, the sole Azure Token Exchange audience, and
at most six minutes long. Do not reuse `shadow-qset`'s token, account, FIC, or
role assignment.

Edit the two installed environment files with the final freeze bindings and
the same immutable writer image/revision values. Set `EXECUTION_FREEZE_ARTIFACT_PATH`
to the reviewed relative blob path, set `EXECUTION_FREEZE_SHA256` equal to
`SHADOW_CODE_FREEZE_SHA256`, and set `SHADOW_CODE_FREEZE_MANIFEST` exactly to
`azure://ACCOUNT/polyedge-qset-v3-control/RELATIVE_PATH`. The sealer passes only
`RELATIVE_PATH` and its hash to `seal-qset-v3-day`; it never passes the Azure URI. Replace only the installed
v3 Quadlet's zero image digest, pull that exact digest after the boot-disk pull
gate, and verify `linux/arm64` and `org.opencontainers.image.revision`. The
v3 bundle intentionally does not use the frozen shared digest-deploy helper.

Before the boundary, enable only the new token timer and start the v3 writer
alongside qset-v2. Its preflight remains paper-only and writes only
`shadow-events/preflight/campaign-2026-08-23-qset-v3`.

```sh
sudo systemctl enable --now polyedge-federated-token@shadow-qset-v3-writer.timer
sudo systemctl start polyedge-shadow-qset-v3.service
sudo podman healthcheck run polyedge-shadow-qset-v3
sudo systemctl show -p MainPID -p ActiveEnterTimestamp polyedge-shadow-qset-v3.service
```

Immediately before and after `2026-08-23T00:00:00Z`, verify the service remains
active with the same `MainPID`. Do not restart it at the boundary. The runtime
switches from the preflight prefix to
`shadow-events/campaign-2026-08-23-qset-v3` from the configured UTC clock.
Check its journal for the observed effective prefix and retain that evidence.

The first seal is one-shot and disabled until the two complete UTC days exist.
At `2026-08-25 02:15 UTC`, it validates exactly August 23 and August 24 while
the writer is healthy, then takes the existing `/run/polyedge/research.lock`,
fences the v3 writer, seals both days, writes deterministic receipts under
`/srv/polyedge-ring/migration/qset-v3-seal/`, and restarts/health-checks only
the v3 writer. It fails closed on any mismatch or a conflicting receipt.

```sh
sudo systemctl enable --now polyedge-qset-v3-first-seal.timer
sudo systemctl list-timers polyedge-qset-v3-first-seal.timer
```

No recurring daily qset-v3 job is enabled by this bundle. Add one only after
the first two receipts and the separate research implementation are accepted.
