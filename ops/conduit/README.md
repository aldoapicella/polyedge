# Conduit host bundle

This is a rootful Podman/systemd bundle for the ARM64 `conduit-dev` host. It
uses immutable image digests, a private Podman network, a loopback-only
frontend, and Caddy as the sole public ingress. It has no Docker, Compose,
secrets, or automatic deployment.

## Hard gates

Do **not** start this stack or enable any timer until an approved OCI-to-Azure
authentication design exists. The active recorder and `with-azure-lease`
commands use Azure managed identity; OCI does not provide the Container Apps
identity endpoint. A read SAS is not sufficient for the writers or lease.

The API Quadlet persists its local recorder under `/srv/polyedge-ring`. Verify
that path is the 150-GB block-volume mount (not a directory on `/`) before every
start: `findmnt -T /srv/polyedge-ring && df -h /srv/polyedge-ring`.

Do **not** enable daily, replay, or qset shadow work on the boot disk. Their
working directories live under `/srv/polyedge-ring/jobs`; the runner refuses to
start with less than 40 GiB free. All Azure jobs remain disabled unless
`/etc/polyedge/ENABLE_AZURE_JOBS` exists, and their timers are not enabled by
the install sequence.

The legacy shadow schedule is intentionally absent. `shadow-qset` permits only
`campaign-2026-07-28-qset-v1` and remains disabled until manually approved.

## Install after both gates are approved

On Ubuntu/Debian, first verify the actual OS and packages, then install the
minimal runtime:

```sh
sudo apt-get install --no-install-recommends podman caddy curl
sudo install -d -m 0750 /etc/polyedge/jobs /srv/polyedge-ring/jobs
sudo install -d -m 0700 /etc/polyedge/credentials/{api,freshness,hourly,daily,replay,shadow-qset}
sudo install -m 0644 ops/conduit/quadlets/* /etc/containers/systemd/
sudo install -m 0644 ops/conduit/systemd/* /etc/systemd/system/
sudo install -m 0755 ops/conduit/bin/polyedge-run-job /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-ring-sync /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-ring-health /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-github-deploy /usr/local/libexec/
sudo install -m 0755 ops/conduit/bin/polyedge-quadlet-deploy /usr/local/sbin/
sudo install -m 0440 ops/conduit/sudoers/polyedge-deploy /etc/sudoers.d/
sudo install -m 0644 ops/conduit/journald/polyedge.conf /etc/systemd/journald.conf.d/
sudo install -m 0644 ops/conduit/caddy/Caddyfile /etc/caddy/Caddyfile
sudo install -m 0600 ops/conduit/env/api.env.example /etc/polyedge/api.env
sudo install -m 0600 ops/conduit/env/frontend.env.example /etc/polyedge/frontend.env
sudo install -m 0600 ops/conduit/env/funded-signer.env.example /etc/polyedge/funded-signer.env
sudo install -m 0600 ops/conduit/env/ring.env.example /etc/polyedge/ring.env
sudo systemctl daemon-reload
sudo systemctl restart systemd-journald
```

The GitHub deploy key belongs to a locked `polyedge-deploy` account. Its only
authorized-key command is `/usr/local/libexec/polyedge-github-deploy` with the
`restrict` option. The wrapper and sudoers rule permit only digest-pinned API,
frontend, or funded-signer deployments through the validated Quadlet helper.

Replace each zero digest and every `REPLACE_...` value in the installed files,
not in this repository. Keep API/frontend image digests in their Quadlets; set
the same reviewed backend digest in each enabled job env file. Configure Caddy
with a real DNS name and allow only SSH, TCP/80, and TCP/443 in OCI and the host
firewall. Never expose port 3000 or 8081.

`ring.env.example` starts with `POLYEDGE_RING_SEAL_ONLY=1`, so it can hash local
segments before Azure identity approval. Set it to `0` only after filling the
digest, tenant, and blob-only client ID and installing the secret file.

External Azure client secrets, when approved, live only at
`/etc/polyedge/credentials/<service>/azure-client-secret` with mode `0600`.
The containers receive those files read-only at `/run/credentials`; secret
values never belong in env files, Quadlets, Git, commands, or logs. Keep API
table access separate from the blob-only research/ring identity.

The funded signer is a separate, no-ingress, read-only container. Only it gets
the Podman wallet secrets and its dedicated Azure funded identity; the API and
research jobs get neither. It has no install target and remains disabled until
the funded identity, exact non-secret environment, origin check, queue repair,
and `FUNDED_EVIDENCE_TRUST_BOUNDARY_READY` review all pass. Root remains the
single-host trust ceiling and can administer both containers.

The recorder writes one fsynced JSONL segment per ten-minute UTC bucket. The
ring timer seals closed segments with SHA-256 manifests, creates immutable
Azure Cool-tier blobs without any remote listing, verifies retry collisions byte for
byte, and retains each local segment for 48 hours to leave job-workspace
headroom on the 150-GB volume. It removes a local segment only after its
immutable remote manifest is re-read successfully. A separate health timer
checks upload age and projected capacity and stops the API before free space
falls below 32 GiB.

After the approved authentication and separate-volume gates, create the marker
and enable only the intended timers:

```sh
sudo touch /etc/polyedge/ENABLE_AZURE_JOBS
sudo systemctl enable --now polyedge-api.service polyedge-frontend.service caddy.service
sudo systemctl enable --now polyedge-ring-sync.timer
sudo systemctl enable --now polyedge-ring-health.timer
sudo systemctl enable --now polyedge-freshness.timer polyedge-hourly.timer
# Enable daily/replay/qset individually only after their validation.
# sudo systemctl enable --now polyedge-daily.timer polyedge-replay.timer
# sudo systemctl enable --now polyedge-shadow-qset.timer
```

## Digest deployment

After the API/frontend are healthy, update one reviewed GHCR ARM64 digest at a
time. The helper accepts only these three units, updates only their installed
`Image=` line, makes a timestamped rollback copy, pulls and checks
`linux/arm64`, then restarts and verifies the running container's exact digest.
Any restart or verification failure restores the prior Quadlet and restarts it.

```sh
sudo /usr/local/sbin/polyedge-quadlet-deploy polyedge-api \
  ghcr.io/OWNER/polyedge-rust-backend@sha256:LOWERCASE_64_HEX_DIGEST
sudo /usr/local/sbin/polyedge-quadlet-deploy polyedge-frontend \
  ghcr.io/OWNER/polyedge-frontend@sha256:LOWERCASE_64_HEX_DIGEST
sudo /usr/local/sbin/polyedge-quadlet-deploy polyedge-funded-signer \
  ghcr.io/OWNER/polyedge-venue-probe@sha256:LOWERCASE_64_HEX_DIGEST
```

Rollback copies live in `/etc/polyedge/rollback/`. The helper never accepts a
tag, a non-GHCR registry, a mismatched image repository, or any unit other than
API, frontend, or funded signer.

Schedules are UTC: freshness every five minutes; hourly quality at `:10`;
primary daily at 00:30; replay at 03:00; qset shadow at 02:15. The runner uses
one `flock -w 36h /run/polyedge/research.lock`, so writers serialize even when
timers collide. It caps the daemons at 0.5 CPU / 1 GiB, daily/replay at 2 CPU /
4 GiB, and qset at 3 CPU / 8 GiB to reserve capacity on the 4-OCPU host.

## Verify, reboot, rollback

```sh
sudo systemd-analyze verify /etc/systemd/system/polyedge-job@.service
systemd-analyze calendar '*-*-* *:00/5:00 UTC' '*-*-* 00:30:00 UTC' '*-*-* 02:15:00 UTC'
systemctl list-timers 'polyedge-*'
podman ps --format '{{.Names}} {{.Status}}'
curl -fsS https://YOUR_DOMAIN/api/backend/health | jq -e '.ok and .execution_mode == "paper" and .kill_switch == false'
curl -fsS https://YOUR_DOMAIN/api/backend/status | jq -e '.task_health.api == "ok" and .task_health.runtime_loop == "running"'
journalctl -u polyedge-api.service -u polyedge-frontend.service -u caddy.service -b
sudo jq . /srv/polyedge-ring/status.json
```

For a no-data lock test:

```sh
sudo systemd-run --unit=polyedge-lock-a --collect /usr/bin/flock /run/polyedge/research.lock /bin/sleep 20
sudo systemd-run --unit=polyedge-lock-b --collect /usr/bin/flock /run/polyedge/research.lock /usr/bin/date
journalctl -u polyedge-lock-a -u polyedge-lock-b --since '1 minute ago'
```

Reboot only after health passes, then repeat the health, status, `podman ps`,
and `systemctl list-timers` checks. For rollback, restore the prior saved
`polyedge-api.container` and `polyedge-frontend.container` (both immutable
digests), run `sudo systemctl daemon-reload`, restart the two services, and
repeat the health checks. Retain the prior images until the rollback window
closes.
