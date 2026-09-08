#!/usr/bin/env bash
set -euo pipefail

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
stage=$repo/ops/conduit/bin/polyedge-parity-stage-funded-active-20260901T020000Z
first=$repo/ops/conduit/bin/polyedge-parity-collect-first-hour-20260901T020000Z
collector=$repo/ops/conduit/bin/polyedge-parity-hourly-20260831T200000Z
validator=$repo/ops/conduit/bin/polyedge-reboot-attestation-20260831T200000Z

bash -n "$stage" "$first" "$collector" "$validator"

(
  POLYEDGE_TEST_SOURCE_ONLY=1 . "$stage"
  test "$window" = 2026-09-01T02:00:00Z
  test "$window_epoch" = 1788228000
  test "$ledger" = /srv/polyedge-ring/parity/20260901T020000Z-funded-active.json
  test "$first_evidence" = /srv/polyedge-ring/parity/hourly/20260901T02/evidence.json
  test "$collector_sha" = "sha256:$(sha256sum "$collector" | cut -d' ' -f1)"
  test "$validator_sha" = "sha256:$(sha256sum "$validator" | cut -d' ' -f1)"
)

(
  POLYEDGE_TEST_SOURCE_ONLY=1 . "$first"
  test "$window" = 2026-09-01T02:00:00Z
  test "$collect_not_before" = 1788232680
  test "$enable_deadline" = 1788235200
  test "$ledger" = /srv/polyedge-ring/parity/20260901T020000Z-funded-active.json
  test "$evidence" = /srv/polyedge-ring/parity/hourly/20260901T02/evidence.json
  test "$collector_sha" = "sha256:$(sha256sum "$collector" | cut -d' ' -f1)"
  test "$validator_sha" = "sha256:$(sha256sum "$validator" | cut -d' ' -f1)"
)

! rg -q '2026-08-31T20:00:00Z|1788206400|1788211080|1788213600|20260831T200000Z-funded-active|hourly/20260831T20' "$stage" "$first"
