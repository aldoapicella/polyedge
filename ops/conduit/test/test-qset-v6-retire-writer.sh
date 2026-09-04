#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
helper=$root/bin/polyedge-qset-v6-retire-writer

run_helper() {
  source "$helper"
  receipt_root=$QSET_V6_RETIRE_TEST_ROOT/receipts
  receipt=$receipt_root/campaign-2026-09-01-qset-v6-writer.json
  lock_file=$QSET_V6_RETIRE_TEST_ROOT/retirement.lock
  wait_seconds=2

  systemctl() {
    case "$1" in
      is-active) test -e "$QSET_V6_RETIRE_TEST_ROOT/active" ;;
      show)
        case "$*" in *InvocationID*) cat "$QSET_V6_RETIRE_TEST_ROOT/invocation" ;; *MainPID*) echo 4321 ;; *) return 2 ;; esac ;;
      stop)
        echo stop >>"$QSET_V6_RETIRE_TEST_ROOT/actions"
        if test ! -e "$QSET_V6_RETIRE_TEST_ROOT/stop-failed"; then touch "$QSET_V6_RETIRE_TEST_ROOT/stop-failed"; return 75; fi
        rm -f "$QSET_V6_RETIRE_TEST_ROOT/active" ;;
      *) return 2 ;;
    esac
  }
  podman() {
    case "$1" in
      inspect)
        jq -nc --arg id "$(cat "$QSET_V6_RETIRE_TEST_ROOT/container")" --arg image "$(cat "$QSET_V6_RETIRE_TEST_ROOT/image")" --arg digest "$(cat "$QSET_V6_RETIRE_TEST_ROOT/digest")" '[{Id:$id,Config:{Image:$image},ImageDigest:$digest,State:{Status:"running"}}]' ;;
      image) cat "$QSET_V6_RETIRE_TEST_ROOT/revision" ;;
      kill) echo USR1 >>"$QSET_V6_RETIRE_TEST_ROOT/actions" ;;
      *) return 2 ;;
    esac
  }
  journalctl() {
    case "$*" in
      *--sync*) return ;;
      *--show-cursor*) echo '-- cursor: qset-v6-before' ;;
      *--after-cursor*)
        jq -nc --arg invocation "$(cat "$QSET_V6_RETIRE_TEST_ROOT/invocation")" --arg container "$(cat "$QSET_V6_RETIRE_TEST_ROOT/container")" --arg message "$(cat "$QSET_V6_RETIRE_TEST_ROOT/journal-message")" \
          '{_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}' ;;
      *) return 2 ;;
    esac
  }
  main
}

if test "${1:-}" = run; then run_helper; exit; fi

test_root=$(mktemp -d); trap 'rm -rf "$test_root"' EXIT HUP INT TERM
export QSET_V6_RETIRE_TEST_ROOT=$test_root
state=$test_root/fakeroot.state
fake_root() {
  if test -n "${FAKEROOTKEY:-}"; then bash "$0" run
  elif test -e "$state"; then fakeroot -i "$state" -s "$state" -- bash "$0" run
  else fakeroot -s "$state" -- bash "$0" run
  fi
}
digest="sha256:$(printf 'b%.0s' {1..64})"; revision=$(printf 'a%.0s' {1..40}); container_id=$(printf 'c%.0s' {1..64}); invocation=$(printf 'd%.0s' {1..32})
printf '%s\n' "$digest" >"$test_root/digest"
printf 'ghcr.io/example/polyedge-rust-backend@%s\n' "$digest" >"$test_root/image"
printf '%s\n' "$revision" >"$test_root/revision"
printf '%s\n' "$container_id" >"$test_root/container"
printf '%s\n' "$invocation" >"$test_root/invocation"
touch "$test_root/active" "$test_root/actions"
jq -nc --arg digest "$digest" --arg revision "$revision" '{schema:"polyedge.qset_v6_writer_retirement_receipt.v1",status:"prepared_for_retirement",retired_at:"2026-08-22T10:00:00Z",campaign_id:"campaign-2026-09-01-qset-v6",app_name:"polyedge-shadow-qset-v6",image_digest:$digest,source_revision:$revision,recorder_instance_id:"11111111-2222-4333-8444-555555555555",final_assigned_sequence:9,final_enqueued_sequence:9,final_enqueued_total:9,final_persisted_sequence:9,final_persisted_total:9,final_queued:0,flush_success:true}' >"$test_root/journal-message"

if fake_root; then echo 'first stop failure was accepted' >&2; exit 1; fi
evidence=$test_root/receipts/campaign-2026-09-01-qset-v6-writer.json
test -s "$evidence"
test "$(grep -c '^USR1$' "$test_root/actions")" = 1
test "$(grep -c '^stop$' "$test_root/actions")" = 1
if test -n "${FAKEROOTKEY:-}"; then stat -c '%u:%g:%a' "$evidence"; else fakeroot -i "$state" -- stat -c '%u:%g:%a' "$evidence"; fi | grep -Fx '0:0:640' >/dev/null
jq -e --arg invocation "$invocation" --arg container "$container_id" --arg digest "$digest" '.service.invocationId==$invocation and .container.id==$container and .container.imageDigest==$digest and .receipt==(.journal.message|fromjson)' "$evidence" >/dev/null

printf '%s\n' "$(printf 'e%.0s' {1..32})" >"$test_root/invocation"
if fake_root >/dev/null 2>&1; then echo 'invocation mismatch was accepted' >&2; exit 1; fi
printf '%s\n' "$invocation" >"$test_root/invocation"
printf '%s\n' "$(printf 'f%.0s' {1..64})" >"$test_root/container"
if fake_root >/dev/null 2>&1; then echo 'container mismatch was accepted' >&2; exit 1; fi
printf '%s\n' "$container_id" >"$test_root/container"
other_digest="sha256:$(printf '9%.0s' {1..64})"; printf '%s\n' "$other_digest" >"$test_root/digest"; printf 'ghcr.io/example/polyedge-rust-backend@%s\n' "$other_digest" >"$test_root/image"
if fake_root >/dev/null 2>&1; then echo 'image mismatch was accepted' >&2; exit 1; fi
printf '%s\n' "$digest" >"$test_root/digest"; printf 'ghcr.io/example/polyedge-rust-backend@%s\n' "$digest" >"$test_root/image"

fake_root >/dev/null
test ! -e "$test_root/active"
test "$(grep -c '^USR1$' "$test_root/actions")" = 1
test "$(grep -c '^stop$' "$test_root/actions")" = 2

bash -n "$helper" "$0"
