#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
handoff=$root/bin/polyedge-qset-v4-processor-handoff
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
uid=$(id -u)
gid=$(id -g)

write_env() {
  cat >"$env_file" <<'EOF'
UNCHANGED=before
POLYEDGE_QSET_V4_DAY1_RECEIPT_SHA256=
POLYEDGE_QSET_V4_DAY1_INVENTORY_SHA256=
POLYEDGE_QSET_V4_DAY2_RECEIPT_SHA256=
POLYEDGE_QSET_V4_DAY2_INVENTORY_SHA256=
EOF
  chmod 0640 "$env_file"
}

setup_case() {
  case_dir=$tmp/$1
  state=$case_dir/state
  receipts=$case_dir/receipts
  rollback=$case_dir/rollback
  env_file=$case_dir/qset-v4-processor.env
  gate=$case_dir/ENABLE_QSET_V4_PROCESSOR_MANUAL
  lock=$case_dir/handoff.lock
  preflight=$case_dir/preflight
  disk_guard=$case_dir/disk-guard
  systemctl=$case_dir/systemctl
  rbac_checker=$case_dir/rbac-checker
  mkdir -p "$state" "$receipts"
  printf '%032d\n' 0 >"$state/invocation"
  printf 'inactive\n' >"$state/active"
  printf 'success\n' >"$state/result"
  printf '0\n' >"$state/starts"
  printf '0\n' >"$state/disks"
  printf '0\n' >"$state/invocation-queries"
  : >"$state/order"
  for date in 2026-08-24 2026-08-25; do
    digest=$(printf '%064d' "${date#2026-08-}")
    printf '{"schema":"polyedge.qset_v4_closed_day_seal.v1","campaign_id":"campaign-2026-08-24-qset-v4","date":"%s","source_inventory_sha256":"sha256:%s"}\n' "$date" "$digest" >"$receipts/$date.json"
    chmod 0640 "$receipts/$date.json"
  done
  write_env
  cat >"$preflight" <<'EOF'
#!/bin/sh
test "${HANDOFF_PREFLIGHT_FAIL:-0}" = 0
test "${POLYEDGE_QSET_V4_DAY1_RECEIPT_SHA256#sha256:}" != "$POLYEDGE_QSET_V4_DAY1_RECEIPT_SHA256"
EOF
  cat >"$disk_guard" <<'EOF'
#!/bin/sh
test "$1" = --assert-headroom
count=$(($(cat "$HANDOFF_STATE/disks") + 1))
printf '%s\n' "$count" >"$HANDOFF_STATE/disks"
test "${HANDOFF_DISK_FAIL_AFTER:-0}" -ne "$count"
EOF
  cat >"$rbac_checker" <<'EOF'
#!/bin/sh
set -eu
test "$1" = verify-live
test ! -e "$(dirname "$0")/state/rbac-fail"
printf '%s\n' '{"schema":"polyedge.qset_v4_rbac_verify_live.v1"}'
EOF
  cat >"$systemctl" <<'EOF'
#!/bin/sh
state=$HANDOFF_STATE
case "$1" in
  is-enabled) printf '%s\n' static ;;
  is-active) test "$(cat "$state/active")" = active ;;
  show)
    case "$2" in
      --property=InvocationID)
        if test -f "$state/pending-invocation"; then
          queries=$(($(cat "$state/invocation-queries") + 1))
          printf '%s\n' "$queries" >"$state/invocation-queries"
          delay=$(cat "$state/invocation-delay")
          if test "$queries" -ge "$delay"; then
            mv "$state/pending-invocation" "$state/invocation"
            rm -f "$state/invocation-delay"
          fi
        fi
        cat "$state/invocation"
        ;;
      --property=ActiveState) cat "$state/active" ;;
      --property=Result) cat "$state/result" ;;
      *) exit 64 ;;
    esac
    ;;
  start)
    test "$2" = --no-block
    test "$3" = polyedge-qset-v4-processor.service
    test -f "$HANDOFF_ATTEMPT"
    test -f "$HANDOFF_DISPATCHED"
    test -f "$HANDOFF_GATE"
    printf '%s\n' attempt-before-gate >>"$state/order"
    count=$(($(cat "$state/starts") + 1))
    printf '%s\n' "$count" >"$state/starts"
    if test "${HANDOFF_SUPPRESS_INVOCATION:-0}" != 1; then
      printf '%032d\n' "$count" >"$state/pending-invocation"
      printf '%s\n' "${HANDOFF_INVOCATION_DELAY:-1}" >"$state/invocation-delay"
    fi
    printf '%s\n' inactive >"$state/active"
    printf '%s\n' "${HANDOFF_RESULT:-success}" >"$state/result"
    ;;
  *) exit 64 ;;
esac
EOF
  chmod 0755 "$preflight" "$disk_guard" "$rbac_checker" "$systemctl"
}

run_handoff() {
  QSET_V4_PROCESSOR_HANDOFF_TEST_ONLY=1 \
  QSET_V4_PROCESSOR_HANDOFF_ENV_FILE_TEST_ONLY="$env_file" \
  QSET_V4_PROCESSOR_HANDOFF_RECEIPT_ROOT_TEST_ONLY="$receipts" \
  QSET_V4_PROCESSOR_HANDOFF_GATE_FILE_TEST_ONLY="$gate" \
  QSET_V4_PROCESSOR_HANDOFF_PREFLIGHT_TEST_ONLY="$preflight" \
  QSET_V4_PROCESSOR_HANDOFF_DISK_GUARD_TEST_ONLY="$disk_guard" \
  QSET_V4_PROCESSOR_HANDOFF_SYSTEMCTL_TEST_ONLY="$systemctl" \
  QSET_V4_PROCESSOR_HANDOFF_RBAC_CHECKER_TEST_ONLY="$rbac_checker" \
  QSET_V4_PROCESSOR_HANDOFF_LOCK_FILE_TEST_ONLY="$lock" \
  QSET_V4_PROCESSOR_HANDOFF_ROLLBACK_ROOT_TEST_ONLY="$rollback" \
  QSET_V4_PROCESSOR_HANDOFF_EXPECTED_UID_TEST_ONLY="$uid" \
  QSET_V4_PROCESSOR_HANDOFF_EXPECTED_GID_TEST_ONLY="$gid" \
  QSET_V4_PROCESSOR_HANDOFF_START_ATTEMPTS_TEST_ONLY=5 \
  QSET_V4_PROCESSOR_HANDOFF_WAIT_ATTEMPTS_TEST_ONLY=5 \
  HANDOFF_STATE="$state" HANDOFF_ATTEMPT="$rollback/attempt.json" \
  HANDOFF_DISPATCHED="$rollback/dispatched.json" HANDOFF_GATE="$gate" \
  "$handoff"
}

grep -F 'start_attempts=60' "$handoff" >/dev/null
grep -F 'wait_attempts=18030' "$handoff" >/dev/null

setup_case completed
run_handoff >/dev/null
test "$(cat "$state/starts")" = 1
test "$(cat "$state/order")" = attempt-before-gate
test ! -e "$gate"
test -f "$rollback/attempt.json"
test -f "$rollback/dispatched.json"
test -f "$rollback/started.json"
test -f "$rollback/completed.json"
test "$(stat -c '%a:%h' "$rollback/attempt.json")" = 640:1
jq -e '.schema=="polyedge.qset_v4_processor_attempt.v2"' "$rollback/attempt.json" >/dev/null
printf '%032d\n' 0 >"$state/invocation"
run_handoff >/dev/null
test "$(cat "$state/starts")" = 1

setup_case empty-prior
: >"$state/invocation"
run_handoff >/dev/null
test "$(cat "$state/starts")" = 1
jq -e '.priorInvocationId==""' "$rollback/attempt.json" >/dev/null

setup_case delayed-invocation
HANDOFF_INVOCATION_DELAY=3
export HANDOFF_INVOCATION_DELAY
run_handoff >/dev/null
unset HANDOFF_INVOCATION_DELAY
test "$(cat "$state/starts")" = 1
test "$(cat "$state/invocation-queries")" -ge 3

setup_case crash-after-attempt
QSET_V4_PROCESSOR_HANDOFF_CRASH_AFTER_ATTEMPT_TEST_ONLY=1
export QSET_V4_PROCESSOR_HANDOFF_CRASH_AFTER_ATTEMPT_TEST_ONLY
if run_handoff >/dev/null 2>&1; then echo 'crash-after-attempt injection was accepted' >&2; exit 1; fi
unset QSET_V4_PROCESSOR_HANDOFF_CRASH_AFTER_ATTEMPT_TEST_ONLY
test -f "$rollback/attempt.json"
test -f "$rollback/qset-v4-processor.env.before"
test ! -e "$rollback/started.json"
test ! -e "$gate"
test "$(cat "$state/starts")" = 0
cmp -s "$env_file" "$rollback/qset-v4-processor.env.before"
run_handoff >/dev/null
test "$(cat "$state/starts")" = 1
test -f "$rollback/completed.json"

setup_case exit-after-env
QSET_V4_PROCESSOR_HANDOFF_EXIT_AFTER_ENV_TEST_ONLY=1
export QSET_V4_PROCESSOR_HANDOFF_EXIT_AFTER_ENV_TEST_ONLY
if run_handoff >/dev/null 2>&1; then echo 'exit-after-env injection was accepted' >&2; exit 1; fi
unset QSET_V4_PROCESSOR_HANDOFF_EXIT_AFTER_ENV_TEST_ONLY
test -f "$rollback/attempt.json"
test ! -e "$rollback/started.json"
test ! -e "$gate"
test "$(cat "$state/starts")" = 0
run_handoff >/dev/null
test "$(cat "$state/starts")" = 1
test -f "$rollback/completed.json"

setup_case accepted-no-invocation
HANDOFF_SUPPRESS_INVOCATION=1
export HANDOFF_SUPPRESS_INVOCATION
if run_handoff >/dev/null 2>&1; then echo 'dispatch without invocation was accepted' >&2; exit 1; fi
unset HANDOFF_SUPPRESS_INVOCATION
test "$(cat "$state/starts")" = 1
test -f "$rollback/dispatched.json"
test ! -e "$rollback/started.json"
test ! -e "$gate"
if run_handoff >/dev/null 2>&1; then echo 'unknown dispatch was replayed' >&2; exit 1; fi
test "$(cat "$state/starts")" = 1

setup_case post-disk
HANDOFF_DISK_FAIL_AFTER=3
export HANDOFF_DISK_FAIL_AFTER
if run_handoff >/dev/null 2>&1; then echo 'post-start disk failure was accepted' >&2; exit 1; fi
unset HANDOFF_DISK_FAIL_AFTER
test "$(cat "$state/starts")" = 1
test ! -e "$gate"
test -f "$rollback/attempt.json"
test -f "$rollback/started.json"
test ! -e "$rollback/completed.json"
run_handoff >/dev/null
test "$(cat "$state/starts")" = 1
test -f "$rollback/completed.json"

setup_case failed-invocation
HANDOFF_RESULT=failed
export HANDOFF_RESULT
if run_handoff >/dev/null 2>&1; then echo 'failed invocation was accepted' >&2; exit 1; fi
unset HANDOFF_RESULT
test "$(cat "$state/starts")" = 1
test ! -e "$gate"
if run_handoff >/dev/null 2>&1; then echo 'failed invocation replayed' >&2; exit 1; fi
test "$(cat "$state/starts")" = 1

setup_case corrupt-marker
mkdir -m 0700 "$rollback"
printf '{bad}\n' >"$rollback/attempt.json"
chmod 0640 "$rollback/attempt.json"
if run_handoff >/dev/null 2>&1; then echo 'corrupt durable attempt was accepted' >&2; exit 1; fi
test "$(cat "$state/starts")" = 0

setup_case corrupt-snapshot
QSET_V4_PROCESSOR_HANDOFF_CRASH_AFTER_ATTEMPT_TEST_ONLY=1
export QSET_V4_PROCESSOR_HANDOFF_CRASH_AFTER_ATTEMPT_TEST_ONLY
run_handoff >/dev/null 2>&1 || true
unset QSET_V4_PROCESSOR_HANDOFF_CRASH_AFTER_ATTEMPT_TEST_ONLY
chmod 0600 "$rollback/qset-v4-processor.env.before"
if run_handoff >/dev/null 2>&1; then echo 'unsafe durable snapshot was accepted' >&2; exit 1; fi
test "$(cat "$state/starts")" = 0

setup_case rbac-error
cp "$env_file" "$case_dir/before"
touch "$state/rbac-fail"
if run_handoff >/dev/null 2>&1; then echo 'RBAC error was accepted' >&2; exit 1; fi
cmp -s "$case_dir/before" "$env_file"
test "$(cat "$state/starts")" = 0
test ! -e "$rollback/attempt.json"
test ! -e "$gate"

printf '%s\n' 'qset-v4 processor handoff tests passed'
