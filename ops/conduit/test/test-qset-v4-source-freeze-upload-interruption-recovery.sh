#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/bin"

receipt_root=$tmp/receipts
freeze=$tmp/polyedge-qset-v4-source-freeze
sed "s#receipt_root=/srv/polyedge-ring/migration/qset-v4/source-freeze#receipt_root=$receipt_root#" "$root/bin/polyedge-qset-v4-source-freeze" >"$freeze"
chmod 0755 "$freeze"

commit=1111111111111111111111111111111111111111
tree=2222222222222222222222222222222222222222
image=ghcr.io/example/polyedge-rust-backend@sha256:3333333333333333333333333333333333333333333333333333333333333333
manifest=$tmp/source-freeze.json
jq -n --arg commit "$commit" --arg tree "$tree" --arg image "$image" '{
  campaign_id:"campaign-2026-08-24-qset-v4",
  evidence_version:"protocol-v3-qset-v4",
  source_commit:$commit,
  git_tree:$tree,
  research_image:$image
}' >"$manifest"
digest=$(sha256sum "$manifest" | cut -d' ' -f1)
remote=$tmp/remote.json
cp "$manifest" "$remote"

cat >"$tmp/bin/git" <<'EOF'
#!/bin/sh
case "$*" in
  *"rev-parse HEAD^{tree}") printf '%s\n' "$MOCK_TREE" ;;
  *"rev-parse HEAD") printf '%s\n' "$MOCK_COMMIT" ;;
  *"show HEAD:research/configs/campaign_freeze_2026-08-24_qset_v4.json")
    printf '%s\n' '{"protected_files":["infra/test"]}'
    ;;
  *"show HEAD:") printf '%s\n' source ;;
  *"cat-file -s HEAD:"*) printf '%s\n' 7 ;;
  *"cat-file -e HEAD:"*|*"ls-files --error-unmatch "*) : ;;
  *) exit 64 ;;
esac
EOF

cat >"$tmp/bin/podman" <<'EOF'
#!/bin/sh
case "$1 $2" in
  "image inspect") printf 'linux/arm64|%s\n' "$MOCK_COMMIT" ;;
  "manifest inspect") printf '{"manifests":[{"platform":{"os":"linux","architecture":"amd64"}},{"platform":{"os":"linux","architecture":"arm64"}}]}\n' ;;
  *) exit 64 ;;
esac
EOF

cat >"$tmp/bin/install" <<'EOF'
#!/bin/sh
for directory do :; done
mkdir -p "$directory"
chmod 0750 "$directory"
EOF

cat >"$tmp/bin/chown" <<'EOF'
#!/bin/sh
exit 0
EOF

cat >"$tmp/bin/date" <<'EOF'
#!/bin/sh
printf '%s\n' "$MOCK_DATE"
EOF

cat >"$tmp/bin/az" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOCK_AZ_LOG"
case "$*" in
  *"storage container immutability-policy show"*)
    printf '{"state":"%s","immutabilityPeriodSinceCreationInDays":%s,"etag":"policy-etag"}\n' "${MOCK_POLICY_STATE:-Locked}" "${MOCK_POLICY_DAYS:-90}"
    ;;
  *"storage container immutability-policy lock"*)
    :
    ;;
  *"storage blob exists"*)
    printf '{"exists":true}\n'
    ;;
  *"storage blob upload"*)
    exit 99
    ;;
  *"storage blob download"*)
    destination=
    while test "$#" -gt 0; do
      if test "$1" = --file; then destination=$2; break; fi
      shift
    done
    cp "$MOCK_REMOTE" "$destination"
    ;;
  *"storage blob show"*)
    name= container=
    while test "$#" -gt 0; do
      case "$1" in
        --name) name=$2; shift ;;
        --container-name) container=$2; shift ;;
      esac
      shift
    done
    metadata='{}'
    test "${MOCK_METADATA_OK:-true}" = true || metadata='{"unexpected":"value"}'
    policy=null
    jq -n \
      --arg container "$container" --arg name "$name" --argjson metadata "$metadata" --argjson policy "$policy" \
      --argjson bytes "$(stat -c '%s' "$MOCK_REMOTE")" '{
        container:$container,
        name:$name,
        metadata:$metadata,
        immutabilityPolicy:$policy,
        properties:{
          etag:"blob-etag",
          blobType:"BlockBlob",
          contentLength:$bytes
        }
      }'
    ;;
  *) exit 64 ;;
esac
EOF
chmod 0755 "$tmp/bin/"*

export PATH=$tmp/bin:$PATH
export AZURE_RESOURCE_GROUP=rg-test
export AZURE_STORAGE_ACCOUNT_NAME=storageaccount
export MOCK_AZ_LOG=$tmp/az.log
export MOCK_COMMIT=$commit
export MOCK_TREE=$tree
export MOCK_DIGEST=$digest
export MOCK_REMOTE=$remote
export MOCK_DATE=2026-08-22T01:02:03Z
export TMPDIR=$tmp

built=$tmp/built-source-freeze.json
FREEZE_RESEARCH_IMAGE=$image "$freeze" build "$built" >"$tmp/build.out" 2>"$tmp/build.err"
test ! -s "$tmp/build.err"
jq -e --arg image "$image" '.research_image==$image and (.critical_files|length)==1' "$built" >/dev/null
! find "$tmp" -maxdepth 1 -type f \( -name 'tmp.*' -o -name '.qset-v4-freeze.*' \) | grep -q .

"$freeze" lock-and-upload "$manifest" >"$tmp/first.out" 2>"$tmp/first.err"
test ! -s "$tmp/first.err"
! find "$tmp" -maxdepth 1 -type f -name 'tmp.*' | grep -q .
! grep -F 'storage blob upload' "$MOCK_AZ_LOG"
receipt=$receipt_root/source-$digest.json
test -f "$receipt"
jq -e --arg digest "sha256:$digest" --arg image "$image" --arg commit "$commit" --arg tree "$tree" '
  .generatedAtUtc=="2026-08-22T01:02:03Z" and .manifest.sha256==$digest and
  .researchImage==$image and .sourceCommit==$commit and .gitTree==$tree and
  .immutabilityPolicy=={state:"Locked",days:90}
' "$receipt" >/dev/null

before=$(sha256sum "$receipt")
MOCK_DATE=2026-08-22T02:03:04Z "$freeze" lock-and-upload "$manifest" >"$tmp/second.out" 2>"$tmp/second.err"
test ! -s "$tmp/second.err"
! find "$tmp" -maxdepth 1 -type f -name 'tmp.*' | grep -q .
test "$(sha256sum "$receipt")" = "$before"
cmp -s "$tmp/first.out" "$tmp/second.out"

if MOCK_METADATA_OK=false "$freeze" lock-and-upload "$manifest" >/dev/null 2>&1; then
  echo 'recovery accepted mismatched blob metadata' >&2
  exit 1
fi
test "$(sha256sum "$receipt")" = "$before"

if MOCK_POLICY_STATE=Unlocked "$freeze" lock-and-upload "$manifest" >/dev/null 2>&1; then
  echo 'recovery accepted an unlocked container policy' >&2
  exit 1
fi
test "$(sha256sum "$receipt")" = "$before"

if MOCK_POLICY_DAYS=89 "$freeze" lock-and-upload "$manifest" >/dev/null 2>&1; then
  echo 'recovery accepted a short container policy' >&2
  exit 1
fi
test "$(sha256sum "$receipt")" = "$before"

printf '{"different":true}\n' >"$remote"
if "$freeze" lock-and-upload "$manifest" >/dev/null 2>&1; then
  echo 'recovery accepted different remote bytes' >&2
  exit 1
fi
test "$(sha256sum "$receipt")" = "$before"
cp "$manifest" "$remote"

printf '{"conflict":true}\n' >"$receipt"
conflict=$(sha256sum "$receipt")
if "$freeze" lock-and-upload "$manifest" >/dev/null 2>&1; then
  echo 'recovery accepted a conflicting local receipt' >&2
  exit 1
fi
test "$(sha256sum "$receipt")" = "$conflict"
