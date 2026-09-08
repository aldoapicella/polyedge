#!/bin/sh

polyedge_raw_input() {
  day=${1:?usage: polyedge_raw_input YYYY/MM/DD}
  case "$day" in
    [0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]) ;;
    *) echo "invalid raw-input day: $day" >&2; return 64 ;;
  esac

  if [ -n "${POLYEDGE_LOCAL_RAW_ROOT:-}" ]; then
    case "$POLYEDGE_LOCAL_RAW_ROOT" in
      /*) ;;
      *) echo 'POLYEDGE_LOCAL_RAW_ROOT must be absolute' >&2; return 64 ;;
    esac
    case "$POLYEDGE_LOCAL_RAW_ROOT" in
      *..*) echo 'POLYEDGE_LOCAL_RAW_ROOT must not contain ..' >&2; return 64 ;;
    esac
    printf '%s/%s\n' "${POLYEDGE_LOCAL_RAW_ROOT%/}" "$day"
    return
  fi

  : "${AZURE_STORAGE_ACCOUNT_NAME:?set AZURE_STORAGE_ACCOUNT_NAME}"
  : "${AZURE_STORAGE_CONTAINER_NAME:?set AZURE_STORAGE_CONTAINER_NAME}"
  prefix=${POLYEDGE_RAW_EVENT_PREFIX:-events}
  case "$prefix" in
    ''|/*|*..*|*[!A-Za-z0-9._/-]*)
      echo "invalid POLYEDGE_RAW_EVENT_PREFIX: $prefix" >&2
      return 64
      ;;
  esac
  prefetch=${POLYEDGE_RESEARCH_PREFETCH_BLOBS:-16}
  case "$prefetch" in
    ''|*[!0-9]*) echo "invalid POLYEDGE_RESEARCH_PREFETCH_BLOBS: $prefetch" >&2; return 64 ;;
  esac
  [ "$prefetch" -gt 0 ] || {
    echo 'POLYEDGE_RESEARCH_PREFETCH_BLOBS must be positive' >&2
    return 64
  }
  printf 'azure://%s/%s/%s/%s/?prefetch_blobs=%s\n' \
    "$AZURE_STORAGE_ACCOUNT_NAME" "$AZURE_STORAGE_CONTAINER_NAME" "$prefix" "$day" "$prefetch"
}
