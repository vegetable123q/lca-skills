#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="process-from-flow-batch.service"
ENV_FILE="${HOME}/.config/process-from-flow-batch/env"

FLOW_FILE=""
FLOW_JSON=""
FLOW_STDIN=0
NAME_HINT=""
START_SERVICE=1

usage() {
  cat <<'USAGE'
Usage: submit-process-from-flow.sh [options]

Submit one reference flow JSON into the batch daemon queue.

Options:
  --flow-file <path>     Path to flow JSON file
  --flow-json <json>     Inline flow JSON string
  --flow-stdin           Read flow JSON from stdin
  --name <hint>          Optional filename hint (without extension)
  --no-start-service     Only enqueue file, do not start service
  -h, --help             Show this message
USAGE
}

sanitize_name() {
  local raw="$1"
  local safe
  safe="$(printf '%s' "${raw}" | tr -cs '0-9A-Za-z._-' '-')"
  safe="${safe#-}"
  safe="${safe%-}"
  if [[ -z "${safe}" ]]; then
    safe="flow"
  fi
  printf '%s' "${safe}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --flow-file)
      FLOW_FILE="$2"; shift 2 ;;
    --flow-json)
      FLOW_JSON="$2"; shift 2 ;;
    --flow-stdin)
      FLOW_STDIN=1; shift ;;
    --name)
      NAME_HINT="$2"; shift 2 ;;
    --no-start-service)
      START_SERVICE=0; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2 ;;
  esac
done

inputs=0
[[ -n "${FLOW_FILE}" ]] && inputs=$((inputs + 1))
[[ -n "${FLOW_JSON}" ]] && inputs=$((inputs + 1))
[[ "${FLOW_STDIN}" -eq 1 ]] && inputs=$((inputs + 1))
if [[ "${inputs}" -ne 1 ]]; then
  echo "Specify exactly one of --flow-file, --flow-json, --flow-stdin." >&2
  exit 2
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing env file: ${ENV_FILE}" >&2
  exit 2
fi

# shellcheck source=/dev/null
source "${ENV_FILE}"

if [[ -z "${FLOW_DIR:-}" ]]; then
  echo "FLOW_DIR is not set in ${ENV_FILE}" >&2
  exit 2
fi

mkdir -p "${FLOW_DIR}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
rand_suffix="$(printf '%06x' "$((RANDOM<<8 | RANDOM))")"
if [[ -n "${NAME_HINT}" ]]; then
  base_name="$(sanitize_name "${NAME_HINT}")"
elif [[ -n "${FLOW_FILE}" ]]; then
  base_name="$(sanitize_name "$(basename "${FLOW_FILE}" .json)")"
else
  base_name="flow"
fi

target="${FLOW_DIR}/${base_name}_${timestamp}_${rand_suffix}.json"

if [[ -n "${FLOW_FILE}" ]]; then
  cp "${FLOW_FILE}" "${target}"
elif [[ -n "${FLOW_JSON}" ]]; then
  printf '%s' "${FLOW_JSON}" > "${target}"
else
  cat > "${target}"
fi

if ! python3 -m json.tool "${target}" >/dev/null 2>&1; then
  rm -f "${target}"
  echo "Invalid JSON payload, rejected." >&2
  exit 2
fi

if [[ "${START_SERVICE}" -eq 1 ]]; then
  systemctl --user enable --now "${SERVICE_NAME}" >/dev/null
fi

echo "Enqueued flow file: ${target}"
echo "Queue service: ${SERVICE_NAME}"
