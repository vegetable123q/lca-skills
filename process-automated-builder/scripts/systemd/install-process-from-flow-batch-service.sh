#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SKILL_DIR="$(cd -- "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"

SYSTEMD_USER_DIR="${HOME}/.config/systemd/user"
ENV_DIR="${HOME}/.config/process-from-flow-batch"

mkdir -p "${SYSTEMD_USER_DIR}" "${ENV_DIR}"

cp "${SCRIPT_DIR}/process-from-flow-batch.service" "${SYSTEMD_USER_DIR}/process-from-flow-batch.service"

if [[ ! -f "${ENV_DIR}/env" ]]; then
  cp "${SCRIPT_DIR}/process-from-flow-batch.env.example" "${ENV_DIR}/env"
  echo "Created ${ENV_DIR}/env from example. Please edit it before starting service."
else
  echo "Env file already exists: ${ENV_DIR}/env"
fi

echo "Installed service file to ${SYSTEMD_USER_DIR}/process-from-flow-batch.service"
echo
echo "Next steps:"
echo "  1) Edit: ${ENV_DIR}/env"
echo "  2) systemctl --user daemon-reload"
echo "  3) systemctl --user enable --now process-from-flow-batch.service"
echo "  4) journalctl --user -u process-from-flow-batch.service -f"
