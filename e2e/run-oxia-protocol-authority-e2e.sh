#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
oxia_checkout="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
oxia_port="${NEREUS_DELAY_OXIA_PROTOCOL_AUTHORITY_PORT:-31510}"
gradle_home="${NEREUS_DELAY_OXIA_PROTOCOL_AUTHORITY_GRADLE_USER_HOME:-/private/tmp/nereus-delay-oxia-protocol-authority-gradle}"
compose_file="${script_dir}/docker-compose.oxia.yml"
compose_project="nereus-delay-oxia-protocol-authority-e2e-$(date +%s)-$$"
oxia_image="${compose_project}-oxia"
log_file="${NEREUS_DELAY_OXIA_PROTOCOL_AUTHORITY_LOG:-${delay_dir}/build/oxia-protocol-authority-e2e.log}"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "real Oxia protocol authority E2E requires docker compose" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "real Oxia protocol authority E2E requires curl" >&2
  exit 1
fi
if [[ ! "${oxia_port}" =~ ^[0-9]+$ ]]; then
  echo "NEREUS_DELAY_OXIA_PROTOCOL_AUTHORITY_PORT must be numeric" >&2
  exit 1
fi
if [[ ! -e "${oxia_checkout}/.git" ]]; then
  echo "Oxia checkout is missing: ${oxia_checkout}" >&2
  exit 1
fi
if [[ -n "$(git -C "${oxia_checkout}" status --porcelain)" ]]; then
  echo "Oxia checkout is dirty: ${oxia_checkout}" >&2
  exit 1
fi
if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"${oxia_port}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Oxia protocol authority port is already listening: ${oxia_port}" >&2
  exit 1
fi

mkdir -p "$(dirname "${log_file}")" "${gradle_home}"
compose() {
  docker compose --project-name "${compose_project}" --file "${compose_file}" "$@"
}
wait_for_health() {
  for attempt in $(seq 1 60); do
    if compose exec --no-TTY oxia oxia health --host 127.0.0.1 --port 6648 --timeout 2s \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  compose logs oxia >&2 || true
  return 1
}
cleanup() {
  local status=$?
  compose down --remove-orphans --volumes --rmi local >/dev/null 2>&1 || true
  if docker image inspect "${oxia_image}" >/dev/null 2>&1; then
    docker image rm "${oxia_image}" >/dev/null 2>&1 || true
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

oxia_source="$(git -C "${oxia_checkout}" rev-parse HEAD)"
echo "Oxia source: ${oxia_source}"
echo "Compose project: ${compose_project}"
echo "Oxia endpoint: 127.0.0.1:${oxia_port}"
{
  echo "Oxia source: ${oxia_source}"
  echo "Compose project: ${compose_project}"
  echo "Oxia endpoint: 127.0.0.1:${oxia_port}"
} >>"${log_file}"
export NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}"
export NEREUS_DELAY_OXIA_E2E_PORT="${oxia_port}"
compose up --build --detach
wait_for_health

set +e
(
  cd "${delay_dir}"
  env NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:${oxia_port}" \
    GRADLE_USER_HOME="${gradle_home}" \
    ./gradlew test \
      --tests io.nereusstream.delay.ownership.OxiaRealProtocolCapabilitySmokeTest \
      --tests io.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest \
      --tests io.nereusstream.delay.route.OxiaRealRouteWorkerAssignmentSmokeTest \
      --rerun-tasks --no-daemon --console=plain
) >"${log_file}" 2>&1
test_status=$?
set -e
cat "${log_file}"
if [[ "${test_status}" != "0" ]]; then
  echo "real Oxia protocol authority E2E failed: exit=${test_status}" >&2
  exit "${test_status}"
fi
echo "Oxia protocol activation authority E2E passed: external capability-before-marker, signed control and Route/assignment CAS" \
  | tee -a "${log_file}"
