#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
gradle_user_home="${NEREUS_DELAY_PULSAR_GRADLE_USER_HOME:-/tmp/nereus-delay-pulsar-e2e-gradle}"
compose_project="nereus-delay-pulsar-e2e-$(date +%s)-$$"
compose_file="${script_dir}/docker-compose.pulsar.yml"
compose=(docker compose -p "${compose_project}" -f "${compose_file}")
image="nereus-delay-pulsar-p1:${compose_project}"
image_context="$(mktemp -d -t nereus-delay-p1-image.XXXXXX)"
runtime_dir="$(mktemp -d -t nereus-delay-p1-runtime.XXXXXX)"
base_port=$((19650 + ($$ % 300)))
broker_port="${PULSAR_BROKER_PORT:-${base_port}}"
web_port="${PULSAR_WEB_PORT:-$((base_port + 1))}"
tarball="${NEREUS_DELAY_PULSAR_TARBALL:-${pulsar_dir}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz}"
topic="${PULSAR_DELAY_E2E_TOPIC:-p1-real-client-${compose_project##*-}}"
service_url="pulsar://127.0.0.1:${broker_port}"
admin_url="http://127.0.0.1:${web_port}"
pulsar_client_cp="${pulsar_dir}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:${pulsar_dir}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:${pulsar_dir}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"
IFS=: read -r -a pulsar_client_artifacts <<< "${pulsar_client_cp}"

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  docker image rm "${image}" >/dev/null 2>&1 || true
  rm -rf "${image_context}"
  rm -rf "${runtime_dir}"
}
trap cleanup EXIT INT TERM

require_clean_pulsar_checkout() {
  test -z "$(git -C "${pulsar_dir}" status --porcelain)"
  test "$(git -C "${pulsar_dir}" branch --show-current)" = "nereus/delay-resource-guard-v1"
  git -C "${pulsar_dir}" merge-base --is-ancestor \
    8dae0236c0a0d405ed7f8303081080520fe91551 HEAD
}

wait_for_service() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${admin_url}/admin/v2/clusters" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Pulsar standalone did not become ready: ${admin_url}" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs >&2 || true
  return 1
}

cd "${delay_dir}"
require_clean_pulsar_checkout
test -s "${tarball}"
tar -xzf "${tarball}" -C "${runtime_dir}" --strip-components=1 "apache-pulsar-5.0.0-M1/lib"
test -n "$(find "${runtime_dir}/lib" -type f -name '*.jar' -print -quit)"
for artifact in "${pulsar_client_artifacts[@]}"; do
  test -s "${artifact}"
done
test -x "${delay_dir}/gradlew"

cp "${tarball}" "${image_context}/apache-pulsar-5.0.0-M1-bin.tar.gz"
cp "${script_dir}/Dockerfile.pulsar-p1" "${image_context}/Dockerfile"
cp "${script_dir}/pulsar-p1-entrypoint.sh" "${image_context}/pulsar-p1-entrypoint.sh"
docker build --pull=false -t "${image}" "${image_context}"
image_id="$(docker image inspect "${image}" --format '{{.Id}}')"

export PULSAR_P1_IMAGE="${image}"
export PULSAR_BROKER_PORT="${broker_port}"
export PULSAR_WEB_PORT="${web_port}"

echo "P1 checkout: $(git -C "${pulsar_dir}" rev-parse HEAD)"
echo "P1 distribution SHA256: $(shasum -a 256 "${tarball}" | awk '{print $1}')"
echo "P1 client SHA256:"
for artifact in "${pulsar_client_artifacts[@]}"; do
  shasum -a 256 "${artifact}"
done
echo "P1 image ID: ${image_id}"
echo "P1 runtime library count: $(find "${runtime_dir}/lib" -type f -name '*.jar' | wc -l | tr -d ' ')"
echo "Compose project: ${compose_project}"
echo "P1 ports: broker=${broker_port},web=${web_port}"

"${compose[@]}" up -d
wait_for_service

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarServiceSmoke \
  -PpulsarClientClasspath="${pulsar_client_cp}" \
  -PpulsarRuntimeDir="${runtime_dir}/lib" \
  -PpulsarServiceUrl="${service_url}" \
  -PpulsarAdminUrl="${admin_url}" \
  -PpulsarTopic="${topic}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarSourceSmoke \
  -PpulsarClientClasspath="${pulsar_client_cp}" \
  -PpulsarRuntimeDir="${runtime_dir}/lib" \
  -PpulsarServiceUrl="${service_url}" \
  -PpulsarAdminUrl="${admin_url}" \
  -PpulsarTopic="${topic}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarWorkerSmoke \
  -PpulsarClientClasspath="${pulsar_client_cp}" \
  -PpulsarRuntimeDir="${runtime_dir}/lib" \
  -PpulsarServiceUrl="${service_url}" \
  -PpulsarAdminUrl="${admin_url}" \
  -PpulsarTopic="${topic}" \
  --no-daemon --console=plain

echo "Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, Broker timestamp, Worker recovery/apply, and ACK handoff."
