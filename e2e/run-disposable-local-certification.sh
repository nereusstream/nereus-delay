#!/usr/bin/env bash
set -Eeuo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_root="$(cd "${script_dir}/.." && pwd)"
run_suffix="$(date -u +%Y%m%d%H%M%S)-$$-${RANDOM}"
artifact_base="${NEREUS_DELAY_DISPOSABLE_ARTIFACT_DIR:-}"
if [[ -n "${artifact_base}" ]]; then
  mkdir -p "${artifact_base}"
  artifact_dir="$(cd "${artifact_base}" && pwd)/${run_suffix}"
  mkdir -p "${artifact_dir}"
else
  artifact_dir="$(mktemp -d -t nereus-delay-ndip1-cert.XXXXXX)"
fi
mkdir -p "${artifact_dir}/logs" "${artifact_dir}/evidence" "${artifact_dir}/recovery"

records_file="${artifact_dir}/matrix-records.tsv"
supporting_file="${artifact_dir}/supporting-checks.tsv"
cleanup_json="${artifact_dir}/cleanup.json"
context_json="${artifact_dir}/context.json"
attestation_path="${artifact_dir}/disposable-local-attestation.json"
receipt_path="${artifact_dir}/disposable-local-certification-receipt.json"
: >"${records_file}"
: >"${supporting_file}"

p1_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_root}/../pulsar-worktrees/nereus-delay-p1}"
oxia_checkout="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_root}/../oxia}"
p1_expected_commit="0a2536484cd3932801a98dc88ff112b2df88a1c7"
p1_tarball="${NEREUS_DELAY_PULSAR_TARBALL:-${p1_dir}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz}"
p1_client_cp="${p1_dir}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:${p1_dir}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:${p1_dir}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"
IFS=: read -r -a p1_client_artifacts <<<"${p1_client_cp}"

minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
minio_digest_hex="${minio_digest#sha256:}"
minio_region="us-east-1"
minio_access_key="ndip1${run_suffix//-/}"
minio_secret_key="ndip1-secret-${run_suffix//-/}-x"
minio_bucket="ndip1-${run_suffix}"

compose_project="nereus-delay-ndip1-${run_suffix}"
resource_prefix="${compose_project}-resource"
p1_image="${resource_prefix}-pulsar-p1:local"
image_context=""
runtime_dir=""
compose_started=0
cleanup_started=0
test_process_pid=""
oxia_bootstrap_log="${artifact_dir}/logs/oxia-bootstrap.log"
oxia_admin_address=""

remove_exact_directory() {
  local directory="$1"
  [[ -d "${directory}" ]] || return 0
  find "${directory}" -depth -type f -delete 2>/dev/null || true
  find "${directory}" -depth -type l -delete 2>/dev/null || true
  find "${directory}" -depth -type d -empty -delete 2>/dev/null || true
  rmdir "${directory}" 2>/dev/null || true
}

cleanup_on_exit() {
  local saved_exit=$?
  trap - EXIT INT TERM
  set +e
  if [[ "${cleanup_started:-0}" != 1 ]]; then
    if [[ -n "${test_process_pid:-}" ]]; then
      kill "${test_process_pid}" >/dev/null 2>&1 || true
      wait "${test_process_pid}" >/dev/null 2>&1 || true
    fi
    if [[ "${compose_started:-0}" == 1 ]]; then
      "${compose[@]}" down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
    fi
    [[ -z "${p1_image:-}" ]] || docker image rm "${p1_image}" >/dev/null 2>&1 || true
    remove_exact_directory "${image_context:-}"
    remove_exact_directory "${runtime_dir:-}"
  fi
  exit "${saved_exit}"
}
trap cleanup_on_exit EXIT INT TERM

base_port="${NEREUS_DELAY_DISPOSABLE_BASE_PORT:-$((24000 + ($$ % 800)))}"
if [[ ! "${base_port}" =~ ^[0-9]+$ ]]; then
  echo "NEREUS_DELAY_DISPOSABLE_BASE_PORT must be numeric" >&2
  exit 2
fi
if (( base_port < 1024 || base_port + 31 > 65535 )); then
  echo "disposable certification port range is outside the user TCP range" >&2
  exit 2
fi
broker_1_port="${PULSAR_BROKER_1_PORT:-${base_port}}"
web_1_port="${PULSAR_WEB_1_PORT:-$((base_port + 1))}"
broker_2_port="${PULSAR_BROKER_2_PORT:-$((base_port + 2))}"
web_2_port="${PULSAR_WEB_2_PORT:-$((base_port + 3))}"
oxia_coordinator_1_port="${NEREUS_DELAY_OXIA_COORDINATOR_1_PORT:-$((base_port + 11))}"
oxia_coordinator_2_port="${NEREUS_DELAY_OXIA_COORDINATOR_2_PORT:-$((base_port + 12))}"
oxia_coordinator_3_port="${NEREUS_DELAY_OXIA_COORDINATOR_3_PORT:-$((base_port + 13))}"
oxia_data_1_port="${NEREUS_DELAY_OXIA_DATA_SERVER_1_PORT:-$((base_port + 21))}"
oxia_data_2_port="${NEREUS_DELAY_OXIA_DATA_SERVER_2_PORT:-$((base_port + 22))}"
oxia_data_3_port="${NEREUS_DELAY_OXIA_DATA_SERVER_3_PORT:-$((base_port + 23))}"
minio_port="${NEREUS_DELAY_MINIO_PORT:-$((base_port + 31))}"

for port in "${broker_1_port}" "${web_1_port}" "${broker_2_port}" "${web_2_port}" \
  "${oxia_coordinator_1_port}" "${oxia_coordinator_2_port}" "${oxia_coordinator_3_port}" \
  "${oxia_data_1_port}" "${oxia_data_2_port}" "${oxia_data_3_port}" "${minio_port}"; do
  if [[ ! "${port}" =~ ^[0-9]+$ ]] || (( port < 1024 || port > 65535 )); then
    echo "disposable certification port is invalid: ${port}" >&2
    exit 2
  fi
done

compose_files=(
  "${script_dir}/docker-compose.pulsar-cluster.yml"
  "${script_dir}/docker-compose.oxia-cluster.yml"
  "${script_dir}/docker-compose.ndip1-disposable.yml"
)
compose=(docker compose --project-name "${compose_project}")
for compose_file in "${compose_files[@]}"; do
  compose+=(--file "${compose_file}")
done

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

fail_preflight() {
  echo "DISPOSABLE_LOCAL preflight failed: $*" >&2
  printf '%s\n' "$*" >"${artifact_dir}/preflight-failure.txt"
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail_preflight "$1 is required"
}

require_command docker
require_command curl
require_command git
require_command python3
require_command shasum
require_command tar
require_command rg
docker info >"${artifact_dir}/logs/docker-info.log" 2>&1 || fail_preflight "Docker daemon is not available"
docker compose version >"${artifact_dir}/logs/docker-compose-version.log" 2>&1 \
  || fail_preflight "Docker Compose is not available"
curl --help all 2>/dev/null | grep -F -- '--aws-sigv4' >/dev/null \
  || fail_preflight "curl with --aws-sigv4 is required for MinIO bucket setup"

[[ "$(git -C "${delay_root}" branch --show-current)" == "main" ]] \
  || fail_preflight "Delay checkout must be on main"
[[ -z "$(git -C "${delay_root}" status --porcelain)" ]] \
  || fail_preflight "Delay checkout is not clean"
git -C "${delay_root}" fetch origin main >"${artifact_dir}/logs/delay-fetch.log" 2>&1 \
  || fail_preflight "could not fetch origin/main"
read -r delay_ahead delay_behind <<<"$(git -C "${delay_root}" rev-list --left-right --count origin/main...HEAD)"
[[ "${delay_ahead}" == 0 && "${delay_behind}" == 0 ]] \
  || fail_preflight "Delay main is not cleanly synchronized with origin/main"

[[ -d "${p1_dir}" ]] || fail_preflight "P1 checkout is missing: ${p1_dir}"
[[ "$(git -C "${p1_dir}" rev-parse --verify HEAD)" == "${p1_expected_commit}" ]] \
  || fail_preflight "P1 checkout is not the locked commit ${p1_expected_commit}"
[[ -z "$(git -C "${p1_dir}" status --porcelain)" ]] \
  || fail_preflight "P1 checkout is not clean"
[[ -s "${p1_tarball}" ]] || fail_preflight "locked P1 distribution is missing: ${p1_tarball}"
for artifact in "${p1_client_artifacts[@]}"; do
  [[ -s "${artifact}" ]] || fail_preflight "locked P1 client artifact is missing: ${artifact}"
done
[[ -d "${oxia_checkout}" ]] || fail_preflight "Oxia checkout is missing: ${oxia_checkout}"
[[ "$(git -C "${oxia_checkout}" rev-parse --verify HEAD)" != "" ]] \
  || fail_preflight "Oxia checkout is not a Git checkout"
[[ -z "$(git -C "${oxia_checkout}" status --porcelain)" ]] \
  || fail_preflight "Oxia checkout is not clean"

accepted_log="${artifact_dir}/logs/accepted-package.log"
if ! (cd "${delay_root}" && python3 -B scripts/verify-ndip-package.py \
  --package-dir docs/ndip/NDIP-1 \
  --receipt docs/ndip/NDIP-1/acceptance-receipt.json \
  --require-accepted >"${accepted_log}" 2>&1); then
  sed -n '1,160p' "${accepted_log}" >&2 || true
  fail_preflight "Accepted NDIP-1 verifier failed"
fi

[[ "$(docker image inspect "${minio_image}" --format '{{join .RepoDigests "\\n"}}' 2>/dev/null \
  | grep -F "@${minio_digest}" || true)" != "" ]] \
  || fail_preflight "locked MinIO image or repository digest is not present: ${minio_image}@${minio_digest}"

check_port_free() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    ! lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return
  fi
  python3 - "${port}" <<'PY'
import socket
import sys

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    sock.close()
PY
}

for port in "${broker_1_port}" "${web_1_port}" "${broker_2_port}" "${web_2_port}" \
  "${oxia_coordinator_1_port}" "${oxia_coordinator_2_port}" "${oxia_coordinator_3_port}" \
  "${oxia_data_1_port}" "${oxia_data_2_port}" "${oxia_data_3_port}" "${minio_port}"; do
  check_port_free "${port}" || fail_preflight "TCP port is already in use: ${port}"
done

delay_commit="$(git -C "${delay_root}" rev-parse HEAD)"
p1_commit="$(git -C "${p1_dir}" rev-parse HEAD)"
p1_branch="$(git -C "${p1_dir}" branch --show-current)"
oxia_commit="$(git -C "${oxia_checkout}" rev-parse HEAD)"
p1_distribution_sha256="$(sha256_file "${p1_tarball}")"
p1_client_sha256=()
for artifact in "${p1_client_artifacts[@]}"; do
  p1_client_sha256+=("$(sha256_file "${artifact}")")
done

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export NEREUS_DELAY_DISPOSABLE_STARTED_AT="${started_at}"
runtime_dir="$(mktemp -d -t nereus-delay-ndip1-runtime.XXXXXX)"
image_context="$(mktemp -d -t nereus-delay-ndip1-image.XXXXXX)"
tar -xzf "${p1_tarball}" -C "${runtime_dir}" --strip-components=1 "apache-pulsar-5.0.0-M1/lib" \
  || fail_preflight "could not extract the locked P1 runtime"
[[ -n "$(find "${runtime_dir}/lib" -type f -name '*.jar' -print -quit)" ]] \
  || fail_preflight "extracted P1 runtime has no jars"
cp "${p1_tarball}" "${image_context}/apache-pulsar-5.0.0-M1-bin.tar.gz"
cp "${script_dir}/Dockerfile.pulsar-p1" "${image_context}/Dockerfile"
cp "${script_dir}/pulsar-p1-entrypoint.sh" "${image_context}/pulsar-p1-entrypoint.sh"
cp "${script_dir}/pulsar-p1-cluster-entrypoint.sh" "${image_context}/pulsar-p1-cluster-entrypoint.sh"
docker build --pull=false -t "${p1_image}" "${image_context}" \
  >"${artifact_dir}/logs/p1-image-build.log" 2>&1 \
  || fail_preflight "could not build the source-locked P1 image"

export PULSAR_P1_IMAGE="${p1_image}"
export PULSAR_CLUSTER_NAME="standalone"
export PULSAR_BROKER_1_PORT="${broker_1_port}"
export PULSAR_WEB_1_PORT="${web_1_port}"
export PULSAR_BROKER_2_PORT="${broker_2_port}"
export PULSAR_WEB_2_PORT="${web_2_port}"
export NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}"
export NEREUS_DELAY_OXIA_COORDINATOR_1_PORT="${oxia_coordinator_1_port}"
export NEREUS_DELAY_OXIA_COORDINATOR_2_PORT="${oxia_coordinator_2_port}"
export NEREUS_DELAY_OXIA_COORDINATOR_3_PORT="${oxia_coordinator_3_port}"
export NEREUS_DELAY_OXIA_DATA_SERVER_1_PORT="${oxia_data_1_port}"
export NEREUS_DELAY_OXIA_DATA_SERVER_2_PORT="${oxia_data_2_port}"
export NEREUS_DELAY_OXIA_DATA_SERVER_3_PORT="${oxia_data_3_port}"
export NEREUS_DELAY_MINIO_IMAGE="${minio_image}"
export NEREUS_DELAY_MINIO_ACCESS_KEY="${minio_access_key}"
export NEREUS_DELAY_MINIO_SECRET_KEY="${minio_secret_key}"
export NEREUS_DELAY_MINIO_PORT="${minio_port}"
export NEREUS_DELAY_DISPOSABLE_RESOURCE_PREFIX="${resource_prefix}"
export NEREUS_DELAY_PULSAR_DELAYED_DELIVERY_STRICT="true"

compose_config="${artifact_dir}/compose-config.yaml"
"${compose[@]}" config --no-interpolate --format yaml >"${compose_config}" \
  || fail_preflight "disposable compose configuration did not parse"
compose_config_sha256="$(sha256_file "${compose_config}")"

command_topic="${resource_prefix}-command"
evidence_topic="${resource_prefix}-evidence"
business_topic="${resource_prefix}-business"
native_topic_names=(
  "${resource_prefix}-native-shared-strict"
  "${resource_prefix}-native-shared-non-strict"
  "${resource_prefix}-native-shared-disabled"
  "${resource_prefix}-native-key-shared-strict"
  "${resource_prefix}-native-key-shared-non-strict"
  "${resource_prefix}-native-key-shared-disabled"
  "${resource_prefix}-native-exclusive-immediate"
  "${resource_prefix}-native-failover-immediate"
)
created_topics_file="${artifact_dir}/created-topics.txt"
: >"${created_topics_file}"

accepted_package_sha256="$(python3 - "${delay_root}/docs/ndip/NDIP-1/acceptance-receipt.json" <<'PY'
import json
import sys
print(json.loads(open(sys.argv[1], encoding="utf-8").read())["normativePackage"]["digest"])
PY
)"

python3 - "${attestation_path}" "${delay_commit}" "${p1_commit}" "${oxia_commit}" \
  "${command_topic}" "${evidence_topic}" "${business_topic}" "${started_at}" \
  "${compose_project}" "${resource_prefix}" "${#native_topic_names[@]}" "${native_topic_names[@]}" <<'PY'
import json
import sys
from pathlib import Path

(
    output,
    delay_commit,
    p1_commit,
    oxia_commit,
    command_topic,
    evidence_topic,
    business_topic,
    created_at,
    compose_project,
    resource_prefix,
    native_count,
    *native_topics,
) = sys.argv[1:]
native_topics = native_topics[-int(native_count) :]
services = [
    "zk", "pulsar-init", "bookie", "pulsar-broker-1", "pulsar-broker-2",
    "coordinator-1", "coordinator-2", "coordinator-3", "data-server-1",
    "data-server-2", "data-server-3", "minio",
]
volumes = ["zk-data", "bookie-data", "broker-1-data", "broker-1-logs", "broker-2-data", "broker-2-logs", "minio-data"]
resources = {
    "containers": [f"{compose_project}-{service}-1" for service in services],
    "volumes": [f"{resource_prefix}-{volume}" for volume in volumes],
    "networks": [f"{resource_prefix}-pulsar", f"{resource_prefix}-services"],
    "topics": [command_topic, evidence_topic, business_topic, *native_topics],
}
attestation = {
    "schema": "nereus-delay.disposable-local-attestation-r1",
    "classification": "DISPOSABLE_LOCAL",
    "authority": False,
    "gateC": False,
    "shadow": False,
    "enabled": False,
    "delayCommit": delay_commit,
    "p1Commit": p1_commit,
    "oxiaCommit": oxia_commit,
    "acceptedPackageSha256": "PENDING_PACKAGE_DIGEST",
    "composeProject": compose_project,
    "resourcePrefix": resource_prefix,
    "resources": resources,
    "createdAt": created_at,
}
Path(output).write_text(json.dumps(attestation, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

python3 - "${attestation_path}" "${accepted_package_sha256}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
value = json.loads(path.read_text(encoding="utf-8"))
value["acceptedPackageSha256"] = sys.argv[2]
path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
attestation_sha256="$(sha256_file "${attestation_path}")"

python3 - "${context_json}" "${delay_root}" "${delay_commit}" "${p1_dir}" "${p1_commit}" \
  "${p1_expected_commit}" "${p1_branch}" "${oxia_checkout}" "${oxia_commit}" \
  "${delay_root}/docs/ndip/NDIP-1" "${delay_root}/docs/ndip/NDIP-1/acceptance-receipt.json" \
  "${accepted_package_sha256}" "${p1_tarball}" "${p1_distribution_sha256}" \
  "${compose_config}" "${compose_config_sha256}" "${attestation_path}" "${attestation_sha256}" \
  "${p1_client_cp}" "${compose_project}" "${resource_prefix}" "${p1_image}" "${minio_image}" \
  "${minio_digest_hex}" "${broker_1_port}" "${web_1_port}" "${broker_2_port}" "${web_2_port}" \
  "${oxia_data_1_port}" "${oxia_data_2_port}" "${oxia_data_3_port}" "${minio_port}" \
  "${command_topic}" "${evidence_topic}" "${business_topic}" "${oxia_bootstrap_log}" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path

(
    output, delay_root, delay_commit, p1_dir, p1_commit, p1_expected, p1_branch,
    oxia_dir, oxia_commit, package_dir, accepted_receipt, package_sha, p1_tarball,
    p1_tarball_sha, compose_config, compose_config_sha, attestation, attestation_sha,
    client_cp, compose_project, resource_prefix, p1_image, minio_image, minio_digest,
    broker_1, web_1, broker_2, web_2, oxia_data_1, oxia_data_2, oxia_data_3,
    minio_port, command_topic, evidence_topic, business_topic, oxia_bootstrap_log,
) = sys.argv[1:]
client_entries = [
    {"path": path, "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()}
    for path in client_cp.split(":")
]
compose_paths = [
    str(Path(delay_root) / "e2e" / "docker-compose.pulsar-cluster.yml"),
    str(Path(delay_root) / "e2e" / "docker-compose.oxia-cluster.yml"),
    str(Path(delay_root) / "e2e" / "docker-compose.ndip1-disposable.yml"),
]
compose_entries = [
    {"path": path, "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()}
    for path in compose_paths
]
services = [
    "zk", "pulsar-init", "bookie", "pulsar-broker-1", "pulsar-broker-2",
    "coordinator-1", "coordinator-2", "coordinator-3", "data-server-1",
    "data-server-2", "data-server-3", "minio",
]
volumes = ["zk-data", "bookie-data", "broker-1-data", "broker-1-logs", "broker-2-data", "broker-2-logs", "minio-data"]
context = {
    "startedAt": os.environ["NEREUS_DELAY_DISPOSABLE_STARTED_AT"],
    "source": {
        "delayCheckout": delay_root,
        "delayCommit": delay_commit,
        "p1Checkout": p1_dir,
        "p1Commit": p1_commit,
        "p1ExpectedCommit": p1_expected,
        "p1Branch": p1_branch,
        "oxiaCheckout": oxia_dir,
        "oxiaCommit": oxia_commit,
        "acceptedPackageDir": package_dir,
        "acceptedReceiptPath": accepted_receipt,
        "acceptedPackageSha256": package_sha,
        "p1DistributionPath": p1_tarball,
        "p1DistributionSha256": p1_tarball_sha,
        "p1ClientArtifacts": client_entries,
        "composeFiles": compose_entries,
        "composeConfigPath": compose_config,
        "composeConfigSha256": compose_config_sha,
        "attestationPath": attestation,
        "attestationSha256": attestation_sha,
    },
    "environment": {
        "attestation": "DISPOSABLE_LOCAL",
        "composeProject": compose_project,
        "resourcePrefix": resource_prefix,
        "p1Image": p1_image,
        "minioImage": minio_image,
        "minioRepoDigest": minio_digest,
        "ports": {
            "broker1": int(broker_1), "web1": int(web_1), "broker2": int(broker_2), "web2": int(web_2),
            "oxiaData1": int(oxia_data_1), "oxiaData2": int(oxia_data_2), "oxiaData3": int(oxia_data_3),
            "minio": int(minio_port),
        },
        "topics": {"command": command_topic, "evidence": evidence_topic, "business": business_topic},
        "workers": ["worker-a", "worker-b"],
        "resources": {
            "containers": [f"{compose_project}-{service}-1" for service in services],
            "volumes": [f"{resource_prefix}-{volume}" for volume in volumes],
            "networks": [f"{resource_prefix}-pulsar", f"{resource_prefix}-services"],
            "topics": [command_topic, evidence_topic, business_topic],
        },
        "oxiaBootstrap": {
            "status": "PENDING",
            "command": (
                f"docker compose --project-name {compose_project} exec coordinator-1 oxia admin "
                "register data-server-1..3 with host public addresses and create default "
                "namespace with initial-shards=1 replication-factor=3; verify RUNNING"
            ),
            "logPath": oxia_bootstrap_log,
            "resultSha256": "0" * 64,
            "namespace": {
                "name": "default",
                "initialShards": 1,
                "replicationFactor": 3,
                "notifications": True,
            },
            "dataServers": [
                {
                    "name": f"data-server-{index}",
                    "public": f"127.0.0.1:{port}",
                    "internal": f"data-server-{index}:6649",
                    "state": "PENDING",
                }
                for index, port in enumerate((oxia_data_1, oxia_data_2, oxia_data_3), 1)
            ],
        },
    },
}
Path(output).write_text(json.dumps(context, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

write_generic_evidence() {
  local path="$1"
  local cell_id="$2"
  local status="$3"
  local reason="$4"
  local log_path="$5"
  python3 - "${path}" "${cell_id}" "${status}" "${reason}" "${log_path}" <<'PY'
import json
import sys
from pathlib import Path

path, cell_id, status, reason, log_path = sys.argv[1:]
Path(path).parent.mkdir(parents=True, exist_ok=True)
Path(path).write_text(json.dumps({
    "schema": "nereus-delay.disposable-local.runner-evidence-r1",
    "classification": "DISPOSABLE_LOCAL",
    "cellId": cell_id,
    "status": status,
    "reason": reason,
    "logPath": log_path,
}, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

record_cell() {
  local cell_id="$1"
  local category="$2"
  local expected="$3"
  local status="$4"
  local skipped="$5"
  local command_text="$6"
  local log_path="$7"
  local evidence_path="$8"
  local reason="$9"
  reason="${reason//$'\t'/ }"
  reason="${reason//$'\n'/ }"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${cell_id}" "${category}" "${expected}" "${status}" "${skipped}" \
    "${command_text}" "${log_path}" "${evidence_path}" "${reason}" >>"${records_file}"
}

record_supporting() {
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >>"${supporting_file}"
}

assert_focused_test_executed() {
  python3 - "$1" "$2" <<'PY'
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

selector, marker_text = sys.argv[1:]
marker = Path(marker_text)
if not marker.is_file():
    raise SystemExit("focused-test execution marker is missing")
class_name, method_name = selector.rsplit(".", 1)
result_root = Path("build/test-results/test")
if not result_root.is_dir():
    raise SystemExit("Gradle test-results directory is missing")
minimum_mtime = marker.stat().st_mtime_ns
paths = [
    path
    for path in result_root.rglob("*.xml")
    if path.is_file() and path.stat().st_mtime_ns > minimum_mtime
]
matches = []
for path in paths:
    try:
        suite = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise SystemExit(f"invalid Gradle test result XML: {path}: {exc}") from exc
    for case in suite.findall("testcase"):
        if case.get("classname") != class_name:
            continue
        case_method = (case.get("name") or "").split("(", 1)[0]
        if case_method == method_name:
            matches.append(case)
if len(matches) != 1:
    raise SystemExit(
        f"focused test did not execute exactly once: selector={selector}, matches={len(matches)}"
    )
case = matches[0]
if case.find("skipped") is not None:
    raise SystemExit(f"focused test was skipped: {selector}")
if case.find("failure") is not None or case.find("error") is not None:
    raise SystemExit(f"focused test result contains failure or error: {selector}")
print(f"focused test executed exactly once without skip: {selector}")
PY
}

validate_native_evidence() {
  python3 - "$1" "$2" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], encoding="utf-8"))
if value.get("classification") != "DISPOSABLE_LOCAL":
    raise SystemExit("native evidence classification is not DISPOSABLE_LOCAL")
if value.get("cellId") != sys.argv[2]:
    raise SystemExit("native evidence cell id does not match the command")
if value.get("verdict") != "PASS":
    raise SystemExit("native evidence verdict is not PASS")
PY
}

wait_for_admin() {
  local url="$1"
  local label="$2"
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${url}/admin/v2/brokers/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Pulsar ${label} did not become ready: ${url}" >&2
  "${compose[@]}" ps >&2 || true
  return 1
}

wait_for_minio() {
  local endpoint="http://127.0.0.1:${minio_port}"
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${endpoint}/minio/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "MinIO did not become ready: ${endpoint}" >&2
  "${compose[@]}" logs minio >&2 || true
  return 1
}

wait_for_oxia() {
  local deadline=$((SECONDS + 180))
  local services=(
    "data-server-1:6648"
    "data-server-2:6648"
    "data-server-3:6648"
    "coordinator-1:6651"
    "coordinator-2:6651"
    "coordinator-3:6651"
  )
  while (( SECONDS < deadline )); do
    local all_ready=1
    local service port
    for service_port in "${services[@]}"; do
      service="${service_port%%:*}"
      port="${service_port##*:}"
      if ! "${compose[@]}" exec --no-TTY "${service}" oxia health \
        --host 127.0.0.1 --port "${port}" --timeout 2s >/dev/null 2>&1; then
        all_ready=0
        break
      fi
    done
    if [[ "${all_ready}" == 1 ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Oxia data-server-1 did not become ready" >&2
  "${compose[@]}" logs data-server-1 coordinator-1 >&2 || true
  return 1
}

oxia_admin_at() {
  local endpoint="$1"
  shift
  "${compose[@]}" exec --no-TTY coordinator-1 oxia admin \
    --admin-address "${endpoint}" "$@"
}

oxia_admin() {
  [[ -n "${oxia_admin_address}" ]] || return 1
  oxia_admin_at "${oxia_admin_address}" "$@"
}

oxia_admin_is_ready() {
  local endpoint response
  for endpoint in coordinator-1:6651 coordinator-2:6651 coordinator-3:6651; do
    if response="$(oxia_admin_at "${endpoint}" dataserver get --output json 2>/dev/null)" \
        && [[ -n "${response}" ]] \
        && python3 -c '
import json
import sys

value = json.load(sys.stdin)
sys.exit(0 if isinstance(value, list) else 1)
' <<<"${response}" >/dev/null 2>&1; then
      oxia_admin_address="${endpoint}"
      echo "Oxia coordinator admin is initialized at ${endpoint}"
      return 0
    fi
  done
  return 1
}

wait_for_oxia_admin() {
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if oxia_admin_is_ready; then
      return 0
    fi
    sleep 2
  done
  echo "Oxia coordinator admin did not become initialized" >&2
  "${compose[@]}" logs coordinator-1 coordinator-2 coordinator-3 >&2 || true
  return 1
}

bootstrap_oxia_cluster() {
  local data_servers_ready=0
  local namespace_ready=0
  local namespace_created=0
  local deadline
  local server port_var public_port registered dataservers_json namespace_json
  : >"${oxia_bootstrap_log}"
  {
    echo "Oxia disposable bootstrap: register three data servers with host-authority public addresses"
    if ! wait_for_oxia_admin; then
      return 1
    fi
    for server in 1 2 3; do
      port_var="oxia_data_${server}_port"
      public_port="${!port_var}"
      registered=0
      deadline=$((SECONDS + 180))
      while (( SECONDS < deadline )); do
        if oxia_admin dataserver create "data-server-${server}" \
            --public "127.0.0.1:${public_port}" \
            --internal "data-server-${server}:6649" --output json; then
          registered=1
          break
        fi
        sleep 2
      done
      if [[ "${registered}" != 1 ]]; then
        echo "Oxia data-server-${server} registration failed" >&2
        return 1
      fi
    done

    deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
      if dataservers_json="$(oxia_admin dataserver get --output json 2>/dev/null)" \
          && python3 -c '
import json
import sys

values = json.load(sys.stdin)
expected = {"data-server-1", "data-server-2", "data-server-3"}
observed = {
    entry.get("data_server", {}).get("identity", {}).get("name")
    for entry in values
    if isinstance(entry, dict)
}
states = {
    entry.get("data_server", {}).get("identity", {}).get("name"):
    entry.get("data_server_status", {}).get("state")
    for entry in values
    if isinstance(entry, dict)
}
sys.exit(0 if observed == expected and all(states.get(name) == "DATA_SERVER_STATE_RUNNING" for name in expected) else 1)
' <<<"${dataservers_json}" >/dev/null 2>&1; then
        data_servers_ready=1
        break
      fi
      sleep 2
    done
    if [[ "${data_servers_ready}" != 1 ]]; then
      echo "Oxia data servers did not all reach DATA_SERVER_STATE_RUNNING" >&2
      return 1
    fi

    deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
      if oxia_admin namespace get default --output json >/dev/null 2>&1; then
        namespace_created=1
        break
      fi
      if oxia_admin namespace create default --initial-shards 1 --replication-factor 3 --output json; then
        namespace_created=1
        break
      fi
      sleep 2
    done
    if [[ "${namespace_created}" != 1 ]]; then
      echo "Oxia default namespace creation failed" >&2
      return 1
    fi

    deadline=$((SECONDS + 180))
    while (( SECONDS < deadline )); do
      if namespace_json="$(oxia_admin namespace get default --output json 2>/dev/null)" \
          && python3 -c '
import json
import sys

value = json.load(sys.stdin)
status = value.get("namespace_status", {})
shards = status.get("shards", {})
ready = (
    status.get("replication_factor") == 3
    and len(shards) == 1
    and all(
        shard.get("status") == 1
        and shard.get("leader", {}).get("public", "").startswith("127.0.0.1:")
        and len(shard.get("ensemble", [])) == 3
        for shard in shards.values()
    )
)
sys.exit(0 if ready else 1)
' <<<"${namespace_json}" >/dev/null 2>&1; then
        namespace_ready=1
        break
      fi
      sleep 2
    done
    if [[ "${namespace_ready}" != 1 ]]; then
      echo "Oxia default namespace did not reach a three-server ready shard" >&2
      return 1
    fi
    oxia_admin dataserver get --output json
    oxia_admin namespace get default --output json
    echo "Oxia disposable bootstrap passed: three RUNNING data servers and default RF=3 namespace"
  } >>"${oxia_bootstrap_log}" 2>&1
}

finalize_oxia_bootstrap_context() {
  local bootstrap_sha256
  bootstrap_sha256="$(sha256_file "${oxia_bootstrap_log}")"
  python3 - "${context_json}" "${bootstrap_sha256}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
value = json.loads(path.read_text(encoding="utf-8"))
bootstrap = value["environment"]["oxiaBootstrap"]
bootstrap["status"] = "PASS"
bootstrap["resultSha256"] = sys.argv[2]
for server in bootstrap["dataServers"]:
    server["state"] = "DATA_SERVER_STATE_RUNNING"
path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

topic_guard_body() {
  python3 - "$1" <<'PY'
import base64
import hashlib
import json
import sys

topic = sys.argv[1]
incarnation = base64.urlsafe_b64encode(hashlib.sha256(("ndip1-disposable\\0" + topic).encode()).digest()).decode().rstrip("=")
print(json.dumps({
    "nereus.resource.guard.version": "1",
    "nereus.resource.incarnation": incarnation,
    "nereus.resource.created-at": str(int(__import__("time").time() * 1000)),
}, separators=(",", ":")))
PY
}

create_topic() {
  local topic="$1"
  local body
  body="$(topic_guard_body "${topic}")"
  local path="http://127.0.0.1:${web_1_port}/admin/v2/persistent/public/default/${topic}"
  local attempt code
  for attempt in $(seq 1 60); do
    code="$(curl --silent --show-error --location --output /dev/null --write-out '%{http_code}' \
      --header 'Content-Type: application/json' --request PUT --data "${body}" "${path}" || true)"
    if [[ "${code}" =~ ^2[0-9][0-9]$ ]]; then
      printf '%s\n' "${topic}" >>"${created_topics_file}"
      return 0
    fi
    if [[ "${code}" == "412" || "${code}" == "503" ]]; then
      sleep 1
      continue
    fi
    echo "could not create unique disposable topic ${topic}: HTTP ${code}" >&2
    return 1
  done
  echo "timed out creating disposable topic: ${topic}" >&2
  return 1
}

delete_topic_if_present() {
  local topic="$1"
  local admin_port="${web_1_port}"
  if ! curl --silent --fail "http://127.0.0.1:${web_1_port}/admin/v2/brokers/ready" >/dev/null 2>&1; then
    admin_port="${web_2_port}"
  fi
  local url="http://127.0.0.1:${admin_port}/admin/v2/persistent/public/default/${topic}?force=true"
  local code
  code="$(curl --silent --show-error --location --output /dev/null --write-out '%{http_code}' \
    --request DELETE "${url}" || true)"
  [[ "${code}" == "404" || "${code}" =~ ^2[0-9][0-9]$ ]]
}

run_supporting_check() {
  local id="$1"
  local command_text="$2"
  local log_path="$3"
  shift 3
  local status="FAIL"
  if "$@" >"${log_path}" 2>&1; then
    status="PASS"
  fi
  record_supporting "${id}" "${status}" "${command_text}" "${log_path}"
}

service_url_1="pulsar://127.0.0.1:${broker_1_port}"
service_url_2="pulsar://127.0.0.1:${broker_2_port}"
service_url_failover="${service_url_1},${service_url_2}"
admin_url_1="http://127.0.0.1:${web_1_port}"
admin_url_2="http://127.0.0.1:${web_2_port}"
oxia_endpoint="127.0.0.1:${oxia_data_1_port}"
minio_endpoint="http://127.0.0.1:${minio_port}"
gradle_user_home="${NEREUS_DELAY_DISPOSABLE_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"

run_junit_cell() {
  local cell_id="$1"
  local expected="$2"
  local selector="$3"
  local evidence_path="$4"
  local reason_on_pass="$5"
  shift 5
  local log_path="${artifact_dir}/logs/${cell_id//[^A-Za-z0-9_.-]/_}.log"
  local execution_marker="${artifact_dir}/recovery/${cell_id//[^A-Za-z0-9_.-]/_}.test-start"
  local env_command=(env "GRADLE_USER_HOME=${gradle_user_home}" "$@")
  local command_text="GRADLE_USER_HOME=${gradle_user_home} ./gradlew test --tests ${selector} --no-daemon --console=plain"
  local result_status="EXECUTED_FAIL"
  local result_reason="Gradle focused test failed"
  local evidence_status="FAIL"
  touch "${execution_marker}"
  if "${env_command[@]}" ./gradlew test --tests "${selector}" --rerun-tasks --no-daemon --console=plain \
      >"${log_path}" 2>&1; then
    if assert_focused_test_executed "${selector}" "${execution_marker}" >>"${log_path}" 2>&1; then
      result_status="EXECUTED_PASS"
      result_reason="${reason_on_pass}"
      evidence_status="PASS"
    else
      result_reason="focused test exited successfully but was not proven as exactly one non-skipped test"
    fi
  fi
  write_generic_evidence "${evidence_path}" "${cell_id}" "${evidence_status}" "${result_reason}" "${log_path}"
  record_cell "${cell_id}" "recovery" "${expected}" "${result_status}" "0" \
    "${command_text}" "${log_path}" "${evidence_path}" "${result_reason}"
}

mark_not_covered() {
  local cell_id="$1"
  local expected="$2"
  local reason="$3"
  local evidence_path="${artifact_dir}/evidence/${cell_id//[^A-Za-z0-9_.-]/_}.json"
  local log_path="${artifact_dir}/logs/${cell_id//[^A-Za-z0-9_.-]/_}.log"
  printf '%s\n' "NOT_COVERED: ${reason}" >"${log_path}"
  write_generic_evidence "${evidence_path}" "${cell_id}" "NOT_COVERED" "${reason}" "${log_path}"
  record_cell "${cell_id}" "recovery" "${expected}" "NOT_COVERED" "0" \
    "not-run: no safe independently controllable current-source fault cut" \
    "${log_path}" "${evidence_path}" "${reason}"
}

run_native_cell() {
  local cell_id="$1"
  local topic="$2"
  local subscription_type="$3"
  local policy_mode="$4"
  local broker_strictness="$5"
  local expected="$6"
  local evidence_path="${artifact_dir}/evidence/${cell_id//[^A-Za-z0-9_.-]/_}.json"
  local log_path="${artifact_dir}/logs/${cell_id//[^A-Za-z0-9_.-]/_}.log"
  local command_text="./gradlew runRealPulsarNativeMatrixSmoke -PpulsarNativeMatrixTopic=${topic} -PpulsarNativeMatrixSubscriptionType=${subscription_type} -PpulsarNativeMatrixPolicyMode=${policy_mode} -PpulsarNativeMatrixBrokerStrictness=${broker_strictness}"
  local result_status="EXECUTED_FAIL"
  local result_reason="P1 native matrix smoke failed"
  local evidence_status="FAIL"
  unlink "${evidence_path}" 2>/dev/null || true
  if GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarNativeMatrixSmoke \
      -PpulsarClientClasspath="${p1_client_cp}" \
      -PpulsarRuntimeDir="${runtime_dir}/lib" \
      -PpulsarNativeMatrixServiceUrl="${service_url_1}" \
      -PpulsarNativeMatrixAdminUrl="${admin_url_1}" \
      -PpulsarNativeMatrixTopic="${topic}" \
      -PpulsarNativeMatrixSubscriptionType="${subscription_type}" \
      -PpulsarNativeMatrixPolicyMode="${policy_mode}" \
      -PpulsarNativeMatrixBrokerStrictness="${broker_strictness}" \
      -PpulsarNativeMatrixEvidencePath="${evidence_path}" \
      --no-daemon --console=plain >"${log_path}" 2>&1; then
    if validate_native_evidence "${evidence_path}" "${cell_id}" >>"${log_path}" 2>&1; then
      result_status="EXECUTED_PASS"
      result_reason="${expected}"
      evidence_status="PASS"
    else
      result_reason="P1 native smoke exited successfully but did not produce valid PASS evidence"
    fi
  fi
  if [[ ! -f "${evidence_path}" ]]; then
    write_generic_evidence "${evidence_path}" "${cell_id}" "${evidence_status}" "${result_reason}" "${log_path}"
  fi
  record_cell "${cell_id}" "native" "${expected}" "${result_status}" "0" \
    "${command_text}" "${log_path}" "${evidence_path}" "${result_reason}"
}

set_broker_strictness() {
  local value="$1"
  local log_path="${artifact_dir}/logs/broker-strictness-${value}.log"
  export NEREUS_DELAY_PULSAR_DELAYED_DELIVERY_STRICT="${value}"
  if ! "${compose[@]}" up --detach --no-deps --force-recreate pulsar-broker-1 pulsar-broker-2 \
      >"${log_path}" 2>&1; then
    return 1
  fi
  wait_for_admin "${admin_url_1}" "broker-1-${value}" >>"${log_path}" 2>&1 \
    && wait_for_admin "${admin_url_2}" "broker-2-${value}" >>"${log_path}" 2>&1
}

run_real_destination_response_loss_cell() {
  local cell_id="recovery.response_loss_after_send_async_before_ack"
  local topic="${resource_prefix}-destination-response-loss"
  local evidence_path="${artifact_dir}/evidence/${cell_id//[^A-Za-z0-9_.-]/_}.json"
  local log_path="${artifact_dir}/logs/${cell_id//[^A-Za-z0-9_.-]/_}.log"
  local expected="real producer newMessage/sendAsync commit response loss is resolved by exact typed evidence without resend"
  local result_status="EXECUTED_FAIL"
  local result_reason="real destination response-loss smoke failed"
  local evidence_status="FAIL"
  printf '%s\n' "${topic}" >>"${created_topics_file}"
  if NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS=1 \
      GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarDestinationSmoke \
      -PpulsarClientClasspath="${p1_client_cp}" \
      -PpulsarRuntimeDir="${runtime_dir}/lib" \
      -PpulsarServiceUrl="${service_url_1}" \
      -PpulsarAdminUrl="${admin_url_1}" \
      -PpulsarDestinationTopic="${topic}" \
      --no-daemon --console=plain >"${log_path}" 2>&1; then
    if rg -q "response-loss smoke passed" "${log_path}"; then
      result_status="EXECUTED_PASS"
      result_reason="${expected}"
      evidence_status="PASS"
    else
      result_reason="destination task passed without its response-loss assertion"
    fi
  fi
  write_generic_evidence "${evidence_path}" "${cell_id}" "${evidence_status}" "${result_reason}" "${log_path}"
  record_cell "${cell_id}" "recovery" "${expected}" "${result_status}" "0" \
    "GRADLE_USER_HOME=${gradle_user_home} ./gradlew runRealPulsarDestinationSmoke -PpulsarDestinationTopic=${topic} with response-loss injection" \
    "${log_path}" "${evidence_path}" "${result_reason}"
}

run_real_worker_cell() {
  local cell_id="$1"
  local topic="$2"
  local destination_topic="$3"
  local expected="$4"
  local log_path="${artifact_dir}/logs/${cell_id//[^A-Za-z0-9_.-]/_}.log"
  local evidence_path="${artifact_dir}/evidence/${cell_id//[^A-Za-z0-9_.-]/_}.json"
  local result_status="EXECUTED_FAIL"
  local result_reason="real Pulsar Worker response-loss smoke failed"
  local evidence_status="FAIL"
  printf '%s\n' "${topic}" "${destination_topic}" >>"${created_topics_file}"
  if GRADLE_USER_HOME="${gradle_user_home}" \
      NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
      NEREUS_DELAY_OXIA_NAMESPACE=default \
      NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1 \
      ./gradlew runRealPulsarWorkerSmoke \
      -PpulsarClientClasspath="${p1_client_cp}" \
      -PpulsarRuntimeDir="${runtime_dir}/lib" \
      -PpulsarServiceUrl="${service_url_1}" \
      -PpulsarAdminUrl="${admin_url_1}" \
      -PpulsarTopic="${topic}" \
      -PpulsarWorkerMode=run \
      -PpulsarWorkerDestinationTopic="${destination_topic}" \
      -PpulsarWithOxia=true \
      --no-daemon --console=plain >"${log_path}" 2>&1; then
    if rg -q "destination response-loss.*smoke passed|Worker authority smoke passed" "${log_path}"; then
      result_status="EXECUTED_PASS"
      result_reason="${expected}"
      evidence_status="PASS"
    else
      result_reason="Worker task passed without its required response-loss assertion"
    fi
  fi
  write_generic_evidence "${evidence_path}" "${cell_id}" "${evidence_status}" "${result_reason}" "${log_path}"
  record_cell "${cell_id}" "recovery" "${expected}" "${result_status}" "0" \
    "GRADLE_USER_HOME=${gradle_user_home} ./gradlew runRealPulsarWorkerSmoke -PpulsarTopic=${topic} -PpulsarWorkerDestinationTopic=${destination_topic} with destination response-loss injection" \
    "${log_path}" "${evidence_path}" "${result_reason}"
}

run_real_source_ack_response_loss_cell() {
  local cell_id="recovery.response_loss_handed_off_before_checkpoint"
  local topic="${resource_prefix}-source-ack-response-loss"
  local expected="real source ACK response loss keeps the durable handoff fail-closed before final checkpoint"
  local log_path="${artifact_dir}/logs/${cell_id//[^A-Za-z0-9_.-]/_}.log"
  local evidence_path="${artifact_dir}/evidence/${cell_id//[^A-Za-z0-9_.-]/_}.json"
  local result_status="EXECUTED_FAIL"
  local result_reason="real source ACK response-loss smoke failed"
  local evidence_status="FAIL"
  printf '%s\n' "${topic}" >>"${created_topics_file}"
  if GRADLE_USER_HOME="${gradle_user_home}" \
      NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
      NEREUS_DELAY_OXIA_NAMESPACE=default \
      NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1 \
      ./gradlew runRealPulsarWorkerSmoke \
      -PpulsarClientClasspath="${p1_client_cp}" \
      -PpulsarRuntimeDir="${runtime_dir}/lib" \
      -PpulsarServiceUrl="${service_url_1}" \
      -PpulsarAdminUrl="${admin_url_1}" \
      -PpulsarTopic="${topic}" \
      -PpulsarWorkerMode=run \
      -PpulsarWithOxia=true \
      --no-daemon --console=plain >"${log_path}" 2>&1; then
    if rg -q "source ACK response-loss smoke passed" "${log_path}"; then
      result_status="EXECUTED_PASS"
      result_reason="${expected}"
      evidence_status="PASS"
    else
      result_reason="Worker task passed without its source ACK response-loss assertion"
    fi
  fi
  write_generic_evidence "${evidence_path}" "${cell_id}" "${evidence_status}" "${result_reason}" "${log_path}"
  record_cell "${cell_id}" "recovery" "${expected}" "${result_status}" "0" \
    "GRADLE_USER_HOME=${gradle_user_home} ./gradlew runRealPulsarWorkerSmoke -PpulsarTopic=${topic} with source ACK response-loss injection" \
    "${log_path}" "${evidence_path}" "${result_reason}"
}

run_oxia_restart_cell() {
  local cell_id="recovery.oxia_restart_reopen"
  local expected="real Oxia route session and cache recover after an actual data-server restart"
  local log_path="${artifact_dir}/logs/${cell_id//[^A-Za-z0-9_.-]/_}.log"
  local evidence_path="${artifact_dir}/evidence/${cell_id//[^A-Za-z0-9_.-]/_}.json"
  local gate_path="${artifact_dir}/recovery/oxia-route-release"
  local ready_path="${artifact_dir}/recovery/oxia-route-ready"
  local execution_marker="${artifact_dir}/recovery/${cell_id//[^A-Za-z0-9_.-]/_}.test-start"
  local result_status="EXECUTED_FAIL"
  local result_reason="real Oxia restart/reopen smoke failed"
  local evidence_status="FAIL"
  unlink "${gate_path}" "${ready_path}" 2>/dev/null || true
  touch "${execution_marker}"
  : >"${log_path}"
  (
    GRADLE_USER_HOME="${gradle_user_home}" \
    NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    NEREUS_DELAY_OXIA_ROUTE_RESTART_GATE="${gate_path}" \
    NEREUS_DELAY_OXIA_ROUTE_RESTART_READY="${ready_path}" \
      ./gradlew test \
      --tests com.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRouteProviderRecoversAfterRealOxiaRestart \
      --rerun-tasks --no-daemon --console=plain
  ) >"${log_path}" 2>&1 &
  test_process_pid=$!
  local ready=0
  local attempt
  for attempt in $(seq 1 180); do
    if [[ -f "${ready_path}" ]]; then
      ready=1
      break
    fi
    if ! kill -0 "${test_process_pid}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  if [[ "${ready}" == 1 ]]; then
    if "${compose[@]}" stop data-server-1 >>"${log_path}" 2>&1 \
        && sleep 2 \
        && "${compose[@]}" start data-server-1 >>"${log_path}" 2>&1 \
        && wait_for_oxia >>"${log_path}" 2>&1; then
      touch "${gate_path}"
    else
      result_reason="could not perform the exact data-server-1 stop/start cut"
    fi
  else
    result_reason="route provider did not reach the controlled restart gate"
  fi
  local test_status=0
  wait "${test_process_pid}" || test_status=$?
  test_process_pid=""
  if [[ "${test_status}" == 0 && "${ready}" == 1 ]] \
      && assert_focused_test_executed \
          "com.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRouteProviderRecoversAfterRealOxiaRestart" \
          "${execution_marker}" >>"${log_path}" 2>&1; then
    result_status="EXECUTED_PASS"
    result_reason="${expected}"
    evidence_status="PASS"
  elif [[ "${result_reason}" == "real Oxia restart/reopen smoke failed" && "${test_status}" != 0 ]]; then
    result_reason="real Oxia route restart test exited with status ${test_status}"
  elif [[ "${result_reason}" == "real Oxia restart/reopen smoke failed" ]]; then
    result_reason="route restart test completed without exact no-skip execution proof"
  fi
  write_generic_evidence "${evidence_path}" "${cell_id}" "${evidence_status}" "${result_reason}" "${log_path}"
  record_cell "${cell_id}" "recovery" "${expected}" "${result_status}" "0" \
    "GRADLE_USER_HOME=${gradle_user_home} ./gradlew test --tests OxiaRealRouteAuthoritySmokeTest.signedRouteProviderRecoversAfterRealOxiaRestart with data-server-1 stop/start" \
    "${log_path}" "${evidence_path}" "${result_reason}"
}

capture_broker_listener_config() {
  local service="$1"
  local log_path="$2"
  printf '%s\n' "--- ${service} effective broker listener config ---" >>"${log_path}"
  "${compose[@]}" exec -T "${service}" sh -c \
    "grep -E '^(brokerServicePort|webServicePort|bindAddress|bindAddresses|advertisedAddress|advertisedListeners|internalListenerName)=' /opt/pulsar/conf/broker.conf" \
    >>"${log_path}" 2>&1 || printf '%s\n' "could not read ${service} broker.conf" >>"${log_path}"
}

capture_topic_lookup() {
  local admin_endpoint="$1"
  local topic="$2"
  local log_path="$3"
  local label="$4"
  printf '%s\n' "--- ${label} HTTP topic lookup (listenerName=external) ---" >>"${log_path}"
  curl --silent --show-error --fail --location \
    --header 'X-Pulsar-ListenerName: external' \
    "${admin_endpoint}/lookup/v2/topic/persistent/public/default/${topic}?listenerName=external&authoritative=false" \
    >>"${log_path}" 2>&1 || printf '%s\n' "topic lookup failed for ${label}" >>"${log_path}"
  printf '\n' >>"${log_path}"
}

run_broker_failover_cell() {
  local cell_id="recovery.broker_restart_failover"
  local topic="${resource_prefix}-broker-failover"
  local expected="same real Worker topic resumes through broker-2 after broker-1 stop and broker-1 rejoins"
  local log_path="${artifact_dir}/logs/${cell_id//[^A-Za-z0-9_.-]/_}.log"
  local evidence_path="${artifact_dir}/evidence/${cell_id//[^A-Za-z0-9_.-]/_}.json"
  local dump_dir="${artifact_dir}/recovery/broker-state"
  local result_status="EXECUTED_FAIL"
  local result_reason="real two-Broker failover smoke failed"
  local evidence_status="FAIL"
  local broker_stopped=0
  local broker_rejoined=1
  mkdir -p "${dump_dir}"
  printf '%s\n' "${topic}" >>"${created_topics_file}"
  : >"${log_path}"
  capture_broker_listener_config pulsar-broker-1 "${log_path}"
  capture_broker_listener_config pulsar-broker-2 "${log_path}"
  if GRADLE_USER_HOME="${gradle_user_home}" \
      NEREUS_DELAY_PULSAR_LISTENER_NAME=external ./gradlew runRealPulsarWorkerSmoke \
      -PpulsarClientClasspath="${p1_client_cp}" \
      -PpulsarRuntimeDir="${runtime_dir}/lib" \
      -PpulsarServiceUrl="${service_url_1}" \
      -PpulsarAdminUrl="${admin_url_1}" \
      -PpulsarTopic="${topic}" \
      -PpulsarWorkerMode=prepare \
      --no-daemon --console=plain >>"${log_path}" 2>&1; then
    capture_topic_lookup "${admin_url_1}" "${topic}" "${log_path}" "before-broker-1-stop"
    if NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_CELL=ndip1-disposable-broker-failover \
        NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_PHASE=before \
        NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_DUMP_DIR="${dump_dir}" \
        NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT="${admin_url_1}" \
        GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarBrokerRecoveryStateSmoke \
        -PpulsarClientClasspath="${p1_client_cp}" \
        -PpulsarRuntimeDir="${runtime_dir}/lib" \
        -PpulsarAdminUrl="${admin_url_1}" \
        -PpulsarBrokerRecoveryTopic="${topic}" \
        --no-daemon --console=plain >>"${log_path}" 2>&1; then
      if "${compose[@]}" stop pulsar-broker-1 >>"${log_path}" 2>&1; then
        broker_stopped=1
        if wait_for_admin "${admin_url_2}" "broker-2-failover" >>"${log_path}" 2>&1 \
            && NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_CELL=ndip1-disposable-broker-failover \
            NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_PHASE=after \
            NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_DUMP_DIR="${dump_dir}" \
            NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT="${admin_url_2}" \
            GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarBrokerRecoveryStateSmoke \
            -PpulsarClientClasspath="${p1_client_cp}" \
            -PpulsarRuntimeDir="${runtime_dir}/lib" \
            -PpulsarAdminUrl="${admin_url_2}" \
            -PpulsarBrokerRecoveryTopic="${topic}" \
            --no-daemon --console=plain >>"${log_path}" 2>&1; then
          capture_topic_lookup "${admin_url_2}" "${topic}" "${log_path}" "after-broker-1-stop"
          if NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
              NEREUS_DELAY_OXIA_NAMESPACE=default \
              NEREUS_DELAY_PULSAR_LISTENER_NAME=external \
              GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarWorkerSmoke \
              -PpulsarClientClasspath="${p1_client_cp}" \
              -PpulsarRuntimeDir="${runtime_dir}/lib" \
              -PpulsarServiceUrl="${service_url_failover}" \
              -PpulsarAdminUrl="${admin_url_2}" \
              -PpulsarTopic="${topic}" \
              -PpulsarWorkerMode=resume \
              -PpulsarWithOxia=true \
              --no-daemon --console=plain >>"${log_path}" 2>&1; then
            result_status="EXECUTED_PASS"
            result_reason="${expected}"
            evidence_status="PASS"
          else
            result_reason="Worker resume through broker-2 failed"
          fi
        else
          result_reason="broker-2 did not retain the durable topic state"
        fi
      else
        result_reason="could not stop the exact broker-1 service"
      fi
    else
      result_reason="broker-1 pre-failover durable-state read failed"
    fi
  else
    result_reason="Worker preparation against broker-1 failed"
  fi
  if [[ "${broker_stopped}" == 1 ]]; then
    if ! "${compose[@]}" start pulsar-broker-1 >>"${log_path}" 2>&1 \
        || ! wait_for_admin "${admin_url_1}" "broker-1-rejoin" >>"${log_path}" 2>&1; then
      broker_rejoined=0
    fi
  fi
  if [[ "${result_status}" == "EXECUTED_PASS" ]] \
      && [[ ! -f "${dump_dir}/before.json" || ! -f "${dump_dir}/after.json" ]]; then
    result_status="EXECUTED_FAIL"
    result_reason="failover command passed without both durable-state receipts"
    evidence_status="FAIL"
  fi
  if [[ "${result_status}" == "EXECUTED_PASS" && "${broker_rejoined}" != 1 ]]; then
    result_status="EXECUTED_FAIL"
    result_reason="broker-1 did not rejoin after the failover handoff"
    evidence_status="FAIL"
  fi
  if [[ "${result_status}" != "EXECUTED_PASS" ]]; then
    printf '%s\n' '--- tail of Pulsar broker logs after failover ---' >>"${log_path}"
    "${compose[@]}" logs --no-color --tail=400 pulsar-broker-1 pulsar-broker-2 >>"${log_path}" 2>&1 || true
  fi
  write_generic_evidence "${evidence_path}" "${cell_id}" "${evidence_status}" "${result_reason}" "${log_path}"
  record_cell "${cell_id}" "recovery" "${expected}" "${result_status}" "0" \
    "real P1 Worker prepare -> broker-1 stop -> broker-2 state read/resume -> broker-1 rejoin" \
    "${log_path}" "${evidence_path}" "${result_reason}"
}

cleanup_exact() {
  [[ "${cleanup_started}" == 1 ]] && return 0
  cleanup_started=1
  cleanup_in_progress=1
  set +e
  if [[ -n "${test_process_pid}" ]]; then
    kill "${test_process_pid}" >/dev/null 2>&1 || true
    wait "${test_process_pid}" >/dev/null 2>&1 || true
    test_process_pid=""
  fi

  local topic_failure_file="${artifact_dir}/cleanup-topic-failures.txt"
  : >"${topic_failure_file}"
  if [[ -f "${created_topics_file}" ]]; then
    while IFS= read -r topic; do
      [[ -n "${topic}" ]] || continue
      delete_topic_if_present "${topic}" || printf '%s\n' "${topic}" >>"${topic_failure_file}"
    done <"${created_topics_file}"
  fi

  if [[ "${compose_started}" == 1 ]]; then
    "${compose[@]}" down --volumes --remove-orphans --rmi local \
      >"${artifact_dir}/logs/compose-down.log" 2>&1 || true
  fi
  docker image rm "${p1_image}" >"${artifact_dir}/logs/p1-image-remove.log" 2>&1 || true

  local compose_absent="true"
  local containers_file="${artifact_dir}/cleanup-containers.txt"
  local volumes_file="${artifact_dir}/cleanup-volumes.txt"
  local networks_file="${artifact_dir}/cleanup-networks.txt"
  local images_file="${artifact_dir}/cleanup-images.txt"
  local processes_file="${artifact_dir}/cleanup-processes.txt"
  local credentials_file="${artifact_dir}/cleanup-credentials.txt"
  local topics_file="${artifact_dir}/cleanup-topics.txt"
  : >"${containers_file}"; : >"${volumes_file}"; : >"${networks_file}"
  : >"${images_file}"; : >"${processes_file}"; : >"${credentials_file}"; : >"${topics_file}"
  docker ps -a --format '{{.Names}}' \
    | awk -v prefix="${compose_project}-" 'index($0, prefix) == 1 {print}' >"${containers_file}"
  docker volume ls --format '{{.Name}}' \
    | awk -v prefix="${resource_prefix}-" 'index($0, prefix) == 1 {print}' >"${volumes_file}"
  docker network ls --format '{{.Name}}' \
    | awk -v prefix="${resource_prefix}-" 'index($0, prefix) == 1 {print}' >"${networks_file}"
  docker image ls --format '{{.Repository}}:{{.Tag}}' \
    | awk -v project="${compose_project}" -v resource="${resource_prefix}" \
        'index($0, project) == 1 || index($0, resource) == 1 {print}' >"${images_file}"
  if [[ -d "${artifact_dir}" ]]; then
    rg -l --hidden --fixed-strings -- "${minio_access_key}" "${artifact_dir}" >>"${credentials_file}" 2>/dev/null || true
    rg -l --hidden --fixed-strings -- "${minio_secret_key}" "${artifact_dir}" >>"${credentials_file}" 2>/dev/null || true
  fi
  sort -u "${credentials_file}" -o "${credentials_file}"
  if [[ -n "${test_process_pid}" ]]; then
    printf '%s\n' "${test_process_pid}" >"${processes_file}"
  fi
  if [[ -s "${topic_failure_file}" ]]; then
    cp "${topic_failure_file}" "${topics_file}"
  fi

  local cleanup_status="PASS"
  for file in "${containers_file}" "${volumes_file}" "${networks_file}" "${images_file}" \
      "${processes_file}" "${credentials_file}" "${topics_file}"; do
    [[ ! -s "${file}" ]] || cleanup_status="FAIL"
  done
  [[ ! -s "${containers_file}" ]] || compose_absent="false"
  python3 - "${cleanup_json}" "${cleanup_status}" "${compose_absent}" \
      "${containers_file}" "${volumes_file}" "${networks_file}" "${images_file}" \
      "${topics_file}" "${processes_file}" "${credentials_file}" <<'PY'
import json
import sys
from pathlib import Path

output, status, compose_absent, *files = sys.argv[1:]
def lines(path):
    return [line for line in Path(path).read_text(encoding="utf-8").splitlines() if line]

containers, volumes, networks, images, topics, processes, credentials = map(lines, files)
value = {
    "status": status,
    "composeProjectAbsent": compose_absent == "true",
    "containersRemaining": containers,
    "volumesRemaining": volumes,
    "networksRemaining": networks,
    "imagesRemaining": images,
    "topicsRemaining": topics,
    "processesRemaining": processes,
    "temporaryCredentialsRemaining": credentials,
    "method": "exact compose project down --volumes --rmi local plus exact resource-prefix audit; no broad prune",
}
Path(output).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  remove_exact_directory "${image_context}"
  remove_exact_directory "${runtime_dir}"
  return 0
}

run_oxia_minio_junit_cell() {
  local cell_id="$1"
  local selector="$2"
  local expected="$3"
  run_junit_cell "${cell_id}" "${expected}" "${selector}" \
    "${artifact_dir}/evidence/${cell_id//[^A-Za-z0-9_.-]/_}.json" \
    "${expected}" \
    "NEREUS_DELAY_OXIA_ENDPOINT=${oxia_endpoint}" \
    "NEREUS_DELAY_OXIA_NAMESPACE=default" \
    "NEREUS_DELAY_MINIO_ENDPOINT=${minio_endpoint}" \
    "NEREUS_DELAY_MINIO_REGION=${minio_region}" \
    "NEREUS_DELAY_MINIO_BUCKET=${minio_bucket}" \
    "NEREUS_DELAY_MINIO_ACCESS_KEY=${minio_access_key}" \
    "NEREUS_DELAY_MINIO_SECRET_KEY=${minio_secret_key}"
}

cd "${delay_root}"

run_supporting_check "p1.compileRealPulsar" \
  "GRADLE_USER_HOME=${gradle_user_home} ./gradlew compileRealPulsar -PpulsarClientClasspath=<locked-P1-client-graph> -PpulsarRuntimeDir=<locked-P1-lib>" \
  "${artifact_dir}/logs/p1-compileRealPulsar.log" \
  env "GRADLE_USER_HOME=${gradle_user_home}" ./gradlew compileRealPulsar \
    -PpulsarClientClasspath="${p1_client_cp}" -PpulsarRuntimeDir="${runtime_dir}/lib" \
    --rerun-tasks --no-daemon --console=plain
run_supporting_check "p1.h0" \
  "GRADLE_USER_HOME=${gradle_user_home} ./gradlew runRealPulsarH0Smoke -PpulsarClientClasspath=<locked-P1-client-graph>" \
  "${artifact_dir}/logs/p1-h0.log" \
  env "GRADLE_USER_HOME=${gradle_user_home}" ./gradlew runRealPulsarH0Smoke \
    -PpulsarClientClasspath="${p1_client_cp}" --no-daemon --console=plain

compose_started=1
"${compose[@]}" up --build --detach >"${artifact_dir}/logs/compose-up.log" 2>&1 \
  || fail_preflight "disposable real-service Compose startup failed"
wait_for_admin "${admin_url_1}" "broker-1" || fail_preflight "broker-1 did not become ready"
wait_for_admin "${admin_url_2}" "broker-2" || fail_preflight "broker-2 did not become ready"
wait_for_minio || fail_preflight "MinIO did not become ready"
wait_for_oxia || fail_preflight "Oxia did not become ready"
bootstrap_oxia_cluster || fail_preflight "Oxia disposable cluster bootstrap failed"
finalize_oxia_bootstrap_context || fail_preflight "could not bind Oxia bootstrap to the context"

curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
  --user "${minio_access_key}:${minio_secret_key}" --request PUT \
  --url "${minio_endpoint}/${minio_bucket}" \
  >"${artifact_dir}/logs/minio-bucket-create.log" 2>&1 \
  || fail_preflight "could not create the unique MinIO bucket"
curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
  --user "${minio_access_key}:${minio_secret_key}" --request PUT \
  --header 'Content-Type: application/xml' \
  --data-binary '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>' \
  --url "${minio_endpoint}/${minio_bucket}?versioning" \
  >"${artifact_dir}/logs/minio-versioning.log" 2>&1 \
  || fail_preflight "could not enable MinIO bucket versioning"

create_topic "${command_topic}" || fail_preflight "could not create command topic"
create_topic "${evidence_topic}" || fail_preflight "could not create evidence topic"
create_topic "${business_topic}" || fail_preflight "could not create business topic"
printf '%s\n' "${native_topic_names[@]}" >>"${created_topics_file}"

set_broker_strictness true || fail_preflight "could not enable strict Pulsar delayed delivery"
run_native_cell native.shared.strict "${native_topic_names[0]}" shared strict strict \
  "strict mode rejects early delivery at deliverAt"
set_broker_strictness false || fail_preflight "could not disable strict Pulsar delayed delivery"
run_native_cell native.shared.non_strict "${native_topic_names[1]}" shared non-strict non-strict \
  "non-strict mode records Pulsar tick-precision delivery risk"
run_native_cell native.shared.disabled "${native_topic_names[2]}" shared disabled disabled \
  "disabled delayed delivery permits immediate delivery"
set_broker_strictness true || fail_preflight "could not re-enable strict Pulsar delayed delivery"
run_native_cell native.key_shared.strict "${native_topic_names[3]}" key_shared strict strict \
  "strict Key_Shared mode rejects early delivery at deliverAt"
set_broker_strictness false || fail_preflight "could not disable strict Pulsar delayed delivery for Key_Shared"
run_native_cell native.key_shared.non_strict "${native_topic_names[4]}" key_shared non-strict non-strict \
  "non-strict Key_Shared mode records Pulsar tick-precision delivery risk"
run_native_cell native.key_shared.disabled "${native_topic_names[5]}" key_shared disabled disabled \
  "disabled Key_Shared delayed delivery permits immediate delivery"
run_native_cell native.exclusive.immediate "${native_topic_names[6]}" exclusive immediate non-strict \
  "Exclusive immediate delivery after persistence is an expected native PASS"
run_native_cell native.failover.immediate "${native_topic_names[7]}" failover immediate non-strict \
  "Failover immediate delivery after persistence is an expected native PASS"

run_junit_cell recovery.candidate_claim \
  "one durable Candidate/Claim transition and recovery requeue" \
  com.nereusstream.delay.runtime.DelayShardTest.localClaimIsDurableAndRecoveryRequeueRestoresSemanticTimelineAtomically \
  "${artifact_dir}/evidence/recovery.candidate_claim.json" \
  "the focused unit test passed without a conditional skip"
run_junit_cell recovery.admission \
  "one exact local Claim is consumed before Publish Admission" \
  com.nereusstream.delay.runtime.DelayShardTest.publishAdmissionConsumesExactLocalClaimBeforeCreatingAttemptLedger \
  "${artifact_dir}/evidence/recovery.admission.json" \
  "the focused unit test passed without a conditional skip"
run_junit_cell recovery.journal_mapping \
  "durable Journal mapping precedes target transaction and exact retry is idempotent" \
  com.nereusstream.delay.adapter.KafkaReceiptJournalTest.mappingMustBeDurableBeforeTargetTransactionAndExactRetryIsIdempotent \
  "${artifact_dir}/evidence/recovery.journal_mapping.json" \
  "the focused unit test passed without a conditional skip"
run_real_destination_response_loss_cell
run_real_worker_cell recovery.response_loss_after_ack_before_outcome \
  "${resource_prefix}-worker-destination-response-loss" \
  "${resource_prefix}-worker-destination" \
  "real Worker destination SEND response loss resolves typed evidence before the definitive Outcome"
mark_not_covered recovery.response_loss_after_outcome_before_handoff \
  "Outcome-before-handoff response loss" \
  "The current source has no safe independently controllable cut between definitive Outcome persistence and handoff; a mock or renamed disposable run is not promoted."
run_real_source_ack_response_loss_cell
run_junit_cell recovery.worker_ownership_transfer \
  "second real Oxia session acquires the next owner epoch after the first session closes" \
  com.nereusstream.delay.ownership.OxiaRealServiceSmokeTest.ownerLeaseCasAndEphemeralSessionWorkAgainstRealService \
  "${artifact_dir}/evidence/recovery.worker_ownership_transfer.json" \
  "real Oxia ownership transfer passed with two session owners" \
  "NEREUS_DELAY_OXIA_ENDPOINT=${oxia_endpoint}" "NEREUS_DELAY_OXIA_NAMESPACE=default"
run_broker_failover_cell
run_oxia_restart_cell
run_oxia_minio_junit_cell recovery.oxia_minio_checkpoint \
  com.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.workerCheckpointRuntimePublishesToRealMinioAndOxia \
  "real Oxia Intent/Catalog plus MinIO immutable checkpoint publication and restore"
run_oxia_minio_junit_cell recovery.oxia_minio_reaping \
  com.nereusstream.delay.store.OxiaRealCheckpointReapingSmokeTest.realOxiaOwnerAbandonmentReapsExactMinioCheckpointPrefix \
  "real Oxia owner abandonment reaps the exact versioned MinIO checkpoint prefix"
run_oxia_minio_junit_cell recovery.minio_idempotent_restore \
  com.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.immutableCheckpointUploadsIdempotentlyAndRestoresAgainstMinio \
  "real MinIO immutable checkpoint upload is idempotent and restores exact bytes"
run_junit_cell recovery.rocksdb_reopen_retention \
  "RocksDB reopen, fence/floor compaction and old message identity expiry" \
  com.nereusstream.delay.runtime.DelayShardTest.retiredMessageIdentityCompactsOnlyAfterFenceAndFloorThenExpiresOldId \
  "${artifact_dir}/evidence/recovery.rocksdb_reopen_retention.json" \
  "the focused RocksDB retention test passed without a conditional skip"

finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cleanup_exact
python3 scripts/build-disposable-local-certification.py \
  --context "${context_json}" \
  --records "${records_file}" \
  --supporting "${supporting_file}" \
  --cleanup "${cleanup_json}" \
  --output "${receipt_path}" \
  --delay-root "${delay_root}" \
  --finished-at "${finished_at}" \
  >"${artifact_dir}/logs/receipt-build.log" 2>&1 \
  || fail_preflight "could not build the disposable-local receipt"
python3 scripts/verify-disposable-local-certification.py --receipt "${receipt_path}" \
  >"${artifact_dir}/logs/receipt-verify.log" 2>&1 \
  || fail_preflight "disposable-local receipt verification failed"
receipt_status="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["status"])' "${receipt_path}")"
echo "DISPOSABLE_LOCAL receipt: ${receipt_path}"
echo "DISPOSABLE_LOCAL status: ${receipt_status}"
echo "DISPOSABLE_LOCAL artifacts: ${artifact_dir}"
if [[ "${receipt_status}" == PASS ]]; then
  exit 0
fi
exit 1
