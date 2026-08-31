#!/usr/bin/env bash
set -Eeuo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_root="$(cd "${script_dir}/.." && pwd)"
environment_id="local-docker-staging-ndip1"
classification="STAGING"
staging_root="${NEREUS_DELAY_STAGING_ROOT:-${delay_root}-staging/${environment_id}}"
resource_prefix="${NEREUS_DELAY_STAGING_RESOURCE_PREFIX:-ndip1-local-docker-staging}"
pulsar_cluster_name="ndip1-staging"
artifact_root="${staging_root}/evidence"
run_id="$(date -u +%Y%m%d%H%M%S)-$$"
run_dir="${artifact_root}/${run_id}"
gradle_home="${staging_root}/gradle-user-home"
runtime_dir="${staging_root}/p1-runtime"
image_context="${staging_root}/p1-image-context"
cert_dir="${staging_root}/certificates"
key_dir="${staging_root}/authority"
deployment_dir="${staging_root}/deployment"
deployment_pointer="${deployment_dir}/current.json"
compose_project="${resource_prefix}"
compose_file_pulsar="${script_dir}/docker-compose.pulsar-cluster.yml"
compose_file_oxia="${script_dir}/docker-compose.oxia-cluster.yml"
compose_file_staging="${script_dir}/docker-compose.ndip1-staging.yml"

pulsar_checkout="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_root}/../pulsar-worktrees/nereus-delay-p1}"
oxia_base_checkout="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_root}/../oxia}"
oxia_checkout="${staging_root}/source/oxia-staging-persistent"
oxia_cli="${oxia_checkout}/bin/oxia"
oxia_base_sha=""
oxia_patch_sha256=""
oxia_source_manifest_sha256=""
oxia_patch="${script_dir}/oxia-patches/raft-status-watch-replay-buffer.patch"
oxia_expected_base_sha="37a17bef17202d5fd6e23282da5fd26d94865484"
pulsar_tarball="${NEREUS_DELAY_PULSAR_TARBALL:-${pulsar_checkout}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz}"
pulsar_client_cp="${pulsar_checkout}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:${pulsar_checkout}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:${pulsar_checkout}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"
pulsar_image="${resource_prefix}-pulsar:0a2536484cd3932801a98dc88ff112b2df88a1c7"
minio_image="${NEREUS_DELAY_MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z}"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
minio_access_key="${NEREUS_DELAY_MINIO_ACCESS_KEY:-nereusdelayndip1}"
minio_secret_key="${NEREUS_DELAY_MINIO_SECRET_KEY:-nereus-delay-ndip1-staging-secret}"
minio_bucket="${NEREUS_DELAY_MINIO_BUCKET:-nereus-delay-ndip1-staging}"
minio_region="${NEREUS_DELAY_MINIO_REGION:-us-east-1}"

broker_1_port="${PULSAR_BROKER_1_PORT:-21961}"
web_1_port="${PULSAR_WEB_1_PORT:-21962}"
broker_2_port="${PULSAR_BROKER_2_PORT:-21963}"
web_2_port="${PULSAR_WEB_2_PORT:-21964}"
coordinator_1_port="${NEREUS_DELAY_OXIA_COORDINATOR_1_PORT:-16691}"
coordinator_2_port="${NEREUS_DELAY_OXIA_COORDINATOR_2_PORT:-16692}"
coordinator_3_port="${NEREUS_DELAY_OXIA_COORDINATOR_3_PORT:-16693}"
data_server_1_port="${NEREUS_DELAY_OXIA_DATA_SERVER_1_PORT:-16681}"
data_server_2_port="${NEREUS_DELAY_OXIA_DATA_SERVER_2_PORT:-16682}"
data_server_3_port="${NEREUS_DELAY_OXIA_DATA_SERVER_3_PORT:-16683}"
minio_port="${NEREUS_DELAY_MINIO_PORT:-21970}"
minio_proxy_port="${NEREUS_DELAY_MINIO_PROXY_PORT:-21971}"
gateway_port="${NEREUS_DELAY_GATEWAY_PORT:-22358}"
minio_fault_mode_sequence=0

service_url="pulsar://127.0.0.1:${broker_1_port}"
admin_url="http://127.0.0.1:${web_1_port}"
oxia_endpoint="127.0.0.1:${data_server_1_port}"
minio_endpoint="http://127.0.0.1:${minio_port}"
minio_proxy_endpoint="http://127.0.0.1:${minio_proxy_port}"

command_topic="ndip1-command-${run_id}"
system_topic="ndip1-system-${run_id}"
mutation_topic="ndip1-mutation-${run_id}"
worker_topic="ndip1-worker-${run_id}"
worker_destination_topic="ndip1-worker-destination-${run_id}"
route_worker_topic="ndip1-route-worker-${run_id}"
native_topic="ndip1-native-${run_id}"
broker_recovery_topic="ndip1-broker-recovery-${run_id}"
evidence_topic="ndip1-evidence-${run_id}"

command_resource="persistent://public/default/${command_topic}"
system_resource="persistent://public/default/${system_topic}"
worker_resource="persistent://public/default/${worker_topic}"
attempt_journal_resource="persistent://public/default/${worker_destination_topic}-attempt-journal"
evidence_resource="persistent://public/default/${evidence_topic}"
rocksdb_resource="${run_dir}/worker-store"
checkpoint_resource="oxia://default/${resource_prefix}/${run_id}/checkpoint-catalog"
profile_resource="oxia://default/${resource_prefix}/${run_id}/profile-state"
policy_resource="oxia://default/${resource_prefix}/${run_id}/runtime-policy"
oxia_policy_key_prefix="${resource_prefix}/${run_id}/runtime-policy"
payload_resource="s3://${minio_bucket}/${resource_prefix}/${run_id}/payload-reservation"
cursor_resource="${evidence_resource}/subscription/ndip1"
query_resource="oxia://default/${resource_prefix}/${run_id}/query-dedupe"
obligation_resource="oxia://default/${resource_prefix}/${run_id}/obligation-index"
incarnation_resource="oxia://default/${resource_prefix}/${run_id}/resource-incarnation"
worker_registry_resource="oxia://default/${resource_prefix}/${run_id}/worker-registry"

accepted_package_digest="13caab8ecdc201901f06e905f1c0bf9792780e50c6f5948f93abf2bdb8f4d21b"
p1_source_lock="0a2536484cd3932801a98dc88ff112b2df88a1c7"
p1_source_lock_digest=""
disposable_receipt="${NEREUS_DELAY_DISPOSABLE_RECEIPT:-}"
disposable_receipt_sha256="${NEREUS_DELAY_DISPOSABLE_RECEIPT_SHA256:-}"
staging_data_decision="${NEREUS_DELAY_STAGING_DATA_DECISION:-}"
staging_operator="${NEREUS_DELAY_STAGING_OPERATOR:-}"
staging_external_user_data="${NEREUS_DELAY_STAGING_EXTERNAL_USER_DATA:-}"

scope_config_path=""
assessment_config_path=""
manifest_config_path=""
gate_c_receipt=""
gate_c_receipt_sha256=""
shadow_policy_envelope=""
shadow_receipt=""
shadow_receipt_sha256=""
enabled_policy_envelope=""
canary_receipt=""
canary_receipt_sha256=""
disabled_policy_envelope=""
policy_oxia_version=0
policy_generation=0
enabled_policy_valid_until_epoch_ms=0
policy_scope_digest=""
native_target_incarnation_hex="1111111111111111111111111111111111111111111111111111111111111111"
assessed_final_summary=""
assessed_run_dir=""
assessed_run_id=""
assessed_scope_path=""
assessed_mode=""
data_disposition_payload="${run_dir}/authority/data-disposition-declaration.json"
data_disposition_envelope="${run_dir}/authority/data-disposition-declaration.signed.json"
data_disposition_envelope_sha256=""
certification_validation_envelope=""
certification_validation_envelope_sha256=""
candidate_scope_path="${run_dir}/authority/candidate-scope.json"
enabled_policy_activation_started=0
enabled_policy_rollback_attempted=0

mkdir -p "${run_dir}/logs" "${run_dir}/results" "${run_dir}/g0" "${run_dir}/authority" \
  "${staging_root}/pulsar" "${staging_root}/oxia" "${staging_root}/minio/data" \
  "${staging_root}/worker" "${staging_root}/chaos" "${staging_root}/source" "${runtime_dir}" "${image_context}" \
  "${cert_dir}" "${key_dir}"
mkdir -p "${deployment_dir}"
if [[ -e "${run_dir}/run-status.json" ]]; then
  echo "refusing to reuse evidence run directory: ${run_dir}" >&2
  exit 1
fi

fail() {
  echo "NDIP-1 persistent staging BLOCKED: $*" >&2
  exit 1
}

final_status="BLOCKED"
write_run_status() {
  local exit_code="${1:-1}"
  [[ -e "${run_dir}/run-status.json" ]] && return 0
  command -v jq >/dev/null 2>&1 || return 0
  jq -n --arg status "${final_status}" --arg environmentId "${environment_id}" \
    --arg classification "${classification}" --arg runId "${run_id}" \
    --arg candidateCommit "${candidate_commit:-unknown}" --arg root "${staging_root}" \
    --argjson exitCode "${exit_code}" \
    '{schema:"nereus-delay.ndip1-persistent-staging-run",schemaGeneration:1,status:$status,
      environmentId:$environmentId,classification:$classification,runId:$runId,
      candidateCommit:$candidateCommit,persistentRoot:$root,exitCode:$exitCode}' \
    >"${run_dir}/run-status.json.tmp" && mv -n "${run_dir}/run-status.json.tmp" "${run_dir}/run-status.json" \
    || true
}

on_exit() {
  local status=$?
  set +e
  if [[ "${status}" != 0 && "${enabled_policy_activation_started:-0}" == 1 \
      && "${enabled_policy_rollback_attempted:-0}" == 0 ]]; then
    enabled_policy_rollback_attempted=1
    disable_enabled_policy || true
  fi
  write_run_status "${status}"
  exit "${status}"
}
trap on_exit EXIT

for required_command in docker curl jq openssl python3 shasum tar xxd base64 go git make; do
  command -v "${required_command}" >/dev/null 2>&1 || fail "required command is missing: ${required_command}"
done
docker compose version >/dev/null 2>&1 || fail "docker compose is unavailable"

prepare_oxia_staging_checkout() {
  local source_root="${staging_root}/source"
  local source_manifest="${source_root}/oxia-staging-persistent-source.json"
  local source_manifest_tmp="${source_manifest}.tmp"
  local base_commit patch_digest patched_commit

  [[ -f "${oxia_patch}" ]] || fail "Oxia staging patch is missing: ${oxia_patch}"
  [[ -d "${oxia_base_checkout}/.git" ]] || fail "Oxia base checkout is not a Git checkout: ${oxia_base_checkout}"
  [[ -z "$(git -C "${oxia_base_checkout}" status --porcelain)" ]] \
    || fail "Oxia base checkout is dirty: ${oxia_base_checkout}"
  [[ "$(git -C "${oxia_base_checkout}" branch --show-current)" == "main" ]] \
    || fail "Oxia base checkout is not on main: ${oxia_base_checkout}"
  base_commit="$(git -C "${oxia_base_checkout}" rev-parse HEAD)"
  [[ "${base_commit}" == "${oxia_expected_base_sha}" ]] \
    || fail "Oxia base checkout is not the staging-locked source: ${base_commit}"
  patch_digest="$(shasum -a 256 "${oxia_patch}" | awk '{print $1}')"

  if [[ ! -e "${oxia_checkout}" ]]; then
    git clone --no-hardlinks --branch main --single-branch "${oxia_base_checkout}" "${oxia_checkout}" \
      >"${run_dir}/logs/oxia-staging-source-clone.log" 2>&1 \
      || fail "could not create the persistent Oxia staging source checkout"
    [[ "$(git -C "${oxia_checkout}" rev-parse HEAD)" == "${base_commit}" ]] \
      || fail "new Oxia staging source checkout did not start at the locked base"
    git -C "${oxia_checkout}" apply --unidiff-zero --check "${oxia_patch}" \
      >"${run_dir}/logs/oxia-staging-source-patch-check.log" 2>&1 \
      || fail "Oxia staging patch does not apply to the locked base"
    git -C "${oxia_checkout}" apply --unidiff-zero "${oxia_patch}" \
      >"${run_dir}/logs/oxia-staging-source-patch-apply.log" 2>&1 \
      || fail "could not apply the Oxia staging patch"
    [[ "$(git -C "${oxia_checkout}" diff --name-only)" == "oxiad/coordinator/metadata/factory.go" ]] \
      || fail "Oxia staging patch changed an unexpected path"
    git -C "${oxia_checkout}" add oxiad/coordinator/metadata/factory.go
    GIT_AUTHOR_NAME="Nereus Delay staging" \
      GIT_AUTHOR_EMAIL="nereus-delay-staging@localhost" \
      GIT_COMMITTER_NAME="Nereus Delay staging" \
      GIT_COMMITTER_EMAIL="nereus-delay-staging@localhost" \
      git -C "${oxia_checkout}" commit --no-verify \
        --message "fix(staging): replay raft status watch after restart" \
        >"${run_dir}/logs/oxia-staging-source-commit.log" 2>&1 \
        || fail "could not commit the Oxia staging patch"
    patched_commit="$(git -C "${oxia_checkout}" rev-parse HEAD)"
    printf '%s\n' "${patched_commit}" >"${source_root}/oxia-staging-persistent-commit.txt"
    jq -n --arg schema "nereus-delay.ndip1-oxia-staging-source" \
      --arg baseCheckout "${oxia_base_checkout}" --arg baseCommit "${base_commit}" \
      --arg patchPath "${oxia_patch}" --arg patchSha256 "${patch_digest}" \
      --arg patchedCheckout "${oxia_checkout}" --arg patchedCommit "${patched_commit}" \
      '{schema:$schema,schemaGeneration:1,stagingOnly:true,productionAuthority:false,
        baseCheckout:$baseCheckout,baseCommit:$baseCommit,patchPath:$patchPath,
        patchSha256:$patchSha256,patchedCheckout:$patchedCheckout,patchedCommit:$patchedCommit,
        buildCommand:"make -C <patchedCheckout>"}' \
      >"${source_manifest_tmp}" \
      && mv -n "${source_manifest_tmp}" "${source_manifest}" \
      || fail "could not persist the Oxia staging source manifest"
  else
    [[ -f "${source_manifest}" ]] || fail "existing Oxia staging source has no source manifest"
    jq -e --arg baseCheckout "${oxia_base_checkout}" --arg baseCommit "${base_commit}" \
      --arg patchPath "${oxia_patch}" --arg patchSha256 "${patch_digest}" \
      --arg patchedCheckout "${oxia_checkout}" \
      '.stagingOnly == true and .productionAuthority == false and
       .baseCheckout == $baseCheckout and .baseCommit == $baseCommit and
       .patchPath == $patchPath and .patchSha256 == $patchSha256 and
       .patchedCheckout == $patchedCheckout and (.patchedCommit | type == "string")' \
      "${source_manifest}" >/dev/null \
      || fail "existing Oxia staging source manifest does not match the locked patch"
    patched_commit="$(jq -r '.patchedCommit' "${source_manifest}")"
    [[ "$(git -C "${oxia_checkout}" rev-parse HEAD)" == "${patched_commit}" ]] \
      || fail "existing Oxia staging source HEAD differs from its manifest"
    [[ "$(git -C "${oxia_checkout}" rev-list --parents -n1 HEAD | awk '{print $2}')" == "${base_commit}" ]] \
      || fail "existing Oxia staging source is not a single patch commit on the locked base"
  fi

  [[ -z "$(git -C "${oxia_checkout}" status --porcelain)" ]] \
    || fail "Oxia staging source checkout is dirty: ${oxia_checkout}"
  [[ "$(git -C "${oxia_checkout}" branch --show-current)" == "main" ]] \
    || fail "Oxia staging source checkout is not on main: ${oxia_checkout}"
  [[ "$(git -C "${oxia_checkout}" rev-list --parents -n1 HEAD | awk '{print $2}')" == "${base_commit}" ]] \
    || fail "Oxia staging source parent is not the locked base"
  if [[ ! -x "${oxia_cli}" ]]; then
    make -C "${oxia_checkout}" >"${run_dir}/logs/oxia-staging-source-build.log" 2>&1 \
      || fail "could not build the persistent Oxia staging CLI"
  fi
  [[ -x "${oxia_cli}" ]] || fail "Oxia staging CLI is missing or not executable: ${oxia_cli}"
  [[ -z "$(git -C "${oxia_checkout}" status --porcelain)" ]] \
    || fail "Oxia staging source became dirty while building the CLI"
  oxia_base_sha="${base_commit}"
  oxia_patch_sha256="${patch_digest}"
  oxia_source_manifest_sha256="$(shasum -a 256 "${source_manifest}" | awk '{print $1}')"
  cp -p "${source_manifest}" "${run_dir}/g0/oxia-staging-persistent-source.json"
  cp -p "${oxia_patch}" "${run_dir}/g0/oxia-staging-persistent-source.patch"
  [[ "$(shasum -a 256 "${run_dir}/g0/oxia-staging-persistent-source.json" | awk '{print $1}')" == \
    "${oxia_source_manifest_sha256}" ]] || fail "G0 Oxia source manifest copy changed"
  [[ "$(shasum -a 256 "${run_dir}/g0/oxia-staging-persistent-source.patch" | awk '{print $1}')" == \
    "${oxia_patch_sha256}" ]] || fail "G0 Oxia staging patch copy changed"
}

git -C "${delay_root}" fetch origin main >/dev/null
[[ "$(git -C "${delay_root}" branch --show-current)" == "main" ]] \
  || fail "Delay checkout is not on main"
[[ -z "$(git -C "${delay_root}" status --porcelain)" ]] \
  || fail "Delay checkout is dirty"
[[ "$(git -C "${delay_root}" rev-parse HEAD)" == "$(git -C "${delay_root}" rev-parse origin/main)" ]] \
  || fail "Delay main is not synchronized with origin/main"
git -C "${delay_root}" merge-base --is-ancestor \
  61d1dbf196834f9667f860b813e021cc1b998d96 HEAD \
  || fail "Delay HEAD is not based on the frozen baseline"

[[ -z "$(git -C "${pulsar_checkout}" status --porcelain)" ]] \
  || fail "P1 checkout is dirty: ${pulsar_checkout}"
[[ "$(git -C "${pulsar_checkout}" rev-parse HEAD)" == "${p1_source_lock}" ]] \
  || fail "P1 checkout does not match the locked source"
prepare_oxia_staging_checkout
[[ -s "${pulsar_tarball}" ]] || fail "locked P1 distribution is missing: ${pulsar_tarball}"
for artifact in ${pulsar_client_cp//:/ }; do
  [[ -s "${artifact}" ]] || fail "locked P1 client artifact is missing: ${artifact}"
done

candidate_commit="$(git -C "${delay_root}" rev-parse HEAD)"
pulsar_sha="$(git -C "${pulsar_checkout}" rev-parse HEAD)"
pulsar_ref="$(git -C "${pulsar_checkout}" branch --show-current)"
[[ -n "${pulsar_ref}" ]] || pulsar_ref="DETACHED:${pulsar_sha}"
oxia_sha="$(git -C "${oxia_checkout}" rev-parse HEAD)"
p1_source_lock_digest="$(printf 'nereus/delay-resource-guard@%s' "${p1_source_lock}" | shasum -a 256 | awk '{print $1}')"
[[ "${p1_source_lock_digest}" == "e38d97ddcd3ba17d010fb1c75b132061551230a724785de52dee7eba9f5c34ed" ]] \
  || fail "P1 source-lock digest does not match the accepted canonical lock"
[[ -x "${oxia_cli}" ]] || fail "Oxia CLI is missing or not executable: ${oxia_cli}"
oxia_cli_build_info="$(go version -m "${oxia_cli}" 2>/dev/null || true)"
printf '%s\n' "${oxia_cli_build_info}" >"${run_dir}/g0/oxia-cli-build-info.txt"
printf '%s\n' "${oxia_cli_build_info}" | rg -F "vcs.revision=${oxia_sha}" >/dev/null \
  || fail "Oxia CLI VCS revision does not match the clean Oxia checkout: ${oxia_cli}"
oxia_cli_sha256="$(shasum -a 256 "${oxia_cli}" | awk '{print $1}')"
pulsar_tarball_sha256="$(shasum -a 256 "${pulsar_tarball}" | awk '{print $1}')"
[[ -n "${disposable_receipt}" && -n "${disposable_receipt_sha256}" ]] \
  || fail "exact disposable receipt path and SHA-256 must be supplied"
[[ -f "${disposable_receipt}" && ! -L "${disposable_receipt}" ]] \
  || fail "the disposable receipt must be a regular non-symlink file"
disposable_actual_sha256="$(shasum -a 256 "${disposable_receipt}" | awk '{print $1}')"
[[ "${disposable_actual_sha256}" == "${disposable_receipt_sha256}" ]] \
  || fail "the recorded disposable receipt is missing or has a different digest"
python3 "${delay_root}/scripts/verify-disposable-local-certification.py" \
  --receipt "${disposable_receipt}" >"${run_dir}/logs/disposable-receipt-verifier.log" 2>&1 \
  || fail "the exact disposable receipt failed independent verification"
[[ "$(jq -r '.source.delayCommit' "${disposable_receipt}")" == "${candidate_commit}" \
    && "$(jq -r '.status' "${disposable_receipt}")" == PASS \
    && "$(jq -r '.receiptSchemaGeneration' "${disposable_receipt}")" == 3 \
    && "$(jq -r '.classification' "${disposable_receipt}")" == DISPOSABLE_LOCAL \
    && "$(jq -r '.matrix | length' "${disposable_receipt}")" == 24 \
    && "$(jq -r '.authority' "${disposable_receipt}")" == false \
    && "$(jq -r '.gateC' "${disposable_receipt}")" == false \
    && "$(jq -r '.shadow' "${disposable_receipt}")" == false \
    && "$(jq -r '.enabled' "${disposable_receipt}")" == false ]] \
  || fail "the disposable receipt is not a 24/24 non-authoritative certification for the exact candidate"

python3 "${delay_root}/scripts/verify-ndip-package.py" \
  --package-dir "${delay_root}/docs/ndip/NDIP-1" \
  --receipt "${delay_root}/docs/ndip/NDIP-1/acceptance-receipt.json" \
  --require-accepted >"${run_dir}/logs/accepted-package-verifier.log" 2>&1 \
  || fail "accepted NDIP-1 package verifier failed"

export NEREUS_DELAY_STAGING_ROOT="${staging_root}"
export NEREUS_DELAY_STAGING_RESOURCE_PREFIX="${resource_prefix}"
export PULSAR_P1_IMAGE="${pulsar_image}"
export PULSAR_CLUSTER_NAME="${pulsar_cluster_name}"
export NEREUS_DELAY_PULSAR_CLUSTER_ID="${pulsar_cluster_name}"
export PULSAR_BROKER_1_PORT="${broker_1_port}"
export PULSAR_WEB_1_PORT="${web_1_port}"
export PULSAR_BROKER_2_PORT="${broker_2_port}"
export PULSAR_WEB_2_PORT="${web_2_port}"
export NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}"
export NEREUS_DELAY_OXIA_COORDINATOR_1_PORT="${coordinator_1_port}"
export NEREUS_DELAY_OXIA_COORDINATOR_2_PORT="${coordinator_2_port}"
export NEREUS_DELAY_OXIA_COORDINATOR_3_PORT="${coordinator_3_port}"
export NEREUS_DELAY_OXIA_DATA_SERVER_1_PORT="${data_server_1_port}"
export NEREUS_DELAY_OXIA_DATA_SERVER_2_PORT="${data_server_2_port}"
export NEREUS_DELAY_OXIA_DATA_SERVER_3_PORT="${data_server_3_port}"
export NEREUS_DELAY_MINIO_IMAGE="${minio_image}"
export NEREUS_DELAY_MINIO_ACCESS_KEY="${minio_access_key}"
export NEREUS_DELAY_MINIO_SECRET_KEY="${minio_secret_key}"
export NEREUS_DELAY_MINIO_PORT="${minio_port}"

compose=(docker compose --project-name "${compose_project}" \
  --file "${compose_file_pulsar}" --file "${compose_file_oxia}" --file "${compose_file_staging}")

if ! docker image inspect "${minio_image}" >/dev/null 2>&1; then
  fail "locked MinIO image is not present locally: ${minio_image}"
fi
docker image inspect --format '{{join .RepoDigests "\n"}}' "${minio_image}" \
  | rg -F "@${minio_digest}" >/dev/null \
  || fail "local MinIO image does not carry the locked digest ${minio_digest}"

runtime_client_api="${runtime_dir}/lib/org.apache.pulsar-pulsar-client-api-5.0.0-M1.jar"
if [[ ! -s "${runtime_client_api}" ]]; then
  if [[ -n "$(find "${runtime_dir}" -mindepth 1 -print -quit)" ]]; then
    fail "P1 runtime directory is non-empty but its lib is incomplete: ${runtime_dir}"
  fi
  tar -xzf "${pulsar_tarball}" -C "${runtime_dir}" --strip-components=1 "apache-pulsar-5.0.0-M1/lib"
fi
[[ -s "${runtime_client_api}" ]] || fail "P1 runtime extraction failed"

if [[ ! -s "${image_context}/apache-pulsar-5.0.0-M1-bin.tar.gz" ]]; then
  if [[ -n "$(find "${image_context}" -mindepth 1 -print -quit)" ]]; then
    fail "P1 image context is non-empty but incomplete: ${image_context}"
  fi
  cp "${pulsar_tarball}" "${image_context}/apache-pulsar-5.0.0-M1-bin.tar.gz"
  cp "${script_dir}/Dockerfile.pulsar-p1" "${image_context}/Dockerfile"
  cp "${script_dir}/pulsar-p1-entrypoint.sh" "${image_context}/pulsar-p1-entrypoint.sh"
  cp "${script_dir}/pulsar-p1-cluster-entrypoint.sh" "${image_context}/pulsar-p1-cluster-entrypoint.sh"
fi
[[ "$(shasum -a 256 "${image_context}/apache-pulsar-5.0.0-M1-bin.tar.gz" | awk '{print $1}')" == "${pulsar_tarball_sha256}" ]] \
  || fail "persistent P1 image context tarball digest differs from the locked distribution"
if ! docker image inspect "${pulsar_image}" >/dev/null 2>&1; then
  docker build --pull=false --tag "${pulsar_image}" "${image_context}" \
    >"${run_dir}/logs/pulsar-image-build.log" 2>&1 \
    || fail "locked P1 Docker image build failed"
fi
pulsar_image_id="$(docker image inspect --format '{{.Id}}' "${pulsar_image}")"

write_tls_if_needed() {
  local required=(ca.crt server.crt server.key client.crt client.key rotated-ca.crt rotated-server.crt \
    rotated-server.key rotated-client.crt rotated-client.key)
  local present=0
  for file in "${required[@]}"; do
    [[ -e "${cert_dir}/${file}" ]] && present=$((present + 1))
  done
  if (( present == ${#required[@]} )); then
    return 0
  fi
  (( present == 0 )) || fail "persistent Gateway certificate directory is incomplete"
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "${cert_dir}/ca.key" -out "${cert_dir}/ca.crt" \
    -days 365 -sha256 -subj "/CN=Nereus Delay NDIP-1 staging CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    >"${run_dir}/logs/tls-ca.log" 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "${cert_dir}/server.key" -out "${cert_dir}/server.csr" \
    -subj "/CN=localhost" >"${run_dir}/logs/tls-server-key.log" 2>&1
  openssl x509 -req -in "${cert_dir}/server.csr" -CA "${cert_dir}/ca.crt" -CAkey "${cert_dir}/ca.key" \
    -CAcreateserial -out "${cert_dir}/server.crt" -days 365 -sha256 \
    -extfile <(printf '%s\n' 'subjectAltName=IP:127.0.0.1' 'extendedKeyUsage=serverAuth') \
    >"${run_dir}/logs/tls-server-cert.log" 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "${cert_dir}/client.key" -out "${cert_dir}/client.csr" \
    -subj "/CN=nereus-delay-ndip1-client" >"${run_dir}/logs/tls-client-key.log" 2>&1
  openssl x509 -req -in "${cert_dir}/client.csr" -CA "${cert_dir}/ca.crt" -CAkey "${cert_dir}/ca.key" \
    -CAcreateserial -out "${cert_dir}/client.crt" -days 365 -sha256 \
    -extfile <(printf '%s\n' 'extendedKeyUsage=clientAuth') \
    >"${run_dir}/logs/tls-client-cert.log" 2>&1
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "${cert_dir}/rotated-ca.key" -out "${cert_dir}/rotated-ca.crt" \
    -days 365 -sha256 -subj "/CN=Nereus Delay NDIP-1 rotated staging CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    >"${run_dir}/logs/tls-rotated-ca.log" 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "${cert_dir}/rotated-server.key" -out "${cert_dir}/rotated-server.csr" \
    -subj "/CN=localhost" >"${run_dir}/logs/tls-rotated-server-key.log" 2>&1
  openssl x509 -req -in "${cert_dir}/rotated-server.csr" -CA "${cert_dir}/rotated-ca.crt" \
    -CAkey "${cert_dir}/rotated-ca.key" -CAcreateserial -out "${cert_dir}/rotated-server.crt" \
    -days 365 -sha256 -extfile <(printf '%s\n' 'subjectAltName=IP:127.0.0.1' 'extendedKeyUsage=serverAuth') \
    >"${run_dir}/logs/tls-rotated-server-cert.log" 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "${cert_dir}/rotated-client.key" -out "${cert_dir}/rotated-client.csr" \
    -subj "/CN=nereus-delay-ndip1-rotated-client" >"${run_dir}/logs/tls-rotated-client-key.log" 2>&1
  openssl x509 -req -in "${cert_dir}/rotated-client.csr" -CA "${cert_dir}/rotated-ca.crt" \
    -CAkey "${cert_dir}/rotated-ca.key" -CAcreateserial -out "${cert_dir}/rotated-client.crt" \
    -days 365 -sha256 -extfile <(printf '%s\n' 'extendedKeyUsage=clientAuth') \
    >"${run_dir}/logs/tls-rotated-client-cert.log" 2>&1
  chmod 600 "${cert_dir}"/*.key
}

wait_for_http() {
  local url="$1" deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  fail "HTTP endpoint did not become ready: ${url}"
}

wait_for_oxia_service() {
  local service="$1" port="$2" deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if "${compose[@]}" exec --no-TTY "${service}" oxia health --host 127.0.0.1 --port "${port}" --timeout 2s \
      >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  "${compose[@]}" logs "${service}" >"${run_dir}/logs/${service}-health-failure.log" 2>&1 || true
  fail "Oxia service did not become healthy: ${service}"
}

wait_for_oxia_client_ready() {
  local label="$1" ready_path="$2" deadline=$((SECONDS + 180)) attempts=0 output output_sha256
  mkdir -p "$(dirname "${ready_path}")"
  while (( SECONDS < deadline )); do
    attempts=$((attempts + 1))
    if output="$(oxia_client list --key-min "" --key-max "" 2>"${ready_path}.last-error")"; then
      output_sha256="$(printf '%s' "${output}" | shasum -a 256 | awk '{print $1}')"
      jq -n --arg schema "nereus-delay.oxia-client-readiness" --arg status PASS \
        --arg label "${label}" --arg endpoint "${oxia_endpoint}" \
        --arg checkedAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg outputSha256 "${output_sha256}" --argjson attempts "${attempts}" \
        --argjson outputLineCount "$(printf '%s\n' "${output}" | wc -l | tr -d ' ')" \
        '{schema:$schema,schemaGeneration:1,status:$status,label:$label,endpoint:$endpoint,
          checkedAt:$checkedAt,attempts:$attempts,command:"oxia client list --key-min '' --key-max ''",
          outputSha256:$outputSha256,outputLineCount:$outputLineCount}' >"${ready_path}"
      return 0
    fi
    sleep 2
  done
  fail "Oxia client endpoint did not become ready for ${label}: ${oxia_endpoint}"
}

wait_for_oxia_route_session_expiry() {
  local label="$1" evidence_path="$2" grace_seconds=20 started_at ended_at
  mkdir -p "$(dirname "${evidence_path}")"
  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sleep "${grace_seconds}"
  ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n --arg schema "nereus-delay.oxia-route-session-expiry-grace" --arg status PASS \
    --arg label "${label}" --arg endpoint "${oxia_endpoint}" \
    --arg startedAt "${started_at}" --arg endedAt "${ended_at}" \
    --argjson graceSeconds "${grace_seconds}" \
    '{schema:$schema,schemaGeneration:1,status:$status,label:$label,endpoint:$endpoint,
      startedAt:$startedAt,endedAt:$endedAt,graceSeconds:$graceSeconds,
      basis:"exceeds the 15-second Route restart smoke-test session timeout by 5 seconds"}' \
    >"${evidence_path}"
}

oxia_admin() {
  local coordinator output
  for coordinator in coordinator-1 coordinator-2 coordinator-3; do
    if output=$("${compose[@]}" exec --no-TTY "${coordinator}" oxia admin \
      --admin-address "${coordinator}:6651" "$@" 2>/dev/null); then
      printf '%s\n' "${output}"
      return 0
    fi
  done
  return 1
}

oxia_client() {
  "${oxia_cli}" client \
    --service-address "${oxia_endpoint}" --namespace default --request-timeout 5s "$@"
}

wait_for_oxia_admin_ready() {
  local phase="${1:-initial}" deadline=$((SECONDS + 180)) attempts=0 output
  local attempts_path="${run_dir}/logs/oxia-admin-ready-${phase}-attempts.log"
  local output_path="${run_dir}/g0/oxia-admin-ready-${phase}.json"
  local receipt_path="${run_dir}/g0/oxia-admin-readiness-${phase}.json"
  : >"${attempts_path}"
  while (( SECONDS < deadline )); do
    attempts=$((attempts + 1))
    if output="$(oxia_admin dataserver get -o json 2>/dev/null)"; then
      printf 'attempt=%s ready=true\n' "${attempts}" >>"${attempts_path}"
      printf '%s\n' "${output}" >"${output_path}"
      jq -n --arg schema "nereus-delay.oxia-admin-readiness" --arg status PASS \
        --arg phase "${phase}" --arg outputPath "${output_path}" \
        --argjson attempts "${attempts}" --arg outputSha256 "$(printf '%s' "${output}" | shasum -a 256 | awk '{print $1}')" \
        '{schema:$schema,schemaGeneration:1,status:$status,attempts:$attempts,
          phase:$phase,command:"oxia admin dataserver get -o json",outputPath:$outputPath,
          outputSha256:$outputSha256}' >"${receipt_path}"
      return 0
    fi
    printf 'attempt=%s ready=false\n' "${attempts}" >>"${attempts_path}"
    sleep 2
  done
  fail "Oxia admin API did not become ready after coordinator election"
}

wait_for_namespace() {
  local deadline=$((SECONDS + 180)) view
  while (( SECONDS < deadline )); do
    view="$(oxia_admin namespace get default -o json 2>/dev/null || true)"
    if printf '%s' "${view}" | jq -e '.namespace_status.shards["0"].leader.name' >/dev/null 2>&1; then
      printf '%s\n' "${view}" >"${run_dir}/g0/oxia-namespace-ready.json"
      return 0
    fi
    sleep 2
  done
  fail "Oxia default namespace did not expose a leader"
}

bootstrap_oxia() {
  wait_for_oxia_service coordinator-1 6651
  wait_for_oxia_service coordinator-2 6651
  wait_for_oxia_service coordinator-3 6651
  wait_for_oxia_service data-server-1 6648
  wait_for_oxia_service data-server-2 6648
  wait_for_oxia_service data-server-3 6648
  wait_for_oxia_admin_ready initial
  local output
  if ! output="$(oxia_admin dataserver get ds-1 -o json 2>/dev/null)"; then
    oxia_admin dataserver create ds-1 --public "127.0.0.1:${data_server_1_port}" \
      --internal data-server-1:6649 -o json >"${run_dir}/logs/oxia-ds-1-create.json"
  else
    printf '%s\n' "${output}" >"${run_dir}/logs/oxia-ds-1-existing.json"
  fi
  if ! output="$(oxia_admin dataserver get ds-2 -o json 2>/dev/null)"; then
    oxia_admin dataserver create ds-2 --public "127.0.0.1:${data_server_2_port}" \
      --internal data-server-2:6649 -o json >"${run_dir}/logs/oxia-ds-2-create.json"
  else
    printf '%s\n' "${output}" >"${run_dir}/logs/oxia-ds-2-existing.json"
  fi
  if ! output="$(oxia_admin dataserver get ds-3 -o json 2>/dev/null)"; then
    oxia_admin dataserver create ds-3 --public "127.0.0.1:${data_server_3_port}" \
      --internal data-server-3:6649 -o json >"${run_dir}/logs/oxia-ds-3-create.json"
  else
    printf '%s\n' "${output}" >"${run_dir}/logs/oxia-ds-3-existing.json"
  fi
  if ! output="$(oxia_admin namespace get default -o json 2>/dev/null)"; then
    oxia_admin namespace create default --initial-shards 1 --replication-factor 3 \
      --notifications --key-sorting hierarchical -o json >"${run_dir}/logs/oxia-namespace-create.json"
  else
    printf '%s\n' "${output}" >"${run_dir}/logs/oxia-namespace-existing.json"
  fi
  wait_for_namespace
}

run_oxia_coordinator_restart() {
  local restart_dir="${run_dir}/chaos/oxia-coordinator-restart"
  local restart_started container_id
  mkdir -p "${restart_dir}"
  "${compose[@]}" ps --all >"${restart_dir}/before-ps.txt"
  container_id="$("${compose[@]}" ps -q coordinator-1)"
  [[ -n "${container_id}" ]] || fail "Oxia coordinator-1 container is missing before restart"
  docker inspect --format '{{.Id}} {{.Image}} {{.State.Status}}' "${container_id}" \
    >"${restart_dir}/before-container.txt"
  restart_started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  "${compose[@]}" restart coordinator-1 >"${restart_dir}/restart.log" 2>&1 \
    || fail "Oxia coordinator-1 restart command failed"
  wait_for_oxia_service coordinator-1 6651
  wait_for_oxia_service coordinator-2 6651
  wait_for_oxia_service coordinator-3 6651
  wait_for_oxia_admin_ready after-coordinator-restart
  "${compose[@]}" ps --all >"${restart_dir}/after-ps.txt"
  container_id="$("${compose[@]}" ps -q coordinator-1)"
  [[ -n "${container_id}" ]] || fail "Oxia coordinator-1 container is missing after restart"
  docker inspect --format '{{.Id}} {{.Image}} {{.State.Status}}' "${container_id}" \
    >"${restart_dir}/after-container.txt"
  "${compose[@]}" logs --since "${restart_started}" --no-color coordinator-1 \
    >"${restart_dir}/after-restart.log" 2>&1 \
    || fail "could not capture Oxia coordinator-1 restart logs"
  if rg -n -e 'panic:' -e 'metadata bad version' "${restart_dir}/after-restart.log" >/dev/null; then
    fail "Oxia coordinator-1 emitted a panic or metadata version failure after restart"
  fi
  oxia_admin namespace get default -o json >"${restart_dir}/namespace-after.json" \
    || fail "Oxia namespace read failed after coordinator restart"
  oxia_client list --key-min "" --key-max "" >"${restart_dir}/keys-after.txt" \
    || fail "Oxia client read failed after coordinator restart"
  jq -n --arg schema "nereus-delay.ndip1-oxia-coordinator-restart" \
    --arg status PASS --arg service coordinator-1 --arg started "${restart_started}" \
    --arg namespace "${restart_dir}/namespace-after.json" --arg keys "${restart_dir}/keys-after.txt" \
    '{schema:$schema,schemaGeneration:1,status:$status,service:$service,restartStartedAt:$started,
      allCoordinatorsHealthy:true,noPanicAfterRestart:true,namespaceReadBack:$namespace,
      clientReadBack:$keys,productionAuthority:false}' >"${restart_dir}/receipt.json"
}

init_minio() {
  wait_for_http "${minio_endpoint}/minio/health/ready"
  local create_status version_status
  create_status="$(curl --silent --show-error --output "${run_dir}/logs/minio-bucket-create.xml" \
    --write-out '%{http_code}' --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" --request PUT \
    --url "${minio_endpoint}/${minio_bucket}")"
  [[ "${create_status}" == 2?? || "${create_status}" == 409 ]] \
    || fail "MinIO bucket create/readback did not return 2xx/409: ${create_status}"
  version_status="$(curl --silent --show-error --output "${run_dir}/logs/minio-versioning.xml" \
    --write-out '%{http_code}' --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" --request PUT \
    --header 'Content-Type: application/xml' \
    --data-binary '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>' \
    --url "${minio_endpoint}/${minio_bucket}?versioning")"
  [[ "${version_status}" == 2?? ]] || fail "MinIO versioning was not enabled: ${version_status}"
}

start_fault_proxy() {
  local pid_file="${staging_root}/fault-proxy.pid"
  if curl --silent --fail "${minio_proxy_endpoint}/__health" >/dev/null 2>&1; then
    [[ -s "${pid_file}" ]] || fail "healthy MinIO fault proxy has no persistent PID file"
    local existing_pid
    existing_pid="$(<"${pid_file}")"
    [[ "${existing_pid}" =~ ^[0-9]+$ ]] || fail "persistent MinIO fault proxy PID is not numeric"
    [[ "$(ps -p "${existing_pid}" -o command= 2>/dev/null || true)" == *minio-fault-proxy.py* ]] \
      || fail "healthy MinIO fault proxy PID is not the expected proxy process"
    echo "reusing healthy persistent MinIO fault proxy" >"${run_dir}/logs/minio-proxy-reused.log"
    return 0
  fi
  if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"${minio_proxy_port}" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "MinIO fault proxy port is occupied by an unknown process: ${minio_proxy_port}"
  fi
  nohup python3 "${script_dir}/minio-fault-proxy.py" --listen-port "${minio_proxy_port}" \
    --backend-port "${minio_port}" >>"${staging_root}/fault-proxy.log" 2>&1 &
  printf '%s\n' "$!" >"${pid_file}"
  wait_for_http "${minio_proxy_endpoint}/__health"
}

set_minio_fault_mode() {
  local mode="$1"
  minio_fault_mode_sequence=$((minio_fault_mode_sequence + 1))
  local log_file="${run_dir}/logs/minio-fault-${minio_fault_mode_sequence}-${mode}.log"
  curl --silent --show-error --fail --request POST --data-binary "${mode}" \
    "${minio_proxy_endpoint}/__fault" >"${log_file}" \
    || fail "could not set persistent MinIO fault proxy mode: ${mode}"
}

write_candidate_scope() {
  local resources_json
  resources_json="$(jq -n \
    --arg command "${command_resource}" --arg system "${system_resource}" \
    --arg rocksdb "${rocksdb_resource}" --arg checkpoint "${checkpoint_resource}" \
    --arg profile "${profile_resource}" --arg policy "${policy_resource}" \
    --arg payload "${payload_resource}" --arg journal "${attempt_journal_resource}" \
    --arg cursor "${cursor_resource}" --arg query "${query_resource}" \
    --arg obligation "${obligation_resource}" --arg incarnation "${incarnation_resource}" \
    --arg workerRegistry "${worker_registry_resource}" \
    '[{kind:"COMMAND_TOPIC",identity:$command},{kind:"SYSTEM_TOPIC",identity:$system},
      {kind:"ROCKSDB_STORE",identity:$rocksdb},{kind:"CHECKPOINT_CATALOG",identity:$checkpoint},
      {kind:"PROFILE_OXIA_STATE",identity:$profile},{kind:"RUNTIME_POLICY_STATE",identity:$policy},
      {kind:"PAYLOAD_RESERVATION_OBJECT_STATE",identity:$payload},
      {kind:"PULSAR_ATTEMPT_JOURNAL",identity:$journal},{kind:"EVIDENCE_TOPIC_CURSOR",identity:$cursor},
      {kind:"QUERY_DEDUPE_STATE",identity:$query},{kind:"OBLIGATION_INDEX",identity:$obligation},
      {kind:"RESOURCE_INCARNATION_REGISTRY",identity:$incarnation},
      {kind:"WORKER_REGISTRY",identity:$workerRegistry}]')"
  jq -n --arg environmentId "${environment_id}" --arg classification "${classification}" \
    --arg deploymentId "${resource_prefix}/${run_id}" --arg workerId "worker-ndip1-a" \
    --arg workerBId "worker-ndip1-b" --argjson resources "${resources_json}" \
    '{scope:{environmentId:$environmentId,environmentClassification:$classification,
      deploymentId:$deploymentId,tenantIds:["tenant-ndip1"],routeIds:["route-ndip1"],
      shardIds:["route-ndip1/0"],eligibleWorkerIds:[$workerId,$workerBId],resources:$resources}}' \
    >"${candidate_scope_path}"
}

resolve_assessed_deployment() {
  local pointer_digest="" pointer_summary="" pointer_scope="" pointer_scope_digest="" latest=""
  if [[ -e "${deployment_pointer}" ]]; then
    [[ -f "${deployment_pointer}" && ! -L "${deployment_pointer}" ]] \
      || fail "deployment pointer is not a regular non-symlink file: ${deployment_pointer}"
    [[ "$(jq -r '.schema' "${deployment_pointer}")" == "nereus-delay.ndip1-deployment-pointer" \
        && "$(jq -r '.status' "${deployment_pointer}")" == CURRENT ]] \
      || fail "deployment pointer is not a canonical CURRENT record"
    pointer_summary="$(jq -r '.finalSummary' "${deployment_pointer}")"
    pointer_digest="$(jq -r '.finalSummarySha256' "${deployment_pointer}")"
    pointer_scope="$(jq -r '.deploymentScope' "${deployment_pointer}")"
    pointer_scope_digest="$(jq -r '.deploymentScopeSha256' "${deployment_pointer}")"
    [[ -f "${pointer_summary}" && ! -L "${pointer_summary}" \
        && "$(sha256_file "${pointer_summary}")" == "${pointer_digest}" ]] \
      || fail "deployment pointer does not bind an immutable final summary"
    [[ -f "${pointer_scope}" && ! -L "${pointer_scope}" \
        && "$(sha256_file "${pointer_scope}")" == "${pointer_scope_digest}" ]] \
      || fail "deployment pointer does not bind an immutable exact scope"
    assessed_final_summary="${pointer_summary}"
  else
    latest="$(find "${artifact_root}" -mindepth 2 -maxdepth 2 -name final-summary.json -type f \
      -not -path "${run_dir}/*" -print | LC_ALL=C sort | tail -1)"
    assessed_final_summary="${latest}"
  fi
  if [[ -n "${assessed_final_summary}" ]]; then
    [[ "$(jq -r '.status' "${assessed_final_summary}")" == COMPLETED ]] \
      || fail "assessed deployment final summary is not COMPLETED"
    assessed_run_dir="$(cd "$(dirname "${assessed_final_summary}")" && pwd)"
    assessed_run_id="$(jq -r '.runId' "${assessed_final_summary}")"
    [[ "${assessed_run_dir}" == "${artifact_root}/${assessed_run_id}" ]] \
      || fail "assessed deployment run identity differs from its immutable path"
    assessed_scope_path="$(jq -r '.deploymentScope // empty' "${assessed_final_summary}")"
    [[ -n "${assessed_scope_path}" ]] \
      || assessed_scope_path="${assessed_run_dir}/authority/scope.json"
    if [[ -n "${pointer_scope}" && "${assessed_scope_path}" != "${pointer_scope}" ]]; then
      fail "deployment pointer and final summary disagree on the exact scope"
    fi
    [[ -f "${assessed_scope_path}" && ! -L "${assessed_scope_path}" ]] \
      || fail "assessed deployment has no exact closed scope"
    assessed_mode=EXISTING
  else
    assessed_mode=CREATE_NEW
    assessed_run_id="${run_id}"
  fi
  jq -n --arg schema "nereus-delay.ndip1-assessed-deployment" --arg mode "${assessed_mode}" \
    --arg runId "${assessed_run_id}" --arg finalSummary "${assessed_final_summary}" \
    --arg scope "${assessed_scope_path}" --arg pointer "${deployment_pointer}" \
    '{schema:$schema,schemaGeneration:1,mode:$mode,runId:$runId,finalSummary:$finalSummary,
      scope:$scope,pointer:$pointer,readOnlyResolution:true} | with_entries(select(.value != ""))' \
    >"${run_dir}/g0/assessed-deployment.json"
}

write_data_disposition_declaration() {
  local expected_decision assessed_final_sha256="" assessed_scope_sha256=""
  case "${assessed_mode}" in
    EXISTING) expected_decision=RESET_INTERNAL_ONLY ;;
    CREATE_NEW) expected_decision=CREATE_NEW_INTERNAL_ONLY ;;
    *) fail "unsupported assessed deployment mode: ${assessed_mode}" ;;
  esac
  [[ "${staging_data_decision}" == "${expected_decision}" ]] \
    || fail "explicit staging data decision must be ${expected_decision} for ${assessed_mode}"
  [[ "${staging_external_user_data}" == false ]] \
    || fail "persistent staging reset requires an explicit false external-user-data declaration"
  [[ "${staging_operator}" =~ ^[A-Za-z0-9._@-]+$ ]] \
    || fail "persistent staging operator must be an explicit safe identifier"
  if [[ "${assessed_mode}" == EXISTING ]]; then
    [[ "$(jq -r '.environmentId' "${assessed_final_summary}")" == "${environment_id}" \
        && "$(jq -r '.productionAuthority' "${assessed_final_summary}")" == false ]] \
      || fail "only this non-production staging deployment can be explicitly reincarnated"
    [[ "$(jq -n --slurpfile prior "${assessed_scope_path}" --slurpfile candidate "${candidate_scope_path}" \
      '[($prior[0].scope.resources[].identity) as $old | $candidate[0].scope.resources[].identity | select(. == $old)] | length')" == 0 ]] \
      || fail "candidate resource identities overlap the existing deployment during RESET"
    assessed_final_sha256="$(sha256_file "${assessed_final_summary}")"
    assessed_scope_sha256="$(sha256_file "${assessed_scope_path}")"
  fi
  jq -n --arg schema "nereus-delay.ndip1-staging-data-disposition" \
    --arg environmentId "${environment_id}" --arg classification "${classification}" \
    --arg operator "${staging_operator}" --arg decision "${staging_data_decision}" \
    --arg assessedMode "${assessed_mode}" --arg assessedRunId "${assessed_run_id}" \
    --arg assessedFinalSummary "${assessed_final_summary}" \
    --arg assessedFinalSummarySha256 "${assessed_final_sha256}" \
    --arg assessedScope "${assessed_scope_path}" --arg assessedScopeSha256 "${assessed_scope_sha256}" \
    --arg candidateScope "${candidate_scope_path}" \
    --arg candidateScopeSha256 "$(sha256_file "${candidate_scope_path}")" \
    --arg candidateCommit "${candidate_commit}" --arg issuedAtEpochMs "$(now_epoch_ms)" \
    '{schema:$schema,schemaGeneration:1,environmentId:$environmentId,
      environmentClassification:$classification,operator:$operator,decision:$decision,
      externalUserDataPresent:false,existingResourcesAreInternalStagingOnly:true,
      assessedDeployment:{mode:$assessedMode,runId:$assessedRunId,finalSummary:$assessedFinalSummary,
        finalSummarySha256:$assessedFinalSummarySha256,scope:$assessedScope,scopeSha256:$assessedScopeSha256},
      candidateScope:$candidateScope,candidateScopeSha256:$candidateScopeSha256,
      candidateCommit:$candidateCommit,replacementDisposition:"REINCARNATE",
      destructiveOperationsAuthorized:false,operatorAuthorization:"EXPLICIT_ENVIRONMENT_INPUT",
      productionAuthority:false,issuedAtEpochMs:($issuedAtEpochMs|tonumber)}' \
    >"${data_disposition_payload}"
  sign_staging_payload data-disposition-declaration \
    "${data_disposition_payload}" "${data_disposition_envelope}"
  data_disposition_envelope_sha256="$(sha256_file "${data_disposition_envelope}")"
}

read_absent_persistent_topic() {
  local topic="$1" topic_url="${admin_url}/admin/v2/persistent/public/default/${topic}/stats"
  local output_path="${run_dir}/g0/g0-topic-${topic}.json"
  local attempts_path="${run_dir}/g0/g0-topic-${topic}.attempts.log"
  local deadline=$((SECONDS + 60)) attempt=0 topic_status
  : >"${attempts_path}"
  while :; do
    attempt=$((attempt + 1))
    topic_status="$(curl --silent --show-error --location --max-redirs 5 --max-time 15 \
      --output "${output_path}" --write-out '%{http_code}' "${topic_url}" \
      2>>"${attempts_path}" || true)"
    printf 'attempt=%s http=%s\n' "${attempt}" "${topic_status}" >>"${attempts_path}"
    [[ "${topic_status}" == 404 ]] && return 0
    (( SECONDS < deadline )) || fail "G0 exact topic read was not absent for ${topic}: HTTP ${topic_status}"
    sleep 2
  done
}

collect_assessed_resource_observations() {
  local output_dir="${run_dir}/g0/existing-resources" rows="${run_dir}/g0/resource-observations.ndjson"
  local resources_source index=0 resource kind identity raw metadata present status prefix topic_uri topic
  local external_retention replacement_disposition
  mkdir -p "${output_dir}"
  : >"${rows}"
  [[ -f "${data_disposition_envelope}" && ! -L "${data_disposition_envelope}" \
      && "$(sha256_file "${data_disposition_envelope}")" == "${data_disposition_envelope_sha256}" \
      && "$(jq -r '.externalUserDataPresent' "${data_disposition_payload}")" == false \
      && "$(jq -r '.replacementDisposition' "${data_disposition_payload}")" == REINCARNATE ]] \
    || fail "G0 has no exact signed data-disposition declaration"
  external_retention=NONE
  replacement_disposition=REINCARNATE
  if [[ "${assessed_mode}" == EXISTING ]]; then
    resources_source="${assessed_scope_path}"
  else
    resources_source="${run_dir}/authority/candidate-scope.json"
  fi
  while IFS= read -r resource; do
    index=$((index + 1))
    kind="$(jq -r '.kind' <<<"${resource}")"
    identity="$(jq -r '.identity' <<<"${resource}")"
    raw="${output_dir}/$(printf '%02d' "${index}")-${kind}.raw"
    metadata="${output_dir}/$(printf '%02d' "${index}")-${kind}.json"
    present=false
    status=COMPLETE
    case "${kind}" in
      COMMAND_TOPIC|SYSTEM_TOPIC|PULSAR_ATTEMPT_JOURNAL|EVIDENCE_TOPIC_CURSOR)
        topic_uri="${identity%%/subscription/*}"
        [[ "${topic_uri}" == persistent://public/default/* ]] \
          || fail "G0 Pulsar identity is outside the closed public/default scope: ${identity}"
        topic="${topic_uri##*/}"
        local http_status
        http_status="$(curl --silent --show-error --location --max-redirs 5 --max-time 15 \
          --output "${raw}" --write-out '%{http_code}' \
          "${admin_url}/admin/v2/persistent/public/default/${topic}/stats" || true)"
        [[ "${http_status}" == 200 || "${http_status}" == 404 ]] \
          || fail "G0 could not inspect existing Pulsar resource ${identity}: HTTP ${http_status}"
        if [[ "${http_status}" == 200 ]]; then
          present=true
          jq -e 'type == "object" and
            ((.publishers // []) | length) == 0 and
            ([.subscriptions[]?.consumers // [] | length] | all(. == 0))' "${raw}" >/dev/null \
            || fail "G0 found an active publisher/consumer or malformed stats for ${identity}"
        fi
        ;;
      ROCKSDB_STORE)
        if [[ -d "${identity}" && ! -L "${identity}" ]]; then
          [[ -z "$(find "${identity}" -type l -print -quit)" ]] \
            || fail "G0 RocksDB inventory contains a symlink: ${identity}"
          printf 'DIRECTORY_PRESENT\n' >"${raw}"
          while IFS= read -r -d '' file; do
            shasum -a 256 "${file}" >>"${raw}"
          done < <(find "${identity}" -type f -print0 | LC_ALL=C sort -z)
          present=true
        elif [[ ! -e "${identity}" ]]; then
          printf 'ABSENT\n' >"${raw}"
        else
          fail "G0 RocksDB resource is not a regular directory or absent: ${identity}"
        fi
        ;;
      PAYLOAD_RESERVATION_OBJECT_STATE)
        prefix="${identity#s3://${minio_bucket}/}"
        curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
          --user "${minio_access_key}:${minio_secret_key}" \
          "${minio_endpoint}/${minio_bucket}?list-type=2&prefix=${prefix}" >"${raw}"
        rg -F '<Key>' "${raw}" >/dev/null && present=true || true
        ;;
      CHECKPOINT_CATALOG|PROFILE_OXIA_STATE|RUNTIME_POLICY_STATE|QUERY_DEDUPE_STATE|OBLIGATION_INDEX|RESOURCE_INCARNATION_REGISTRY|WORKER_REGISTRY)
        prefix="${identity#oxia://default/}"
        : >"${raw}"
        while IFS= read -r key; do
          [[ -n "${key}" ]] || continue
          printf 'KEY %s\n' "${key}" >>"${raw}"
          oxia_client get --hex "${key}" >>"${raw}" \
            || fail "G0 could not read existing Oxia record: ${key}"
        done < <(rg -F "${prefix}" "${run_dir}/g0/oxia-keys-before.txt" || true)
        [[ -s "${raw}" ]] && present=true
        if [[ "${kind}" == OBLIGATION_INDEX ]] \
          && rg -a -e 'PUBLISHING' -e 'UNCERTAIN' "${raw}" >/dev/null; then
          fail "G0 found a non-terminal obligation in the existing Oxia index"
        fi
        ;;
      *) fail "G0 resource collector does not recognize closed kind: ${kind}" ;;
    esac
    jq -n --arg kind "${kind}" --arg identity "${identity}" --arg status "${status}" \
      --argjson present "${present}" --arg rawEvidence "${raw}" \
      --arg rawEvidenceSha256 "$(sha256_file "${raw}")" \
      --arg dispositionEnvelope "${data_disposition_envelope}" \
      --arg dispositionEnvelopeSha256 "${data_disposition_envelope_sha256}" \
      --arg externalRetention "${external_retention}" \
      --arg replacementDisposition "${replacement_disposition}" \
      '{kind:$kind,identity:$identity,accessStatus:$status,present:$present,
        rawEvidence:$rawEvidence,rawEvidenceSha256:$rawEvidenceSha256,
        dispositionEnvelope:$dispositionEnvelope,dispositionEnvelopeSha256:$dispositionEnvelopeSha256,
        externalRetention:$externalRetention,replacementDisposition:$replacementDisposition}' >"${metadata}"
    jq -c --arg evidenceSha256 "$(sha256_file "${metadata}")" \
      '{kind,identity,accessStatus,externalRetention,replacementDisposition,evidenceSha256:$evidenceSha256}' \
      "${metadata}" >>"${rows}"
  done < <(jq -c '.scope.resources[]' "${resources_source}")
  jq -s '.' "${rows}" >"${run_dir}/g0/resource-observations.json"
  [[ "$(jq 'length' "${run_dir}/g0/resource-observations.json")" == 13 ]] \
    || fail "G0 resource collector did not cover the closed 13-resource set"
}

write_environment_snapshot() {
  docker version >"${run_dir}/g0/docker-version.txt"
  "${compose[@]}" config >"${run_dir}/g0/compose-config.yaml"
  "${compose[@]}" ps --all >"${run_dir}/g0/compose-ps.txt"
  curl --silent --show-error "${admin_url}/admin/v2/brokers/ready" >"${run_dir}/g0/pulsar-broker-ready.json"
  curl --silent --show-error --location "${admin_url}/admin/v2/persistent/public/default" \
    >"${run_dir}/g0/pulsar-topics-before.json"
  local topic
  for topic in "${command_topic}" "${system_topic}" "${mutation_topic}" "${worker_topic}" \
    "${worker_destination_topic}" "${worker_destination_topic}-attempt-journal" "${route_worker_topic}" \
    "${native_topic}" "${broker_recovery_topic}" "${evidence_topic}"; do
    read_absent_persistent_topic "${topic}"
  done
  oxia_admin namespace get default -o json >"${run_dir}/g0/oxia-namespace-before.json"
  oxia_admin dataserver list -o json >"${run_dir}/g0/oxia-dataservers-before.json"
  oxia_client list --key-min "" --key-max "" >"${run_dir}/g0/oxia-keys-before.txt"
  ! rg -F "${resource_prefix}/${run_id}/" "${run_dir}/g0/oxia-keys-before.txt" \
    || fail "G0 found an existing Oxia key in the new run scope"
  curl --silent --show-error --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" \
    "${minio_endpoint}/${minio_bucket}?list-type=2&prefix=${resource_prefix}/${run_id}" \
    >"${run_dir}/g0/minio-objects-before.xml"
  ! rg -F "<Key>${resource_prefix}/${run_id}/" "${run_dir}/g0/minio-objects-before.xml" \
    || fail "G0 found an existing MinIO object in the new run scope"
  [[ ! -e "${rocksdb_resource}" ]] || fail "G0 found an existing RocksDB incarnation in the new run scope"
  collect_assessed_resource_observations
  find "${staging_root}" -mindepth 1 -maxdepth 6 -print \
    | LC_ALL=C sort >"${run_dir}/g0/persistent-paths-before.txt"
  docker ps -a --filter "label=com.docker.compose.project=${compose_project}" \
    >"${run_dir}/g0/docker-resources-before.txt"
  ps -axo pid=,command= \
    | awk '/PulsarClientArtifactWorkerSmoke|PulsarClientArtifactNativeSmoke/ && ! /awk/' \
    >"${run_dir}/g0/worker-processes-before.txt"
  find "${staging_root}/worker" "${staging_root}/chaos" -type f -print 2>/dev/null \
    | LC_ALL=C sort >"${run_dir}/g0/local-state-before.txt" || true
  if [[ "${assessed_mode}" == EXISTING ]]; then
    local assessed_run_status="${assessed_run_dir}/run-status.json"
    local assessed_final_state="${assessed_run_dir}/authority/final-state.json"
    local prior_gate prior_shadow prior_canary prior_policy prior_rollback prior_label prior_envelope
    [[ ! -s "${run_dir}/g0/worker-processes-before.txt" ]] \
      || fail "the existing deployment still has a native/Worker process"
    [[ "$(jq -r '.status' "${assessed_final_summary}")" == COMPLETED \
        && "$(jq -r '.status' "${assessed_run_status}")" == COMPLETED \
        && "$(jq -r '.status' "${assessed_final_state}")" == DISABLED \
        && "$(jq -r '.activeLeaseCount' "${assessed_final_state}")" == 0 \
        && "$(jq -r '.activeSendCount' "${assessed_final_state}")" == 0 ]] \
      || fail "the existing deployment has no closed zero-obligation terminal evidence"
    prior_gate="$(jq -r '.gateCReceipt' "${assessed_final_summary}")"
    prior_shadow="$(jq -r '.shadowReceipt' "${assessed_final_summary}")"
    prior_canary="$(jq -r '.canaryReceipt' "${assessed_final_summary}")"
    prior_policy="$(jq -r '.finalPolicy' "${assessed_final_summary}")"
    prior_rollback="$(jq -r '.rollbackReceipt' "${assessed_final_state}")"
    for prior_label in gate-c shadow canary policy rollback; do
      case "${prior_label}" in
        gate-c) prior_envelope="${prior_gate}" ;;
        shadow) prior_envelope="${prior_shadow}" ;;
        canary) prior_envelope="${prior_canary}" ;;
        policy) prior_envelope="${prior_policy}" ;;
        rollback) prior_envelope="${prior_rollback}" ;;
      esac
      [[ "${prior_envelope}" == "${assessed_run_dir}"/* \
          && -f "${prior_envelope}" && ! -L "${prior_envelope}" ]] \
        || fail "existing deployment ${prior_label} evidence is outside its immutable run"
      jq -n --arg signedEnvelopePath "${prior_envelope}" \
        --arg publicKeyPath "${key_dir}/issuer-ed25519-public.der" \
        '{signedEnvelopePath:$signedEnvelopePath,publicKeyPath:$publicKeyPath}' \
        >"${run_dir}/g0/prior-${prior_label}-verify-config.json"
      authority_task "prior-${prior_label}-verify" verify-json \
        "${run_dir}/g0/prior-${prior_label}-verify-config.json" >/dev/null
    done
    [[ "$(sha256_file "${prior_gate}")" == "$(jq -r '.gateCReceiptSha256' "${assessed_final_summary}")" \
        && "$(sha256_file "${prior_shadow}")" == "$(jq -r '.shadowReceiptSha256' "${assessed_final_summary}")" \
        && "$(sha256_file "${prior_canary}")" == "$(jq -r '.canaryReceiptSha256' "${assessed_final_summary}")" \
        && "$(sha256_file "${prior_policy}")" == "$(jq -r '.disabledPolicyEnvelopeSha256' "${assessed_final_state}")" \
        && "$(sha256_file "${prior_rollback}")" == "$(jq -r '.rollbackReceiptSha256' "${assessed_final_state}")" ]] \
      || fail "existing deployment authority digest chain is inconsistent"
    jq -n --arg gateC "${prior_gate}" --arg gateCSha256 "$(sha256_file "${prior_gate}")" \
      --arg shadow "${prior_shadow}" --arg shadowSha256 "$(sha256_file "${prior_shadow}")" \
      --arg canary "${prior_canary}" --arg canarySha256 "$(sha256_file "${prior_canary}")" \
      --arg policy "${prior_policy}" --arg policySha256 "$(sha256_file "${prior_policy}")" \
      --arg rollback "${prior_rollback}" --arg rollbackSha256 "$(sha256_file "${prior_rollback}")" \
      '{status:"PASS",trustedKeyMatch:true,signaturesVerified:true,
        gateC:{path:$gateC,sha256:$gateCSha256},shadow:{path:$shadow,sha256:$shadowSha256},
        canary:{path:$canary,sha256:$canarySha256},policy:{path:$policy,sha256:$policySha256},
        rollback:{path:$rollback,sha256:$rollbackSha256}}' \
      >"${run_dir}/g0/prior-authority-verification.json"
    jq -n --arg finalSummary "${assessed_final_summary}" --arg runStatus "${assessed_run_status}" \
      --arg finalState "${assessed_final_state}" \
      --arg finalSummarySha256 "$(sha256_file "${assessed_final_summary}")" \
      --arg runStatusSha256 "$(sha256_file "${assessed_run_status}")" \
      --arg finalStateSha256 "$(sha256_file "${assessed_final_state}")" \
      --arg authorityVerification "${run_dir}/g0/prior-authority-verification.json" \
      --arg authorityVerificationSha256 "$(sha256_file "${run_dir}/g0/prior-authority-verification.json")" \
      '{unresolvedPublishing:false,unresolvedUncertain:false,finalSummary:$finalSummary,
        finalSummarySha256:$finalSummarySha256,runStatus:$runStatus,runStatusSha256:$runStatusSha256,
        finalState:$finalState,finalStateSha256:$finalStateSha256,
        authorityVerification:$authorityVerification,
        authorityVerificationSha256:$authorityVerificationSha256}' \
      >"${run_dir}/g0/unresolved-obligations.txt"
    unresolved_obligations=false
  else
    unresolved_obligations=false
    jq -n '{unresolvedPublishing:false,unresolvedUncertain:false,reason:"CREATE_NEW exact scope is absent"}' \
      >"${run_dir}/g0/unresolved-obligations.txt"
  fi
  g0_snapshot_sha256="$(find "${run_dir}/g0" -type f -not -name 'g0-snapshot.json' -print0 \
    | sort -z | xargs -0 shasum -a 256 | shasum -a 256 | awk '{print $1}')"
  jq -n \
    --arg schema 'nereus-delay.ndip1-g0-data-reset-snapshot' \
    --arg environmentId "${environment_id}" --arg classification "${classification}" \
    --arg runId "${run_id}" --arg candidateCommit "${candidate_commit}" \
    --arg p1SourceLock "${p1_source_lock}" --arg oxiaSource "${oxia_sha}" \
    --arg oxiaBaseSource "${oxia_base_sha}" --arg oxiaPatchSha256 "${oxia_patch_sha256}" \
    --arg oxiaSourceCheckout "${oxia_checkout}" \
    --arg oxiaSourceManifest "${run_dir}/g0/oxia-staging-persistent-source.json" \
    --arg oxiaSourceManifestSha256 "${oxia_source_manifest_sha256}" \
    --arg pulsarSource "${pulsar_sha}" --arg pulsarRef "${pulsar_ref}" \
    --arg packageDigest "${accepted_package_digest}" \
    --arg snapshotDigest "${g0_snapshot_sha256}" --argjson unresolved "${unresolved_obligations}" \
    --arg assessedMode "${assessed_mode}" --arg assessedRunId "${assessed_run_id}" \
    --arg assessedFinalSummary "${assessed_final_summary}" --arg assessedScope "${assessed_scope_path}" \
    --arg dataDisposition "${data_disposition_envelope}" \
    --arg dataDispositionSha256 "${data_disposition_envelope_sha256}" \
    --arg commandTopic "${command_topic}" --arg mutationTopic "${mutation_topic}" \
    --arg workerTopic "${worker_topic}" --arg nativeTopic "${native_topic}" \
    --arg minioBucket "${minio_bucket}" --arg minioPrefix "${resource_prefix}/${run_id}" \
    --arg oxiaCli "${oxia_cli}" --arg oxiaCliSha256 "${oxia_cli_sha256}" \
    --arg persistentRoot "${staging_root}" \
    '{schema:$schema,schemaGeneration:1,environmentId:$environmentId,classification:$classification,runId:$runId,
      candidateCommit:$candidateCommit,p1SourceLock:$p1SourceLock,acceptedPackageDigest:$packageDigest,
      source:{oxia:$oxiaSource,oxiaBase:$oxiaBaseSource,oxiaPatchSha256:$oxiaPatchSha256,
        oxiaSourceCheckout:$oxiaSourceCheckout,oxiaSourceManifest:$oxiaSourceManifest,
        oxiaSourceManifestSha256:$oxiaSourceManifestSha256,pulsar:$pulsarSource,pulsarRef:$pulsarRef},
      snapshotDigest:$snapshotDigest,
      assessedDeployment:{mode:$assessedMode,runId:$assessedRunId,finalSummary:$assessedFinalSummary,
        scope:$assessedScope,resourceObservations:"g0/resource-observations.json",
        dataDispositionEnvelope:$dataDisposition,dataDispositionEnvelopeSha256:$dataDispositionSha256},
      unresolvedPublishingOrUncertain:$unresolved,
      topics:{command:$commandTopic,mutation:$mutationTopic,worker:$workerTopic,native:$nativeTopic},
      oxia:{namespace:"default",coordinators:[16691,16692,16693],dataServers:[16681,16682,16683],
        sourceCheckout:$oxiaSourceCheckout,sourceManifest:$oxiaSourceManifest,
        sourceManifestSha256:$oxiaSourceManifestSha256,clientEndpoint:"127.0.0.1:16681",
        clientBinary:$oxiaCli,clientBinarySha256:$oxiaCliSha256},
      minio:{bucket:$minioBucket,prefix:$minioPrefix},
      persistentRoot:$persistentRoot}' \
    >"${run_dir}/g0/g0-snapshot.json"
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

sha256_text() {
  printf '%s' "$1" | shasum -a 256 | awk '{print $1}'
}

now_epoch_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

authority_task() {
  local label="$1" command="$2" config="$3"
  local output_file="${run_dir}/authority/${label}.log"
  local output
  output="$(GRADLE_USER_HOME="${gradle_home}" "${delay_root}/gradlew" -q \
    runNdip1PersistentAuthority \
    -Pndip1AuthorityCommand="${command}" \
    -Pndip1AuthorityConfig="${config}" \
    --no-build-cache --no-daemon --console=plain 2>"${output_file}")" \
    || { printf '%s\n' "${output}" >>"${output_file}"; fail "authority command failed: ${command} (${label})"; }
  printf '%s\n' "${output}" | tee -a "${output_file}"
}

write_authority_configs() {
  local scope_path="${run_dir}/authority/assessment-scope.json"
  local assessment_config="${run_dir}/authority/assessment-config.json"
  local manifest_config="${run_dir}/authority/manifest-config.json"
  local resources_json candidate_resources_json resource_observations_json worker_ids_json
  local scope_json scope_digest observation_now observation_latest observation_mono
  local scope_evidence obligation_evidence worker_evidence source_evidence
  local resource_evidence worker_identity session_identity tenant_scope_digest route_snapshot_digest
  if [[ "${assessed_mode}" == EXISTING ]]; then
    jq '{scope:.scope}' "${assessed_scope_path}" >"${scope_path}"
  else
    jq '{scope:.scope}' "${candidate_scope_path}" >"${scope_path}"
  fi
  resources_json="$(jq -c '.scope.resources' "${scope_path}")"
  candidate_resources_json="$(jq -c '.scope.resources' "${candidate_scope_path}")"
  resource_observations_json="$(jq -c '.' "${run_dir}/g0/resource-observations.json")"
  worker_ids_json="$(jq -c '.scope.eligibleWorkerIds' "${scope_path}")"
  scope_json="$(jq -c '.scope' "${scope_path}")"
  [[ -n "${scope_json}" && "${scope_json}" != null ]] || fail "canonical G0 scope could not be loaded"
  scope_digest="$(authority_task scope-digest scope-digest "${scope_path}" | sed -n 's/^scopeDigest=//p' | tail -1)"
  [[ "${scope_digest}" =~ ^[0-9a-f]{64}$ ]] || fail "authority scope digest was not canonical"
  observation_now="$(now_epoch_ms)"
  observation_latest="${observation_now}"
  observation_mono="$(python3 -c 'import time; print(time.monotonic_ns())')"
  scope_evidence="$(
    shasum -a 256 "${scope_path}" "${run_dir}/g0/assessed-deployment.json" \
      "${run_dir}/g0/resource-observations.json" "${data_disposition_envelope}" \
      | shasum -a 256 | awk '{print $1}'
  )"
  obligation_evidence="$(sha256_file "${run_dir}/g0/unresolved-obligations.txt")"
  worker_evidence="$(sha256_file "${run_dir}/g0/worker-processes-before.txt")"
  source_evidence="$(sha256_text "${environment_id}|CERTIFIED_HOST_CLOCK|${observation_now}")"
  resource_evidence="$(sha256_file "${run_dir}/g0/resource-observations.json")"
  jq -n \
    --arg scopeDigest "${scope_digest}" --arg scopeEvidence "${scope_evidence}" \
    --arg obligationEvidence "${obligation_evidence}" --arg workerEvidence "${worker_evidence}" \
    --arg sourceEvidence "${source_evidence}" --arg resourceEvidence "${resource_evidence}" \
    --arg earliest "${observation_now}" --arg latest "${observation_latest}" \
    --arg mono "${observation_mono}" --argjson resources "${resources_json}" \
    --argjson resourceObservations "${resource_observations_json}" --argjson workerIds "${worker_ids_json}" \
    --arg packageDigest "${accepted_package_digest}" --arg sourceCommit "${candidate_commit}" \
    --arg receiptPath "${run_dir}/authority/data-reset-assessment.json" \
    --arg signedEnvelopePath "${run_dir}/authority/data-reset-assessment.signed.json" \
    --arg privateKeyPath "${key_dir}/issuer-ed25519-private.der" \
    --arg publicKeyPath "${key_dir}/issuer-ed25519-public.der" \
    --arg workerId "worker-ndip1-a" --arg workerBId "worker-ndip1-b" --argjson scope "${scope_json}" \
    '{ndipPackageDigest:$packageDigest,sourceBaselineCommit:$sourceCommit,
      receiptPath:$receiptPath,signedEnvelopePath:$signedEnvelopePath,
      privateKeyPath:$privateKeyPath,publicKeyPath:$publicKeyPath,issuerKeyGeneration:1,
      scope:$scope,
      scopeDigest:$scopeDigest,
      inventory:{scopeDigest:$scopeDigest,scopeEnumerationComplete:true,scopeEvidenceSha256:$scopeEvidence,
        observationTime:{earliestEpochMs:$earliest,latestEpochMs:$latest,qualified:true,
          source:"CERTIFIED_HOST_CLOCK",sourceId:"ndip1-certified-host-clock",sourceConfigGeneration:"1",
          sampleSequence:"1",monotonicAnchorNs:$mono,sourceEvidenceSha256:$sourceEvidence},
        resourceObservations:$resourceObservations,
        obligationEnumerationComplete:true,obligationEvidenceSha256:$obligationEvidence,obligations:[],
        workerEnumerationComplete:true,workerEvidenceSha256:$workerEvidence,
        workers:($workerIds | map({workerId:.,upgradeStatus:"UPGRADEABLE",evidenceSha256:$workerEvidence}))}}' \
    >"${assessment_config}"
  authority_task assessment assessment "${assessment_config}" >"${run_dir}/authority/assessment-command.log"
  [[ "$(jq -r '.outcome' "${run_dir}/authority/data-reset-assessment.json")" == PASS_* ]] \
    || fail "G0 DataResetAssessment did not produce a decision-ready RESET/RETAIN candidate"
  local reset_generation=1
  if [[ "${assessed_mode}" == EXISTING ]]; then
    local prior_reset_generation
    prior_reset_generation="$(jq -r '.resetGeneration' "${assessed_run_dir}/authority/manifest-config.json")"
    [[ "${prior_reset_generation}" =~ ^[0-9]+$ ]] \
      || fail "existing deployment manifest has no reset generation"
    reset_generation="$((prior_reset_generation + 1))"
  fi
  route_incarnation="$(sha256_text "${environment_id}|route-incarnation|${run_id}")"
  route_incarnation="${route_incarnation:0:32}"
  worker_identity="$(sha256_text "${environment_id}|worker-identity|worker-ndip1-a")"
  session_identity="$(sha256_text "${environment_id}|worker-session|worker-ndip1-a")"
  local worker_b_identity="$(sha256_text "${environment_id}|worker-identity|worker-ndip1-b")"
  local worker_b_session_identity="$(sha256_text "${environment_id}|worker-session|worker-ndip1-b")"
  local worker_b_evidence="$(sha256_text "${environment_id}|worker-capability|worker-ndip1-b|${run_id}")"
  tenant_scope_digest="$(sha256_text tenant-ndip1)"
  route_snapshot_digest="$(sha256_text route-ndip1)"
  jq -n \
    --arg packageDigest "${accepted_package_digest}" --arg p1Lock "${p1_source_lock_digest}" \
    --arg sourceCommit "${candidate_commit}" --arg environmentId "${environment_id}" \
    --arg deploymentId "${resource_prefix}/${run_id}" --arg resetGeneration "${reset_generation}" \
    --arg workerId "worker-ndip1-a" --arg workerBId "worker-ndip1-b" \
    --arg privateKeyPath "${key_dir}/issuer-ed25519-private.der" \
    --arg publicKeyPath "${key_dir}/issuer-ed25519-public.der" \
    --argjson resources "${candidate_resources_json}" --arg routeIncarnation "${route_incarnation}" \
    --arg workerIdentity "${worker_identity}" --arg sessionIdentity "${session_identity}" \
    --arg workerBIdentity "${worker_b_identity}" --arg workerBSessionIdentity "${worker_b_session_identity}" \
    --arg workerBEvidence "${worker_b_evidence}" \
    --arg now "${observation_now}" --arg from "$((${observation_now} + 5000))" \
    --arg until "$((${observation_now} + 86400000))" --arg evidence "${resource_evidence}" \
    --arg mono "${observation_mono}" --arg sourceEvidence "${source_evidence}" \
    --arg scopeEvidence "${scope_evidence}" --arg obligationEvidence "${obligation_evidence}" \
    --arg tenantScopeDigest "${tenant_scope_digest}" --arg routeSnapshotDigest "${route_snapshot_digest}" \
    --arg schemaHash "$(sha256_text pulsar-worker-current-schema-bundle)" \
    --arg manifestPath "${run_dir}/authority/data-reset-manifest.bin" \
    '{p1SourceLockDigest:$p1Lock,resetGeneration:$resetGeneration,canonicalSchemaBundleHash:$schemaHash,
      workerId:$workerId,workerIdentity:$workerIdentity,sessionIdentity:$sessionIdentity,capabilityEpoch:1,
      workerCapabilityEvidenceDigest:$evidence,privateKeyPath:$privateKeyPath,publicKeyPath:$publicKeyPath,
      workers:[{workerId:$workerId,workerIdentity:$workerIdentity,sessionIdentity:$sessionIdentity,capabilityEpoch:1,
          workerCapabilityEvidenceDigest:$evidence},
        {workerId:$workerBId,workerIdentity:$workerBIdentity,sessionIdentity:$workerBSessionIdentity,capabilityEpoch:1,
          workerCapabilityEvidenceDigest:$workerBEvidence}],
      issuerKeyGeneration:1,createdAtEpochMs:$now,activationValidFromEpochMs:$from,
      activationValidUntilEpochMs:$until,environmentId:$environmentId,deploymentId:$deploymentId,
      source:"CERTIFIED_HOST_CLOCK",sourceId:"ndip1-certified-host-clock",sourceConfigGeneration:1,
      sampleSequence:1,monotonicAnchorNs:$mono,sourceEvidenceSha256:$sourceEvidence,
      sourceBaselineCommit:$sourceCommit,tenantScopeDigest:$tenantScopeDigest,routeSnapshotDigest:$routeSnapshotDigest,
      routeIncarnation:$routeIncarnation,shardPartition:0,freshResourceEvidenceDigest:$scopeEvidence,
      obligationEvidenceDigest:$obligationEvidence,manifestPath:$manifestPath,
      resourceIncarnations:($resources | map({kind:.kind,identity:.identity,
        incarnationDigest:$evidence,evidenceDigest:$scopeEvidence,fresh:true}))}' \
    >"${manifest_config}"
  authority_task manifest manifest "${manifest_config}" >"${run_dir}/authority/manifest-command.log"
  authority_task verify-manifest verify-manifest "${manifest_config}" >"${run_dir}/authority/manifest-readback.log"
  scope_config_path="${scope_path}"
  assessment_config_path="${assessment_config}"
  manifest_config_path="${manifest_config}"
}

copy_junit_results() {
  local destination="$1"
  mkdir -p "${destination}"
  local xml
  for xml in build/test-results/test/TEST-*.xml; do
    [[ -f "${xml}" ]] || continue
    cp -p "${xml}" "${destination}/"
  done
}

capture_baseline_results() {
  copy_junit_results "${run_dir}/baseline-results"
  local skip_count
  skip_count="$(python3 - "${run_dir}/baseline-results" <<'PY'
import sys
from pathlib import Path
import xml.etree.ElementTree as ET
count = 0
for path in Path(sys.argv[1]).glob('TEST-*.xml'):
    for case in ET.parse(path).getroot().iter('testcase'):
        if case.find('skipped') is not None:
            count += 1
print(count)
PY
)"
  [[ "${skip_count}" == 41 ]] || fail "baseline JUnit results did not contain exactly 41 conditional skips: ${skip_count}"
}

run_baseline_tests() {
  local -a clean_environment=()
  local name
  while IFS= read -r name; do
    [[ -n "${name}" ]] && clean_environment+=(-u "${name}")
  done < <(env | rg '^NEREUS_DELAY_[A-Za-z0-9_]+=' | sed 's/=.*//')
  for name in PULSAR_P1_IMAGE PULSAR_CLUSTER_NAME PULSAR_BROKER_1_PORT PULSAR_WEB_1_PORT \
    PULSAR_BROKER_2_PORT PULSAR_WEB_2_PORT; do
    clean_environment+=(-u "${name}")
  done
  local log_file="${run_dir}/logs/baseline-test.log"
  env "${clean_environment[@]}" GRADLE_USER_HOME="${gradle_home}" "${delay_root}/gradlew" cleanTest \
    --no-build-cache --no-daemon --console=plain >"${run_dir}/logs/clean-test-baseline.log" 2>&1 \
    || fail "baseline cleanTest failed"
  set +e
  env "${clean_environment[@]}" GRADLE_USER_HOME="${gradle_home}" "${delay_root}/gradlew" test \
    --no-build-cache --no-daemon --console=plain --rerun-tasks >"${log_file}" 2>&1
  local status=$?
  set -e
  [[ "${status}" == 0 ]] || { tail -240 "${log_file}" >&2 || true; fail "baseline test suite failed"; }
  capture_baseline_results
  jq -n --arg log "${log_file}" --arg results "${run_dir}/baseline-results" \
    '{schema:"nereus-delay.ndip1-baseline-test",status:"PASS",log:$log,results:$results,conditionalSkips:41}' \
    >"${run_dir}/baseline-test.json"
}

export_common_test_environment() {
  export NEREUS_DELAY_ENVIRONMENT_CLASSIFICATION="${classification}"
  export NEREUS_DELAY_PULSAR_CLUSTER_ID="${pulsar_cluster_name}"
  export NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}"
  export NEREUS_DELAY_OXIA_NAMESPACE="default"
  export NEREUS_DELAY_MINIO_ENDPOINT="${minio_proxy_endpoint}"
  export NEREUS_DELAY_MINIO_FAULT_CONTROL="${minio_proxy_endpoint}/__fault"
  export NEREUS_DELAY_MINIO_ACCESS_KEY="${minio_access_key}"
  export NEREUS_DELAY_MINIO_SECRET_KEY="${minio_secret_key}"
  export NEREUS_DELAY_MINIO_BUCKET="${minio_bucket}"
  export NEREUS_DELAY_MINIO_REGION="${minio_region}"
  export NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS="5000"
  export NEREUS_DELAY_GATEWAY_PORT="${gateway_port}"
  export NEREUS_DELAY_GATEWAY_SERVER_CERT="${cert_dir}/server.crt"
  export NEREUS_DELAY_GATEWAY_SERVER_KEY="${cert_dir}/server.key"
  export NEREUS_DELAY_GATEWAY_CA_CERT="${cert_dir}/ca.crt"
  export NEREUS_DELAY_GATEWAY_CLIENT_CERT="${cert_dir}/client.crt"
  export NEREUS_DELAY_GATEWAY_CLIENT_KEY="${cert_dir}/client.key"
  export NEREUS_DELAY_GATEWAY_ROTATED_SERVER_CERT="${cert_dir}/rotated-server.crt"
  export NEREUS_DELAY_GATEWAY_ROTATED_SERVER_KEY="${cert_dir}/rotated-server.key"
  export NEREUS_DELAY_GATEWAY_ROTATED_CA_CERT="${cert_dir}/rotated-ca.crt"
  export NEREUS_DELAY_GATEWAY_ROTATED_CLIENT_CERT="${cert_dir}/rotated-client.crt"
  export NEREUS_DELAY_GATEWAY_ROTATED_CLIENT_KEY="${cert_dir}/rotated-client.key"
  export NEREUS_DELAY_PULSAR_WORKER_ID="worker-ndip1-a"
  export NEREUS_DELAY_PULSAR_WORKER_AUTHORITY_PREFIX="${resource_prefix}/${run_id}/worker-authority"
  export NEREUS_DELAY_PULSAR_WORKER_ASSIGNMENT_PREFIX="${resource_prefix}/${run_id}/worker-assignment"
  export NEREUS_DELAY_PULSAR_WORKER_ROOT="${rocksdb_resource}"
  unset NEREUS_DELAY_PERSISTENT_STAGING_GATE_C_RECEIPT \
    NEREUS_DELAY_PERSISTENT_STAGING_SHADOW_RECEIPT \
    NEREUS_DELAY_PERSISTENT_STAGING_POLICY \
    NEREUS_DELAY_PERSISTENT_STAGING_REQUIRE_AUTHORITY \
    NEREUS_DELAY_PERSISTENT_STAGING_TRUSTED_PUBLIC_KEY \
    NEREUS_DELAY_PERSISTENT_STAGING_ISSUER_KEY_GENERATION \
    NEREUS_DELAY_PERSISTENT_STAGING_POLICY_KEY_PREFIX \
    NEREUS_DELAY_PERSISTENT_STAGING_NATIVE_STATE_DIR \
    NEREUS_DELAY_PERSISTENT_STAGING_NATIVE_EVIDENCE \
    NEREUS_DELAY_PERSISTENT_STAGING_MANAGED_EVIDENCE \
    NEREUS_DELAY_PULSAR_WORKER_MANAGED_HANDOFF
}

run_gradle_tests_current_env() {
  local label="$1"
  shift
  local log_file="${run_dir}/logs/test-${label}.log"
  local result_dir="${run_dir}/results/${label}"
  mkdir -p "${result_dir}"
  GRADLE_USER_HOME="${gradle_home}" "${delay_root}/gradlew" cleanTest \
    --no-build-cache --no-daemon --console=plain >"${run_dir}/logs/clean-test-${label}.log" 2>&1 \
    || return 1
  set +e
  GRADLE_USER_HOME="${gradle_home}" "${delay_root}/gradlew" test \
    --no-build-cache --no-daemon --console=plain --rerun-tasks \
    "$@" >"${log_file}" 2>&1
  local status=$?
  set -e
  copy_junit_results "${result_dir}"
  jq -n --arg label "${label}" --argjson exitCode "${status}" \
    --arg log "${log_file}" --arg results "${result_dir}" \
    '{schema:"nereus-delay.ndip1-test-run",label:$label,exitCode:$exitCode,log:$log,results:$results}' \
    >"${result_dir}/run.json"
  if [[ "${status}" != 0 ]]; then
    tail -200 "${log_file}" >&2 || true
    return "${status}"
  fi
}

run_gradle_tests() {
  local label="$1"
  shift
  export_common_test_environment
  run_gradle_tests_current_env "${label}" "$@" \
    || fail "real staging test group failed: ${label}"
}

run_real_gradle() {
  local label="$1"
  shift
  local log_file="${run_dir}/logs/real-${label}.log"
  export_common_test_environment
  set +e
  GRADLE_USER_HOME="${gradle_home}" "${delay_root}/gradlew" \
    "$@" \
    -PpulsarClientClasspath="${pulsar_client_cp}" \
    -PpulsarRuntimeDir="${runtime_dir}/lib" \
    --no-build-cache --no-daemon --console=plain --rerun-tasks >"${log_file}" 2>&1
  local status=$?
  set -e
  jq -n --arg label "${label}" --argjson exitCode "${status}" --arg log "${log_file}" \
    '{schema:"nereus-delay.ndip1-real-pulsar-run",label:$label,exitCode:$exitCode,log:$log}' \
    >"${run_dir}/results/${label}.json"
  if [[ "${status}" != 0 ]]; then
    tail -240 "${log_file}" >&2 || true
    fail "real P1 staging run failed: ${label} (exit ${status})"
  fi
}

topic_incarnation_base64() {
  local topic="$1"
  printf '%s' "$(sha256_text "${environment_id}|topic-incarnation|${topic}")" \
    | xxd -r -p | base64 | tr '+/' '-_' | tr -d '=\n'
}

topic_incarnation_base64_seed() {
  local seed="$1"
  python3 - "${seed}" <<'PY'
import base64
import sys
seed = int(sys.argv[1]) & 0xff
print(base64.urlsafe_b64encode(bytes([seed]) * 32).decode().rstrip('='))
PY
}

create_persistent_topic() {
  local topic="$1" seed="${2:-}" creation="${3:-1}"
  local topic_url="${admin_url}/admin/v2/persistent/public/default/${topic}"
  local body
  local incarnation
  if [[ -n "${seed}" ]]; then
    incarnation="$(topic_incarnation_base64_seed "${seed}")"
  else
    incarnation="$(topic_incarnation_base64 "${topic}")"
  fi
  body="$(jq -nc --arg incarnation "${incarnation}" --arg creation "${creation}" \
    '{"nereus.resource.guard.version":"1","nereus.resource.incarnation":$incarnation,
      "nereus.resource.created-at":$creation}')"
  local status
  status="$(curl --silent --show-error --location --output "${run_dir}/g0/topic-${topic}-create.json" \
    --write-out '%{http_code}' --header 'Content-Type: application/json' \
    --request PUT --data "${body}" "${topic_url}")"
  [[ "${status}" == 2?? || "${status}" == 409 ]] \
    || fail "persistent topic create failed for ${topic}: ${status}"
  local read_status
  read_status="$(curl --silent --show-error --location --output "${run_dir}/g0/topic-${topic}-read.json" \
    --write-out '%{http_code}' "${topic_url}/stats")"
  [[ "${read_status}" == 2?? ]] || fail "persistent topic readback failed for ${topic}: ${read_status}"
}

execute_manifest_operations() {
  create_persistent_topic "${command_topic}" 17 1001
  create_persistent_topic "${system_topic}" 43 2001
  create_persistent_topic "${mutation_topic}" 53 3001
  create_persistent_topic "${route_worker_topic}" 67 4001
  create_persistent_topic "${evidence_topic}"
  "${compose[@]}" exec --no-TTY pulsar-broker-1 bin/pulsar-admin \
    --admin-url http://pulsar-broker-1:8080 topics create-subscription \
    --subscription ndip1 --messageId earliest "${evidence_resource}" \
    >"${run_dir}/g0/evidence-subscription-create.log" 2>&1 \
    || fail "manifest could not create the exact evidence cursor subscription"
  # The managed Worker and broker-failover smoke create these resources in
  # their exact prepare/resume phases.  Pre-creating them here would make a
  # non-resume smoke conflate an existing topic with a fresh incarnation.
  mkdir -p "${rocksdb_resource}" "${rocksdb_resource}/checkpoints"
  printf '%s\n' "${run_id}" >"${rocksdb_resource}/incarnation"
  printf '%s\n' "${route_incarnation}" >"${rocksdb_resource}/route-incarnation"
  local marker="${run_dir}/g0/payload-reservation-marker.bin"
  printf 'ndip1-persistent-staging/%s/payload-reservation\n' "${run_id}" >"${marker}"
  curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" --request PUT \
    --data-binary "@${marker}" \
    "${minio_endpoint}/${minio_bucket}/${resource_prefix}/${run_id}/payload-reservation/marker" \
    >"${run_dir}/g0/minio-payload-marker-response.xml"
  curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" \
    "${minio_endpoint}/${minio_bucket}?list-type=2&prefix=${resource_prefix}/${run_id}" \
    >"${run_dir}/g0/minio-objects-after-manifest.xml"
  find "${rocksdb_resource}" -maxdepth 4 -print | LC_ALL=C sort \
    >"${run_dir}/g0/rocksdb-readback.txt"
  local oxia_marker_value marker_key marker_kind marker_identity
  oxia_marker_value="$(jq -nc --arg runId "${run_id}" --arg candidateCommit "${candidate_commit}" \
    --arg manifestDigest "$(sed -n 's/^manifestDigest=//p' "${run_dir}/authority/manifest-command.log" | tail -1)" \
    '{schema:"nereus-delay.ndip1-resource-incarnation-marker",schemaGeneration:1,
      runId:$runId,candidateCommit:$candidateCommit,manifestDigest:$manifestDigest}')"
  while IFS=$'\t' read -r marker_kind marker_identity; do
    marker_key="${marker_identity#oxia://default/}/incarnation-marker"
    [[ -z "$(oxia_client list --key-min "${marker_key}" --key-max "${marker_key}0")" ]] \
      || fail "manifest Oxia marker already exists: ${marker_key}"
    oxia_client put "${marker_key}" "${oxia_marker_value}" \
      >"${run_dir}/g0/oxia-marker-${marker_kind}-put.txt" \
      || fail "manifest could not create Oxia marker for ${marker_kind}"
    oxia_client get --hex "${marker_key}" >"${run_dir}/g0/oxia-marker-${marker_kind}-readback.txt" \
      || fail "manifest could not read back Oxia marker for ${marker_kind}"
  done < <(jq -r '.scope.resources[] | select(.identity | startswith("oxia://default/")) |
    [.kind,.identity] | @tsv' "${candidate_scope_path}")
  jq -n --arg environmentId "${environment_id}" --arg runId "${run_id}" \
    --arg command "${command_resource}" --arg system "${system_resource}" \
    --arg worker "${worker_resource}" --arg journal "${attempt_journal_resource}" \
    --arg evidence "${evidence_resource}" --arg payload "${payload_resource}" \
    --arg rocksdb "${rocksdb_resource}" --arg oxiaEndpoint "${oxia_endpoint}" \
    '{schema:"nereus-delay.ndip1-manifest-operation-intent",environmentId:$environmentId,runId:$runId,
      operations:{topicsCreated:true,evidenceCursorCreated:true,oxiaIncarnationMarkersCreated:true,
        minioPayloadMarkerCreated:true,rocksdbIncarnationCreated:true,oxiaEndpoint:$oxiaEndpoint},
      resources:{commandTopic:$command,systemTopic:$system,workerTopic:$worker,
        attemptJournal:$journal,evidenceTopic:$evidence,payloadReservation:$payload,rocksdb:$rocksdb},
      destructiveOperations:[],exactScope:true}' \
    >"${run_dir}/authority/manifest-operation-intent.json"
}

audit_manifest_operations() {
  local audit_dir="${run_dir}/g0/manifest-resource-readback"
  local rows="${audit_dir}/resources.ndjson" index=0 resource kind identity raw topic_uri topic marker_key
  mkdir -p "${audit_dir}"
  : >"${rows}"
  while IFS= read -r resource; do
    index=$((index + 1))
    kind="$(jq -r '.kind' <<<"${resource}")"
    identity="$(jq -r '.identity' <<<"${resource}")"
    raw="${audit_dir}/$(printf '%02d' "${index}")-${kind}.raw"
    case "${kind}" in
      COMMAND_TOPIC|SYSTEM_TOPIC|PULSAR_ATTEMPT_JOURNAL|EVIDENCE_TOPIC_CURSOR)
        topic_uri="${identity%%/subscription/*}"
        topic="${topic_uri##*/}"
        curl --silent --show-error --fail --location --max-redirs 5 --max-time 15 \
          "${admin_url}/admin/v2/persistent/public/default/${topic}/stats" >"${raw}" \
          || fail "manifest readback could not inspect Pulsar resource ${identity}"
        jq -e 'type == "object"' "${raw}" >/dev/null \
          || fail "manifest Pulsar readback is not JSON for ${identity}"
        if [[ "${kind}" == EVIDENCE_TOPIC_CURSOR ]]; then
          jq -e '.subscriptions | type == "object" and has("ndip1")' "${raw}" >/dev/null \
            || fail "manifest evidence cursor subscription was not read back"
        fi
        ;;
      ROCKSDB_STORE)
        [[ -d "${identity}" && ! -L "${identity}" \
            && "$(<"${identity}/incarnation")" == "${run_id}" \
            && "$(<"${identity}/route-incarnation")" == "${route_incarnation}" ]] \
          || fail "manifest RocksDB incarnation did not read back exactly"
        find "${identity}" -maxdepth 2 -type f -print0 | LC_ALL=C sort -z \
          | while IFS= read -r -d '' file; do shasum -a 256 "${file}"; done >"${raw}"
        ;;
      PAYLOAD_RESERVATION_OBJECT_STATE)
        curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
          --user "${minio_access_key}:${minio_secret_key}" \
          "${minio_endpoint}/${minio_bucket}?list-type=2&prefix=${resource_prefix}/${run_id}/payload-reservation/marker" \
          >"${raw}"
        rg -F "<Key>${resource_prefix}/${run_id}/payload-reservation/marker</Key>" "${raw}" >/dev/null \
          || fail "manifest MinIO payload marker did not read back exactly"
        ;;
      CHECKPOINT_CATALOG|PROFILE_OXIA_STATE|RUNTIME_POLICY_STATE|QUERY_DEDUPE_STATE|OBLIGATION_INDEX|RESOURCE_INCARNATION_REGISTRY|WORKER_REGISTRY)
        marker_key="${identity#oxia://default/}/incarnation-marker"
        oxia_client get --hex "${marker_key}" >"${raw}" \
          || fail "manifest Oxia incarnation marker did not read back for ${kind}"
        [[ -s "${raw}" ]] || fail "manifest Oxia incarnation marker is empty for ${kind}"
        ;;
      *) fail "manifest readback does not recognize closed resource kind: ${kind}" ;;
    esac
    jq -nc --arg kind "${kind}" --arg identity "${identity}" --arg evidence "${raw}" \
      --arg evidenceSha256 "$(sha256_file "${raw}")" \
      '{kind:$kind,identity:$identity,status:"PASS",evidence:$evidence,evidenceSha256:$evidenceSha256}' \
      >>"${rows}"
  done < <(jq -c '.scope.resources[]' "${candidate_scope_path}")
  jq -s '.' "${rows}" >"${audit_dir}/resources.json"
  [[ "$(jq 'length' "${audit_dir}/resources.json")" == 13 \
      && "$(jq '[.[].identity] | unique | length' "${audit_dir}/resources.json")" == 13 \
      && "$(jq '[.[].status] | all(. == "PASS")' "${audit_dir}/resources.json")" == true ]] \
    || fail "manifest did not read back the exact 13-resource candidate scope"
  jq -n --arg environmentId "${environment_id}" --arg runId "${run_id}" \
    --arg candidateCommit "${candidate_commit}" \
    --arg scope "${candidate_scope_path}" --arg scopeSha256 "$(sha256_file "${candidate_scope_path}")" \
    --arg intent "${run_dir}/authority/manifest-operation-intent.json" \
    --arg intentSha256 "$(sha256_file "${run_dir}/authority/manifest-operation-intent.json")" \
    --arg resources "${audit_dir}/resources.json" \
    --arg resourcesSha256 "$(sha256_file "${audit_dir}/resources.json")" \
    '{schema:"nereus-delay.ndip1-manifest-operation-readback",schemaGeneration:1,
      environmentId:$environmentId,runId:$runId,candidateCommit:$candidateCommit,
      scope:$scope,scopeSha256:$scopeSha256,intent:$intent,intentSha256:$intentSha256,
      resourceReadback:$resources,resourceReadbackSha256:$resourcesSha256,
      operations:{exactScope:true,resourceReadbackCount:13,allFresh:true,
        topicsCreatedAndReadBack:true,evidenceCursorCreatedAndReadBack:true,
        oxiaIncarnationsCreatedAndReadBack:true,minioPayloadMarkerCreatedAndReadBack:true,
        rocksdbIncarnationCreatedAndReadBack:true},destructiveOperations:[]}' \
    >"${run_dir}/authority/manifest-operation-readback.json"
}

wait_for_file() {
  local path="$1" label="$2" deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    [[ -f "${path}" ]] && return 0
    sleep 1
  done
  fail "${label} did not publish its file: ${path}"
}

wait_for_child() {
  local pid="$1" label="$2"
  if wait "${pid}"; then
    return 0
  fi
  fail "${label} exited before the orchestration gate was satisfied"
}

run_gate_c_unit_and_real_checks() {
  local basic_tests=(
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayAdmissionSmokeTest.admissionPoolsAndExpiryWorkAgainstRealService
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayAuditSinkSmokeTest.auditEventIsDurableAndExactlyDeduplicatedAgainstRealService
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.concurrentDuplicateRequestsAcrossTwoGatewayServersUseOneDurableAttempt
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.authenticatedScheduleIsNetworkBoundAndExactlyIdempotentAgainstRealOxia
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.rotatedGatewayCertificatesRejectOldClientAndReuseDurableOutcome
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.gatewayRecoversAfterCommittedOxiaAttemptResponseLoss
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.gatewayRecoversAfterCommittedOxiaRetryAttemptResponseLoss
    --tests com.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest.controlTargetRegistrationCasAndReopenWorkAgainstRealService
    --tests com.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest.controlOperationCasAndReopenWorkAgainstRealService
    --tests com.nereusstream.delay.ownership.OxiaRealProtocolCapabilitySmokeTest.eligibleReaderSetIsSessionBoundAndActivationEvidenceIsExact
    --tests com.nereusstream.delay.ownership.OxiaRealServiceSmokeTest.ownerLeaseCasAndEphemeralSessionWorkAgainstRealService
    --tests com.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRouteNotificationsRefreshAStartedProviderAgainstRealService
    --tests com.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRoutePublicationHeadCasAndRefreshWorkAgainstRealService
    --tests com.nereusstream.delay.route.OxiaRealRouteWorkerAssignmentSmokeTest.signedRoutePublicationFeedsSessionBoundWorkerAssignmentAuthority
    --tests com.nereusstream.delay.route.OxiaRealRouteWorkerAssignmentSmokeTest.signedRoutePublicationPlacesTwoShardsAcrossTwoWorkersWithSessionBoundCas
    --tests com.nereusstream.delay.runtime.OxiaRealProfileCatalogSmokeTest.profileHeadProtectionLeaseAndRotationReopenAgainstRealService
    --tests com.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.workerCheckpointRuntimePublishesAtomicIntentAndCatalogAgainstRealService
    --tests com.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.workerCheckpointRuntimePublishesToRealMinioAndOxia
    --tests com.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.recoveryPinIsSessionBoundAndExpiresWithTheRealPublicationSession
    --tests com.nereusstream.delay.store.OxiaRealObjectStoreCredentialRenewalSmokeTest.renewsRealOxiaLeaseAndFencesTheLiveAdapterAtHeadRotation
    --tests com.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest.checkpointUploadIntentCasAndReopenWorkAgainstRealService
    --tests com.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest.recoveryCatalogCasAndLocalReuseValidationWorkAgainstRealService
    --tests com.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest.recoveryPinIsSessionBoundAndExpiresWithTheRealOxiaSession
  )
  run_gradle_tests gate-c-real-checks "${basic_tests[@]}"
}

run_checkpoint_precommit_fault_case() {
  local chaos_dir="${run_dir}/chaos/checkpoint-precommit"
  mkdir -p "${chaos_dir}"
  export_common_test_environment
  set_minio_fault_mode PUT_503_BEFORE_COMMIT
  set +e
  run_gradle_tests_current_env gate-c-checkpoint-precommit-fault \
    --tests com.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.workerCheckpointRuntimeRemainsPendingWhenMinioCommitFailsBeforeProviderWrite
  local status=$?
  set -e
  set_minio_fault_mode NONE
  [[ "${status}" == 0 ]] || fail "real checkpoint pre-commit fault case failed"
  jq -n --arg mode "PUT_503_BEFORE_COMMIT" --arg reset "NONE" \
    --arg result "${run_dir}/results/gate-c-checkpoint-precommit-fault" \
    '{schema:"nereus-delay.ndip1-checkpoint-precommit-fault",status:"PASS",faultMode:$mode,
      resetMode:$reset,resultDirectory:$result,realMinio:true,failClosed:true}' \
    >"${chaos_dir}/receipt.json"
}

run_fresh_process_authority_checks() {
  local base_dir="${run_dir}/chaos/fresh-process-authorities"
  mkdir -p "${base_dir}/control" "${base_dir}/recovery"
  export_common_test_environment
  export NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PREFIX="${resource_prefix}/${run_id}/fresh-authority"
  export NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PHASE=WRITE
  run_gradle_tests_current_env gate-c-control-fresh-write \
    --tests com.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest.freshProcessPhaseReopensDurableControlAuthority \
    || fail "control authority fresh-process WRITE phase failed"
  export NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PHASE=READ
  run_gradle_tests_current_env gate-c-control-fresh-read \
    --tests com.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest.freshProcessPhaseReopensDurableControlAuthority \
    || fail "control authority fresh-process READ phase failed"

  export NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PREFIX="${resource_prefix}/${run_id}/fresh-recovery"
  export NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PHASE=WRITE
  run_gradle_tests_current_env gate-c-recovery-fresh-write \
    --tests com.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest.freshProcessPhaseReopensDurableRecoveryAuthorities \
    || fail "recovery authority fresh-process WRITE phase failed"
  export NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PHASE=READ
  run_gradle_tests_current_env gate-c-recovery-fresh-read \
    --tests com.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest.freshProcessPhaseReopensDurableRecoveryAuthorities \
    || fail "recovery authority fresh-process READ phase failed"
  unset NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PREFIX NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PHASE
}

run_gateway_session_churn() {
  local chaos_dir="${run_dir}/chaos/gateway-session-churn"
  mkdir -p "${chaos_dir}/state"
  export_common_test_environment
  export NEREUS_DELAY_GATEWAY_SESSION_CHURN_GATE="${chaos_dir}/restart.gate"
  export NEREUS_DELAY_GATEWAY_SESSION_CHURN_READY="${chaos_dir}/restart.ready"
  export NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_GATE="${chaos_dir}/recovery.gate"
  export NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_READY="${chaos_dir}/recovery.ready"
  export NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_STATE_DUMP_DIR="${chaos_dir}/state"
  set +e
  (run_gradle_tests_current_env gate-c-gateway-session-churn \
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.gatewayDurableRecordsRecoverAfterOxiaSessionChurn) &
  local child=$!
  set -e
  wait_for_file "${NEREUS_DELAY_GATEWAY_SESSION_CHURN_READY}" "Gateway Oxia session-churn readiness"
  "${compose[@]}" stop data-server-1 >"${chaos_dir}/data-server-stop.log" 2>&1
  touch "${NEREUS_DELAY_GATEWAY_SESSION_CHURN_GATE}"
  wait_for_file "${NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_READY}" "Gateway Oxia session-churn recovery readiness"
  "${compose[@]}" start data-server-1 >"${chaos_dir}/data-server-start.log" 2>&1
  wait_for_oxia_service data-server-1 6648
  wait_for_oxia_client_ready gateway-session-churn "${chaos_dir}/oxia-client-ready.json"
  touch "${NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_GATE}"
  set +e
  wait "${child}"
  local status=$?
  set -e
  [[ "${status}" == 0 ]] || fail "Gateway Oxia session-churn test failed"
  unset NEREUS_DELAY_GATEWAY_SESSION_CHURN_GATE NEREUS_DELAY_GATEWAY_SESSION_CHURN_READY \
    NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_GATE NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_READY \
    NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_STATE_DUMP_DIR
}

run_gateway_leader_failover() {
  local chaos_dir="${run_dir}/chaos/gateway-leader-failover"
  mkdir -p "${chaos_dir}"
  local leader
  leader="$(oxia_admin namespace get default -o json | jq -er '.namespace_status.shards["0"].leader.name')"
  local stopped_service survivor_endpoint
  case "${leader}" in
    ds-1) stopped_service=data-server-1; survivor_endpoint="127.0.0.1:${data_server_2_port}" ;;
    ds-2) stopped_service=data-server-2; survivor_endpoint="127.0.0.1:${data_server_1_port}" ;;
    ds-3) stopped_service=data-server-3; survivor_endpoint="127.0.0.1:${data_server_1_port}" ;;
    *) fail "Oxia namespace returned an unknown leader: ${leader}" ;;
  esac
  local original_endpoint="${oxia_endpoint}"
  oxia_endpoint="${survivor_endpoint}"
  export_common_test_environment
  export NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_GATE="${chaos_dir}/release.gate"
  export NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_READY="${chaos_dir}/ready"
  set +e
  (run_gradle_tests_current_env gate-c-gateway-leader-failover \
    --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.gatewayRecoversAcrossRealOxiaDataServerFailover) &
  local child=$!
  set -e
  wait_for_file "${NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_READY}" "Gateway leader-failover readiness"
  "${compose[@]}" stop "${stopped_service}" >"${chaos_dir}/stopped.log" 2>&1
  local deadline=$((SECONDS + 120)) current
  while (( SECONDS < deadline )); do
    current="$(oxia_admin namespace get default -o json 2>/dev/null | jq -r '.namespace_status.shards["0"].leader.name' 2>/dev/null || true)"
    [[ -n "${current}" && "${current}" != "${leader}" ]] && break
    sleep 1
  done
  [[ -n "${current}" && "${current}" != "${leader}" ]] \
    || { "${compose[@]}" start "${stopped_service}" >/dev/null 2>&1 || true; fail "Oxia leader did not fail over"; }
  wait_for_oxia_client_ready gateway-leader-failover "${chaos_dir}/oxia-client-ready.json"
  touch "${NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_GATE}"
  set +e
  wait "${child}"
  local status=$?
  set -e
  "${compose[@]}" start "${stopped_service}" >"${chaos_dir}/restarted.log" 2>&1
  wait_for_oxia_service "${stopped_service}" 6648
  oxia_endpoint="${original_endpoint}"
  [[ "${status}" == 0 ]] || fail "Gateway leader-failover test failed"
  unset NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_GATE NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_READY
}

run_route_restart_case() {
  local label="$1" selector="$2"
  local chaos_dir="${run_dir}/chaos/${label}"
  mkdir -p "${chaos_dir}"
  export_common_test_environment
  export NEREUS_DELAY_OXIA_ROUTE_RESTART_GATE="${chaos_dir}/release.gate"
  export NEREUS_DELAY_OXIA_ROUTE_RESTART_READY="${chaos_dir}/ready"
  set +e
  (run_gradle_tests_current_env "gate-c-${label}" --tests "${selector}") &
  local child=$!
  set -e
  wait_for_file "${NEREUS_DELAY_OXIA_ROUTE_RESTART_READY}" "${label} readiness"
  "${compose[@]}" stop data-server-1 >"${chaos_dir}/stop.log" 2>&1
  sleep 3
  "${compose[@]}" start data-server-1 >"${chaos_dir}/start.log" 2>&1
  wait_for_oxia_service data-server-1 6648
  wait_for_oxia_client_ready "${label}" "${chaos_dir}/oxia-client-ready.json"
  wait_for_oxia_route_session_expiry "${label}" "${chaos_dir}/session-expiry-grace.json"
  touch "${NEREUS_DELAY_OXIA_ROUTE_RESTART_GATE}"
  set +e
  wait "${child}"
  local status=$?
  set -e
  [[ "${status}" == 0 ]] || fail "${label} failed"
  unset NEREUS_DELAY_OXIA_ROUTE_RESTART_GATE NEREUS_DELAY_OXIA_ROUTE_RESTART_READY
}

run_two_phase_store_chaos() {
  local label="$1" phase_env="$2" artifact_env="$3" selector="$4" artifact_dir="$5"
  mkdir -p "${artifact_dir}"
  export_common_test_environment
  export "${artifact_env}=${artifact_dir}"
  export "${phase_env}=before"
  run_gradle_tests_current_env "gate-c-${label}-before" --tests "${selector}" \
    || fail "${label} before phase failed"
  export "${phase_env}=after"
  run_gradle_tests_current_env "gate-c-${label}-after" --tests "${selector}" \
    || fail "${label} after phase failed"
  unset "${phase_env}" "${artifact_env}"
}

run_credential_binding_chaos() {
  export NEREUS_DELAY_CREDENTIAL_CHAOS_PREFIX="${resource_prefix}/${run_id}/credential-binding"
  run_two_phase_store_chaos \
    credential-binding-chaos \
    NEREUS_DELAY_CREDENTIAL_CHAOS_PHASE \
    NEREUS_DELAY_CREDENTIAL_CHAOS_ARTIFACT_DIR \
    com.nereusstream.delay.runtime.CredentialBindingDurableChaosTest.rotatesProtectedCredentialBindingAcrossFreshProcess \
    "${run_dir}/chaos/credential-binding"
  unset NEREUS_DELAY_CREDENTIAL_CHAOS_PREFIX
}

run_long_gc_chaos() {
  local previous_java_tool_options="${JAVA_TOOL_OPTIONS-}" java_tool_options_was_set="${JAVA_TOOL_OPTIONS+x}"
  export JAVA_TOOL_OPTIONS="-Xmx512m -XX:+UseSerialGC"
  run_two_phase_store_chaos \
    long-gc-chaos \
    NEREUS_DELAY_LONG_GC_PHASE \
    NEREUS_DELAY_LONG_GC_ARTIFACT_DIR \
    com.nereusstream.delay.scheduler.LongGcDurableChaosTest.realLongGcPausePreservesDurableDueAdmission \
    "${run_dir}/chaos/long-gc"
  if [[ -n "${java_tool_options_was_set}" ]]; then
    export JAVA_TOOL_OPTIONS="${previous_java_tool_options}"
  else
    unset JAVA_TOOL_OPTIONS
  fi
}

run_target_isolation_chaos() {
  run_two_phase_store_chaos \
    target-isolation-chaos \
    NEREUS_DELAY_TARGET_ISOLATION_PHASE \
    NEREUS_DELAY_TARGET_ISOLATION_ARTIFACT_DIR \
    com.nereusstream.delay.scheduler.TargetIsolationDurableChaosTest.targetIsolationSurvivesFreshProcessRecovery \
    "${run_dir}/chaos/target-isolation"
}

run_local_storage_phase() {
  local cell="$1" phase="$2" artifact_dir="$3" storage_root="${4:-}" headroom_file="${5:-}"
  export_common_test_environment
  export NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR="${artifact_dir}"
  export NEREUS_DELAY_STORAGE_CHAOS_CELL="${cell}"
  export NEREUS_DELAY_STORAGE_CHAOS_PHASE="${phase}"
  export NEREUS_DELAY_STORAGE_CHAOS_HOLD_FILE="${artifact_dir}/hold"
  export NEREUS_DELAY_STORAGE_CHAOS_ROOT="${storage_root}"
  export NEREUS_DELAY_STORAGE_CHAOS_HEADROOM_FILE="${headroom_file}"
  run_gradle_tests_current_env "gate-c-local-storage-${cell}-${phase}" \
    --tests com.nereusstream.delay.store.LocalStorageDurableChaosTest.localStorageFailureSurvivesFreshProcessRecovery \
    || fail "local storage ${cell}/${phase} phase failed"
  unset NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR NEREUS_DELAY_STORAGE_CHAOS_CELL \
    NEREUS_DELAY_STORAGE_CHAOS_PHASE NEREUS_DELAY_STORAGE_CHAOS_HOLD_FILE \
    NEREUS_DELAY_STORAGE_CHAOS_ROOT NEREUS_DELAY_STORAGE_CHAOS_HEADROOM_FILE
}

run_local_storage_chaos() {
  local root="${run_dir}/chaos/local-storage"
  mkdir -p "${root}"
  local cell
  for cell in fsync-error sst-corruption; do
    local cell_dir="${root}/${cell}"
    mkdir -p "${cell_dir}"
    run_local_storage_phase "${cell}" before "${cell_dir}"
    run_local_storage_phase "${cell}" after "${cell_dir}"
  done

  local disaster_dir="${root}/disaster-host-fault"
  mkdir -p "${disaster_dir}"
  : >"${disaster_dir}/hold"
  export_common_test_environment
  export NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR="${disaster_dir}"
  export NEREUS_DELAY_STORAGE_CHAOS_CELL=disaster-host-fault
  export NEREUS_DELAY_STORAGE_CHAOS_PHASE=before
  export NEREUS_DELAY_STORAGE_CHAOS_HOLD_FILE="${disaster_dir}/hold"
  export NEREUS_DELAY_STORAGE_CHAOS_ROOT=""
  export NEREUS_DELAY_STORAGE_CHAOS_HEADROOM_FILE=""
  set +e
  (run_gradle_tests_current_env gate-c-local-storage-disaster-before \
    --tests com.nereusstream.delay.store.LocalStorageDurableChaosTest.localStorageFailureSurvivesFreshProcessRecovery) &
  local child=$!
  set -e
  wait_for_file "${disaster_dir}/ready" "local storage disaster readiness"
  local target_pid
  target_pid="$(jq -er '.process_pid | select(type == "number")' "${disaster_dir}/before.json")"
  [[ "$(ps -p "${target_pid}" -o command= 2>/dev/null || true)" == *java* ]] \
    || { kill -TERM "${target_pid}" >/dev/null 2>&1 || true; fail "disaster target PID is not a Java test process"; }
  kill -KILL "${target_pid}"
  jq -n --argjson pid "${target_pid}" \
    '{schema:"nereus-delay.ndip1-local-storage-kill",cell:"disaster-host-fault",signal:"SIGKILL",signalNumber:9,targetPid:$pid,exactTarget:true}' \
    >"${disaster_dir}/kill-receipt.json"
  unlink "${disaster_dir}/hold"
  set +e
  wait "${child}"
  local before_status=$?
  set -e
  [[ "${before_status}" != 0 ]] || fail "disaster local-storage process unexpectedly survived SIGKILL"
  run_local_storage_phase disaster-host-fault after "${disaster_dir}"
  local disaster_before_results="${run_dir}/results/gate-c-local-storage-disaster-before"
  local disaster_after_run="${run_dir}/results/gate-c-local-storage-disaster-host-fault-after/run.json"
  [[ -s "${disaster_before_results}/run.json" ]] || fail "disaster local-storage before run record is missing"
  [[ -s "${disaster_after_run}" ]] || fail "disaster local-storage recovery run record is missing"
  jq -e '.schema == "nereus-delay.ndip1-test-run" and .exitCode == 0' "${disaster_after_run}" \
    >/dev/null || fail "disaster local-storage recovery did not pass"
  local expected_fault_marker="${disaster_before_results}/expected-fault.json"
  jq -n \
    --arg resultDir "${disaster_before_results}" \
    --arg killReceipt "${disaster_dir}/kill-receipt.json" \
    --arg killReceiptSha256 "$(sha256_file "${disaster_dir}/kill-receipt.json")" \
    --arg recoveryRunJson "${disaster_after_run}" \
    --arg recoveryRunJsonSha256 "$(sha256_file "${disaster_after_run}")" \
    '{schema:"nereus-delay.ndip1-expected-fault",schemaGeneration:1,
      classification:"EXPECTED_TERMINATION",cell:"disaster-host-fault",signal:"SIGKILL",signalNumber:9,
      testClass:"com.nereusstream.delay.store.LocalStorageDurableChaosTest",
      testName:"localStorageFailureSurvivesFreshProcessRecovery()",resultDir:$resultDir,
      killReceipt:$killReceipt,killReceiptSha256:$killReceiptSha256,
      recoveryRunJson:$recoveryRunJson,recoveryRunJsonSha256:$recoveryRunJsonSha256}' \
    >"${expected_fault_marker}"

  command -v hdiutil >/dev/null 2>&1 || fail "hdiutil is required for the real ENOSPC staging cell"
  local enospc_dir="${root}/enospc"
  local enospc_image="${enospc_dir}/enospc-fixture.sparsebundle"
  local enospc_mount="${enospc_dir}/enospc-mount"
  mkdir -p "${enospc_dir}" "${enospc_mount}"
  hdiutil create -size 128m -fs HFS+J -volname NereusDelayNdip1 -type SPARSEBUNDLE \
    -quiet "${enospc_image}"
  hdiutil attach -nobrowse -quiet -mountpoint "${enospc_mount}" "${enospc_image}"
  local headroom="${enospc_mount}/headroom.bin"
  dd if=/dev/zero of="${headroom}" bs=1048576 count=80 conv=sync >/dev/null 2>&1
  local enospc_cell_dir="${enospc_dir}/evidence"
  mkdir -p "${enospc_cell_dir}"
  run_local_storage_phase enospc before "${enospc_cell_dir}" \
    "${enospc_mount}/worker-root" "${headroom}"
  unlink "${headroom}"
  run_local_storage_phase enospc after "${enospc_cell_dir}" \
    "${enospc_mount}/worker-root" "${headroom}"
  hdiutil detach "${enospc_mount}" >/dev/null
  jq -n --arg image "${enospc_image}" --arg mount "${enospc_mount}" \
    '{schema:"nereus-delay.ndip1-enospc-fixture",image:$image,mount:$mount,fixtureRetained:true,mountDetachedAfterRecovery:true}' \
    >"${enospc_dir}/fixture-receipt.json"
  unset NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR NEREUS_DELAY_STORAGE_CHAOS_CELL \
    NEREUS_DELAY_STORAGE_CHAOS_PHASE NEREUS_DELAY_STORAGE_CHAOS_HOLD_FILE \
    NEREUS_DELAY_STORAGE_CHAOS_ROOT NEREUS_DELAY_STORAGE_CHAOS_HEADROOM_FILE
}

run_checkpoint_reaping_chaos() {
  local chaos_dir="${run_dir}/chaos/checkpoint-reaping"
  mkdir -p "${chaos_dir}"
  export_common_test_environment
  export NEREUS_DELAY_CHECKPOINT_REAPING_PHASE=WRITE
  export NEREUS_DELAY_CHECKPOINT_REAPING_STATE_DIR="${chaos_dir}/state"
  export NEREUS_DELAY_CHECKPOINT_REAPING_PREFIX="${resource_prefix}/${run_id}/checkpoint-reaping"
  mkdir -p "${NEREUS_DELAY_CHECKPOINT_REAPING_STATE_DIR}"
  run_gradle_tests_current_env gate-c-checkpoint-reaping-write \
    --tests com.nereusstream.delay.store.OxiaRealCheckpointReapingSmokeTest.realOxiaOwnerAbandonmentReapsExactMinioCheckpointPrefix \
    || fail "checkpoint reaping WRITE phase failed"
  export NEREUS_DELAY_CHECKPOINT_REAPING_PHASE=READ
  run_gradle_tests_current_env gate-c-checkpoint-reaping-read \
    --tests com.nereusstream.delay.store.OxiaRealCheckpointReapingSmokeTest.realOxiaOwnerAbandonmentReapsExactMinioCheckpointPrefix \
    || fail "checkpoint reaping READ phase failed"
  unset NEREUS_DELAY_CHECKPOINT_REAPING_PHASE NEREUS_DELAY_CHECKPOINT_REAPING_STATE_DIR \
    NEREUS_DELAY_CHECKPOINT_REAPING_PREFIX
}

run_minio_checks() {
  local chaos_dir="${run_dir}/chaos/minio"
  mkdir -p "${chaos_dir}/state"
  export_common_test_environment
  export NEREUS_DELAY_MINIO_FAULT_STATE_DUMP_DIR="${chaos_dir}/state"
  run_gradle_tests_current_env gate-c-minio-real-cells \
    --tests com.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.immutableCheckpointUploadsIdempotentlyAndRestoresAgainstMinio \
    --tests com.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioFiveHundredAfterCommitResolvesByExactReadback \
    --tests com.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioFiveHundredBeforeCommitRemainsFailClosed \
    --tests com.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioTimeoutAfterCommitResolvesByExactReadback \
    --tests com.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioCredentialConfigurationDriftFailsClosed \
    || fail "real MinIO cells failed"
  run_gradle_tests_current_env gate-c-minio-fresh-recovery \
    --tests com.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioFaultRecoveryRunsInFreshProcess \
    || fail "real MinIO fresh-process recovery failed"
  unset NEREUS_DELAY_MINIO_FAULT_STATE_DUMP_DIR
}

run_real_pulsar_baseline_smoke() {
  export_common_test_environment
  export NEREUS_DELAY_PULSAR_LISTENER_NAME=external
  run_real_gradle gate-c-p1-compile-and-service \
    compileRealPulsar \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${command_topic}"
  run_real_gradle gate-c-p1-service \
    runRealPulsarServiceSmoke \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${command_topic}-service"
  run_real_gradle gate-c-p1-source \
    runRealPulsarSourceSmoke \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${command_topic}"
  run_real_gradle gate-c-p1-mutation \
    runRealPulsarMutationSmoke \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarMutationTopic="${mutation_topic}"
  run_real_gradle gate-c-p1-mutation-worker \
    runRealPulsarMutationWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarMutationWorkerTopic="${worker_topic}" \
    -PpulsarWithOxia=true
  run_real_gradle gate-c-p1-route-worker \
    runRealPulsarRouteWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarRouteWorkerTopic="${route_worker_topic}" \
    -PpulsarWithOxia=true
  run_real_gradle gate-c-p1-worker-prepare \
    runRealPulsarWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${worker_topic}" \
    -PpulsarWorkerMode=prepare
  run_real_gradle gate-c-p1-worker-managed \
    runRealPulsarWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${worker_topic}" \
    -PpulsarWorkerMode=resume \
    -PpulsarWorkerDestinationTopic="${worker_destination_topic}" \
    -PpulsarWithOxia=true
  unset NEREUS_DELAY_PULSAR_LISTENER_NAME
}

run_p1_worker_response_loss_recovery() {
  local chaos_dir="${run_dir}/chaos/p1-worker-response-loss"
  mkdir -p "${chaos_dir}/destination" "${chaos_dir}/source-ack"
  export_common_test_environment
  export NEREUS_DELAY_PULSAR_LISTENER_NAME=external
  export NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1
  export NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR="${chaos_dir}/destination"
  run_real_gradle gate-c-p1-worker-destination-response-loss \
    runRealPulsarWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${worker_topic}-destination-loss" \
    -PpulsarWorkerDestinationTopic="${worker_destination_topic}-destination-loss" \
    -PpulsarWithOxia=true
  unset NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS \
    NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR

  export NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1
  export NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR="${chaos_dir}/source-ack"
  run_real_gradle gate-c-p1-worker-source-ack-response-loss \
    runRealPulsarWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${worker_topic}-source-ack-loss" \
    -PpulsarWithOxia=true
  unset NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS \
    NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR NEREUS_DELAY_PULSAR_LISTENER_NAME
}

run_p1_broker_failover() {
  local chaos_dir="${run_dir}/chaos/p1-broker-failover"
  mkdir -p "${chaos_dir}"
  local topic="${broker_recovery_topic}" broker_1_admin="${admin_url}" broker_2_admin="http://127.0.0.1:${web_2_port}"
  local old_service="pulsar-broker-1" old_admin="${broker_1_admin}" new_admin="${broker_2_admin}"
  export_common_test_environment
  export NEREUS_DELAY_PULSAR_LISTENER_NAME=external
  run_real_gradle gate-c-p1-broker-prepare \
    runRealPulsarWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" -PpulsarAdminUrl="${old_admin}" \
    -PpulsarTopic="${topic}" -PpulsarWorkerMode=prepare
  export NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_CELL=ndip1-staging-broker-failover
  export NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_PHASE=before
  export NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_DUMP_DIR="${chaos_dir}"
  export NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT="${old_admin}"
  run_real_gradle gate-c-p1-broker-state-before \
    runRealPulsarBrokerRecoveryStateSmoke \
    -PpulsarAdminUrl="${old_admin}" -PpulsarBrokerRecoveryTopic="${topic}"
  "${compose[@]}" stop "${old_service}" >"${chaos_dir}/broker-stop.log" 2>&1
  wait_for_http "${new_admin}/admin/v2/brokers/ready"
  export NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_PHASE=after
  export NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT="${new_admin}"
  run_real_gradle gate-c-p1-broker-state-after \
    runRealPulsarBrokerRecoveryStateSmoke \
    -PpulsarAdminUrl="${new_admin}" -PpulsarBrokerRecoveryTopic="${topic}"
  run_real_gradle gate-c-p1-worker-after-broker-failover \
    runRealPulsarWorkerSmoke \
    -PpulsarServiceUrl="pulsar://127.0.0.1:${broker_2_port}" -PpulsarAdminUrl="${new_admin}" \
    -PpulsarTopic="${topic}" -PpulsarWorkerMode=resume -PpulsarWithOxia=true
  "${compose[@]}" start "${old_service}" >"${chaos_dir}/broker-restart.log" 2>&1
  wait_for_http "${old_admin}/admin/v2/brokers/ready"
  unset NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_CELL NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_PHASE \
    NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_DUMP_DIR NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT \
    NEREUS_DELAY_PULSAR_LISTENER_NAME
}

audit_gate_c_results() {
  local audit_path="${run_dir}/authority/staging-skip-audit.json"
  local result_args=()
  local directory
  for directory in "${run_dir}"/results/*; do
    [[ -d "${directory}" ]] || continue
    result_args+=(--result-dir "${directory}")
  done
  set +e
  python3 "${script_dir}/ndip1-staging-skip-audit.py" \
    --baseline "${run_dir}/baseline-results" --out "${audit_path}" "${result_args[@]}"
  local status=$?
  set -e
  [[ "${status}" == 0 ]] || {
    jq '.counts, (.rows[] | select(.status != "PASS"))' "${audit_path}" >&2 || true
    fail "the 41 conditional Gate C cases did not all execute and pass"
  }
  [[ "$(jq -r '.expectedConditionalSkips' "${audit_path}")" == 41 ]] \
    || fail "Gate C skip audit has the wrong baseline cardinality"
  [[ "$(jq -r '.counts.pass' "${audit_path}")" == 41 ]] \
    || fail "Gate C skip audit is not all PASS"
}

write_gate_c_receipt() {
  local assessment_receipt="${run_dir}/authority/data-reset-assessment.json"
  local assessment_envelope="${run_dir}/authority/data-reset-assessment.signed.json"
  local manifest="${run_dir}/authority/data-reset-manifest.bin"
  local assessment_outcome gate_c_resolution
  assessment_outcome="$(jq -r '.outcome' "${assessment_receipt}")"
  [[ "${assessment_outcome}" == PASS_* ]] || fail "Gate C assessment is not decision-ready"
  case "${assessment_outcome}" in
    PASS_DIRECT_REPLACE) gate_c_resolution=RESET ;;
    PASS_RETAIN) gate_c_resolution=RETAIN ;;
    *) fail "Gate C assessment outcome has no closed deployment resolution: ${assessment_outcome}" ;;
  esac
  [[ "$(jq -r '.unresolvedPublishingOrUncertain' "${run_dir}/g0/g0-snapshot.json")" == false ]] \
    || fail "G0 found unresolved PUBLISHING or UNCERTAIN state"
  [[ "$(jq -r '.destructiveOperations | length' "${run_dir}/authority/manifest-operation-readback.json")" == 0 ]] \
    || fail "manifest readback contains an unexpected destructive operation"
  [[ "$(jq -r '.operations.resourceReadbackCount' "${run_dir}/authority/manifest-operation-readback.json")" == 13 \
      && "$(jq -r '.operations | to_entries | all(.value == true or .value == 13)' "${run_dir}/authority/manifest-operation-readback.json")" == true ]] \
    || fail "manifest readback did not prove each operation"
  [[ "$(jq -r '.decision' "${data_disposition_payload}")" == "${staging_data_decision}" \
      && "$(jq -r '.externalUserDataPresent' "${data_disposition_payload}")" == false \
      && "$(jq -r '.candidateCommit' "${data_disposition_payload}")" == "${candidate_commit}" \
      && "$(sha256_file "${data_disposition_envelope}")" == "${data_disposition_envelope_sha256}" ]] \
    || fail "Gate C data-disposition authority is incomplete"
  [[ -s "${run_dir}/results/gate-c-p1-worker-managed.json" ]] \
    || fail "P1 managed Worker result is missing"
  [[ "$(jq -r '.status' "${run_dir}/g0/oxia-admin-readiness-initial.json")" == PASS ]] \
    || fail "Oxia admin readiness gate is not PASS"
  [[ "$(jq -r '.status' "${run_dir}/g0/oxia-admin-readiness-after-coordinator-restart.json")" == PASS ]] \
    || fail "Oxia admin readiness after coordinator restart is not PASS"
  [[ "$(jq -r '.status' "${run_dir}/chaos/oxia-coordinator-restart/receipt.json")" == PASS ]] \
    || fail "persistent Oxia coordinator restart gate is not PASS"
  local passed_checks
  passed_checks="$(jq -r '.counts.pass' "${run_dir}/authority/staging-skip-audit.json")"
  [[ "${passed_checks}" == 41 ]] || fail "Gate C receipt cannot be issued before the skip audit is PASS"
  rg -F "owner lease acquired" "${run_dir}/logs/real-gate-c-p1-worker-managed.log" \
    >/dev/null || fail "P1 Worker did not publish owner-lease/assignment evidence"

  local assessment_receipt_sha256 assessment_envelope_sha256 scope_digest
  local manifest_sha256 manifest_digest public_key_der
  assessment_receipt_sha256="$(sha256_file "${assessment_receipt}")"
  assessment_envelope_sha256="$(sha256_file "${assessment_envelope}")"
  scope_digest="$(authority_task scope-digest scope-digest "${scope_config_path}" | sed -n 's/^scopeDigest=//p' | tail -1)"
  manifest_sha256="$(sha256_file "${manifest}")"
  manifest_digest="$(sed -n 's/^manifestDigest=//p' "${run_dir}/authority/manifest-command.log" | tail -1)"
  public_key_der="$(base64 <"${key_dir}/issuer-ed25519-public.der" | tr -d '\n')"
  [[ "${scope_digest}" =~ ^[0-9a-f]{64}$ && "${manifest_digest}" =~ ^[0-9a-f]{64}$ ]] \
    || fail "Gate C source digests are not canonical"

  local payload="${run_dir}/authority/gate-c-receipt.json"
  jq -n \
    --arg schema "nereus-delay.gate-c" --arg status PASS --arg environmentId "${environment_id}" \
    --arg classification "${classification}" --arg candidateCommit "${candidate_commit}" \
    --arg packageDigest "${accepted_package_digest}" --arg p1Lock "${p1_source_lock}" \
    --arg resolution "${gate_c_resolution}" --arg assessmentEnvelopePath "${assessment_envelope}" \
    --arg assessmentEnvelopeSha256 "${assessment_envelope_sha256}" \
    --arg assessmentReceiptPath "${assessment_receipt}" --arg assessmentReceiptSha256 "${assessment_receipt_sha256}" \
    --arg assessmentScopeDigest "${scope_digest}" --arg manifestPath "${manifest}" \
    --arg manifestSha256 "${manifest_sha256}" --arg manifestDigest "${manifest_digest}" \
    --arg manifestPublicKeyDerBase64 "${public_key_der}" --arg createdAtEpochMs "$(now_epoch_ms)" \
    --arg dataDispositionPath "${data_disposition_envelope}" \
    --arg dataDispositionSha256 "${data_disposition_envelope_sha256}" \
    --arg manifestReadbackPath "${run_dir}/authority/manifest-operation-readback.json" \
    --arg manifestReadbackSha256 "$(sha256_file "${run_dir}/authority/manifest-operation-readback.json")" \
    --argjson passedChecks "${passed_checks}" \
    --arg g0SnapshotPath "${run_dir}/g0/g0-snapshot.json" \
    --arg g0SnapshotSha256 "$(sha256_file "${run_dir}/g0/g0-snapshot.json")" \
    --arg g0ObservationsPath "${run_dir}/g0/resource-observations.json" \
    --arg g0ObservationsSha256 "$(sha256_file "${run_dir}/g0/resource-observations.json")" \
    --arg skipAuditPath "${run_dir}/authority/staging-skip-audit.json" \
    --arg skipAuditSha256 "$(sha256_file "${run_dir}/authority/staging-skip-audit.json")" \
    '{gateCSchema:$schema,gateCSchemaGeneration:1,gateCStatus:$status,environmentId:$environmentId,
      environmentClassification:$classification,candidateCommit:$candidateCommit,ndipPackageDigest:$packageDigest,
      p1SourceLock:$p1Lock,resolution:$resolution,assessmentEnvelopePath:$assessmentEnvelopePath,
      assessmentEnvelopeSha256:$assessmentEnvelopeSha256,assessmentReceiptPath:$assessmentReceiptPath,
      assessmentReceiptSha256:$assessmentReceiptSha256,assessmentScopeDigest:$assessmentScopeDigest,
      manifestPath:$manifestPath,manifestSha256:$manifestSha256,manifestDigest:$manifestDigest,
      manifestPublicKeyDerBase64:$manifestPublicKeyDerBase64,createdAtEpochMs:$createdAtEpochMs,
      dataDispositionPath:$dataDispositionPath,dataDispositionSha256:$dataDispositionSha256,
      manifestReadbackPath:$manifestReadbackPath,manifestReadbackSha256:$manifestReadbackSha256,
      startupAssignmentGate:true,noOldGeneration:true,noUnresolvedPublishing:true,noUnresolvedUncertain:true,
      freshness:true,applicableChecks:41,passedChecks:$passedChecks,
      g0SnapshotPath:$g0SnapshotPath,g0SnapshotSha256:$g0SnapshotSha256,
      g0ObservationsPath:$g0ObservationsPath,g0ObservationsSha256:$g0ObservationsSha256,
      skipAuditPath:$skipAuditPath,skipAuditSha256:$skipAuditSha256,productionAuthority:false,
      evidence:{realOxia:true,realMinio:true,realPulsarP1:true,realGateway:true,realWorker:true,
        oxiaAdminReady:true,oxiaCoordinatorRestart:true,brokerFailover:true,workerOwnershipTransfer:true,responseLossRecovery:true}}' \
    >"${payload}"
  jq -n --arg payloadPath "${payload}" \
    --arg signedEnvelopePath "${run_dir}/authority/gate-c-receipt.signed.json" \
    --arg privateKeyPath "${key_dir}/issuer-ed25519-private.der" \
    --arg publicKeyPath "${key_dir}/issuer-ed25519-public.der" \
    '{payloadPath:$payloadPath,signedEnvelopePath:$signedEnvelopePath,privateKeyPath:$privateKeyPath,
      publicKeyPath:$publicKeyPath,issuerKeyGeneration:1}' \
    >"${run_dir}/authority/gate-c-sign-config.json"
  authority_task gate-c sign-json "${run_dir}/authority/gate-c-sign-config.json" >/dev/null
  authority_task gate-c-verify verify-json "${run_dir}/authority/gate-c-sign-config.json" >/dev/null
  gate_c_receipt="${run_dir}/authority/gate-c-receipt.signed.json"
  gate_c_receipt_sha256="$(sha256_file "${gate_c_receipt}")"
  jq -n --arg environmentId "${environment_id}" --arg candidateCommit "${candidate_commit}" \
    --arg receipt "${gate_c_receipt}" --arg digest "${gate_c_receipt_sha256}" \
    --arg scopeDigest "${scope_digest}" --arg manifestDigest "${manifest_digest}" \
    '{schema:"nereus-delay.ndip1-gate-c-record",status:"PASS",environmentId:$environmentId,
      candidateCommit:$candidateCommit,signedReceipt:$receipt,signedReceiptSha256:$digest,
      assessmentScopeDigest:$scopeDigest,manifestDigest:$manifestDigest,productionAuthority:false}' \
    >"${run_dir}/authority/gate-c-record.json"
}

sign_staging_payload() {
  local label="$1" payload_path="$2" envelope_path="$3"
  local config_path
  config_path="${run_dir}/authority/${label}-sign-config.json"
  jq -n --arg payloadPath "${payload_path}" --arg signedEnvelopePath "${envelope_path}" \
    --arg privateKeyPath "${key_dir}/issuer-ed25519-private.der" \
    --arg publicKeyPath "${key_dir}/issuer-ed25519-public.der" \
    '{payloadPath:$payloadPath,signedEnvelopePath:$signedEnvelopePath,privateKeyPath:$privateKeyPath,
      publicKeyPath:$publicKeyPath,issuerKeyGeneration:1}' >"${config_path}"
  authority_task "${label}-sign" sign-json "${config_path}" >/dev/null
  authority_task "${label}-verify" verify-json "${config_path}" >/dev/null
}

publish_policy_head() {
  local label="$1" mode="$2" lead_ms="$3" allowed_path_bits="$4" lease_ms="$5"
  local effective_disabled_after="$6" gate_digest="$7" shadow_digest="$8" canary_digest="$9"
  local policy_dir="${run_dir}/authority/policy" now valid_from valid_until monotonic source_evidence
  local payload envelope config output observed_version observed_generation observed_scope
  mkdir -p "${policy_dir}"
  policy_generation="$((policy_generation + 1))"
  now="$(now_epoch_ms)"
  valid_from="$((now + 1500))"
  valid_until="$((valid_from + lease_ms))"
  monotonic="$(python3 -c 'import time; print(time.monotonic_ns())')"
  jq -n --arg schema "nereus-delay.policy-trusted-time" --arg label "${label}" \
    --arg observedAtEpochMs "${now}" --arg monotonicAnchorNs "${monotonic}" \
    '{schema:$schema,schemaGeneration:1,label:$label,observedAtEpochMs:($observedAtEpochMs|tonumber),
      monotonicAnchorNs:($monotonicAnchorNs|tonumber),source:"CERTIFIED_HOST_CLOCK"}' \
    >"${policy_dir}/${label}-trusted-time.json"
  source_evidence="$(sha256_file "${policy_dir}/${label}-trusted-time.json")"
  payload="${policy_dir}/${label}.json"
  envelope="${policy_dir}/${label}.signed.json"
  config="${policy_dir}/${label}-config.json"
  jq -n \
    --arg privateKeyPath "${key_dir}/issuer-ed25519-private.der" \
    --arg publicKeyPath "${key_dir}/issuer-ed25519-public.der" \
    --arg manifestPath "${run_dir}/authority/data-reset-manifest.bin" \
    --arg candidateCommit "${candidate_commit}" --arg environmentId "${environment_id}" \
    --arg authenticatedClusterId "${pulsar_cluster_name}" \
    --arg targetResourceIncarnation "${native_target_incarnation_hex}" \
    --arg targetPhysicalTopic "persistent://public/default/${native_topic}" \
    --arg targetCreationTimestamp "1001" --arg expectedPolicyScopeDigest "${policy_scope_digest}" \
    --arg oxiaServiceAddress "${oxia_endpoint}" --arg oxiaNamespace default \
    --arg oxiaClientIdentifier "ndip1-policy-${run_id}-${label}" \
    --arg oxiaKeyPrefix "${oxia_policy_key_prefix}" --arg policyMode "${mode}" \
    --arg policyGeneration "${policy_generation}" --arg expectedOxiaVersion "${policy_oxia_version}" \
    --arg effectiveLeadMs "${lead_ms}" --arg allowedPathBits "${allowed_path_bits}" \
    --arg validFromEpochMs "${valid_from}" --arg validUntilEpochMs "${valid_until}" \
    --arg effectiveDisabledAfterEpochMs "${effective_disabled_after}" --arg issuedAtEpochMs "${now}" \
    --arg sampleSequence "${policy_generation}" --arg monotonicAnchorNs "${monotonic}" \
    --arg sourceEvidenceSha256 "${source_evidence}" --arg payloadPath "${payload}" \
    --arg signedEnvelopePath "${envelope}" --arg gateCEnvelopeSha256 "${gate_digest}" \
    --arg shadowEnvelopeSha256 "${shadow_digest}" --arg canaryEnvelopeSha256 "${canary_digest}" \
    '{privateKeyPath:$privateKeyPath,publicKeyPath:$publicKeyPath,issuerKeyGeneration:1,
      manifestPath:$manifestPath,candidateCommit:$candidateCommit,environmentId:$environmentId,
      authenticatedClusterId:$authenticatedClusterId,targetResourceIncarnation:$targetResourceIncarnation,
      targetPhysicalTopic:$targetPhysicalTopic,targetCreationTimestamp:$targetCreationTimestamp,
      expectedPolicyScopeDigest:$expectedPolicyScopeDigest,
      oxiaServiceAddress:$oxiaServiceAddress,oxiaNamespace:$oxiaNamespace,
      oxiaClientIdentifier:$oxiaClientIdentifier,oxiaRequestTimeoutMs:5000,oxiaKeyPrefix:$oxiaKeyPrefix,
      policyMode:$policyMode,policyGeneration:$policyGeneration,expectedOxiaVersion:$expectedOxiaVersion,
      effectiveLeadMs:$effectiveLeadMs,allowedPathBits:$allowedPathBits,
      validFromEpochMs:$validFromEpochMs,validUntilEpochMs:$validUntilEpochMs,
      effectiveDisabledAfterEpochMs:$effectiveDisabledAfterEpochMs,maximumLeaseMs:300000,
      issuedAtEpochMs:$issuedAtEpochMs,source:"CERTIFIED_HOST_CLOCK",sourceId:"ndip1-policy-clock",
      sourceConfigGeneration:1,sampleSequence:$sampleSequence,monotonicAnchorNs:$monotonicAnchorNs,
      sourceEvidenceSha256:$sourceEvidenceSha256,payloadPath:$payloadPath,signedEnvelopePath:$signedEnvelopePath,
      gateCEnvelopeSha256:$gateCEnvelopeSha256,shadowEnvelopeSha256:$shadowEnvelopeSha256,
      canaryEnvelopeSha256:$canaryEnvelopeSha256}
      | with_entries(select(.value != ""))' \
    >"${config}"
  output="$(authority_task "policy-${label}" publish-policy "${config}")"
  observed_version="$(printf '%s\n' "${output}" | sed -n 's/^policyOxiaVersion=//p' | tail -1)"
  observed_generation="$(printf '%s\n' "${output}" | sed -n 's/^policyGeneration=//p' | tail -1)"
  observed_scope="$(printf '%s\n' "${output}" | sed -n 's/^policyScopeDigest=//p' | tail -1)"
  [[ "${observed_version}" =~ ^[0-9]+$ && "${observed_generation}" == "${policy_generation}" \
      && "${observed_scope}" =~ ^[0-9a-f]{64}$ ]] \
    || fail "canonical policy publication output is incomplete: ${label}"
  if [[ -n "${policy_scope_digest}" && "${observed_scope}" != "${policy_scope_digest}" ]]; then
    fail "policy scope changed across one deployment: ${label}"
  fi
  policy_scope_digest="${observed_scope}"
  policy_oxia_version="${observed_version}"
  last_policy_envelope="${envelope}"
  last_policy_envelope_sha256="$(sha256_file "${envelope}")"
  last_policy_valid_until_epoch_ms="${valid_until}"
  authority_task "policy-${label}-readback" read-policy "${config}" \
    >"${policy_dir}/${label}-readback.log"
}

run_authorized_real_gradle() {
  local label="$1"
  shift
  local log_file="${run_dir}/logs/real-${label}.log"
  mkdir -p "${run_dir}/results"
  set +e
  (GRADLE_USER_HOME="${gradle_home}" "${delay_root}/gradlew" "$@" \
    -PpulsarClientClasspath="${pulsar_client_cp}" \
    -PpulsarRuntimeDir="${runtime_dir}/lib" \
    --no-build-cache --no-daemon --console=plain --rerun-tasks) >"${log_file}" 2>&1 &
  local child=$!
  wait "${child}"
  local status=$?
  set -e
  jq -n --arg label "${label}" --argjson exitCode "${status}" --arg log "${log_file}" \
    '{schema:"nereus-delay.ndip1-authorized-real-pulsar-run",label:$label,exitCode:$exitCode,log:$log}' \
    >"${run_dir}/results/${label}.json"
  if [[ "${status}" != 0 ]]; then
    tail -240 "${log_file}" >&2 || true
    fail "authorized real P1 staging run failed: ${label} (exit ${status})"
  fi
}

run_shadow_worker_command() {
  local log_file="$1"
  shift
  env \
    "GRADLE_USER_HOME=${gradle_home}" \
    "NEREUS_DELAY_OXIA_ENDPOINT=${oxia_endpoint}" \
    "NEREUS_DELAY_OXIA_NAMESPACE=default" \
    "NEREUS_DELAY_ENVIRONMENT_CLASSIFICATION=${classification}" \
    "NEREUS_DELAY_PULSAR_WORKER_ROOT=${shadow_worker_root}" \
    "NEREUS_DELAY_PULSAR_WORKER_ASSIGNMENT_PREFIX=${shadow_assignment_prefix}" \
    "NEREUS_DELAY_PULSAR_WORKER_AUTHORITY_PREFIX=${shadow_authority_prefix}" \
    "NEREUS_DELAY_PULSAR_WORKER_ID=${shadow_worker_id}" \
    "NEREUS_DELAY_PULSAR_CLUSTER_ID=${pulsar_cluster_name}" \
    "NEREUS_DELAY_PULSAR_LISTENER_NAME=external" \
    "$@" \
    "${delay_root}/gradlew" runRealPulsarWorkerSmoke \
    -PpulsarClientClasspath="${pulsar_client_cp}" \
    -PpulsarRuntimeDir="${runtime_dir}/lib" \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${shadow_worker_topic}" \
    -PpulsarWorkerMode="${shadow_worker_mode}" \
    -PpulsarWorkerDestinationTopic="${shadow_worker_destination_topic}" \
    -PpulsarWithOxia=true \
    --no-build-cache --no-daemon --console=plain --rerun-tasks >"${log_file}" 2>&1
}

run_shadow_worker_ownership_transfer() {
  local chaos_dir="${run_dir}/shadow/chaos/shadow-worker-ownership"
  mkdir -p "${chaos_dir}/state" "${chaos_dir}/worker-root"
  shadow_worker_topic="${worker_topic}-shadow-transfer"
  shadow_worker_destination_topic="${worker_destination_topic}-shadow-transfer"
  shadow_worker_root="${chaos_dir}/worker-root"
  shadow_assignment_prefix="${resource_prefix}/${run_id}/shadow-worker-assignment"
  shadow_authority_prefix="${resource_prefix}/${run_id}/shadow-worker-authority"
  shadow_worker_id="worker-ndip1-a"
  shadow_worker_mode=prepare
  export_common_test_environment
  run_shadow_worker_command "${chaos_dir}/prepare.log"

  local gate_path="${chaos_dir}/cut.gate" pid_path="${chaos_dir}/cut.pid"
  local state_dir="${chaos_dir}/state"
  shadow_worker_mode=crash-wait
  set +e
  (run_shadow_worker_command "${chaos_dir}/crash.log" \
    "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY=1" \
    "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_GATE=${gate_path}" \
    "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_PID_FILE=${pid_path}" \
    "NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR=${state_dir}") &
  local child=$!
  set -e
  wait_for_file "${gate_path}" "SHADOW Worker ownership-transfer crash gate"
  wait_for_file "${pid_path}" "SHADOW Worker ownership-transfer PID"
  local target_pid
  target_pid="$(<"${pid_path}")"
  [[ "${target_pid}" =~ ^[0-9]+$ ]] || fail "SHADOW Worker crash PID is not numeric"
  [[ "$(ps -p "${target_pid}" -o command= 2>/dev/null || true)" == *java* ]] \
    || fail "SHADOW Worker crash PID is not the Java Worker process"
  kill -KILL "${target_pid}"
  unlink "${gate_path}"
  set +e
  wait "${child}"
  local crash_status=$?
  set -e
  [[ "${crash_status}" != 0 ]] || fail "SHADOW Worker crash-wait process survived SIGKILL"
  [[ -s "${state_dir}/before-process-crash.json" ]] || fail "SHADOW Worker pre-crash state is missing"

  sleep 17
  shadow_worker_id="worker-ndip1-b"
  shadow_worker_mode=resume
  run_shadow_worker_command "${chaos_dir}/resume.log" \
    "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY=1" \
    "NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR=${state_dir}"
  [[ -s "${state_dir}/after-fresh-process.json" ]] || fail "SHADOW Worker post-recovery state is missing"
  python3 - "${state_dir}/before-process-crash.json" "${state_dir}/after-fresh-process.json" \
    "${chaos_dir}/crash.log" "${chaos_dir}/resume.log" <<'PY'
import json
import re
import sys
from pathlib import Path

before_path, after_path, crash_log_path, resume_log_path = sys.argv[1:]
before = json.loads(Path(before_path).read_text(encoding="utf-8"))
after = json.loads(Path(after_path).read_text(encoding="utf-8"))
crash_log = Path(crash_log_path).read_text(encoding="utf-8")
resume_log = Path(resume_log_path).read_text(encoding="utf-8")

def require(condition, message):
    if not condition:
        raise SystemExit(message)

require(before.get("process_pid") != after.get("process_pid"), "SHADOW Worker did not use a fresh process")
for field in ("store_root", "store_incarnation", "db_identity", "shard"):
    require(before.get(field) == after.get(field), f"SHADOW Worker identity changed: {field}")
require(before.get("dump_forced") is True and after.get("dump_forced") is True,
        "SHADOW Worker durable state dump was not forced")
require(re.search(r"assignment publication/acceptance passed: revision=1, worker=worker-ndip1-a",
                  crash_log) is not None,
        "SHADOW Worker did not publish the first assignment")
require(re.search(r"assignment publication/acceptance passed: revision=2, worker=worker-ndip1-b",
                  resume_log) is not None,
        "SHADOW Worker did not transfer to the replacement assignment")
require("owner lease acquired" in crash_log and "owner lease acquired" in resume_log,
        "SHADOW Worker owner lease evidence is incomplete")
PY
  jq -n --arg topic "${shadow_worker_topic}" --arg destination "${shadow_worker_destination_topic}" \
    --arg before "${state_dir}/before-process-crash.json" --arg after "${state_dir}/after-fresh-process.json" \
    --arg crashLog "${chaos_dir}/crash.log" --arg resumeLog "${chaos_dir}/resume.log" \
    '{schema:"nereus-delay.ndip1-shadow-worker-ownership",status:"PASS",topic:$topic,
      destinationTopic:$destination,beforeState:$before,afterState:$after,crashLog:$crashLog,
      resumeLog:$resumeLog,freshProcess:true,ownershipTransfer:true,ordinaryManagedPath:true}' \
    >"${chaos_dir}/ownership-transfer.json"
}

write_shadow_policy_records() {
  publish_policy_head shadow-initial SHADOW 0 0 120000 0 "${gate_c_receipt_sha256}" "" ""
  publish_policy_head shadow-candidate-add SHADOW 0 0 120000 0 "${gate_c_receipt_sha256}" "" ""
  publish_policy_head shadow-candidate-cancel SHADOW 0 0 120000 0 "${gate_c_receipt_sha256}" "" ""
  shadow_policy_envelope="${last_policy_envelope}"
  shadow_policy_envelope_sha256="${last_policy_envelope_sha256}"
}

run_shadow_observation() {
  local shadow_dir="${run_dir}/shadow"
  mkdir -p "${shadow_dir}/evidence" "${shadow_dir}/logs" "${shadow_dir}/state"
  export_common_test_environment
  local observation_start observation_end observation_seconds
  observation_start="$(now_epoch_ms)"
  write_shadow_policy_records

  run_gradle_tests shadow-policy-controls \
    --tests com.nereusstream.delay.scheduler.HandoffEligibilityResolverTest.shadowAndFifoNeverBecomeNativeCandidates \
    --tests com.nereusstream.delay.scheduler.HandoffEligibilityResolverTest.crossingCandidateBoundaryRequiresAFreshSampleAndDoesNotSchedulePastWork \
    --tests com.nereusstream.delay.scheduler.HandoffEligibilityResolverTest.crossingPolicyLeaseBoundaryRequiresAFreshSample \
    --tests com.nereusstream.delay.protocol.HandoffPolicySnapshotTest.signedSnapshotHeadAndOxiaRevisionRoundTrip \
    --tests com.nereusstream.delay.protocol.HandoffPolicySnapshotTest.casRejectsAStaleOxiaRevisionWithoutReplacingTheCurrentHead \
    --tests com.nereusstream.delay.runtime.PolicyCatalogTest.retryPolicyCatalogRequiresExactSourceVisibleHistory
  run_gradle_tests shadow-state-rebuild \
    --tests com.nereusstream.delay.store.PersistentSloObservationCollectorTest.openAndConservativeFinalSurviveReopen \
    --tests com.nereusstream.delay.store.PersistentSloObservationCollectorTest.separateInstancesRereadTheLatestMerge \
    --tests com.nereusstream.delay.store.WorkerPlacementPolicyTest.hardFilterRejectsOverCapacityInsteadOfChoosingLeastOverfullWorker

  run_shadow_worker_ownership_transfer

  local broker_restart_dir="${shadow_dir}/broker-restart"
  mkdir -p "${broker_restart_dir}"
  "${compose[@]}" stop pulsar-broker-2 >"${broker_restart_dir}/stop.log" 2>&1
  sleep 3
  "${compose[@]}" start pulsar-broker-2 >"${broker_restart_dir}/start.log" 2>&1
  wait_for_http "http://127.0.0.1:${web_2_port}/admin/v2/brokers/ready"
  curl --silent --show-error "http://127.0.0.1:${web_2_port}/admin/v2/brokers/ready" \
    >"${broker_restart_dir}/ready.json"
  export NEREUS_DELAY_PULSAR_LISTENER_NAME=external
  run_real_gradle shadow-broker-post-restart \
    runRealPulsarServiceSmoke \
    -PpulsarServiceUrl="${service_url}" -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${system_topic}-shadow-broker"
  unset NEREUS_DELAY_PULSAR_LISTENER_NAME

  run_route_restart_case shadow-oxia-restart \
    com.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRouteProviderRecoversAfterRealOxiaRestart

  local minio_shadow_dir="${shadow_dir}/minio-outage"
  mkdir -p "${minio_shadow_dir}"
  export_common_test_environment
  export NEREUS_DELAY_MINIO_FAULT_STATE_DUMP_DIR="${minio_shadow_dir}/state"
  run_gradle_tests_current_env shadow-minio-outage \
    --tests com.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioTimeoutAfterCommitResolvesByExactReadback \
    || fail "SHADOW MinIO short-outage observation failed"
  unset NEREUS_DELAY_MINIO_FAULT_STATE_DUMP_DIR

  observation_end="$(now_epoch_ms)"
  observation_seconds="$(( (observation_end - observation_start) / 1000 ))"
  (( observation_seconds >= 10 )) || fail "SHADOW observation window was shorter than 10 seconds"
  python3 "${script_dir}/validate-ndip1-shadow-observation.py" \
    --shadow-dir "${shadow_dir}" \
    --policy-dir "${run_dir}/authority/policy" \
    --trusted-public-key "${key_dir}/issuer-ed25519-public.der" \
    --issuer-key-generation 1 \
    --gate-c-log "${run_dir}/logs/real-gate-c-p1-worker-managed.log" \
    --observation-seconds "${observation_seconds}" \
    >"${shadow_dir}/validation.log" 2>&1 \
    || { cat "${shadow_dir}/validation.log" >&2; fail "SHADOW observation validator failed"; }

  jq -n --arg schema "nereus-delay.shadow-certification" --arg status PASS \
    --arg environmentId "${environment_id}" --arg candidateCommit "${candidate_commit}" \
    --arg gateCEnvelopeSha256 "${gate_c_receipt_sha256}" \
    --arg shadowPolicyEnvelopeSha256 "${shadow_policy_envelope_sha256}" \
    --arg observationStartEpochMs "${observation_start}" --arg observationEndEpochMs "${observation_end}" \
    --argjson observationSeconds "${observation_seconds}" \
    --arg shadowDir "${shadow_dir}" --arg validationLog "${shadow_dir}/validation.log" \
    --arg ownershipEvidence "${shadow_dir}/chaos/shadow-worker-ownership/ownership-transfer.json" \
    --arg brokerEvidence "${shadow_dir}/broker-restart/ready.json" \
    --arg minioEvidence "${shadow_dir}/minio-outage/state" \
    '{shadowSchema:$schema,shadowSchemaGeneration:1,shadowStatus:$status,environmentId:$environmentId,
      candidateCommit:$candidateCommit,gateCEnvelopeSha256:$gateCEnvelopeSha256,
      shadowPolicyEnvelopeSha256:$shadowPolicyEnvelopeSha256,observationStartEpochMs:($observationStartEpochMs|tonumber),
      observationEndEpochMs:($observationEndEpochMs|tonumber),observationSeconds:$observationSeconds,
      workload:{profile:"ndip1-shadow-profile",ordinaryDuePublish:true,finite:true},
      coverage:{normalRun:true,workerRestart:true,workerOwnershipTransfer:true,brokerRestart:true,
        brokerFailover:true,oxiaUnavailable:true,minioUnavailable:true,policyUpdate:true,
        candidateAdd:true,candidateCancel:true,stateRebuild:true},
      nativeAdmission:0,nativeSend:0,handedOff:0,unresolvedPublishing:false,
      unresolvedUncertain:false,attemptJournalLeak:false,generationIncarnationMix:false,
      evidence:{shadowDir:$shadowDir,validationLog:$validationLog,ownershipTransfer:$ownershipEvidence,
        brokerRestart:$brokerEvidence,minioOutage:$minioEvidence,productionAuthority:false}}' \
    >"${run_dir}/authority/shadow-receipt.json"
  sign_staging_payload shadow-receipt "${run_dir}/authority/shadow-receipt.json" \
    "${run_dir}/authority/shadow-receipt.signed.json"
  shadow_receipt="${run_dir}/authority/shadow-receipt.signed.json"
  shadow_receipt_sha256="$(sha256_file "${shadow_receipt}")"
  jq -n --arg environmentId "${environment_id}" --arg candidateCommit "${candidate_commit}" \
    --arg receipt "${shadow_receipt}" --arg digest "${shadow_receipt_sha256}" \
    --arg policy "${shadow_policy_envelope}" --arg policyDigest "${shadow_policy_envelope_sha256}" \
    '{schema:"nereus-delay.ndip1-shadow-record",status:"PASS",environmentId:$environmentId,
      candidateCommit:$candidateCommit,signedReceipt:$receipt,signedReceiptSha256:$digest,
      shadowPolicy:$policy,shadowPolicyEnvelopeSha256:$policyDigest,nativeAdmission:0,nativeSend:0,
      handedOff:0,productionAuthority:false}' >"${run_dir}/authority/shadow-record.json"
}

write_enabled_policy() {
  publish_policy_head enabled ENABLED 7000 3 120000 0 \
    "${gate_c_receipt_sha256}" "${shadow_receipt_sha256}" ""
  enabled_policy_envelope="${last_policy_envelope}"
  enabled_policy_envelope_sha256="${last_policy_envelope_sha256}"
  enabled_policy_valid_until_epoch_ms="${last_policy_valid_until_epoch_ms}"
  enabled_policy_activation_started=1
  sleep 2
}

run_enabled_canary() {
  local canary_dir="${run_dir}/canary"
  mkdir -p "${canary_dir}"
  write_enabled_policy
  export_common_test_environment
  export NEREUS_DELAY_PERSISTENT_STAGING_GATE_C_RECEIPT="${gate_c_receipt}" \
    NEREUS_DELAY_PERSISTENT_STAGING_SHADOW_RECEIPT="${shadow_receipt}" \
    NEREUS_DELAY_PERSISTENT_STAGING_POLICY="${enabled_policy_envelope}" \
    NEREUS_DELAY_PERSISTENT_STAGING_TRUSTED_PUBLIC_KEY="${key_dir}/issuer-ed25519-public.der" \
    NEREUS_DELAY_PERSISTENT_STAGING_ISSUER_KEY_GENERATION=1 \
    NEREUS_DELAY_PERSISTENT_STAGING_POLICY_KEY_PREFIX="${oxia_policy_key_prefix}" \
    NEREUS_DELAY_PERSISTENT_STAGING_NATIVE_STATE_DIR="${canary_dir}/selection-state" \
    NEREUS_DELAY_PERSISTENT_STAGING_NATIVE_EVIDENCE="${canary_dir}/native-canary-evidence.json" \
    NEREUS_DELAY_PERSISTENT_STAGING_REQUIRE_AUTHORITY=true
  authority_task canary-activation verify-activation "${run_dir}/authority/enabled-policy-sign-config.json" \
    >"${canary_dir}/activation-verification.log"
  export NEREUS_DELAY_PULSAR_LISTENER_NAME=external
  run_authorized_real_gradle enabled-canary \
    runRealPulsarNativeSmoke \
    -PpulsarServiceUrl="${service_url}" -PpulsarAdminUrl="${admin_url}" \
    -PpulsarNativeTopic="${native_topic}"
  unset NEREUS_DELAY_PULSAR_LISTENER_NAME
  local native_evidence="${canary_dir}/native-canary-evidence.json"
  [[ -s "${native_evidence}" ]] || fail "ENABLED canary did not persist machine-readable native evidence"
  jq -e --arg environmentId "${environment_id}" --arg candidateCommit "${candidate_commit}" \
    --arg p1Lock "${p1_source_lock_digest}" \
    '.schema == "nereus-delay.persistent-native-canary" and .schemaGeneration == 2 and
     .environmentId == $environmentId and .candidateCommit == $candidateCommit and
     .productionPath == true and .productionAuthority == false and .verdict == "PASS" and
     .nativeAdmission == 1 and .nativeSend == 1 and .handedOff == 0 and .p1SourceLock == $p1Lock and
     .brokerPersistenceTimeEpochMs < .deliverAtEpochMs and
     all([.policyScopeDigest,.policySnapshotDigest,.preparedRecordHash,.sendCommandSha256,
       .authenticatedResponseCommandSha256,.artifactSetDigest][]; test("^[0-9a-f]{64}$"))' \
    "${native_evidence}" >/dev/null \
    || fail "AUTO_FAST canary evidence is not current-source/P1/policy bound"

  local managed_source_topic="${worker_topic}-managed-handoff"
  local managed_evidence="${canary_dir}/managed-handoff-evidence.json"
  export NEREUS_DELAY_PULSAR_WORKER_ROOT="${canary_dir}/managed-worker-store" \
    NEREUS_DELAY_PULSAR_WORKER_AUTHORITY_PREFIX="${resource_prefix}/${run_id}/managed-handoff-owner" \
    NEREUS_DELAY_PULSAR_WORKER_ASSIGNMENT_PREFIX="${resource_prefix}/${run_id}/managed-handoff-assignment"
  export NEREUS_DELAY_PULSAR_LISTENER_NAME=external
  run_authorized_real_gradle enabled-managed-handoff-prepare \
    runRealPulsarWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${managed_source_topic}" -PpulsarWorkerMode=prepare
  export NEREUS_DELAY_PULSAR_WORKER_MANAGED_HANDOFF=1 \
    NEREUS_DELAY_PERSISTENT_STAGING_MANAGED_EVIDENCE="${managed_evidence}" \
    NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1 \
    NEREUS_DELAY_PULSAR_WORKER_ATTEMPT_JOURNAL_RESPONSE_LOSS=1
  run_authorized_real_gradle enabled-managed-handoff \
    runRealPulsarWorkerSmoke \
    -PpulsarServiceUrl="${service_url}" -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${managed_source_topic}" -PpulsarWorkerMode=resume \
    -PpulsarWorkerDestinationTopic="${native_topic}" -PpulsarWithOxia=true
  unset NEREUS_DELAY_PULSAR_LISTENER_NAME NEREUS_DELAY_PULSAR_WORKER_MANAGED_HANDOFF \
    NEREUS_DELAY_PERSISTENT_STAGING_MANAGED_EVIDENCE \
    NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS \
    NEREUS_DELAY_PULSAR_WORKER_ATTEMPT_JOURNAL_RESPONSE_LOSS

  [[ -s "${managed_evidence}" ]] || fail "Managed Handoff canary did not persist machine evidence"
  jq -e --arg policySnapshot "$(jq -r '.policySnapshotDigest' "${native_evidence}")" \
    --arg policyScope "$(jq -r '.policyScopeDigest' "${native_evidence}")" \
    --arg artifactSet "$(jq -r '.artifactSetDigest' "${native_evidence}")" \
    --arg p1Lock "${p1_source_lock_digest}" \
    '.schema == "nereus-delay.managed-handoff-canary-evidence" and
     .schemaGeneration == 2 and
     .verdict == "PASS" and .productionPath == true and .productionAuthority == false and
     .nativeAdmission == 1 and .nativeSend == 1 and .handedOff == 1 and
     .deliveryContract == "PULSAR_NATIVE_DELIVERY" and
     .actionAtEpochMs < .brokerPersistenceTimeEpochMs and
     .brokerPersistenceTimeEpochMs < .deliverAtEpochMs and
     .policyScopeDigest == $policyScope and .policySnapshotDigest == $policySnapshot and
     .artifactSetDigest == $artifactSet and .p1SourceLock == $p1Lock and
     .destinationResponseLossResolved == true and .attemptJournalResponseLossRecoveries == 3 and
     .attemptJournalStartupReplayRecords == 3 and .attemptJournalStartupReplayVerified == true and
     [.journal[].kind] == ["MAPPED","OWNERSHIP_STARTED","PUBLISHED"] and
     all([.policyScopeDigest,.preparedPublishHash,.recordTemplateHash,.preparedRecordHash,.sendCommandSha256,
       .authenticatedResponseCommandSha256,.artifactSetDigest][]; test("^[0-9a-f]{64}$"))' \
    "${managed_evidence}" >/dev/null \
    || fail "Managed Handoff canary evidence is incomplete or not source/Journal/P1 bound"
  rg -F "Pulsar Worker source-applied physical publish passed" \
    "${run_dir}/logs/real-enabled-managed-handoff.log" >/dev/null \
    || fail "Managed Handoff canary did not close the Worker Outcome chain"

  local canary_log="${run_dir}/logs/real-enabled-canary.log"
  rg -F "Pulsar native coordinator typed-evidence smoke passed" "${canary_log}" \
    >/dev/null || fail "ENABLED canary did not publish the native typed-evidence success marker"
  rg -F "deliverAt=" "${canary_log}" >/dev/null || fail "ENABLED canary did not expose deliverAt evidence"
  rg -F "sequence=" "${canary_log}" >/dev/null || fail "ENABLED canary did not expose typed ACK sequence evidence"
  local deliver_at native_marker_count native_admission native_send handed_off
  deliver_at="$(jq -r '.deliverAtEpochMs' "${native_evidence}")"
  native_admission="$(jq -r '.nativeAdmission' "${native_evidence}")"
  native_send="$(jq -r '.nativeSend' "${native_evidence}")"
  handed_off="$(jq -r '.handedOff' "${native_evidence}")"
  native_marker_count="$(rg -c -F "Pulsar native coordinator typed-evidence smoke passed" "${canary_log}")"
  [[ "${deliver_at}" =~ ^[0-9]+$ && "${native_marker_count}" == 1 \
      && "${native_admission}" == 1 && "${native_send}" == 1 && "${handed_off}" == 0 ]] \
    || fail "ENABLED canary counters were not derived from one exact native record"
  native_admission=2
  native_send=2
  handed_off=1

  curl --silent --show-error --fail --location --max-redirs 5 --max-time 15 \
    "${admin_url}/admin/v2/persistent/public/default/${native_topic}/stats" \
    >"${canary_dir}/native-topic-stats.json"
  curl --silent --show-error --fail --location --max-redirs 5 --max-time 15 \
    "${admin_url}/admin/v2/persistent/public/default/${native_topic}/internalStats" \
    >"${canary_dir}/native-topic-internal-stats.json"
  rg -F "Pulsar Worker destination response-loss" \
    "${run_dir}/logs/real-gate-c-p1-worker-destination-response-loss.log" >/dev/null \
    || fail "ENABLED canary is missing the response-loss recovery evidence"
  rg -F "Pulsar Worker source ACK response-loss" \
    "${run_dir}/logs/real-gate-c-p1-worker-source-ack-response-loss.log" >/dev/null \
    || fail "ENABLED canary is missing source ACK response-loss evidence"
  rg -F "Pulsar Worker vertical smoke passed" \
    "${run_dir}/logs/real-gate-c-p1-worker-managed.log" >/dev/null \
    || fail "ENABLED canary is missing ordinary managed-path evidence"
  local broker_failover_before="${run_dir}/chaos/p1-broker-failover/before-process-crash.json"
  local broker_failover_after="${run_dir}/chaos/p1-broker-failover/after-fresh-process.json"
  [[ -s "${broker_failover_before}" && -s "${broker_failover_after}" ]] \
    || fail "ENABLED canary is missing Broker failover state evidence"

  jq -n --arg schema "nereus-delay.enabled-canary" --arg status PASS \
    --arg environmentId "${environment_id}" --arg candidateCommit "${candidate_commit}" \
    --arg packageDigest "${accepted_package_digest}" --arg p1Lock "${p1_source_lock}" \
    --arg gateC "${gate_c_receipt_sha256}" --arg shadow "${shadow_receipt_sha256}" \
    --arg policy "${enabled_policy_envelope}" --arg policyDigest "${enabled_policy_envelope_sha256}" \
    --arg topic "persistent://public/default/${native_topic}" --arg deliverAt "${deliver_at}" \
    --argjson nativeAdmission "${native_admission}" --argjson nativeSend "${native_send}" \
    --argjson handedOff "${handed_off}" --arg nativeEvidence "${native_evidence}" \
    --arg nativeEvidenceSha256 "$(sha256_file "${native_evidence}")" \
    --arg managedEvidence "${managed_evidence}" \
    --arg managedEvidenceSha256 "$(sha256_file "${managed_evidence}")" \
    --arg managedLog "${run_dir}/logs/real-enabled-managed-handoff.log" \
    --arg managedLogSha256 "$(sha256_file "${run_dir}/logs/real-enabled-managed-handoff.log")" \
    --arg stats "${canary_dir}/native-topic-stats.json" \
    --arg statsSha256 "$(sha256_file "${canary_dir}/native-topic-stats.json")" \
    --arg internalStats "${canary_dir}/native-topic-internal-stats.json" \
    --arg internalStatsSha256 "$(sha256_file "${canary_dir}/native-topic-internal-stats.json")" \
    --arg brokerFailoverBefore "${broker_failover_before}" \
    --arg brokerFailoverBeforeSha256 "$(sha256_file "${broker_failover_before}")" \
    --arg brokerFailoverAfter "${broker_failover_after}" \
    --arg brokerFailoverAfterSha256 "$(sha256_file "${broker_failover_after}")" \
    --arg log "${canary_log}" --arg logSha256 "$(sha256_file "${canary_log}")" \
    --arg activatedAt "$(now_epoch_ms)" \
    '{canarySchema:$schema,canarySchemaGeneration:2,canaryStatus:$status,environmentId:$environmentId,
      candidateCommit:$candidateCommit,ndipPackageDigest:$packageDigest,p1SourceLock:$p1Lock,
      gateCEnvelopeSha256:$gateC,shadowEnvelopeSha256:$shadow,enabledPolicyEnvelopeSha256:$policyDigest,
      enabledPolicy:$policy,profile:"ndip1-enabled-canary-profile",topic:$topic,
      subscription:"ndip1-enabled-canary-subscription",nativeAdmission:$nativeAdmission,nativeSend:$nativeSend,
      handedOff:$handedOff,deliverAtEpochMs:($deliverAt|tonumber),maxRecords:2,typedP1SendAck:true,
      branches:{autoFast:{nativeAdmission:1,nativeSend:1,handedOff:0},
        managedHandoff:{nativeAdmission:1,nativeSend:1,handedOff:1}},
      targetRecordReconciled:true,responseLossRecovery:true,brokerFailoverRecovery:true,
      workerOwnershipUnknownDoesNotFallback:true,ordinaryPathUnaffected:true,activatedAtEpochMs:($activatedAt|tonumber),
      evidence:{nativeCanary:{path:$nativeEvidence,sha256:$nativeEvidenceSha256},
        managedHandoff:{path:$managedEvidence,sha256:$managedEvidenceSha256},
        managedHandoffLog:{path:$managedLog,sha256:$managedLogSha256},
        log:{path:$log,sha256:$logSha256},stats:{path:$stats,sha256:$statsSha256},
        internalStats:{path:$internalStats,sha256:$internalStatsSha256},
        brokerFailoverBefore:{path:$brokerFailoverBefore,sha256:$brokerFailoverBeforeSha256},
        brokerFailoverAfter:{path:$brokerFailoverAfter,sha256:$brokerFailoverAfterSha256},
        productionAuthority:false}}' \
    >"${run_dir}/authority/enabled-canary-receipt.json"
  sign_staging_payload enabled-canary-receipt "${run_dir}/authority/enabled-canary-receipt.json" \
    "${run_dir}/authority/enabled-canary-receipt.signed.json"
  canary_receipt="${run_dir}/authority/enabled-canary-receipt.signed.json"
  canary_receipt_sha256="$(sha256_file "${canary_receipt}")"
  jq -n --arg environmentId "${environment_id}" --arg candidateCommit "${candidate_commit}" \
    --arg receipt "${canary_receipt}" --arg digest "${canary_receipt_sha256}" \
    --arg policy "${enabled_policy_envelope}" --arg policyDigest "${enabled_policy_envelope_sha256}" \
    --argjson nativeAdmission "${native_admission}" --argjson nativeSend "${native_send}" \
    --argjson handedOff "${handed_off}" \
    '{schema:"nereus-delay.ndip1-canary-record",status:"PASS",environmentId:$environmentId,
      candidateCommit:$candidateCommit,signedReceipt:$receipt,signedReceiptSha256:$digest,
      enabledPolicy:$policy,enabledPolicyEnvelopeSha256:$policyDigest,nativeAdmission:$nativeAdmission,
      nativeSend:$nativeSend,handedOff:$handedOff,productionAuthority:false}' \
    >"${run_dir}/authority/canary-record.json"
}

disable_enabled_policy() {
  publish_policy_head disabled DISABLED 0 0 120000 "${enabled_policy_valid_until_epoch_ms}" \
    "${gate_c_receipt_sha256}" "${shadow_receipt_sha256}" "${canary_receipt_sha256}"
  disabled_policy_envelope="${last_policy_envelope}"
  disabled_policy_envelope_sha256="${last_policy_envelope_sha256}"

  export_common_test_environment
  export NEREUS_DELAY_PERSISTENT_STAGING_GATE_C_RECEIPT="${gate_c_receipt}" \
    NEREUS_DELAY_PERSISTENT_STAGING_SHADOW_RECEIPT="${shadow_receipt}" \
    NEREUS_DELAY_PERSISTENT_STAGING_POLICY="${enabled_policy_envelope}" \
    NEREUS_DELAY_PERSISTENT_STAGING_TRUSTED_PUBLIC_KEY="${key_dir}/issuer-ed25519-public.der" \
    NEREUS_DELAY_PERSISTENT_STAGING_ISSUER_KEY_GENERATION=1 \
    NEREUS_DELAY_PERSISTENT_STAGING_POLICY_KEY_PREFIX="${oxia_policy_key_prefix}" \
    NEREUS_DELAY_PERSISTENT_STAGING_REQUIRE_AUTHORITY=true
  local verification_log="${run_dir}/logs/stale-enabled-policy-verification.log"
  set +e
  GRADLE_USER_HOME="${gradle_home}" "${delay_root}/gradlew" -q runNdip1PersistentAuthority \
    -Pndip1AuthorityCommand=verify-activation \
    -Pndip1AuthorityConfig="${run_dir}/authority/disabled-policy-sign-config.json" \
    --no-build-cache --no-daemon --console=plain --rerun-tasks >"${verification_log}" 2>&1
  local status=$?
  set -e
  [[ "${status}" != 0 ]] || fail "DISABLED current head accepted a stale ENABLED activation receipt"
  rg -e "current (Oxia )?handoff policy" -e "policy head" -e "does not authorize activation" \
    "${verification_log}" >/dev/null \
    || fail "stale ENABLED rejection did not identify current-head disagreement"

  while (( $(now_epoch_ms) < enabled_policy_valid_until_epoch_ms )); do
    sleep 5
  done
  local disabled_readback="${run_dir}/authority/policy/disabled-readback.log"
  [[ "$(sed -n 's/^policyMode=//p' "${disabled_readback}" | tail -1)" == DISABLED ]] \
    || fail "canonical current policy did not remain DISABLED through lease expiry"
  local effective_disabled_after
  effective_disabled_after="$(sed -n 's/^effectiveDisabledAfterEpochMs=//p' "${disabled_readback}" | tail -1)"
  [[ "${effective_disabled_after}" == "${enabled_policy_valid_until_epoch_ms}" \
      && $(now_epoch_ms) -ge ${effective_disabled_after} ]] \
    || fail "EFFECTIVE_DISABLED was declared before the frozen ENABLED lease expired"
  enabled_policy_activation_started=0

  local native_processes worker_processes native_process_count worker_process_count active_lease_count active_send_count
  local rollback_stats rollback_stats_sha256 rollback_stats_http_status rollback_stats_curl_log
  native_processes="$(pgrep -af 'PulsarClientArtifactNativeSmoke' || true)"
  worker_processes="$(pgrep -af 'PulsarClientArtifactWorkerSmoke' || true)"
  native_process_count="$(printf '%s\n' "${native_processes}" | awk 'NF {count++} END {print count+0}')"
  worker_process_count="$(printf '%s\n' "${worker_processes}" | awk 'NF {count++} END {print count+0}')"
  [[ -z "${native_processes}" && -z "${worker_processes}" ]] \
    || fail "rollback left a native or Worker process active"
  rollback_stats="${run_dir}/authority/rollback-native-topic-stats.json"
  rollback_stats_curl_log="${run_dir}/authority/rollback-native-topic-stats.curl.log"
  rollback_stats_http_status="$(curl --silent --show-error --location --max-redirs 5 --max-time 15 \
    --output "${rollback_stats}" --write-out '%{http_code}' \
    "${admin_url}/admin/v2/persistent/public/default/${native_topic}/stats" \
    2>"${rollback_stats_curl_log}" || true)"
  [[ "${rollback_stats_http_status}" == 200 || "${rollback_stats_http_status}" == 404 ]] \
    || fail "rollback could not inspect the exact native topic: HTTP ${rollback_stats_http_status}"
  if [[ "${rollback_stats_http_status}" == 200 ]]; then
    active_send_count="$(python3 - "${rollback_stats}" <<'PY'
import json
import sys
from pathlib import Path

value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
publishers = value.get("publishers")
if not isinstance(publishers, (list, dict)):
    raise SystemExit("Pulsar topic stats has no closed publishers collection")
print(len(publishers))
PY
    )"
  else
    active_send_count=0
  fi
  rollback_stats_sha256="$(sha256_file "${rollback_stats}")"
  active_lease_count="$(( $(now_epoch_ms) < enabled_policy_valid_until_epoch_ms ? 1 : 0 ))"
  [[ "${active_send_count}" == 0 && "${active_lease_count}" == 0 ]] \
    || fail "rollback retained an active send or unexpired ENABLED lease"
  jq -n --arg schema "nereus-delay.ndip1-rollback" --arg status PASS \
    --arg environmentId "${environment_id}" --arg candidateCommit "${candidate_commit}" \
    --arg policy "${disabled_policy_envelope}" \
    --arg policyDigest "${disabled_policy_envelope_sha256}" --arg verificationLog "${verification_log}" \
    --arg canaryReceipt "${canary_receipt}" --arg canaryDigest "${canary_receipt_sha256}" \
    --arg rollbackStats "${rollback_stats}" --arg rollbackStatsSha256 "${rollback_stats_sha256}" \
    --arg rollbackStatsHttpStatus "${rollback_stats_http_status}" \
    --argjson activeNativeProcessCount "${native_process_count}" \
    --argjson activeWorkerProcessCount "${worker_process_count}" \
    --argjson activeLeaseCount "${active_lease_count}" --argjson activeSendCount "${active_send_count}" \
    '{schema:$schema,schemaGeneration:2,status:$status,environmentId:$environmentId,
      candidateCommit:$candidateCommit,finalPolicy:$policy,
      finalPolicyEnvelopeSha256:$policyDigest,disabledActivationRejected:true,
      activeNativeProcessCount:$activeNativeProcessCount,activeWorkerProcessCount:$activeWorkerProcessCount,
      activeLeaseCount:$activeLeaseCount,activeSendCount:$activeSendCount,
      canaryReceipt:$canaryReceipt,canaryReceiptSha256:$canaryDigest,verificationLog:$verificationLog,
      evidence:{nativeTopicStats:{path:$rollbackStats,sha256:$rollbackStatsSha256},
        nativeTopicStatsHttpStatus:($rollbackStatsHttpStatus|tonumber)},
      environmentReturnedToDisabled:true,productionAuthority:false}' \
    >"${run_dir}/authority/rollback-receipt.json"
  sign_staging_payload rollback-receipt "${run_dir}/authority/rollback-receipt.json" \
    "${run_dir}/authority/rollback-receipt.signed.json"
  jq -n --arg environmentId "${environment_id}" --arg candidateCommit "${candidate_commit}" \
    --arg policy "${disabled_policy_envelope}" \
    --arg policyDigest "${disabled_policy_envelope_sha256}" \
    --arg receipt "${run_dir}/authority/rollback-receipt.signed.json" \
    --arg digest "$(sha256_file "${run_dir}/authority/rollback-receipt.signed.json")" \
    --argjson activeNativeProcessCount "${native_process_count}" \
    --argjson activeWorkerProcessCount "${worker_process_count}" \
    --argjson activeLeaseCount "${active_lease_count}" --argjson activeSendCount "${active_send_count}" \
    '{schema:"nereus-delay.ndip1-final-state",status:"DISABLED",environmentId:$environmentId,
      candidateCommit:$candidateCommit,productionAuthority:false,
      disabledPolicy:$policy,disabledPolicyEnvelopeSha256:$policyDigest,rollbackReceipt:$receipt,
      rollbackReceiptSha256:$digest,activeNativeProcessCount:$activeNativeProcessCount,
      activeWorkerProcessCount:$activeWorkerProcessCount,
      activeLeaseCount:$activeLeaseCount,activeSendCount:$activeSendCount}' \
    >"${run_dir}/authority/final-state.json"
}

run_independent_certification_validation() {
  local payload="${run_dir}/authority/persistent-certification-validation.json"
  local envelope="${run_dir}/authority/persistent-certification-validation.signed.json"
  python3 -B "${script_dir}/validate-ndip1-persistent-certification.py" \
    --run-dir "${run_dir}" \
    --trusted-public-key "${key_dir}/issuer-ed25519-public.der" \
    --expected-candidate "${candidate_commit}" \
    --expected-package-digest "${accepted_package_digest}" \
    --expected-p1-source-lock "${p1_source_lock}" >"${payload}" \
    || fail "independent persistent certification validation failed"
  [[ "$(jq -r '.status' "${payload}")" == PASS \
      && "$(jq -r '.candidateCommit' "${payload}")" == "${candidate_commit}" \
      && "$(jq -r '.resourceReadbackCount' "${payload}")" == 13 \
      && "$(jq -r '.finalPolicy' "${payload}")" == DISABLED ]] \
    || fail "independent persistent certification validator returned an incomplete receipt"
  sign_staging_payload persistent-certification-validation "${payload}" "${envelope}"
  certification_validation_envelope="${envelope}"
  certification_validation_envelope_sha256="$(sha256_file "${envelope}")"
}

complete_run() {
  jq -n --arg environmentId "${environment_id}" --arg classification "${classification}" \
    --arg runId "${run_id}" --arg candidateCommit "${candidate_commit}" \
    --arg root "${staging_root}" --arg g0 "${run_dir}/g0/g0-snapshot.json" \
    --arg assessment "${run_dir}/authority/data-reset-assessment.signed.json" \
    --arg manifest "${run_dir}/authority/data-reset-manifest.bin" \
    --arg gateC "${gate_c_receipt}" --arg gateCDigest "${gate_c_receipt_sha256}" \
    --arg shadow "${shadow_receipt}" --arg shadowDigest "${shadow_receipt_sha256}" \
    --arg canary "${canary_receipt}" --arg canaryDigest "${canary_receipt_sha256}" \
    --arg disabled "${disabled_policy_envelope}" --arg deploymentScope "${candidate_scope_path}" \
    --arg validation "${certification_validation_envelope}" \
    --arg validationSha256 "${certification_validation_envelope_sha256}" \
    --arg dataDisposition "${data_disposition_envelope}" \
    --arg dataDispositionSha256 "${data_disposition_envelope_sha256}" \
    --arg assessedMode "${assessed_mode}" --arg assessedRunId "${assessed_run_id}" \
    '{schema:"nereus-delay.ndip1-final-summary",status:"COMPLETED",environmentId:$environmentId,
      classification:$classification,runId:$runId,candidateCommit:$candidateCommit,persistentRoot:$root,
      g0Snapshot:$g0,assessmentEnvelope:$assessment,manifest:$manifest,gateCReceipt:$gateC,
      gateCReceiptSha256:$gateCDigest,shadowReceipt:$shadow,shadowReceiptSha256:$shadowDigest,
      canaryReceipt:$canary,canaryReceiptSha256:$canaryDigest,finalPolicy:$disabled,
      certificationValidation:$validation,certificationValidationSha256:$validationSha256,
      dataDisposition:$dataDisposition,dataDispositionSha256:$dataDispositionSha256,
      deploymentScope:$deploymentScope,assessedDeployment:{mode:$assessedMode,runId:$assessedRunId},
      productionAuthority:false,stagingResourcesRetained:true}' >"${run_dir}/final-summary.json"
  local historical_pointer="${deployment_dir}/current-${run_id}.json"
  local prior_pointer_sha256=""
  [[ ! -e "${historical_pointer}" ]] || fail "historical deployment pointer already exists"
  [[ ! -e "${deployment_pointer}" ]] || prior_pointer_sha256="$(sha256_file "${deployment_pointer}")"
  jq -n --arg schema "nereus-delay.ndip1-deployment-pointer" --arg status CURRENT \
    --arg environmentId "${environment_id}" --arg runId "${run_id}" \
    --arg finalSummary "${run_dir}/final-summary.json" \
    --arg finalSummarySha256 "$(sha256_file "${run_dir}/final-summary.json")" \
    --arg deploymentScope "${candidate_scope_path}" \
    --arg deploymentScopeSha256 "$(sha256_file "${candidate_scope_path}")" \
    --arg previousPointerSha256 "${prior_pointer_sha256}" \
    '{schema:$schema,schemaGeneration:1,status:$status,environmentId:$environmentId,runId:$runId,
      finalSummary:$finalSummary,finalSummarySha256:$finalSummarySha256,deploymentScope:$deploymentScope,
      deploymentScopeSha256:$deploymentScopeSha256,previousPointerSha256:$previousPointerSha256,
      publishedAtEpochMs:(now * 1000 | floor)}' >"${historical_pointer}"
  jq '.' "${historical_pointer}" >"${deployment_pointer}.tmp"
  mv "${deployment_pointer}.tmp" "${deployment_pointer}"
  final_status="COMPLETED"
  write_run_status 0
}

cd "${delay_root}"

run_baseline_tests
write_tls_if_needed
"${compose[@]}" config >"${run_dir}/logs/compose-config-preflight.yaml"
"${compose[@]}" up --build --detach >"${run_dir}/logs/compose-up.log" 2>&1 \
  || fail "persistent staging Docker Compose startup failed"
wait_for_http "${admin_url}/admin/v2/brokers/ready"
wait_for_http "http://127.0.0.1:${web_2_port}/admin/v2/brokers/ready"
bootstrap_oxia
run_oxia_coordinator_restart
init_minio
start_fault_proxy
write_candidate_scope
resolve_assessed_deployment
write_data_disposition_declaration
write_environment_snapshot
write_authority_configs
execute_manifest_operations

run_gate_c_unit_and_real_checks
run_checkpoint_precommit_fault_case
run_fresh_process_authority_checks
run_gateway_session_churn
run_gateway_leader_failover
run_route_restart_case route-provider-restart \
  com.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRouteProviderRecoversAfterRealOxiaRestart
run_route_restart_case route-notification-restart \
  com.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRouteNotificationsRecoverAfterRealOxiaRestart
run_credential_binding_chaos
run_long_gc_chaos
run_target_isolation_chaos
run_local_storage_chaos
run_checkpoint_reaping_chaos
run_minio_checks
run_real_pulsar_baseline_smoke
run_p1_worker_response_loss_recovery
run_p1_broker_failover
audit_gate_c_results
audit_manifest_operations
write_gate_c_receipt
run_shadow_observation
run_enabled_canary
disable_enabled_policy
run_independent_certification_validation
complete_run
