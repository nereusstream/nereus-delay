#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
kafka_checkout=${NEREUS_DELAY_KAFKA_CHECKOUT:-"$delay_root/../../kafka-worktrees/nereus-delay-k1"}
pulsar_checkout=${NEREUS_DELAY_PULSAR_CHECKOUT:-"$delay_root/../../pulsar-worktrees/nereus-delay-p1"}
oxia_checkout=${NEREUS_DELAY_OXIA_CHECKOUT:-"$delay_root/../../oxia"}

kafka_branch="nereus/delay-guarded-producer-v1"
kafka_base="c300006a7705c240642db6950b5a95fec982bfc5"
kafka_head="05849884ca81fad767fda058444d1e17c7f9cbf9"
pulsar_branch="nereus/delay-resource-guard-v1"
pulsar_base="8dae0236c0a0d405ed7f8303081080520fe91551"
pulsar_head="358ce4a1033bd566faebcd3465c3ba4606f3c83f"
oxia_head="37a17bef17202d5fd6e23282da5fd26d94865484"
delay_worker_head="c124b216"

fail() {
    echo "cross-repo contract audit failed: $*" >&2
    exit 1
}

require_git_checkout() {
    local repo=$1
    git -C "$repo" rev-parse --verify HEAD >/dev/null 2>&1 \
        || fail "not a Git checkout: $repo"
}

require_clean() {
    local repo=$1
    [[ -z "$(git -C "$repo" status --porcelain)" ]] \
        || fail "worktree is not clean: $repo"
}

require_branch() {
    local repo=$1
    local expected=$2
    local actual
    actual=$(git -C "$repo" symbolic-ref --quiet --short HEAD) \
        || fail "detached HEAD in $repo"
    [[ "$actual" == "$expected" ]] \
        || fail "$repo is on $actual, expected $expected"
}

require_head() {
    local repo=$1
    local expected=$2
    local actual
    actual=$(git -C "$repo" rev-parse HEAD)
    [[ "$actual" == "$expected" ]] \
        || fail "$repo is at $actual, expected $expected"
}

require_ancestor() {
    local repo=$1
    local base=$2
    git -C "$repo" merge-base --is-ancestor "$base" HEAD \
        || fail "$repo HEAD is not descended from $base"
}

require_file_text() {
    local file=$1
    local text=$2
    [[ -f "$file" ]] || fail "missing file: $file"
    rg -F --quiet -- "$text" "$file" \
        || fail "missing '$text' in $file"
}

for repo in "$delay_root" "$kafka_checkout" "$pulsar_checkout" "$oxia_checkout"; do
    require_git_checkout "$repo"
    require_clean "$repo"
done

require_branch "$delay_root" "nereus/delay-full-implementation-v1"
require_ancestor "$delay_root" "$delay_worker_head"
require_branch "$kafka_checkout" "$kafka_branch"
require_head "$kafka_checkout" "$kafka_head"
require_ancestor "$kafka_checkout" "$kafka_base"
require_branch "$pulsar_checkout" "$pulsar_branch"
require_head "$pulsar_checkout" "$pulsar_head"
require_ancestor "$pulsar_checkout" "$pulsar_base"
require_head "$oxia_checkout" "$oxia_head"

require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "trunk@c300006a7705c240642db6950b5a95fec982bfc5"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "412441c47cce4e61d3cc015b95c7d3cffcab2f7f"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "72d4accf"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "c72cac90"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "a7fd5fa7dd35d5d8535d3c63e577208d29fc2c5"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "10e21cbf0e6f741f10b353c56a316a0b57b71b9d"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "nereus/delay-full-implementation-v1@2dd2cfff83f4d029972cf7fbeb569fbf4538c026"
require_file_text "$delay_root/docs/IMPLEMENTATION-STATUS.md" \
    "202368d46fedfe12ae414edaa9c3db32cc8e5073"
require_file_text "$delay_root/docs/V1-DESIGN-AUDIT.md" \
    "358ce4a103"
require_file_text "$delay_root/docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md" \
    "PulsarClientArtifactRecoverySourcePositioner"

require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/KafkaBrokerResourceIdentityV1.java" \
    "nativeTopicUuid"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/PulsarBrokerResourceIdentityV1.java" \
    "resourceIncarnation"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/transport/ProductionKafkaProduceTransport.java" \
    "acks != -1 || !idempotenceEnabled || !autoTopicCreationDisabled"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/transport/ProductionPulsarSendTransport.java" \
    "!batchingDisabled || !chunkingDisabled || !autoTopicCreationDisabled"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/RsaSha256GatewayJwtVerifier.java" \
    "SHA256withRSA"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/RsaSha256GatewayJwtVerifier.java" \
    "x5t#S256"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewayAdmissionController.java" \
    "IfVersionIdEquals"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/OxiaGatewayAdmissionController.java" \
    "containsExact"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/gateway/GatewayAdmissionRecordV1.java" \
    "nereus-delay-gateway-admission-record-v1"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaGatewayAdmissionControllerTest.java" \
    "responseLossIsAcceptedOnlyAfterExactRereadForReserveAndRelease"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/gateway/OxiaRealGatewayAdmissionSmokeTest.java" \
    "admissionPoolsAndExpiryWorkAgainstRealService"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/route/OxiaRouteAuthoritySession.java" \
    "reconnectSession"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaSignedRouteSnapshotProviderTest.java" \
    "explicitSessionReconnectRotatesMarkerAfterFenceAndRestoresReads"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/semantic/VerifiedNativePreparationSnapshotCache.java" \
    "NativePreparationEligibilityV1"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/semantic/NativeCapabilitySnapshotIssuer.java" \
    "protectNativeCapability"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/semantic/NativeCapabilityIssuanceAuthority.java" \
    "resolveGuard"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/client/DelayClient.java" \
    "prepareScheduleSubmissionV1"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/protocol/ActivationBarrierV1.java" \
    "toSourceBarrier"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/RouteSourceAssignmentFactory.java" \
    "fromRoute"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/RouteSourceAssignmentResolver.java" \
    "active"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSourceFactory.java" \
    "ACTIVE_FOR_COMMANDS"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSourceFactory.java" \
    "consumer.seek"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRecoverySourceCursor.java" \
    "never commits a group offset"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactRecoverySourceCursor.java" \
    "OwnerRecoveryCoordinator"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSourceFactory.java" \
    "resourceGuardAttestation"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSourceFactory.java" \
    "WorkerShardRuntime"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRecoverySourceCursor.java" \
    "PulsarClientArtifactRecoverySourceCursor"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRecoverySourceCursor.java" \
    "requireProof"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRecoverySourcePositioner.java" \
    "seekAfter"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactRecoverySourcePositioner.java" \
    "awaitStableProof"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactSourceRecordConsumer.java" \
    "GuardedConsumer<byte[]>"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactSourceRecordConsumer.java" \
    "connectionGeneration()"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceRecordConsumer.java" \
    "KafkaClientArtifactSourceRecordConsumer"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceRecordConsumer.java" \
    "consumer.commitSync"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceConsumerFactory.java" \
    "bindResourceGuard"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactFetchEvidence.java" \
    "requireBatch"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/consumer/GuardedConsumer.java" \
    "pollGuarded"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/consumer/GuardedFetchEvidence.java" \
    "fetchResponseBodySha256"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactSourceSmoke.java" \
    "KafkaClientArtifactRecoverySourceCursor"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker vertical smoke passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker authority smoke passed"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "CheckpointFileInventory.collect"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "and final checkpoint"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackend.java" \
    "establishSessionMarker"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncOwnerLeaseBackend.java" \
    "connectUnchecked"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/SourceAssignment.java" \
    "canonicalBytes"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerAssignment.java" \
    "capacityEnvelopeDigest"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/OxiaSyncWorkerAssignmentBackend.java" \
    "nereus-delay-oxia-worker-assignment-record-v1"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerAssignmentCoordinator.java" \
    "requireAccepted"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/RouteWorkerAssignmentCoordinator.java" \
    "snapshotDigest"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/ownership/WorkerSchedulingRuntime.java" \
    "openForActiveOwner"
require_file_text "$delay_root/src/main/java/io/nereusstream/delay/scheduler/PersistentLaneScheduler.java" \
    "forActiveOwner"
require_file_text "$delay_root/build.gradle" \
    "runRealKafkaWorkerSmoke"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "runRealKafkaWorkerSmoke"
require_file_text "$delay_root/e2e/run-kafka-real-client-e2e.sh" \
    "NEREUS_DELAY_KAFKA_WITH_OXIA"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactSourceSmoke.java" \
    "PulsarClientArtifactRecoverySourceCursor"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "Pulsar Worker vertical smoke passed"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "Pulsar Worker assignment publication/acceptance passed"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "CheckpointFileInventory.collect"
require_file_text "$delay_root/src/real-pulsar/java/io/nereusstream/delay/transport/PulsarClientArtifactWorkerSmoke.java" \
    "and final checkpoint"
require_file_text "$delay_root/src/real-kafka/java/io/nereusstream/delay/transport/KafkaClientArtifactWorkerSmoke.java" \
    "Kafka Worker assignment publication/acceptance passed"
require_file_text "$delay_root/src/test/java/io/nereusstream/delay/route/OxiaRealRouteWorkerAssignmentSmokeTest.java" \
    "signedRoutePublicationFeedsSessionBoundWorkerAssignmentAuthority"
require_file_text "$delay_root/e2e/run-oxia-real-service.sh" \
    "OxiaRealRouteWorkerAssignmentSmokeTest"
require_file_text "$delay_root/build.gradle" \
    "runRealPulsarWorkerSmoke"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "runRealPulsarWorkerSmoke"
require_file_text "$delay_root/e2e/run-pulsar-real-client-e2e.sh" \
    "NEREUS_DELAY_PULSAR_WITH_OXIA"
require_file_text "$delay_root/build.gradle" \
    "pulsarWithOxia"

require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/GuardedProducer.java" \
    "sendGuarded"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/GuardedTransactionalProducer.java" \
    "sendGuardedInTransaction"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java" \
    "transactionManager.maybeAddPartition"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/ProducerResourceGuard.java" \
    "expectedTopicId"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java" \
    ".setTopicId(topicId)"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java" \
    "Errors.UNKNOWN_TOPIC_ID"
require_file_text "$kafka_checkout/clients/src/main/java/org/apache/kafka/clients/producer/GuardedResponseEvidence.java" \
    "logAppendTimeMs < -1"

require_file_text "$pulsar_checkout/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/TopicResourceGuard.java" \
    "RESOURCE_INCARNATION_BYTES = 32"
require_file_text "$pulsar_checkout/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/GuardedMessageId.java" \
    "TopicResourceGuard resourceGuard()"
require_file_text "$pulsar_checkout/pulsar-common/src/main/proto/PulsarApi.proto" \
    "ResourceIncarnationMismatch = 26"
require_file_text "$pulsar_checkout/pulsar-common/src/main/proto/PulsarApi.proto" \
    "optional TopicResourceGuard resource_guard = 14"
require_file_text "$pulsar_checkout/pulsar-common/src/main/proto/PulsarApi.proto" \
    "optional TopicResourceGuard resource_guard = 20"
require_file_text "$pulsar_checkout/pulsar-common/src/main/proto/PulsarApi.proto" \
    "optional uint64 connection_generation = 4"
require_file_text "$pulsar_checkout/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/GuardedConsumer.java" \
    "Optional<TopicResourceGuardAttestation> resourceGuardAttestation()"
require_file_text "$pulsar_checkout/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java" \
    "guardedSourceConnectionGeneration"
require_file_text "$pulsar_checkout/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConsumerImpl.java" \
    "small receiver queue can continue past a seek boundary"
require_file_text "$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ServerCnx.java" \
    "guardedSourceConnectionGeneration"
require_file_text "$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/Producer.java" \
    "validateResourceGuardNow()"
require_file_text "$pulsar_checkout/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ProducerImpl.java" \
    "recoverResourceIncarnationMismatch"

echo "cross-repo contract audit passed"
echo "Delay:  $(git -C "$delay_root" rev-parse HEAD)"
echo "Kafka:  $kafka_branch@$kafka_head from $kafka_base"
echo "Pulsar: $pulsar_branch@$pulsar_head from $pulsar_base"
echo "Oxia:   $(git -C "$oxia_checkout" rev-parse HEAD)"
