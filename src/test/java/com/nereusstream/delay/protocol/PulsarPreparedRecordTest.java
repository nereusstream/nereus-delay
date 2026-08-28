package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class PulsarPreparedRecordTest {
    @Test
    void managedJournalMayCarryThePulsarNativeDeliveryContract() {
        final Fixture fixture = fixture(DeliveryContract.PULSAR_NATIVE_DELIVERY);

        assertDoesNotThrow(() -> new PulsarPreparedRecord(
                fixture.template,
                fixture.template.recordTemplateHash(),
                ResolvedPayload.of(fixture.payload),
                PulsarSequenceAuthority.managedJournal(
                        Bytes.sha256(Bytes.utf8("mapping")), 7, Bytes.sha256(Bytes.utf8("producer"))),
                ExternalDeliveryIdentity.publishAttempt(fixture.attemptId),
                fixture.preparedIdentity,
                PulsarReservedProperties.all(fixture.reserved, fixture.attemptId, fixture.preparedIdentity),
                fixture.artifacts.setDigest()));
    }

    @Test
    void producerAssignedBranchRemainsExclusiveToAutoFastNativeIdentity() {
        final Fixture ordinary = fixture(DeliveryContract.NEREUS_MANAGED_NOT_BEFORE);

        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarPreparedRecord(
                        ordinary.template,
                        ordinary.template.recordTemplateHash(),
                        ResolvedPayload.of(ordinary.payload),
                        PulsarSequenceAuthority.producerAssigned(),
                        ExternalDeliveryIdentity.nativeDelivery(Bytes.sha256(Bytes.utf8("native-id"))),
                        ordinary.preparedIdentity,
                        PulsarReservedProperties.all(ordinary.reserved, ordinary.attemptId, ordinary.preparedIdentity),
                        ordinary.artifacts.setDigest()));
    }

    private static Fixture fixture(final DeliveryContract contract) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("attempt"));
        final byte[] payload = Bytes.utf8("payload");
        final byte[] preparedIdentity = Bytes.sha256(Bytes.utf8("prepared"));
        final ReservedPublishMetadata reserved = new ReservedPublishMetadata(
                shard.routeIncarnation(),
                shard.unsignedPartition(),
                messageId,
                1,
                attemptId,
                Bytes.sha256(Bytes.utf8("destination")),
                Bytes.sha256(Bytes.utf8("capability")),
                2_000,
                DeliveryMode.MANAGED);
        final ArtifactGenerationSet artifacts =
                ArtifactGenerationSet.current(1, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("schema")));
        final PulsarRecordTemplate template = new PulsarRecordTemplate(
                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                        "cluster",
                        Bytes.sha256(Bytes.utf8("resource")),
                        "persistent://public/default/native-record",
                        1)),
                0,
                PulsarKey.none(),
                null,
                List.of(),
                null,
                reserved,
                contract,
                contract.isNative() ? 2_000L : null,
                PayloadForPublish.inline(payload),
                artifacts.setDigest());
        return new Fixture(template, reserved, artifacts, attemptId, preparedIdentity, payload);
    }

    private record Fixture(
            PulsarRecordTemplate template,
            ReservedPublishMetadata reserved,
            ArtifactGenerationSet artifacts,
            byte[] attemptId,
            byte[] preparedIdentity,
            byte[] payload) {}
}
