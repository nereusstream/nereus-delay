package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarKey;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarRecordTemplate;
import com.nereusstream.delay.protocol.PulsarReservedProperties;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.ReservedPublishMetadata;
import com.nereusstream.delay.protocol.ResolvedPayload;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.List;
import org.junit.jupiter.api.Test;

class PulsarSendAckEvidenceTest {
    @Test
    void buildsAndRoundTripsTheExactPulsarAckBranch() {
        final PulsarDestinationRequest request = request();
        final byte[] preparedHash = hash("prepared");
        final byte[] producerHash = hash("producer");
        final byte[] responseHash = hash("response");
        final PublishEvidence evidence = PulsarSendAckEvidence.published(
                request, preparedHash, producerHash, 17, 23, 0, 2_001, 42, responseHash);
        final PublishEvidence decoded = PublishEvidence.decode(evidence.canonicalBytes());

        assertEquals(PublishEvidenceKind.PULSAR_SEND_ACK, decoded.evidenceKind());
        assertEquals(EvidenceVerificationStatus.VERIFIED_PUBLISHED, decoded.verificationStatus());
        assertArrayEquals(evidence.evidenceId(), decoded.evidenceId());
        decoded.requireBusinessMutation(request.publishAttemptId(), true);
        PulsarSendAckEvidence.requireExactBinding(decoded, request, preparedHash, producerHash, 2_001);
    }

    @Test
    void rejectsWrongFixedInputsAndNegativePositions() {
        final PulsarDestinationRequest request = request();
        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarSendAckEvidence.published(
                        request, new byte[31], hash("producer"), 17, 23, 0, 2_001, 42, hash("response")));
        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarSendAckEvidence.published(
                        request, hash("prepared"), hash("producer"), -1, 23, 0, 2_001, 42, hash("response")));
        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarSendAckEvidence.published(
                        request, hash("prepared"), hash("producer"), 17, 23, 0, 2_001, 42, new byte[31]));
    }

    @Test
    void evidenceOwnerRemainsBoundToTheExactPublishAttempt() {
        final PulsarDestinationRequest request = request();
        final PublishEvidence evidence = PulsarSendAckEvidence.published(
                request, hash("prepared"), hash("producer"), 17, 23, 0, 2_001, 42, hash("response"));
        assertThrows(
                IllegalArgumentException.class, () -> evidence.requireBusinessMutation(hash("foreign-attempt"), true));
        assertEquals(32, request.publishAttemptId().length);
        assertEquals(32, evidence.evidenceId().length);
    }

    @Test
    void providerEvidenceMustRetainTheExactPreparedHashAndBrokerTime() {
        final PulsarDestinationRequest request = request();
        final byte[] preparedHash = hash("prepared");
        final byte[] producerHash = hash("producer");
        final PublishEvidence evidence = PulsarSendAckEvidence.published(
                request, preparedHash, producerHash, 17, 23, 0, 2_001, 42, hash("response"));

        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarSendAckEvidence.requireExactBinding(
                        evidence, request, hash("foreign-prepared"), producerHash, 2_001));
        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarSendAckEvidence.requireExactBinding(evidence, request, preparedHash, producerHash, 2_002));
    }

    @Test
    void generationTwoEvidenceBindsTheFinalRecordAndArtifactSet() {
        final RecordFixture fixture = recordFixture();
        final PublishEvidence evidence = PulsarSendAckEvidence.publishedRecord(
                fixture.record,
                fixture.artifacts,
                fixture.producerHash,
                17,
                23,
                0,
                1,
                2_001,
                22,
                3,
                4,
                42,
                hash("send"),
                hash("response"));

        final PublishEvidence decoded = PublishEvidence.decode(evidence.canonicalBytes());
        assertEquals(PublishEvidenceKind.PULSAR_SEND_ACK, decoded.evidenceKind());
        decoded.requireBusinessMutation(fixture.attemptId, true);
        PulsarSendAckEvidence.requireExactBindingForRecord(
                decoded,
                fixture.record,
                fixture.artifacts,
                fixture.producerHash,
                17,
                23,
                0,
                1,
                2_001,
                22,
                3,
                4,
                42,
                hash("send"),
                hash("response"));

        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarSendAckEvidence.requireExactBindingForRecord(
                        decoded,
                        fixture.record,
                        fixture.artifacts,
                        hash("foreign-producer"),
                        17,
                        23,
                        0,
                        1,
                        2_001,
                        22,
                        3,
                        4,
                        42,
                        hash("send"),
                        hash("response")));
    }

    private static PulsarDestinationRequest request() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        return new PulsarDestinationRequest(
                "standalone",
                hash("resource"),
                "persistent://public/default/target",
                1_001,
                0,
                DestinationLaneId.derive(Bytes.utf8("lane")),
                new byte[16],
                DelayMessageId.random(shard),
                0,
                hash("attempt"),
                1_000,
                1_000,
                Bytes.utf8("payload"),
                new byte[0]);
    }

    private static byte[] hash(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }

    private static RecordFixture recordFixture() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] attemptId = hash("attempt-record");
        final byte[] destinationHash = hash("destination-profile");
        final byte[] capabilityHash = hash("capability-profile");
        final ReservedPublishMetadata reserved = new ReservedPublishMetadata(
                shard.routeIncarnation(),
                shard.unsignedPartition(),
                messageId,
                7,
                attemptId,
                destinationHash,
                capabilityHash,
                10_000,
                DeliveryMode.MANAGED);
        final ArtifactGenerationSet artifacts =
                ArtifactGenerationSet.current(1, PulsarSourceLock.digest(), hash("canonical-schema-bundle"));
        final BrokerResourceIdentity target = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                "standalone", hash("resource-record"), "persistent://public/default/target-record", 1_001));
        final PulsarRecordTemplate template = new PulsarRecordTemplate(
                target,
                2,
                PulsarKey.utf8("key"),
                Bytes.utf8("ordering"),
                List.of(new PulsarMetadata.Property("caller", "value")),
                9_000L,
                reserved,
                DeliveryContract.NEREUS_MANAGED_NOT_BEFORE,
                null,
                PayloadForPublish.inline(Bytes.utf8("payload")),
                artifacts.setDigest());
        final byte[] producerHash = hash("producer-record");
        final byte[] preparedIdentityHash = hash("prepared-record");
        final PulsarPreparedRecord record = new PulsarPreparedRecord(
                template,
                template.recordTemplateHash(),
                ResolvedPayload.of(Bytes.utf8("payload")),
                PulsarSequenceAuthority.managedJournal(hash("mapping-record"), 42, producerHash),
                ExternalDeliveryIdentity.publishAttempt(attemptId),
                preparedIdentityHash,
                PulsarReservedProperties.all(reserved, attemptId, preparedIdentityHash),
                artifacts.setDigest());
        return new RecordFixture(record, artifacts, producerHash, attemptId);
    }

    private record RecordFixture(
            PulsarPreparedRecord record, ArtifactGenerationSet artifacts, byte[] producerHash, byte[] attemptId) {}
}
