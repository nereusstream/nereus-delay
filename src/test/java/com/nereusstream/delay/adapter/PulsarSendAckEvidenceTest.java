package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import com.nereusstream.delay.protocol.PublishEvidenceKindV1;
import com.nereusstream.delay.protocol.PublishEvidenceV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

class PulsarSendAckEvidenceTest {
    @Test
    void buildsAndRoundTripsTheExactPulsarAckBranch() {
        final PulsarDestinationRequest request = request();
        final byte[] preparedHash = hash("prepared");
        final byte[] producerHash = hash("producer");
        final byte[] responseHash = hash("response");
        final PublishEvidenceV1 evidence = PulsarSendAckEvidence.published(
                request, preparedHash, producerHash, 17, 23, 0, 2_001, 42, responseHash);
        final PublishEvidenceV1 decoded = PublishEvidenceV1.decode(evidence.canonicalBytes());

        assertEquals(PublishEvidenceKindV1.PULSAR_SEND_ACK, decoded.evidenceKind());
        assertEquals(EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, decoded.verificationStatus());
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
        final PublishEvidenceV1 evidence = PulsarSendAckEvidence.published(
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
        final PublishEvidenceV1 evidence = PulsarSendAckEvidence.published(
                request, preparedHash, producerHash, 17, 23, 0, 2_001, 42, hash("response"));

        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarSendAckEvidence.requireExactBinding(
                        evidence, request, hash("foreign-prepared"), producerHash, 2_001));
        assertThrows(
                IllegalArgumentException.class,
                () -> PulsarSendAckEvidence.requireExactBinding(evidence, request, preparedHash, producerHash, 2_002));
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
}
