package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PulsarSendAckEvidenceTest {
    @Test
    void buildsAndRoundTripsTheExactPulsarAckBranch() {
        final PulsarDestinationRequest request = request();
        final byte[] preparedHash = hash("prepared");
        final byte[] producerHash = hash("producer");
        final byte[] responseHash = hash("response");
        final PublishEvidenceV1 evidence = PulsarSendAckEvidence.published(request, preparedHash, producerHash,
                17, 23, 0, 2_001, 42, responseHash);
        final PublishEvidenceV1 decoded = PublishEvidenceV1.decode(evidence.canonicalBytes());

        assertEquals(PublishEvidenceKindV1.PULSAR_SEND_ACK, decoded.evidenceKind());
        assertEquals(EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, decoded.verificationStatus());
        assertArrayEquals(evidence.evidenceId(), decoded.evidenceId());
        decoded.requireBusinessMutation(request.publishAttemptId(), true);
    }

    @Test
    void rejectsWrongFixedInputsAndNegativePositions() {
        final PulsarDestinationRequest request = request();
        assertThrows(IllegalArgumentException.class, () -> PulsarSendAckEvidence.published(request,
                new byte[31], hash("producer"), 17, 23, 0, 2_001, 42, hash("response")));
        assertThrows(IllegalArgumentException.class, () -> PulsarSendAckEvidence.published(request,
                hash("prepared"), hash("producer"), -1, 23, 0, 2_001, 42, hash("response")));
        assertThrows(IllegalArgumentException.class, () -> PulsarSendAckEvidence.published(request,
                hash("prepared"), hash("producer"), 17, 23, 0, 2_001, 42, new byte[31]));
    }

    @Test
    void evidenceOwnerRemainsBoundToTheExactPublishAttempt() {
        final PulsarDestinationRequest request = request();
        final PublishEvidenceV1 evidence = PulsarSendAckEvidence.published(request, hash("prepared"),
                hash("producer"), 17, 23, 0, 2_001, 42, hash("response"));
        assertThrows(IllegalArgumentException.class,
                () -> evidence.requireBusinessMutation(hash("foreign-attempt"), true));
        assertEquals(32, request.publishAttemptId().length);
        assertEquals(32, evidence.evidenceId().length);
    }

    private static PulsarDestinationRequest request() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        return new PulsarDestinationRequest("standalone", hash("resource"),
                "persistent://public/default/target", 1_001, 0, DestinationLaneId.derive(Bytes.utf8("lane")),
                new byte[16], DelayMessageId.random(shard), 0, hash("attempt"), 1_000, 1_000,
                Bytes.utf8("payload"), new byte[0]);
    }

    private static byte[] hash(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }
}
