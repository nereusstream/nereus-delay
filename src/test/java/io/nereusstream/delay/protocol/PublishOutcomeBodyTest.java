package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PublishOutcomeBodyTest {
    @Test
    void initialPublishedBodyIsCanonicalAndRoundTrips() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("published-attempt"));
        final byte[] evidence = evidence("kafka-ack-evidence");
        final byte[] retry = retryDecision(1, StableCode.OK, null);
        final byte[] body = PublishOutcomeBody.encodeInitial(shard, 9_000, attempt, 1, 0, StableCode.OK,
                evidence, charge().canonicalBytes(), observedAt(), retry);
        final PublishOutcomeBody parsed = PublishOutcomeBody.decode(body);

        assertArrayEquals(attempt, parsed.publishAttemptId());
        assertEquals(1, parsed.sideEffect());
        assertEquals(0, parsed.disposition());
        assertEquals(StableCode.OK, parsed.stableCode());
        assertArrayEquals(evidence, parsed.evidence());
        assertArrayEquals(retry, parsed.retryDecision().canonicalBytes());
        assertArrayEquals(body, PublishOutcomeBody.encodeInitial(shard, 9_000, attempt, 1, 0, StableCode.OK,
                evidence, charge().canonicalBytes(), observedAt(), retry));
    }

    @Test
    void initialNotPublishedAndUnknownBranchesUseClosedCombinations() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("not-published-attempt"));
        final byte[] notPublished = PublishOutcomeBody.encodeInitial(shard, 9_000, attempt, 2, 1,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, evidence("guard-rejection"),
                charge().canonicalBytes(), observedAt(), retryDecision(2,
                        StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 3_000L));
        assertEquals(2, PublishOutcomeBody.decode(notPublished).sideEffect());
        assertEquals(1, PublishOutcomeBody.decode(notPublished).disposition());

        final byte[] unknown = PublishOutcomeBody.encodeInitial(shard, 9_000, attempt, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, null, unknownTransfer(), observedAt(),
                unknownRetryPlaceholder());
        final PublishOutcomeBody parsedUnknown = PublishOutcomeBody.decode(unknown);
        assertEquals(3, parsedUnknown.sideEffect());
        assertEquals(4, parsedUnknown.disposition());
        assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, parsedUnknown.stableCode());
        assertEquals(0, parsedUnknown.evidence().length);
    }

    @Test
    void evidenceResolutionValidatesTypedCursorAndRoundTrips() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("resolution-attempt"));
        final EvidenceCursorV1 cursor = EvidenceCursorV1.kafka(Bytes.sha256(Bytes.utf8("lane")), new byte[16],
                java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("topic")), 16), 0, 1, 2_100, 7, 8);
        final byte[] body = PublishOutcomeBody.encodeEvidenceResolution(shard, 9_000, attempt, cursor,
                evidence("receipt"), StableCode.OK, 1, 0, charge().canonicalBytes(), observedAt(),
                retryDecision(1, StableCode.OK, null));
        final PublishOutcomeBody parsed = PublishOutcomeBody.decodeEvidenceResolution(body);
        assertArrayEquals(attempt, parsed.publishAttemptId());
        assertEquals(1, parsed.sideEffect());
        assertEquals(StableCode.OK, parsed.stableCode());
    }

    @Test
    void encoderRejectsWrongInitialCombinationAndNonCanonicalCharge() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("invalid-attempt"));
        assertThrows(IllegalArgumentException.class, () -> PublishOutcomeBody.encodeInitial(shard, 9_000, attempt,
                1, 1, StableCode.OK, evidence("evidence"), charge().canonicalBytes(), observedAt(),
                retryDecision(1, StableCode.OK, null)));

        final byte[] canonicalCharge = charge().canonicalBytes();
        // field 1's value is zero; an overlong varint is not canonical.
        final byte[] nonCanonicalCharge = new byte[canonicalCharge.length + 1];
        nonCanonicalCharge[0] = canonicalCharge[0];
        nonCanonicalCharge[1] = (byte) 0x80;
        nonCanonicalCharge[2] = 0;
        System.arraycopy(canonicalCharge, 2, nonCanonicalCharge, 3, canonicalCharge.length - 2);
        assertThrows(IllegalArgumentException.class, () -> PublishOutcomeBody.encodeInitial(shard, 9_000, attempt,
                2, 1, StableCode.DESTINATION_DEFINITIVE_RETRIABLE, evidence("evidence"),
                nonCanonicalCharge, observedAt(), retryDecision(2,
                        StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 3_000L)));
    }

    private static PublishAdmissionBody.ChargeVector charge() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static byte[] retryDecision(final int kind, final StableCode stableCode, final Long nextRetryAt) {
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            if (nextRetryAt != null) {
                CanonicalProtobuf.int64(output, 6, nextRetryAt);
            }
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static byte[] unknownRetryPlaceholder() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("unknown")));
    }

    private static byte[] unknownTransfer() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("transfer")));
    }

    private static byte[] evidence(final String value) {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8(value)));
    }

    private static TrustedUtcIntervalEvidence observedAt() {
        return new TrustedUtcIntervalEvidence(2_000, 2_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                Bytes.sha256(Bytes.utf8("time-proof")), 0, null);
    }

    private static ShardId shard() {
        return new ShardId(RouteIncarnation.random(), 0);
    }

}
