package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublishOutcomeBodyTest {
    @Test
    void initialPublishedBodyIsCanonicalAndRoundTrips() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("published-attempt"));
        final byte[] evidence = evidence(attempt, true);
        final byte[] retry = retryDecision(1, StableCode.OK, null);
        final byte[] body = PublishOutcomeBody.encodeInitial(shard, 9_000, attempt, 1, 0, StableCode.OK,
                evidence, charge().canonicalBytes(), observedAt(), retry);
        final PublishOutcomeBody parsed = PublishOutcomeBody.decode(body);

        assertArrayEquals(attempt, parsed.publishAttemptId());
        assertArrayEquals(attempt, parsed.initialLogicalOperationIdentity());
        assertEquals(1, parsed.sideEffect());
        assertEquals(0, parsed.disposition());
        assertEquals(StableCode.OK, parsed.stableCode());
        assertArrayEquals(evidence, parsed.evidence());
        assertArrayEquals(retry, parsed.retryDecision().canonicalBytes());
        assertTrue(parsed.retryDecision().hasFullShape());
        assertEquals(StableCode.OK, parsed.retryDecision().cause());
        assertEquals(1, parsed.retryDecision().retryDomain());
        assertArrayEquals(body, PublishOutcomeBody.encodeInitial(shard, 9_000, attempt, 1, 0, StableCode.OK,
                evidence, charge().canonicalBytes(), observedAt(), retry));
    }

    @Test
    void initialNotPublishedAndUnknownBranchesUseClosedCombinations() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("not-published-attempt"));
        final byte[] notPublished = PublishOutcomeBody.encodeInitial(shard, 9_000, attempt, 2, 1,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, evidence(attempt, false),
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
        assertFalse(parsedUnknown.retryDecision().hasFullShape());
    }

    @Test
    void evidenceResolutionValidatesTypedCursorAndRoundTrips() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("resolution-attempt"));
        final EvidenceCursorV1 cursor = EvidenceCursorV1.kafka(Bytes.sha256(Bytes.utf8("lane")), new byte[16],
                java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("topic")), 16), 0, 1, 2_100, 7, 8);
        final byte[] body = PublishOutcomeBody.encodeEvidenceResolution(shard, 9_000, attempt, cursor,
                evidence(attempt, true), StableCode.OK, 1, 0, charge().canonicalBytes(), observedAt(),
                retryDecision(1, StableCode.OK, null));
        final PublishOutcomeBody parsed = PublishOutcomeBody.decodeEvidenceResolution(body);
        assertArrayEquals(attempt, parsed.publishAttemptId());
        assertArrayEquals(Bytes.sha256(Bytes.utf8("nereus-delay-evidence-resolution-logical-id-v1\0"),
                attempt, PublishEvidenceV1.decode(parsed.evidence()).evidenceId()),
                parsed.evidenceResolutionLogicalOperationIdentity());
        assertEquals(1, parsed.sideEffect());
        assertEquals(StableCode.OK, parsed.stableCode());
    }

    @Test
    void acceptsCompleteUnsignedRetryPolicyVersionInOutcome() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("high-bit-policy-attempt"));
        final byte[] evidence = evidence(attempt, true);
        final byte[] body = PublishOutcomeBody.encodeInitial(shard, 9_000, attempt, 1, 0, StableCode.OK,
                evidence, charge().canonicalBytes(), observedAt(),
                retryDecision(1, StableCode.OK, null, Long.MIN_VALUE));

        assertArrayEquals(retryDecision(1, StableCode.OK, null, Long.MIN_VALUE),
                PublishOutcomeBody.decode(body).retryDecision().canonicalBytes());
    }

    @Test
    void rejectsRetryDecisionUnknownFieldAndOutOfWindowNextAt() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("retry-shape-attempt"));
        final byte[] evidence = evidence(attempt, false);
        final byte[] outOfWindow = retryDecision(2, StableCode.DESTINATION_DEFINITIVE_RETRIABLE,
                6_000L, 1, 2_000, 5_000);
        assertThrows(IllegalArgumentException.class, () -> PublishOutcomeBody.encodeInitial(shard, 9_000, attempt,
                2, 1, StableCode.DESTINATION_DEFINITIVE_RETRIABLE, evidence, charge().canonicalBytes(),
                observedAt(), outOfWindow));
        final byte[] reversedWindow = retryDecision(1, StableCode.OK, null, 1, 5_000, 2_000);
        assertThrows(IllegalArgumentException.class, () -> PublishOutcomeBody.encodeInitial(shard, 9_000, attempt,
                1, 0, StableCode.OK, evidence(attempt, true), charge().canonicalBytes(), observedAt(),
                reversedWindow));

        final byte[] withUnknownField = appendUnknownRetryField(retryDecision(1, StableCode.OK, null), 10);
        final byte[] publishedEvidence = evidence(attempt, true);
        assertThrows(IllegalArgumentException.class, () -> PublishOutcomeBody.encodeInitial(shard, 9_000, attempt,
                1, 0, StableCode.OK, publishedEvidence, charge().canonicalBytes(), observedAt(),
                withUnknownField));
    }

    @Test
    void encoderRejectsWrongInitialCombinationAndNonCanonicalCharge() {
        final ShardId shard = shard();
        final byte[] attempt = Bytes.sha256(Bytes.utf8("invalid-attempt"));
        assertThrows(IllegalArgumentException.class, () -> PublishOutcomeBody.encodeInitial(shard, 9_000, attempt,
                1, 1, StableCode.OK, evidence(attempt, true), charge().canonicalBytes(), observedAt(),
                retryDecision(1, StableCode.OK, null)));

        final byte[] canonicalCharge = charge().canonicalBytes();
        // field 1's value is zero; an overlong varint is not canonical.
        final byte[] nonCanonicalCharge = new byte[canonicalCharge.length + 1];
        nonCanonicalCharge[0] = canonicalCharge[0];
        nonCanonicalCharge[1] = (byte) 0x80;
        nonCanonicalCharge[2] = 0;
        System.arraycopy(canonicalCharge, 2, nonCanonicalCharge, 3, canonicalCharge.length - 2);
        assertThrows(IllegalArgumentException.class, () -> PublishOutcomeBody.encodeInitial(shard, 9_000, attempt,
                2, 1, StableCode.DESTINATION_DEFINITIVE_RETRIABLE, evidence(attempt, false),
                nonCanonicalCharge, observedAt(), retryDecision(2,
                        StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 3_000L)));
    }

    private static PublishAdmissionBody.ChargeVector charge() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static byte[] retryDecision(final int kind, final StableCode stableCode, final Long nextRetryAt) {
        return retryDecision(kind, stableCode, nextRetryAt, 1);
    }

    private static byte[] retryDecision(final int kind, final StableCode stableCode, final Long nextRetryAt,
                                        final long policyVersion) {
        return retryDecision(kind, stableCode, nextRetryAt, policyVersion, 2_000, 5_000);
    }

    private static byte[] retryDecision(final int kind, final StableCode stableCode, final Long nextRetryAt,
                                        final long policyVersion, final long firstAttemptAt,
                                        final long retryDeadline) {
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint64Bits(output, 2, policyVersion);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, firstAttemptAt);
            CanonicalProtobuf.int64(output, 5, retryDeadline);
            if (nextRetryAt != null) {
                CanonicalProtobuf.int64(output, 6, nextRetryAt);
            }
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static byte[] appendUnknownRetryField(final byte[] encoded, final int fieldNumber) {
        return CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
            CanonicalProtobuf.uint32(output, fieldNumber, 1);
        });
    }

    private static byte[] unknownRetryPlaceholder() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("unknown")));
    }

    private static byte[] unknownTransfer() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("transfer")));
    }

    private static byte[] evidence(final byte[] attemptId, final boolean published) {
        final ExternalDeliveryIdentityV1 owner = ExternalDeliveryIdentityV1.publishAttempt(attemptId);
        final byte[] branch = published ? CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, kafkaResource());
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 2_000);
            CanonicalProtobuf.bytes(output, 6, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(Bytes.utf8("prepared")));
            CanonicalProtobuf.bytes(output, 8, Bytes.sha256(Bytes.utf8("response")));
        }) : CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nestedMarker());
            CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("prepared")));
            CanonicalProtobuf.uint32(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, Bytes.sha256(Bytes.utf8("request")));
            CanonicalProtobuf.uint32(output, 6, 1);
            CanonicalProtobuf.uint32(output, 7, StableCode.CAPABILITY_UNAVAILABLE.wireValue());
        });
        return PublishEvidenceV1.create(published ? PublishEvidenceKindV1.KAFKA_PRODUCE_ACK
                        : PublishEvidenceKindV1.ADAPTER_NON_SUBMISSION,
                published ? EvidenceVerificationStatusV1.VERIFIED_PUBLISHED
                        : EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED, branch).canonicalBytes();
    }

    private static byte[] kafkaResource() {
        return BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster-a",
                java.util.UUID.nameUUIDFromBytes(Bytes.utf8("topic")))).canonicalBytes();
    }

    private static byte[] nestedMarker() {
        return ProtocolTestFixtures.baselineKafkaChannel();
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
