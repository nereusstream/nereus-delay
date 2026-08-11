package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DlqExportResultBodyTest {
    @Test
    void parsesAttemptOutcomeAndDerivesStableLogicalIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final byte[] exportId = nonZero(32, 1);
        final byte[] messageId = DelayMessageId.random(shard).bytes();
        final byte[] envelope = nonZero(32, 3);
        final byte[] evidence = evidence(exportId);
        final byte[] body = body(shard, exportId, messageId, envelope, 1, 1, 0, StableCode.OK.wireValue(), evidence,
                retry(1, StableCode.OK.wireValue(), 2), 3, 1);

        final DlqExportResultBody decoded = DlqExportResultBody.decode(body);
        assertEquals(DlqExportStateV1.PUBLISHED, decoded.resultingState());
        assertArrayEquals(SystemMutation.computeDlqExportAttemptLogicalIdentity(exportId, 1),
                decoded.logicalOperationIdentity());
        assertArrayEquals(PublishEvidenceV1.decode(evidence).evidenceId(), decoded.evidenceId());
    }

    @Test
    void acceptsCompleteUnsignedTerminalRevisionBits() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final byte[] exportId = nonZero(32, 21);
        final byte[] messageId = DelayMessageId.random(shard).bytes();
        final byte[] envelope = nonZero(32, 22);
        final byte[] body = body(shard, exportId, messageId, envelope, 1, 1, 0, StableCode.OK.wireValue(),
                evidence(exportId), retry(1, StableCode.OK.wireValue(), 2), 3, 1, Long.MIN_VALUE);

        assertEquals(Long.MIN_VALUE, DlqExportResultBody.decode(body).terminalRevision());
    }

    @Test
    void rejectsHighBitRetryDecisionAttemptNumber() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final byte[] exportId = nonZero(32, 23);
        final byte[] messageId = DelayMessageId.random(shard).bytes();
        final byte[] envelope = nonZero(32, 24);
        final byte[] retry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, retryPolicyRef());
            CanonicalProtobuf.uint64Bits(output, 3, Long.MIN_VALUE);
            CanonicalProtobuf.uint64(output, 4, 1_000);
            CanonicalProtobuf.uint64(output, 5, 2_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, StableCode.OK.wireValue());
            CanonicalProtobuf.uint32(output, 9, 2);
        });
        final byte[] body = body(shard, exportId, messageId, envelope, 1, 1, 0, StableCode.OK.wireValue(),
                evidence(exportId), retry, 3, 1);

        assertThrows(IllegalArgumentException.class, () -> DlqExportResultBody.decode(body));
    }

    @Test
    void rejectsRetryDecisionAttemptAboveUint32Range() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final byte[] exportId = nonZero(32, 31);
        final byte[] messageId = DelayMessageId.random(shard).bytes();
        final byte[] envelope = nonZero(32, 32);
        final byte[] retry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, retryPolicyRef());
            CanonicalProtobuf.uint64(output, 3, 0x1_0000_0000L);
            CanonicalProtobuf.uint64(output, 4, 1_000);
            CanonicalProtobuf.uint64(output, 5, 2_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, StableCode.OK.wireValue());
            CanonicalProtobuf.uint32(output, 9, 2);
        });
        final byte[] body = body(shard, exportId, messageId, envelope, 1, 1, 0, StableCode.OK.wireValue(),
                evidence(exportId), retry, 3, 1);

        assertThrows(IllegalArgumentException.class, () -> DlqExportResultBody.decode(body));
    }

    @Test
    void rejectsNonCanonicalRetryPolicyReferenceInsideDecision() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final byte[] exportId = nonZero(32, 25);
        final byte[] messageId = DelayMessageId.random(shard).bytes();
        final byte[] envelope = nonZero(32, 26);
        final byte[] malformedRetry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, CanonicalProtobuf.message(policy -> {
                CanonicalProtobuf.bytes(policy, 1, Bytes.utf8("policy"));
                CanonicalProtobuf.uint32(policy, 2, 1);
                CanonicalProtobuf.bytes(policy, 3, nonZero(31, 27));
            }));
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.uint64(output, 4, 1_000);
            CanonicalProtobuf.uint64(output, 5, 2_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, StableCode.OK.wireValue());
            CanonicalProtobuf.uint32(output, 9, 2);
        });
        final byte[] body = body(shard, exportId, messageId, envelope, 1, 1, 0, StableCode.OK.wireValue(),
                evidence(exportId), malformedRetry, 3, 1);

        assertThrows(IllegalArgumentException.class, () -> DlqExportResultBody.decode(body));
    }

    @Test
    void rejectsRetryNextAtOutsideTheFirstAttemptAndDeadlineInterval() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final byte[] exportId = nonZero(32, 29);
        final byte[] messageId = DelayMessageId.random(shard).bytes();
        final byte[] envelope = nonZero(32, 30);
        final byte[] retryAfterDeadline = retryWithNext(2,
                StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), 2, 2_001);
        final byte[] body = body(shard, exportId, messageId, envelope, 1, 3, 4,
                StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), new byte[0], retryAfterDeadline, 2, 1);

        assertThrows(IllegalArgumentException.class, () -> DlqExportResultBody.decode(body));
    }

    @Test
    void rejectsUnknownAttemptWithDefinitiveEvidenceOrMessageRetryDomain() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final byte[] exportId = nonZero(32, 5);
        final byte[] messageId = DelayMessageId.random(shard).bytes();
        final byte[] envelope = nonZero(32, 7);
        final byte[] unknown = body(shard, exportId, messageId, envelope, 1, 3, 4,
                StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), new byte[0],
                retry(5, StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), 2), 4, 1);
        assertEquals(DlqExportStateV1.UNCERTAIN, DlqExportResultBody.decode(unknown).resultingState());

        final byte[] badEvidence = body(shard, exportId, messageId, envelope, 1, 3, 4,
                StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), evidence(exportId),
                retry(5, StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), 2), 4, 1);
        assertThrows(IllegalArgumentException.class, () -> DlqExportResultBody.decode(badEvidence));

        final byte[] wrongDomain = body(shard, exportId, messageId, envelope, 1, 1, 0, StableCode.OK.wireValue(),
                evidence(exportId), retry(1, StableCode.OK.wireValue(), 1, 1), 3, 1);
        assertThrows(IllegalArgumentException.class, () -> DlqExportResultBody.decode(wrongDomain));
    }

    private static byte[] body(final ShardId shard, final byte[] exportId, final byte[] messageId, final byte[] envelope,
                               final int eventKind, final int sideEffect, final int disposition, final int code,
                               final byte[] evidence, final byte[] retry, final int state, final int attempt) {
        return body(shard, exportId, messageId, envelope, eventKind, sideEffect, disposition, code, evidence, retry,
                state, attempt, 9);
    }

    private static byte[] body(final ShardId shard, final byte[] exportId, final byte[] messageId,
                               final byte[] envelope, final int eventKind, final int sideEffect,
                               final int disposition, final int code, final byte[] evidence, final byte[] retry,
                               final int state, final int attempt, final long terminalRevision) {
        final TrustedUtcIntervalEvidence observed = new TrustedUtcIntervalEvidence(1_000, 1_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, nonZero(16, 10), 1, 1, 1,
                nonZero(32, 11), 0, new byte[0]);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shard).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.DLQ_EXPORT_RESULT.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, exportId);
            CanonicalProtobuf.bytes(output, 11, messageId);
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64Bits(output, 13, terminalRevision);
            CanonicalProtobuf.bytes(output, 14, envelope);
            CanonicalProtobuf.uint32(output, 15, eventKind);
            CanonicalProtobuf.uint32(output, 16, sideEffect);
            CanonicalProtobuf.uint32(output, 17, disposition);
            CanonicalProtobuf.uint32(output, 18, code);
            if (evidence.length > 0) {
                CanonicalProtobuf.bytes(output, 19, evidence);
            }
            CanonicalProtobuf.bytes(output, 20, chargeVector());
            CanonicalProtobuf.bytes(output, 21, observed.canonicalBytes());
            CanonicalProtobuf.bytes(output, 22, retry);
            CanonicalProtobuf.uint32(output, 23, state);
            CanonicalProtobuf.uint32(output, 24, attempt);
        });
    }

    private static byte[] retry(final int kind, final int cause, final int domain) {
        return retry(kind, cause, domain, -1);
    }

    private static byte[] retry(final int kind, final int cause, final int domain, final int ignored) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind);
            CanonicalProtobuf.bytes(output, 2, retryPolicyRef());
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.uint64(output, 4, 1_000);
            CanonicalProtobuf.uint64(output, 5, 2_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, cause);
            CanonicalProtobuf.uint32(output, 9, domain);
        });
    }

    private static byte[] retryWithNext(final int kind, final int cause, final int domain,
                                        final long nextRetryAt) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind);
            CanonicalProtobuf.bytes(output, 2, retryPolicyRef());
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.uint64(output, 4, 1_000);
            CanonicalProtobuf.uint64(output, 5, 2_000);
            CanonicalProtobuf.uint64(output, 6, nextRetryAt);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, cause);
            CanonicalProtobuf.uint32(output, 9, domain);
        });
    }

    private static byte[] evidence(final byte[] exportId) {
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, kafkaResource());
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 1_001);
            CanonicalProtobuf.bytes(output, 6, ExternalDeliveryIdentityV1.dlqExport(exportId)
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, nonZero(32, 12));
            CanonicalProtobuf.bytes(output, 8, nonZero(32, 13));
        });
        return PublishEvidenceV1.create(PublishEvidenceKindV1.KAFKA_PRODUCE_ACK,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch).canonicalBytes();
    }

    private static byte[] kafkaResource() {
        return BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster-a",
                java.util.UUID.nameUUIDFromBytes(Bytes.utf8("topic")))).canonicalBytes();
    }

    private static byte[] retryPolicyRef() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint64Bits(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, nonZero(32, 28));
        });
    }

    private static byte[] chargeVector() {
        return CanonicalProtobuf.message(output -> {
            for (int field = 1; field <= 17; field++) {
                CanonicalProtobuf.uint64(output, field, 0);
            }
        });
    }

    private static byte[] nonZero(final int length, final int seed) {
        final byte[] value = new byte[length];
        Arrays.fill(value, (byte) seed);
        return value;
    }
}
