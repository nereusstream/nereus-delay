package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DlqExportResultBodyTest {
    @Test
    void parsesAttemptOutcomeAndDerivesStableLogicalIdentity() {
        final byte[] exportId = nonZero(32, 1);
        final byte[] messageId = nonZero(41, 2);
        final byte[] envelope = nonZero(32, 3);
        final byte[] evidence = evidence(1, 1, nonZero(32, 4));
        final byte[] body = body(exportId, messageId, envelope, 1, 1, 0, StableCode.OK.wireValue(), evidence,
                retry(1, StableCode.OK.wireValue(), 2), 3, 1);

        final DlqExportResultBody decoded = DlqExportResultBody.decode(body);
        assertEquals(DlqExportStateV1.PUBLISHED, decoded.resultingState());
        assertArrayEquals(SystemMutation.computeDlqExportAttemptLogicalIdentity(exportId, 1),
                decoded.logicalOperationIdentity());
        assertArrayEquals(nonZero(32, 4), decoded.evidenceId());
    }

    @Test
    void rejectsUnknownAttemptWithDefinitiveEvidenceOrMessageRetryDomain() {
        final byte[] exportId = nonZero(32, 5);
        final byte[] messageId = nonZero(41, 6);
        final byte[] envelope = nonZero(32, 7);
        final byte[] unknown = body(exportId, messageId, envelope, 1, 3, 4,
                StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), new byte[0],
                retry(5, StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), 2), 4, 1);
        assertEquals(DlqExportStateV1.UNCERTAIN, DlqExportResultBody.decode(unknown).resultingState());

        final byte[] badEvidence = body(exportId, messageId, envelope, 1, 3, 4,
                StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), evidence(1, 1, nonZero(32, 8)),
                retry(5, StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue(), 2), 4, 1);
        assertThrows(IllegalArgumentException.class, () -> DlqExportResultBody.decode(badEvidence));

        final byte[] wrongDomain = body(exportId, messageId, envelope, 1, 1, 0, StableCode.OK.wireValue(),
                evidence(1, 1, nonZero(32, 9)), retry(1, StableCode.OK.wireValue(), 1, 1), 3, 1);
        assertThrows(IllegalArgumentException.class, () -> DlqExportResultBody.decode(wrongDomain));
    }

    private static byte[] body(final byte[] exportId, final byte[] messageId, final byte[] envelope,
                               final int eventKind, final int sideEffect, final int disposition, final int code,
                               final byte[] evidence, final byte[] retry, final int state, final int attempt) {
        final TrustedUtcIntervalEvidence observed = new TrustedUtcIntervalEvidence(1_000, 1_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, nonZero(16, 10), 1, 1, 1,
                nonZero(32, 11), 0, new byte[0]);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new byte[]{1});
            CanonicalProtobuf.bytes(output, 2, new byte[]{2});
            CanonicalProtobuf.bytes(output, 3, new byte[]{3});
            CanonicalProtobuf.bytes(output, 10, exportId);
            CanonicalProtobuf.bytes(output, 11, messageId);
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64(output, 13, 9);
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
            CanonicalProtobuf.bytes(output, 2, nestedMarker());
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.uint64(output, 4, 1_000);
            CanonicalProtobuf.uint64(output, 5, 2_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, cause);
            CanonicalProtobuf.uint32(output, 9, domain);
        });
    }

    private static byte[] evidence(final int kind, final int status, final byte[] id) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind);
            CanonicalProtobuf.uint32(output, 2, status);
            CanonicalProtobuf.bytes(output, 3, id);
            CanonicalProtobuf.bytes(output, 10, nestedMarker());
        });
    }

    private static byte[] nestedMarker() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 1));
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
