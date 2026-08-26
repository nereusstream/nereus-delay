package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ShardCapacityEnvelopeTest {
    @Test
    void roundTripsEnvelopeAndValidatesComponentProjection() {
        final PublishAdmissionBody.ChargeVector limit =
                new PublishAdmissionBody.ChargeVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
        final QuotaGrantRef logical = new QuotaGrantRef(Bytes.sha256(Bytes.utf8("logical-grant")), 3, limit);
        final CapacityVector committed = committed(limit, 100);
        final ShardCapacityEnvelope envelope = new ShardCapacityEnvelope(
                Bytes.sha256(Bytes.utf8("envelope")),
                4,
                logical,
                committed,
                grant(CapacityGrantKind.OUTCOME_RESERVE, 20, 0),
                grant(CapacityGrantKind.NON_OUTCOME_CONTROL, 0, 30),
                grant(CapacityGrantKind.RECOVERY_WORKING, 0, 20),
                grant(CapacityGrantKind.EMERGENCY_HEADROOM, 0, 10),
                Bytes.sha256(Bytes.utf8("capacity-artifact")));

        assertEquals(envelope, ShardCapacityEnvelope.decode(envelope.canonicalBytes()));
        assertEquals(limit, QuotaGrantRef.decode(logical.canonicalBytes()).limit());
    }

    @Test
    void preservesCompleteUnsigned64BitGrantAndEnvelopeVersions() {
        final PublishAdmissionBody.ChargeVector limit =
                new PublishAdmissionBody.ChargeVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
        final QuotaGrantRef logical =
                new QuotaGrantRef(Bytes.sha256(Bytes.utf8("high-bit-logical-grant")), Long.MIN_VALUE, limit);
        final ShardCapacityEnvelope envelope = new ShardCapacityEnvelope(
                Bytes.sha256(Bytes.utf8("high-bit-envelope")),
                Long.MIN_VALUE,
                logical,
                committed(limit, 100),
                grant(CapacityGrantKind.OUTCOME_RESERVE, 0, 0, Long.MIN_VALUE),
                grant(CapacityGrantKind.NON_OUTCOME_CONTROL, 0, 0, Long.MIN_VALUE),
                grant(CapacityGrantKind.RECOVERY_WORKING, 0, 0, Long.MIN_VALUE),
                grant(CapacityGrantKind.EMERGENCY_HEADROOM, 0, 0, Long.MIN_VALUE),
                Bytes.sha256(Bytes.utf8("high-bit-capacity-artifact")));

        final ShardCapacityEnvelope decoded = ShardCapacityEnvelope.decode(envelope.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.envelopeVersion());
        assertEquals(Long.MIN_VALUE, decoded.logicalGrant().grantVersion());
        decoded.componentGrants().forEach(grant -> assertEquals(Long.MIN_VALUE, grant.reserveSourceVersion()));
        assertEquals(envelope, decoded);
    }

    @Test
    void rejectsWrongGrantKindOverflowAndLogicalProjectionDrift() {
        final PublishAdmissionBody.ChargeVector limit =
                new PublishAdmissionBody.ChargeVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
        final QuotaGrantRef logical = new QuotaGrantRef(Bytes.sha256(Bytes.utf8("logical-grant")), 3, limit);
        final CapacityVector committed = committed(limit, 100);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ShardCapacityEnvelope(
                        Bytes.sha256(Bytes.utf8("envelope")),
                        4,
                        logical,
                        committed,
                        grant(CapacityGrantKind.NON_OUTCOME_CONTROL, 0, 20),
                        grant(CapacityGrantKind.NON_OUTCOME_CONTROL, 0, 30),
                        grant(CapacityGrantKind.RECOVERY_WORKING, 0, 20),
                        grant(CapacityGrantKind.EMERGENCY_HEADROOM, 0, 10),
                        Bytes.sha256(Bytes.utf8("capacity-artifact"))));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ShardCapacityEnvelope(
                        Bytes.sha256(Bytes.utf8("envelope")),
                        4,
                        logical,
                        committed,
                        grant(CapacityGrantKind.OUTCOME_RESERVE, 0, 101),
                        grant(CapacityGrantKind.NON_OUTCOME_CONTROL, 0, 0),
                        grant(CapacityGrantKind.RECOVERY_WORKING, 0, 0),
                        grant(CapacityGrantKind.EMERGENCY_HEADROOM, 0, 0),
                        Bytes.sha256(Bytes.utf8("capacity-artifact"))));

        final long[] driftedAmounts = committed.amounts();
        driftedAmounts[0]++;
        assertThrows(
                IllegalArgumentException.class,
                () -> new ShardCapacityEnvelope(
                        Bytes.sha256(Bytes.utf8("envelope")),
                        4,
                        logical,
                        new CapacityVector(driftedAmounts),
                        grant(CapacityGrantKind.OUTCOME_RESERVE, 0, 0),
                        grant(CapacityGrantKind.NON_OUTCOME_CONTROL, 0, 0),
                        grant(CapacityGrantKind.RECOVERY_WORKING, 0, 0),
                        grant(CapacityGrantKind.EMERGENCY_HEADROOM, 0, 0),
                        Bytes.sha256(Bytes.utf8("capacity-artifact"))));
    }

    @Test
    void rejectsEnvelopeDigestTampering() {
        final PublishAdmissionBody.ChargeVector limit =
                new PublishAdmissionBody.ChargeVector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
        final QuotaGrantRef logical = new QuotaGrantRef(Bytes.sha256(Bytes.utf8("logical-grant")), 3, limit);
        final ShardCapacityEnvelope envelope = new ShardCapacityEnvelope(
                Bytes.sha256(Bytes.utf8("envelope")),
                4,
                logical,
                committed(limit, 100),
                grant(CapacityGrantKind.OUTCOME_RESERVE, 0, 0),
                grant(CapacityGrantKind.NON_OUTCOME_CONTROL, 0, 0),
                grant(CapacityGrantKind.RECOVERY_WORKING, 0, 0),
                grant(CapacityGrantKind.EMERGENCY_HEADROOM, 0, 0),
                Bytes.sha256(Bytes.utf8("capacity-artifact")));
        final byte[] encoded = envelope.canonicalBytes();
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ShardCapacityEnvelope.decode(encoded));
    }

    private static CapacityVector committed(final PublishAdmissionBody.ChargeVector limit, final long extraAmount) {
        final long[] amounts = limit.toCapacityVector().amounts();
        amounts[CapacityDimension.CONTROL_RESERVE_BYTES.wireValue() - 1] = extraAmount;
        amounts[CapacityDimension.CONTROL_RESERVE_RECORDS.wireValue() - 1] = extraAmount;
        amounts[CapacityDimension.DB_INSTANCES.wireValue() - 1] = extraAmount;
        return new CapacityVector(amounts);
    }

    private static CapacityGrant grant(
            final CapacityGrantKind kind, final long controlBytes, final long controlRecords) {
        return grant(kind, controlBytes, controlRecords, 1);
    }

    private static CapacityGrant grant(
            final CapacityGrantKind kind,
            final long controlBytes,
            final long controlRecords,
            final long sourceVersion) {
        final long[] amounts = new long[CapacityDimension.COUNT];
        amounts[CapacityDimension.CONTROL_RESERVE_BYTES.wireValue() - 1] = controlBytes;
        amounts[CapacityDimension.CONTROL_RESERVE_RECORDS.wireValue() - 1] = controlRecords;
        return new CapacityGrant(
                kind,
                Bytes.sha256(Bytes.utf8(kind.name() + controlBytes + controlRecords + sourceVersion)),
                sourceVersion,
                new CapacityVector(amounts));
    }
}
