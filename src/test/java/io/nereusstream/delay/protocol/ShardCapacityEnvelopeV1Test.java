package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShardCapacityEnvelopeV1Test {
    @Test
    void roundTripsEnvelopeAndValidatesComponentProjection() {
        final PublishAdmissionBody.ChargeVector limit = new PublishAdmissionBody.ChargeVector(
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
        final QuotaGrantRefV1 logical = new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("logical-grant")), 3, limit);
        final CapacityVectorV1 committed = committed(limit, 100);
        final ShardCapacityEnvelopeV1 envelope = new ShardCapacityEnvelopeV1(
                Bytes.sha256(Bytes.utf8("envelope")), 4, logical, committed,
                grant(CapacityGrantKindV1.OUTCOME_RESERVE, 20, 0),
                grant(CapacityGrantKindV1.NON_OUTCOME_CONTROL, 0, 30),
                grant(CapacityGrantKindV1.RECOVERY_WORKING, 0, 20),
                grant(CapacityGrantKindV1.EMERGENCY_HEADROOM, 0, 10),
                Bytes.sha256(Bytes.utf8("capacity-artifact")));

        assertEquals(envelope, ShardCapacityEnvelopeV1.decode(envelope.canonicalBytes()));
        assertEquals(limit, QuotaGrantRefV1.decode(logical.canonicalBytes()).limit());
    }

    @Test
    void preservesCompleteUnsigned64BitGrantAndEnvelopeVersions() {
        final PublishAdmissionBody.ChargeVector limit = new PublishAdmissionBody.ChargeVector(
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
        final QuotaGrantRefV1 logical = new QuotaGrantRefV1(
                Bytes.sha256(Bytes.utf8("high-bit-logical-grant")), Long.MIN_VALUE, limit);
        final ShardCapacityEnvelopeV1 envelope = new ShardCapacityEnvelopeV1(
                Bytes.sha256(Bytes.utf8("high-bit-envelope")), Long.MIN_VALUE, logical,
                committed(limit, 100),
                grant(CapacityGrantKindV1.OUTCOME_RESERVE, 0, 0, Long.MIN_VALUE),
                grant(CapacityGrantKindV1.NON_OUTCOME_CONTROL, 0, 0, Long.MIN_VALUE),
                grant(CapacityGrantKindV1.RECOVERY_WORKING, 0, 0, Long.MIN_VALUE),
                grant(CapacityGrantKindV1.EMERGENCY_HEADROOM, 0, 0, Long.MIN_VALUE),
                Bytes.sha256(Bytes.utf8("high-bit-capacity-artifact")));

        final ShardCapacityEnvelopeV1 decoded = ShardCapacityEnvelopeV1.decode(envelope.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.envelopeVersion());
        assertEquals(Long.MIN_VALUE, decoded.logicalGrant().grantVersion());
        decoded.componentGrants().forEach(grant -> assertEquals(Long.MIN_VALUE, grant.reserveSourceVersion()));
        assertEquals(envelope, decoded);
    }

    @Test
    void rejectsWrongGrantKindOverflowAndLogicalProjectionDrift() {
        final PublishAdmissionBody.ChargeVector limit = new PublishAdmissionBody.ChargeVector(
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
        final QuotaGrantRefV1 logical = new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("logical-grant")), 3, limit);
        final CapacityVectorV1 committed = committed(limit, 100);
        assertThrows(IllegalArgumentException.class, () -> new ShardCapacityEnvelopeV1(
                Bytes.sha256(Bytes.utf8("envelope")), 4, logical, committed,
                grant(CapacityGrantKindV1.NON_OUTCOME_CONTROL, 0, 20),
                grant(CapacityGrantKindV1.NON_OUTCOME_CONTROL, 0, 30),
                grant(CapacityGrantKindV1.RECOVERY_WORKING, 0, 20),
                grant(CapacityGrantKindV1.EMERGENCY_HEADROOM, 0, 10),
                Bytes.sha256(Bytes.utf8("capacity-artifact"))));
        assertThrows(IllegalArgumentException.class, () -> new ShardCapacityEnvelopeV1(
                Bytes.sha256(Bytes.utf8("envelope")), 4, logical, committed,
                grant(CapacityGrantKindV1.OUTCOME_RESERVE, 0, 101),
                grant(CapacityGrantKindV1.NON_OUTCOME_CONTROL, 0, 0),
                grant(CapacityGrantKindV1.RECOVERY_WORKING, 0, 0),
                grant(CapacityGrantKindV1.EMERGENCY_HEADROOM, 0, 0),
                Bytes.sha256(Bytes.utf8("capacity-artifact"))));

        final long[] driftedAmounts = committed.amounts();
        driftedAmounts[0]++;
        assertThrows(IllegalArgumentException.class, () -> new ShardCapacityEnvelopeV1(
                Bytes.sha256(Bytes.utf8("envelope")), 4, logical, new CapacityVectorV1(driftedAmounts),
                grant(CapacityGrantKindV1.OUTCOME_RESERVE, 0, 0),
                grant(CapacityGrantKindV1.NON_OUTCOME_CONTROL, 0, 0),
                grant(CapacityGrantKindV1.RECOVERY_WORKING, 0, 0),
                grant(CapacityGrantKindV1.EMERGENCY_HEADROOM, 0, 0),
                Bytes.sha256(Bytes.utf8("capacity-artifact"))));
    }

    @Test
    void rejectsEnvelopeDigestTampering() {
        final PublishAdmissionBody.ChargeVector limit = new PublishAdmissionBody.ChargeVector(
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17);
        final QuotaGrantRefV1 logical = new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("logical-grant")), 3, limit);
        final ShardCapacityEnvelopeV1 envelope = new ShardCapacityEnvelopeV1(
                Bytes.sha256(Bytes.utf8("envelope")), 4, logical, committed(limit, 100),
                grant(CapacityGrantKindV1.OUTCOME_RESERVE, 0, 0),
                grant(CapacityGrantKindV1.NON_OUTCOME_CONTROL, 0, 0),
                grant(CapacityGrantKindV1.RECOVERY_WORKING, 0, 0),
                grant(CapacityGrantKindV1.EMERGENCY_HEADROOM, 0, 0),
                Bytes.sha256(Bytes.utf8("capacity-artifact")));
        final byte[] encoded = envelope.canonicalBytes();
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ShardCapacityEnvelopeV1.decode(encoded));
    }

    private static CapacityVectorV1 committed(final PublishAdmissionBody.ChargeVector limit,
                                              final long extraAmount) {
        final long[] amounts = limit.toCapacityVector().amounts();
        amounts[CapacityDimensionV1.CONTROL_RESERVE_BYTES.wireValue() - 1] = extraAmount;
        amounts[CapacityDimensionV1.CONTROL_RESERVE_RECORDS.wireValue() - 1] = extraAmount;
        amounts[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = extraAmount;
        return new CapacityVectorV1(amounts);
    }

    private static CapacityGrantV1 grant(final CapacityGrantKindV1 kind, final long controlBytes,
                                         final long controlRecords) {
        return grant(kind, controlBytes, controlRecords, 1);
    }

    private static CapacityGrantV1 grant(final CapacityGrantKindV1 kind, final long controlBytes,
                                         final long controlRecords, final long sourceVersion) {
        final long[] amounts = new long[CapacityDimensionV1.COUNT];
        amounts[CapacityDimensionV1.CONTROL_RESERVE_BYTES.wireValue() - 1] = controlBytes;
        amounts[CapacityDimensionV1.CONTROL_RESERVE_RECORDS.wireValue() - 1] = controlRecords;
        return new CapacityGrantV1(kind, Bytes.sha256(Bytes.utf8(kind.name() + controlBytes + controlRecords
                + sourceVersion)), sourceVersion,
                new CapacityVectorV1(amounts));
    }
}
