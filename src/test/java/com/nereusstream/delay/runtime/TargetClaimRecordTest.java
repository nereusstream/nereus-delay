package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Minimal development check of reversible work identity; live Store/Owner/Broker validation is separate. */
class TargetClaimRecordTest {
    @Test
    void nativeClaimRoundTripRestoresSemanticWorkWithNewInstanceAndUnspentAdmissions() throws Exception {
        final var message = TargetMessageRecord.decode(raw("target-identity", "message.initial"));
        final var template = TargetQuotaClaimCharge.decode(raw("target-quota-claim", "native"));
        final var work = message.runtime().timeline();
        final var head = new TargetHeadRef(
                work.nativeKey(), work.locator().messageId(), work.locator().generation(), work.deliverAtEpochMs());
        final var claim = new TargetClaimRecord(
                message.runtime(),
                message.stateVersion(),
                message.digest(),
                head,
                template.owner(),
                template.storeIncarnation(),
                3,
                99,
                128,
                1,
                template.creation());
        final var decoded = TargetClaimRecord.decode(claim.canonicalBytes());
        assertArrayEquals(claim.key(), decoded.key());
        final var charge = new TargetQuotaClaimCharge(
                claim.claimId(),
                work,
                template.primaryIdentity(),
                template.tenantScope(),
                template.accounting(),
                claim.owner(),
                claim.storeIncarnation(),
                claim.sequence(),
                claim.deadlineEpochMs(),
                claim.executionBytes(),
                Bytes.sha256(claim.canonicalBytes()),
                claim.creation(),
                template.recoveryLineage());
        decoded.requireCharge(charge);
        final var claimed = decoded.claimed(message);
        assertEquals(CurrentSendWorkKind.CLAIMED, claimed.runtime().currentWorkKind());
        assertArrayEquals(claim.claimId(), claimed.runtime().claimId());
        decoded.requireCurrent(claimed);
        assertThrows(IllegalStateException.class, () -> decoded.requireCurrent(message));
        final var restored = decoded.revoked(claimed);
        assertEquals(CurrentSendWorkKind.TIMELINE, restored.runtime().currentWorkKind());
        assertArrayEquals(work.ordinaryKey(), restored.runtime().timeline().ordinaryKey());
        assertArrayEquals(work.nativeKey(), restored.runtime().timeline().nativeKey());
        assertArrayEquals(
                work.semanticWorkDigest(), restored.runtime().timeline().semanticWorkDigest());
        assertFalse(Arrays.equals(
                work.workInstanceDigest(), restored.runtime().timeline().workInstanceDigest()));
        assertEquals(message.runtime().runtimeRevision() + 2, restored.runtime().runtimeRevision());
        assertEquals(message.runtime().admissionsUsed(), restored.runtime().admissionsUsed());
        assertEquals(
                message.runtime().uncertainRetryAdmissionsUsed(),
                restored.runtime().uncertainRetryAdmissionsUsed());
        assertEquals(message.runtime().attemptObligations(), restored.runtime().attemptObligations());
        assertThrows(IllegalStateException.class, () -> decoded.revoked(restored));
    }

    private static byte[] raw(final String resource, final String key) throws Exception {
        final var values = new Properties();
        try (var stream =
                TargetClaimRecordTest.class.getResourceAsStream("/ndip3/" + resource + "-vectors.properties")) {
            values.load(stream);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
