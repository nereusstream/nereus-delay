package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.store.KeyCodec;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
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

    @Test
    void admissionConsumesExactClaimIntoOnePublishingObligation() throws Exception {
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
        final var claimed = claim.claimed(message);
        final byte[] attemptId = repeated(0x41);
        final var obligation = new AttemptObligationRef(
                attemptId,
                work.locator().generation(),
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, claim.owner().ownerEpoch(), attemptId));

        final var admitted = claim.admitted(claimed, obligation);

        assertEquals(TargetQueueState.nextRevision(claimed.stateVersion()), admitted.stateVersion());
        assertEquals(
                TargetQueueState.nextRevision(claimed.runtime().runtimeRevision()),
                admitted.runtime().runtimeRevision());
        assertEquals(CurrentSendWorkKind.PUBLISHING, admitted.runtime().currentWorkKind());
        assertEquals(GenerationAggregateState.PUBLISHING, admitted.runtime().aggregateState());
        assertArrayEquals(attemptId, admitted.runtime().publishAttemptId());
        assertEquals(List.of(obligation), admitted.runtime().attemptObligations());
        assertEquals(1, admitted.runtime().admissionsUsed());
        assertEquals(0, admitted.runtime().uncertainRetryAdmissionsUsed());
        assertFalse(admitted.runtime().possibleDestinationDuplicate());
        assertThrows(IllegalStateException.class, () -> claim.admitted(message, obligation));
        assertThrows(IllegalStateException.class, () -> claim.admitted(admitted, obligation));
    }

    @Test
    void admissionRejectsObligationsFromAnotherOwnerEpochOrGeneration() throws Exception {
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
        final var claimed = claim.claimed(message);
        final byte[] attemptId = repeated(0x42);
        final var wrongOwner = new AttemptObligationRef(
                attemptId,
                work.locator().generation(),
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, claim.owner().ownerEpoch() + 1, attemptId));
        final var wrongGeneration = new AttemptObligationRef(
                attemptId,
                work.locator().generation() + 1,
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, claim.owner().ownerEpoch(), attemptId));

        assertThrows(IllegalArgumentException.class, () -> claim.admitted(claimed, wrongOwner));
        assertThrows(IllegalArgumentException.class, () -> claim.admitted(claimed, wrongGeneration));
    }

    @Test
    void uncertainRetryAdmissionPreservesOldObligationAndMarksPossibleDuplicate() throws Exception {
        final var initial = TargetMessageRecord.decode(raw("target-identity", "message.initial"));
        final var template = TargetQuotaClaimCharge.decode(raw("target-quota-claim", "native"));
        final var initialWork = initial.runtime().timeline();
        final long workRevision = TargetQueueState.nextRevision(initialWork.runtimeRevision());
        final var retryWork = new TargetTimelineWorkRef(
                initialWork.locator(),
                TimelineWorkKind.UNCERTAIN_RETRY,
                initialWork.deliverAtEpochMs(),
                initialWork.retryEligibilityAtEpochMs(),
                initialWork.sourceOrderToken(),
                2,
                workRevision,
                UncertainRetryAuthority.PINNED_POLICY,
                null,
                null,
                false);
        final byte[] oldAttemptId = repeated(0x77);
        final var oldObligation = new AttemptObligationRef(
                oldAttemptId,
                initialWork.locator().generation(),
                AttemptLedgerState.UNCERTAIN,
                KeyCodec.inflight((byte) 3, 7, oldAttemptId));
        final var retryRuntime = new TargetGenerationRuntimeIndex(
                initial.runtime().generation(),
                GenerationAggregateState.UNCERTAIN,
                CurrentSendWorkKind.TIMELINE,
                retryWork,
                null,
                null,
                List.of(oldObligation),
                1,
                0,
                true,
                workRevision);
        final var retryMessage = new TargetMessageRecord(
                initial.locator(),
                TargetQueueState.nextRevision(initial.stateVersion()),
                initial.deliverAtEpochMs(),
                initial.expireAtEpochMs(),
                initial.retryEligibilityAtEpochMs(),
                initial.nativeDeliveryPolicy(),
                initial.scheduleSource(),
                initial.inlinePayload(),
                null,
                retryRuntime);
        final var head = new TargetHeadRef(
                retryWork.ordinaryKey(),
                retryWork.locator().messageId(),
                retryWork.locator().generation(),
                retryWork.ordinaryEligibilityAtEpochMs());
        final var claim = new TargetClaimRecord(
                retryRuntime,
                retryMessage.stateVersion(),
                retryMessage.digest(),
                head,
                template.owner(),
                template.storeIncarnation(),
                3,
                99,
                128,
                1,
                template.creation());
        final var claimed = claim.claimed(retryMessage);
        final byte[] retryAttemptId = repeated(0x22);
        final var publishingAttempt = new AttemptObligationRef(
                retryAttemptId,
                retryRuntime.generation(),
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, claim.owner().ownerEpoch(), retryAttemptId));

        final var admitted = claim.admitted(claimed, publishingAttempt);

        assertEquals(GenerationAggregateState.UNCERTAIN, admitted.runtime().aggregateState());
        assertEquals(2, admitted.runtime().admissionsUsed());
        assertEquals(1, admitted.runtime().uncertainRetryAdmissionsUsed());
        assertEquals(2, admitted.runtime().attemptObligations().size());
        assertArrayEquals(retryAttemptId, admitted.runtime().attemptObligations().get(0).publishAttemptId());
        assertArrayEquals(oldAttemptId, admitted.runtime().attemptObligations().get(1).publishAttemptId());
        assertTrue(admitted.runtime().possibleDestinationDuplicate());
    }

    private static byte[] repeated(final int value) {
        final byte[] bytes = new byte[32];
        Arrays.fill(bytes, (byte) value);
        return bytes;
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
