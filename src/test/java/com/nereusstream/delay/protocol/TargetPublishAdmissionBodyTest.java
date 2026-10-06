package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.AttemptObligationRef;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetPublishAdmissionBodyTest {
    private static final ShardId SHARD = new ShardId(new RouteIncarnation(repeated(16, 0x21)), 3);
    private static final OwnerIdentity OWNER = new OwnerIdentity(
            Bytes.utf8("target-deployment"), Bytes.utf8("worker-run"), 7, repeated(32, 0x22));

    @Test
    void targetAdmissionHasItsOwnCanonicalBodyAndActivatedTuple() throws Exception {
        final var body = body();
        final byte[] encoded = body.canonicalBytes();
        SystemMutationBodyCodec.validate(SystemMutationType.TARGET_PUBLISH_ADMISSION, encoded);
        final var decoded = TargetPublishAdmissionBody.decode(encoded);
        assertArrayEquals(encoded, decoded.canonicalBytes());
        assertArrayEquals(body.publishAttemptId(), decoded.publishAttemptId());
        assertArrayEquals(body.commitment().canonicalBytes(), decoded.commitment().canonicalBytes());
        assertEquals(3, ProtocolTuple.targetPublishAdmission().bodyVersion());
        assertNotEquals(ProtocolTuple.currentSystemMutation(), ProtocolTuple.targetPublishAdmission());

        final var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var author = AuthorIdentity.owner(
                OWNER.deploymentId(), OWNER.workerRunId(), OWNER.ownerEpoch(), OWNER.leaseFencingDigest());
        final var mutation = SystemMutation.signed(
                SHARD,
                SystemMutationType.TARGET_PUBLISH_ADMISSION,
                body.retryUntilEpochMs(),
                body.publishAttemptId(),
                encoded,
                author.canonicalBytes(),
                1,
                key.getPrivate());
        assertEquals(
                SystemMutationType.TARGET_PUBLISH_ADMISSION,
                SystemMutation.decodeFrame(mutation.encodeFrame()).type());
        assertEquals(1, SystemMutation.BODY_VERSION);
        assertEquals(3, ProtocolTuple.targetPublishAdmission().bodyVersion());
    }

    @Test
    void targetAdmissionRejectsAnotherClaimIdentityOrOwnerEpochKey() {
        final var body = body();
        final byte[] wrongClaim = repeated(32, 0x33);
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetPublishAdmissionBody(
                        SHARD,
                        body.retryUntilEpochMs(),
                        OWNER,
                        body.storeIncarnation(),
                        wrongClaim,
                        body.locator(),
                        body.attemptNo(),
                        body.publishAttemptId(),
                        body.obligation(),
                        body.executionBytes(),
                        body.commitment(),
                        body.allocated(),
                        body.decisionTime()));

        final var foreignOwner = new OwnerIdentity(
                OWNER.deploymentId(), OWNER.workerRunId(), OWNER.ownerEpoch() + 1, OWNER.leaseFencingDigest());
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetPublishAdmissionBody(
                        SHARD,
                        body.retryUntilEpochMs(),
                        foreignOwner,
                        body.storeIncarnation(),
                        body.claimId(),
                        body.locator(),
                        body.attemptNo(),
                        body.publishAttemptId(),
                        body.obligation(),
                        body.executionBytes(),
                        body.commitment(),
                        body.allocated(),
                        body.decisionTime()));
    }

    @Test
    void targetAdmissionRejectsNonOutcomeQuotaDimensions() {
        final var body = body();
        final long[] invalid = body.commitment().amounts();
        invalid[CapacityDimension.ACTIVE_MESSAGES.wireValue() - 1] = 1;
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetPublishAdmissionBody(
                        SHARD,
                        body.retryUntilEpochMs(),
                        OWNER,
                        body.storeIncarnation(),
                        body.claimId(),
                        body.locator(),
                        body.attemptNo(),
                        body.publishAttemptId(),
                        body.obligation(),
                        body.executionBytes(),
                        new CapacityVector(invalid),
                        body.allocated(),
                        body.decisionTime()));
    }

    @Test
    void targetAdmissionRequiresItsExactActivatedTupleAndHistoricalWriterKey() throws Exception {
        final var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var body = body();
        final var author = AuthorIdentity.owner(
                OWNER.deploymentId(), OWNER.workerRunId(), OWNER.ownerEpoch(), OWNER.leaseFencingDigest());
        final var mutation = SystemMutation.signed(
                SHARD,
                SystemMutationType.TARGET_PUBLISH_ADMISSION,
                body.retryUntilEpochMs(),
                body.publishAttemptId(),
                body.canonicalBytes(),
                author.canonicalBytes(),
                1,
                key.getPrivate());
        final var source = new KafkaSourcePosition(
                SHARD, "source-cluster", UUID.fromString("00000000-0000-0000-0000-000000000011"), 9, 2, 110);
        final var scope = new TargetQuotaScope(SHARD, repeated(32, 0x31), null);
        final TargetPublishAdmissionVerifier.Authority exact = (actualScope, writer, entry, position) -> {
            assertEquals(scope, actualScope);
            assertEquals(author, writer);
            assertEquals(mutation, entry);
            assertEquals(source, position);
            return new TargetPublishAdmissionVerifier.Authorization(
                    key.getPublic(),
                    ProtocolTuple.targetPublishAdmission(),
                    10,
                    10,
                    100,
                    (a, b, c, evidence) -> true);
        };
        final var accepted = TargetPublishAdmissionVerifier.decideFirstApplication(scope, mutation, source, exact);
        assertNull(accepted.rejection());
        assertArrayEquals(body.publishAttemptId(), accepted.body().publishAttemptId());

        final var legacyTuple = TargetPublishAdmissionVerifier.decideFirstApplication(
                scope,
                mutation,
                source,
                (actualScope, writer, entry, position) -> new TargetPublishAdmissionVerifier.Authorization(
                        key.getPublic(),
                        ProtocolTuple.currentSystemMutation(),
                        10,
                        10,
                        100,
                        (a, b, c, evidence) -> true));
        assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, legacyTuple.rejection());
    }

    private static TargetPublishAdmissionBody body() {
        final byte[] claimId = repeated(32, 0x23);
        final var messageId = DelayMessageId.random(SHARD);
        final byte[] attemptId = SystemMutation.computePublishAttemptLogicalIdentity(claimId, messageId, 4, 1);
        final var obligation = new AttemptObligationRef(
                attemptId,
                4,
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, OWNER.ownerEpoch(), attemptId));
        final var locator = new TargetMessageLocator(
                messageId,
                4,
                new TargetPartitionId(repeated(32, 0x24)),
                new TargetKeyCodec.Domain(1, 9),
                repeated(16, 0x25),
                OrderingMode.BEST_EFFORT,
                null,
                repeated(32, 0x26));
        final long[] reserved = new long[CapacityDimension.COUNT];
        reserved[CapacityDimension.RESULT_BYTES.wireValue() - 1] = 256;
        final var time = new TrustedUtcIntervalEvidence(
                100,
                105,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("host-clock"),
                1,
                1,
                1,
                repeated(32, 0x27),
                0,
                null);
        return new TargetPublishAdmissionBody(
                SHARD,
                10_000,
                OWNER,
                repeated(16, 0x28),
                claimId,
                locator,
                1,
                attemptId,
                obligation,
                32,
                new CapacityVector(reserved),
                CapacityVector.empty(),
                time);
    }

    private static byte[] repeated(final int count, final int value) {
        final byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
