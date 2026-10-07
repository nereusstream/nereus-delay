package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetPublishOutcomeVerifierTest {
    @Test
    void historicalWriterSurvivesOwnerReplacementButStillRequiresItsKeyTupleAndTimeProof() throws Exception {
        final var shard = new ShardId(new RouteIncarnation(repeated(16, 1)), 0);
        final var source = new KafkaSourcePosition(shard, "cluster", new UUID(1, 2), 4, null, 100);
        final var scope = new TargetQuotaScope(shard, repeated(32, 2), null);
        final var writer = new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("old-run"), 7, repeated(32, 3));
        final var active = new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("new-run"), 8, repeated(32, 4));
        final var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var wrongKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var observation = new TrustedUtcIntervalEvidence(
                100, 105, TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("host-clock"), 1, 1, 1, repeated(32, 5), 0, null);
        final byte[] placeholder = CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("unknown")));
        final byte[] attempt = repeated(32, 6);
        final byte[] body = PublishOutcomeBody.encodeInitial(
                shard, 10_000, attempt, 3, 4, StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                null, placeholder, observation, placeholder);
        final var mutation = SystemMutation.signed(
                shard, SystemMutationType.PUBLISH_OUTCOME, 10_000, attempt, body,
                AuthorIdentity.owner(
                                writer.deploymentId(), writer.workerRunId(),
                                writer.ownerEpoch(), writer.leaseFencingDigest())
                        .canonicalBytes(),
                1, key.getPrivate());
        final var accepted = TargetPublishOutcomeVerifier.decideFirstApplication(
                scope, mutation, source,
                (a, b, c, d) -> authorization(key.getPublic(), ProtocolTuple.currentSystemMutation(), active, true));
        assertNull(accepted.rejection());
        assertArrayEquals(attempt, accepted.body().publishAttemptId());
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetPublishOutcomeVerifier.decideFirstApplication(
                                scope, mutation, source,
                                (a, b, c, d) -> authorization(
                                        wrongKey.getPublic(), ProtocolTuple.currentSystemMutation(), active, true))
                        .rejection());
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetPublishOutcomeVerifier.decideFirstApplication(
                                scope, mutation, source,
                                (a, b, c, d) -> authorization(
                                        key.getPublic(), ProtocolTuple.targetPublishAdmission(), active, true))
                        .rejection());
        assertEquals(
                StableCode.STALE_SYSTEM_MUTATION,
                TargetPublishOutcomeVerifier.decideFirstApplication(
                                scope, mutation, source,
                                (a, b, c, d) -> authorization(
                                        key.getPublic(), ProtocolTuple.currentSystemMutation(), active, false))
                        .rejection());
        assertThrows(IllegalStateException.class, () -> TargetPublishOutcomeVerifier.decideFirstApplication(
                scope, mutation, source, (a, b, c, d) -> {
                    throw new IllegalStateException("source authority unavailable");
                }));
    }

    private static TargetPublishOutcomeVerifier.Authorization authorization(
            PublicKey key, ProtocolTuple tuple, OwnerIdentity active, boolean trustedTime) {
        return new TargetPublishOutcomeVerifier.Authorization(
                key, tuple, active, 10, 10, 100, (a, b, c, d) -> trustedTime);
    }

    private static byte[] repeated(int length, int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
