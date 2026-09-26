package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetExpireGenerationVerifierTest {
    @Test
    void acceptsCanonicalSignedOwnerExpiryWithHistoricallyVerifiedTime() throws Exception {
        final var fixture = fixture(200);
        final var replayed = SystemMutation.decodeFrame(fixture.mutation().encodeFrame());
        final var decoded = TargetExpireGenerationBody.decode(replayed.canonicalBody());
        assertArrayEquals(fixture.body().canonicalBytes(), decoded.canonicalBytes());
        assertArrayEquals(fixture.body().logicalOperationIdentity(), replayed.logicalOperationIdentity());

        final var decision = TargetExpireGenerationVerifier.decideFirstApplication(
                fixture.scope(), replayed, fixture.source(), authority(fixture.key(), true));
        assertNull(decision.rejection());
        assertArrayEquals(fixture.body().canonicalBytes(), decision.body().canonicalBytes());
    }

    @Test
    void deniesUnverifiedOwnerOrTimeWithoutConvertingAuthorityFailureToRejection() throws Exception {
        final var fixture = fixture(200);
        final var otherKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetExpireGenerationVerifier.decideFirstApplication(
                                fixture.scope(), fixture.mutation(), fixture.source(), authority(otherKey, true))
                        .rejection());
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetExpireGenerationVerifier.decideFirstApplication(
                                fixture.scope(), fixture.mutation(), fixture.source(), authority(fixture.key(), false))
                        .rejection());
        final var wrongIdentity = SystemMutation.signed(
                fixture.source().shardId(),
                SystemMutationType.EXPIRE_GENERATION,
                fixture.body().retryUntil(),
                bytes(32, 8),
                fixture.body().canonicalBytes(),
                fixture.mutation().authorIdentity(),
                1,
                fixture.key().getPrivate());
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetExpireGenerationVerifier.decideFirstApplication(
                                fixture.scope(), wrongIdentity, fixture.source(), authority(fixture.key(), true))
                        .rejection());
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetExpireGenerationVerifier.decideFirstApplication(
                                fixture.scope(),
                                fixture.mutation(),
                                fixture.source(),
                                (scope, owner, mutation, source) -> new TargetExpireGenerationVerifier.Authorization(
                                        fixture.key().getPublic(), 4, (shardScope, writer, position, evidence) -> true))
                        .rejection());
        final var early = fixture(199);
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetExpireGenerationVerifier.decideFirstApplication(
                                early.scope(), early.mutation(), early.source(), authority(early.key(), true))
                        .rejection());
        final var lateSource =
                new KafkaSourcePosition(fixture.source().shardId(), "cluster", new UUID(1, 2), 1, 1, 301);
        assertEquals(
                StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                TargetExpireGenerationVerifier.decideFirstApplication(
                                fixture.scope(), fixture.mutation(), lateSource, authority(fixture.key(), true))
                        .rejection());
        assertThrows(
                IllegalStateException.class,
                () -> TargetExpireGenerationVerifier.decideFirstApplication(
                        fixture.scope(), fixture.mutation(), fixture.source(), (scope, owner, mutation, source) -> {
                            throw new IllegalStateException("historical Owner authority unavailable");
                        }));
    }

    private static TargetExpireGenerationVerifier.Authority authority(final KeyPair key, final boolean validTime) {
        return (scope, owner, mutation, source) -> new TargetExpireGenerationVerifier.Authorization(
                key.getPublic(), 10, (shardScope, writer, position, evidence) -> validTime);
    }

    private static Fixture fixture(final long earliest) throws Exception {
        final var shard = new ShardId(new RouteIncarnation(bytes(16, 1)), 1);
        final var scope = new TargetQuotaScope(shard, bytes(32, 2), null);
        final var source = new KafkaSourcePosition(shard, "cluster", new UUID(1, 2), 1, 1, 100);
        final var proof = new TrustedUtcIntervalEvidence(
                earliest,
                earliest + 5,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(32, 3),
                1,
                1,
                1,
                bytes(32, 4),
                0,
                null);
        final var body = new TargetExpireGenerationBody(shard, 300, DelayMessageId.random(shard), 1, 200, proof);
        final var key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var mutation = SystemMutation.signed(
                shard,
                SystemMutationType.EXPIRE_GENERATION,
                body.retryUntil(),
                body.logicalOperationIdentity(),
                body.canonicalBytes(),
                AuthorIdentity.owner(bytes(16, 5), bytes(16, 6), 1, bytes(32, 7))
                        .canonicalBytes(),
                1,
                key.getPrivate());
        return new Fixture(scope, source, body, mutation, key);
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private record Fixture(
            TargetQuotaScope scope,
            KafkaSourcePosition source,
            TargetExpireGenerationBody body,
            SystemMutation mutation,
            KeyPair key) {}
}
