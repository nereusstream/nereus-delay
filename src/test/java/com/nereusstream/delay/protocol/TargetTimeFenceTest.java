package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.runtime.TargetTimeFenceVerifier;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TargetTimeFenceTest {
    private static final ShardId SHARD = new ShardId(new RouteIncarnation(repeated(16, 17)), -1);
    private static final TargetQuotaScope SCOPE = new TargetQuotaScope(SHARD, repeated(32, 23), null);
    private static final AuthorIdentity AUTHOR = AuthorIdentity.fence(repeated(256, 31), -1L);

    @ParameterizedTest
    @EnumSource(TrustedUtcIntervalEvidence.Source.class)
    void registeredBodyAndRealSignatureRequireExactHistoricalSourceAndAuthenticatedEvidence(
            TrustedUtcIntervalEvidence.Source type) throws Exception {
        final var key = keys();
        final var proof = proof(type, 120, 125, 256);
        final var body = new TargetTimeFenceBody(SHARD, 500, 100, -1, proof);
        final byte[] expectedId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof\0"),
                repeated(16, 17),
                new byte[] {-1, -1, -1, -1},
                Bytes.i64be(100),
                new byte[] {-1, -1, -1, -1},
                Bytes.lp32(proof.canonicalBytes()));
        assertArrayEquals(expectedId, body.proofId());
        final byte[] registered = registeredBody(body, proof.canonicalBytes(), expectedId);
        SystemMutationBodyCodec.validate(SystemMutationType.TIME_FENCE, registered);
        assertArrayEquals(registered, body.canonicalBytes());
        assertArrayEquals(registered, TargetTimeFenceBody.decode(registered).canonicalBytes());
        body.proofId()[0] = 0;
        assertArrayEquals(expectedId, body.proofId());
        final var mutation = signed(body, key, -1, body.proofId(), AUTHOR);
        final var source = source(500);
        final var calls = new AtomicInteger();
        final var decision = TargetTimeFenceVerifier.decideFirstApplication(SCOPE, mutation, source, (s, a, m, p) -> {
            assertEquals(SCOPE, s);
            assertArrayEquals(AUTHOR.canonicalBytes(), a.canonicalBytes());
            assertSame(mutation, m);
            assertSame(source, p);
            calls.incrementAndGet();
            return new TargetTimeFenceVerifier.Authorization(
                    key.getPublic(), 20, 5, (scope, author, position, sample) -> {
                        assertSame(s, scope);
                        assertSame(a, author);
                        assertSame(p, position);
                        assertEquals(proof, sample);
                        calls.incrementAndGet();
                        return true;
                    });
        });
        assertNull(decision.rejection());
        assertNotNull(decision.body());
        assertArrayEquals(registered, decision.body().canonicalBytes());
        assertEquals(2, calls.get());
    }

    @Test
    void rejectsOversizedAndMalformedNestedInputsBeforeGeneralDecode() {
        final var body = body(100, 120, 125);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimeFenceBody.decode(new byte[TargetTimeFenceBody.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimeFenceBody.decode(Bytes.concat(body.canonicalBytes(), new byte[] {112, 1})));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimeFenceBody.decode(
                        registeredBody(body, body.proof().canonicalBytes(), repeated(32, 99))));
        final var tooLong = proof(TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, 120, 125, 257);
        assertThrows(IllegalArgumentException.class, () -> new TargetTimeFenceBody(SHARD, 500, 100, -1, tooLong));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimeFenceBody.decode(registeredBody(body, tooLong.canonicalBytes(), body.proofId())));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetTimeFenceBody.decode(registeredBody(
                        body, Bytes.concat(body.proof().canonicalBytes(), new byte[] {88, 1}), body.proofId())));
        assertThrows(IllegalArgumentException.class, () -> new TargetTimeFenceBody(SHARD, 500, 100, 0, body.proof()));
    }

    @Test
    void outerIdentityKeyAndRetryBoundaryAreCheckedBeforeExternalAuthority() throws Exception {
        final var body = body(100, 120, 125);
        final var key = keys();
        final TargetTimeFenceVerifier.Authority unused = (s, a, m, p) -> {
            throw new AssertionError("invalid outer identity must not consult an external authority");
        };
        rejected(signed(body, key, 1, body.proofId(), AUTHOR), unused);
        rejected(signed(body, key, -1, repeated(32, 99), AUTHOR), unused);
        rejected(signed(body, key, -1, body.proofId(), AuthorIdentity.fence(repeated(257, 31), 1)), unused);
        final var mutation = signed(body, key, -1, body.proofId(), AUTHOR);
        final var expired = TargetTimeFenceVerifier.decideFirstApplication(SCOPE, mutation, source(501), unused);
        assertEquals(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED, expired.rejection());
        assertNull(expired.body());
        assertThrows(
                IllegalStateException.class,
                () -> TargetTimeFenceVerifier.decideFirstApplication(
                        SCOPE.forTarget(new TargetPartitionId(repeated(32, 11))), mutation, source(100), unused));
    }

    @Test
    void signatureSampleAuthenticationAndCheckedTimingEachRejectWithoutAcceptingAFence() throws Exception {
        final var body = body(100, 120, 125);
        final var key = keys();
        final var mutation = signed(body, key, -1, body.proofId(), AUTHOR);
        rejected(mutation, (s, a, m, p) -> null);
        rejected(mutation, authorization(keys(), 20, 5, false));
        rejected(mutation, authorization(key, 20, 5, false));
        rejected(mutation, authorization(key, 21, 5, true));
        rejected(mutation, authorization(key, 20, 4, true));
        final var overflow = body(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        rejected(signed(overflow, key, -1, overflow.proofId(), AUTHOR), authorization(key, 1, 0, true));
    }

    @Test
    void externalFailuresRemainFailuresEvenWhenTheyUseCodecOrTimingExceptionTypes() throws Exception {
        final var body = body(100, 120, 125);
        final var key = keys();
        final var mutation = signed(body, key, -1, body.proofId(), AUTHOR);
        for (RuntimeException unavailable : new RuntimeException[] {
            new IllegalArgumentException("historical config incomplete"),
            new ArithmeticException("external evidence arithmetic failed"),
            new IllegalStateException("authenticated sample unavailable")
        }) {
            assertSame(
                    unavailable,
                    assertThrows(
                            unavailable.getClass(),
                            () -> TargetTimeFenceVerifier.decideFirstApplication(
                                    SCOPE, mutation, source(500), (s, a, m, p) -> {
                                        throw unavailable;
                                    })));
            assertSame(
                    unavailable,
                    assertThrows(
                            unavailable.getClass(),
                            () -> TargetTimeFenceVerifier.decideFirstApplication(
                                    SCOPE,
                                    mutation,
                                    source(500),
                                    (s, a, m, p) -> new TargetTimeFenceVerifier.Authorization(
                                            key.getPublic(), 20, 5, (scope, author, position, proof) -> {
                                                throw unavailable;
                                            }))));
        }
    }

    private static void rejected(SystemMutation mutation, TargetTimeFenceVerifier.Authority authority) {
        final var decision = TargetTimeFenceVerifier.decideFirstApplication(SCOPE, mutation, source(500), authority);
        assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, decision.rejection());
        assertNull(decision.body());
    }

    private static TargetTimeFenceVerifier.Authority authorization(
            KeyPair key, long margin, long width, boolean evidenceValid) {
        return (s, a, m, p) -> new TargetTimeFenceVerifier.Authorization(
                key.getPublic(), margin, width, (scope, author, position, proof) -> evidenceValid);
    }

    private static SystemMutation signed(
            TargetTimeFenceBody body, KeyPair key, int keyVersion, byte[] logical, AuthorIdentity author) {
        return SystemMutation.signed(
                SHARD,
                SystemMutationType.TIME_FENCE,
                body.retryUntil(),
                logical,
                body.canonicalBytes(),
                author.canonicalBytes(),
                keyVersion,
                key.getPrivate());
    }

    private static TargetTimeFenceBody body(long boundary, long earliest, long latest) {
        return new TargetTimeFenceBody(
                SHARD,
                500,
                boundary,
                -1,
                proof(TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, earliest, latest, 32));
    }

    private static TrustedUtcIntervalEvidence proof(
            TrustedUtcIntervalEvidence.Source source, long earliest, long latest, int sourceIdLength) {
        final boolean signed = source == TrustedUtcIntervalEvidence.Source.SIGNED_TIME_SERVICE;
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                source,
                repeated(sourceIdLength, 51),
                -1L,
                -1L,
                -1L,
                repeated(32, 61),
                signed ? -1 : 0,
                signed ? repeated(64, 71) : new byte[0]);
    }

    private static byte[] registeredBody(TargetTimeFenceBody body, byte[] evidence, byte[] proofId) {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(SHARD).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, 4);
            CanonicalProtobuf.int64(out, 3, body.retryUntil());
            CanonicalProtobuf.int64(out, 10, body.closeThrough());
            CanonicalProtobuf.uint32Bits(out, 11, body.fenceKeyVersion());
            CanonicalProtobuf.bytes(out, 12, proofId);
            CanonicalProtobuf.bytes(out, 13, evidence);
        });
    }

    private static KafkaSourcePosition source(long time) {
        return new KafkaSourcePosition(SHARD, "fence-cluster", new UUID(1, 2), 7, -1, time);
    }

    private static KeyPair keys() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static byte[] repeated(int length, int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
