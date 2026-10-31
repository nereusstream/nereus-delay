package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TargetInitialControlSnapshotTest {
    private final KeyPair keys = keyPair();
    private final TargetQuotaGrant template = template();
    private final TargetQuotaScope scope = template.scope().shardScope();
    private final List<ProtocolTuple> tuples = List.of(ProtocolTuple.currentSystemMutation());

    @Test
    void signedReaderAndProfileSetIsCanonicalAndDoesNotTrustAnotherKey() {
        final var source = source("persistent://test/default/source");
        final var destination = new ProfileRef(bytes(32, 1), 1, bytes(32, 2), ProfileKind.DESTINATION);
        final var capability = new ProfileRef(bytes(32, 3), 1, bytes(32, 4), ProfileKind.DELIVERY_CAPABILITY);
        final var first = input(source, List.of(destination, capability));
        final var reordered = input(source, List.of(capability, destination));
        assertArrayEquals(first.canonicalBytes(), reordered.canonicalBytes());
        final var decoded = TargetControlSnapshotInput.decode(first.canonicalBytes());
        assertArrayEquals(first.canonicalBytes(), decoded.canonicalBytes());
        assertTrue(decoded.verifySignature(keys.getPublic()));
        assertFalse(decoded.verifySignature(keyPair().getPublic()));
        final byte[] tampered = first.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertFalse(TargetControlSnapshotInput.decode(tampered).verifySignature(keys.getPublic()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetControlSnapshotInput.decode(Bytes.concat(
                        first.canonicalBytes(),
                        CanonicalProtobuf.message(out -> CanonicalProtobuf.uint32(out, 12, 1)))));
    }

    @Test
    void duplicateImmutableVersionAndOversizedSourceSnapshotAreRejected() {
        final var source = source("persistent://test/default/source");
        final var first = new ProfileRef(bytes(32, 1), 1, bytes(32, 2), ProfileKind.DESTINATION);
        final var changed = new ProfileRef(bytes(32, 1), 1, bytes(32, 3), ProfileKind.DESTINATION);
        assertThrows(IllegalArgumentException.class, () -> input(source, List.of(first, changed)));
        final var maximumSource = source("s".repeat(TargetSourcePosition.MAX_TOPIC_UTF8_BYTES));
        assertThrows(IllegalArgumentException.class, () -> input(maximumSource, List.of())
                .canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetControlSnapshotInput.decode(new byte[TargetControlSnapshotInput.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void largeSourceProjectionRoundTripsWithEveryRepeatedSourceStamp() {
        final var source = source("s".repeat(900_000));
        final var grant = new TargetQuotaGrant(
                scope,
                template.grantId(),
                1,
                template.accounting(),
                template.limit(),
                template.tenantPolicyVersion(),
                template.tenantPolicyHash());
        final var request = new TargetQuotaGrantControlRequest(grant, null, null);
        final var ref = new ControlRef(
                bytes(32, 5),
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var stamp = new TargetQuotaMutation(1, source, bytes(32, 6));
        final byte[] hash = bytes(32, 7);
        final var activation = new TargetQuotaGrantActivation(
                request,
                ref,
                stamp,
                SystemMutation.computeSystemMutationId(
                        scope.shard(),
                        SystemMutationType.APPLY_SHARD_CONTROL,
                        ref.logicalOperationIdentity(TargetQuotaGrantControlRequest.CONTROL_KIND),
                        hash),
                hash,
                null);
        final var input = TargetControlSnapshotInput.signed(
                scope,
                bytes(16, 8),
                grant.digest(),
                source,
                stamp.mutationDigest(),
                tuples,
                List.of(),
                1,
                keys.getPrivate());
        final var identity =
                new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(), bytes(16, 9), null, null);
        final byte[] author = AuthorIdentity.control(bytes(32, 10), bytes(32, 11), bytes(32, 12))
                .canonicalBytes();
        final var snapshot = new TargetInitialControlSnapshot(input, identity, stamp, activation, author);
        assertTrue(snapshot.canonicalBytes().length > 2 * TargetControlSnapshotInput.MAX_CANONICAL_BYTES);
        assertTrue(snapshot.canonicalBytes().length <= TargetInitialControlSnapshot.MAX_CANONICAL_BYTES);
        assertArrayEquals(
                snapshot.canonicalBytes(),
                TargetInitialControlSnapshot.decode(snapshot.canonicalBytes()).canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetInitialControlSnapshot(
                        input,
                        identity,
                        new TargetQuotaMutation(2, source, stamp.mutationDigest()),
                        activation,
                        author));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetInitialControlSnapshot(
                        input,
                        identity,
                        stamp,
                        activation,
                        AuthorIdentity.service(bytes(32, 13), bytes(32, 14), 1).canonicalBytes()));
    }

    private TargetControlSnapshotInput input(SourcePosition source, List<ProfileRef> profiles) {
        return TargetControlSnapshotInput.signed(
                scope, bytes(16, 8), template.digest(), source, bytes(32, 6), tuples, profiles, 1, keys.getPrivate());
    }

    private PulsarSourcePosition source(String topic) {
        return new PulsarSourcePosition(
                scope.shard(), bytes(32, 15), topic, 1, 0, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 1);
    }

    private static KeyPair keyPair() {
        try {
            return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static TargetQuotaGrant template() {
        final var vectors = new Properties();
        try (var input = TargetInitialControlSnapshotTest.class.getResourceAsStream(
                "/ndip3/target-quota-grant-vectors.properties")) {
            vectors.load(input);
            return TargetQuotaGrantActivation.decode(
                            HexFormat.of().parseHex(vectors.getProperty("target.initial.activation")))
                    .grant();
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static byte[] bytes(int length, int seed) {
        final byte[] value = new byte[length];
        Arrays.fill(value, (byte) seed);
        return value;
    }
}
