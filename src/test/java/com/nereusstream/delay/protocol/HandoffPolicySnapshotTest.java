package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.semantic.HandoffPolicyAuthority;
import com.nereusstream.delay.semantic.InMemoryHandoffPolicyAuthority;
import com.nereusstream.delay.semantic.InMemoryHandoffPolicyTrustStore;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class HandoffPolicySnapshotTest {
    @Test
    void signedSnapshotHeadAndOxiaRevisionRoundTrip() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] scope = bytes(32, 1);
        final byte[] artifactDigest = bytes(32, 2);
        final HandoffPolicySnapshot snapshot = snapshot(keys, scope, artifactDigest, HandoffPolicyMode.ENABLED);

        assertTrue(snapshot.verifySignature(keys.getPublic()));
        assertEquals(snapshot, HandoffPolicySnapshot.decode(snapshot.canonicalBytes()));

        final HandoffPolicyHead head = new HandoffPolicyHead(scope, 7, HandoffPolicyMode.ENABLED, snapshot, 2_000);
        final InMemoryHandoffPolicyAuthority authority = new InMemoryHandoffPolicyAuthority();
        final HandoffPolicyAuthority.Publication publication = authority.compareAndSet(scope, 0, head);

        assertEquals(1, publication.oxiaVersion());
        assertEquals(new HandoffPolicyHeadRef(scope, 7, snapshot.snapshotDigest(), 1), head.ref(1));
        assertEquals(head, HandoffPolicyHead.decode(head.canonicalBytes()));
    }

    @Test
    void signatureTamperingIsDecodedButCannotBeTrusted() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final HandoffPolicySnapshot snapshot = snapshot(keys, bytes(32, 11), bytes(32, 12), HandoffPolicyMode.ENABLED);
        final byte[] tampered = snapshot.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;

        final HandoffPolicySnapshot decoded = HandoffPolicySnapshot.decode(tampered);
        assertFalse(decoded.verifySignature(keys.getPublic()));
    }

    @Test
    void historicalTrustRequiresTheExactSourcePositionAndArtifactSet() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] scope = bytes(32, 21);
        final byte[] artifactDigest = bytes(32, 22);
        final HandoffPolicySnapshot snapshot = snapshot(keys, scope, artifactDigest, HandoffPolicyMode.ENABLED);
        final SourcePosition position = new KafkaSourcePosition(
                new ShardId(new RouteIncarnation(bytes(16, 23)), 0),
                "source-cluster",
                UUID.nameUUIDFromBytes(Bytes.utf8("policy-source")),
                9,
                null,
                1_100);
        final InMemoryHandoffPolicyTrustStore store = new InMemoryHandoffPolicyTrustStore();
        store.installIssuerKey(snapshot.issuerKeyGeneration(), keys.getPublic(), position);
        store.activatePolicy(scope, snapshot.generation(), position);

        store.requireTrusted(snapshot, scope, artifactDigest, position);
        assertThrows(
                IllegalArgumentException.class, () -> store.requireTrusted(snapshot, scope, bytes(32, 24), position));
        assertThrows(
                IllegalArgumentException.class,
                () -> store.requireTrusted(snapshot, bytes(32, 25), artifactDigest, position));
    }

    @Test
    void casRejectsAStaleOxiaRevisionWithoutReplacingTheCurrentHead() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] scope = bytes(32, 31);
        final InMemoryHandoffPolicyAuthority authority = new InMemoryHandoffPolicyAuthority();
        final HandoffPolicyHead first = new HandoffPolicyHead(
                scope,
                7,
                HandoffPolicyMode.DISABLED,
                snapshot(keys, scope, bytes(32, 32), HandoffPolicyMode.DISABLED),
                0);
        authority.compareAndSet(scope, 0, first);

        assertThrows(IllegalStateException.class, () -> authority.compareAndSet(scope, 0, first));
        assertEquals(first, authority.requireCurrent(scope).head());
    }

    @Test
    void activeLeaseMustFullyContainTheTrustedInterval() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final HandoffPolicySnapshot snapshot = snapshot(keys, bytes(32, 41), bytes(32, 42), HandoffPolicyMode.ENABLED);

        snapshot.requireActiveAt(trustedTime(1_000, 1_999));
        assertThrows(IllegalArgumentException.class, () -> snapshot.requireActiveAt(trustedTime(999, 1_001)));
        assertThrows(IllegalArgumentException.class, () -> snapshot.requireActiveAt(trustedTime(1_999, 2_000)));
    }

    private static TrustedUtcIntervalEvidence trustedTime(final long earliest, final long latest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("active-policy-clock"),
                1,
                2,
                3,
                bytes(32, 43),
                0,
                null);
    }

    private static HandoffPolicySnapshot snapshot(
            final KeyPair keys, final byte[] scope, final byte[] artifactDigest, final HandoffPolicyMode mode) {
        final long lead = mode == HandoffPolicyMode.DISABLED ? 0 : 100;
        final int paths = mode == HandoffPolicyMode.DISABLED ? 0 : HandoffPath.MANAGED_HANDOFF;
        return HandoffPolicySnapshot.create(
                scope,
                7,
                mode,
                lead,
                1_000,
                2_000,
                paths,
                new TrustedUtcIntervalEvidence(
                        900,
                        910,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        Bytes.utf8("policy-clock"),
                        1,
                        1,
                        1,
                        bytes(32, 40),
                        0,
                        null),
                3,
                artifactDigest,
                keys.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
