package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.DlqExportMode;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.UncertainPolicy;
import com.nereusstream.delay.runtime.ProfileCatalog;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Pure closed-codec/input-graph tests; catalog visibility, root allocation and protection are explicit fixtures. */
class TargetCheckpointSemanticSnapshotTest {
    private static final TargetCheckpointSemanticSnapshot.Limits LIMITS =
            new TargetCheckpointSemanticSnapshot.Limits(64, 1 << 20, 1 << 20);

    @Test
    void completeValuesIncludeTransitiveCapabilityAndRoundTripExactSourceCut() throws Exception {
        final var fixture = fixture();
        final var guards = new java.util.concurrent.atomic.AtomicInteger();
        final var snapshot = collect(fixture, guards::incrementAndGet);
        assertEquals(2, snapshot.profiles().size());
        assertTrue(snapshot.profiles().contains(fixture.capability()));
        assertEquals(List.of(fixture.retry()), snapshot.retryPolicies());
        assertEquals(List.of(fixture.trust()), snapshot.trustSets());
        final var restored = TargetCheckpointSemanticSnapshot.decode(snapshot.canonicalBytes(), LIMITS);
        restored.validateAgainst(fixture.dependencies(), LIMITS);
        assertArrayEquals(snapshot.canonicalBytes(), restored.canonicalBytes());
        assertArrayEquals(snapshot.snapshotDigest(), restored.snapshotDigest());
        assertTrue(guards.get() > 2);
        final byte[] corrupt = snapshot.canonicalBytes();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetCheckpointSemanticSnapshot.decode(corrupt, LIMITS));
        final var changed = new TargetCheckpointDependencies.Builder(new TargetCheckpointRootVerifier.RootProof(
                fixture.dependencies().root().metadata(), fixture.source(), 2,
                fixture.dependencies().root().root(), fixture.dependencies().root().bookkeeping(),
                fixture.dependencies().root().aggregate())).finish(List.of());
        assertThrows(IllegalArgumentException.class, () -> restored.validateAgainst(changed, LIMITS));
    }

    @Test
    void missingTransitiveValuesWrongSemanticsBudgetAndProtectionCannotComplete() throws Exception {
        final var fixture = fixture();
        assertThrows(IllegalStateException.class, () -> TargetCheckpointSemanticSnapshot.collect(
                fixture.dependencies(), List.of(), profiles(fixture, false), (ref, at) -> fixture.retry(),
                ref -> fixture.trust(), budget(64), LIMITS, () -> {}));
        assertThrows(ReadIncompleteException.class, () -> TargetCheckpointSemanticSnapshot.collect(
                fixture.dependencies(), List.of(), profiles(fixture, true), (ref, at) -> fixture.retry(),
                ref -> fixture.trust(), budget(1), LIMITS, () -> {}));
        final var calls = new java.util.concurrent.atomic.AtomicInteger();
        assertThrows(IllegalStateException.class, () -> collect(fixture, () -> {
            if (calls.incrementAndGet() == 4) { throw new IllegalStateException("protection lost"); }
        }));
        assertThrows(IllegalArgumentException.class, () -> TargetCheckpointSemanticSnapshot.decode(
                collect(fixture, () -> {}).canonicalBytes(), new TargetCheckpointSemanticSnapshot.Limits(1, 128, 128)));
    }

    private static TargetCheckpointSemanticSnapshot collect(Fixture fixture, Runnable guard) {
        return TargetCheckpointSemanticSnapshot.collect(fixture.dependencies(), List.of(), profiles(fixture, true),
                (ref, at) -> {
                    assertEquals(fixture.source(), at);
                    return fixture.retry();
                }, ref -> fixture.trust(), budget(64), LIMITS, guard);
    }

    private static ProfileCatalog profiles(Fixture fixture, boolean capabilityPresent) {
        return new ProfileCatalog() {
            @Override
            public ProfileSemanticEnvelope resolve(ProfileRef reference) {
                if (reference.equals(fixture.destination().ref())) { return fixture.destination(); }
                return capabilityPresent && reference.equals(fixture.capability().ref()) ? fixture.capability() : null;
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBinding resolveBinding(
                    ProfileRef profile, long generation) {
                throw new AssertionError("semantic collection must not read private credentials");
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBindingHead resolveHead(ProfileRef profile) {
                throw new AssertionError("semantic collection must not read mutable credential heads");
            }

            @Override
            public com.nereusstream.delay.protocol.CredentialBindingProtection resolveProtection(
                    ProfileRef profile, long generation) {
                throw new AssertionError("protection belongs to the external guard");
            }
        };
    }

    private record Fixture(TargetCheckpointDependencies dependencies, ProfileSemanticEnvelope destination,
            ProfileSemanticEnvelope capability, RetryPolicySemantic retry, PayloadProofTrustSetSemantic trust,
            KafkaSourcePosition source) {}

    private static Fixture fixture() throws Exception {
        final var dispatch = TargetDispatchCompatibility.decode(vector(
                "target-compatibility-vectors.properties", "pulsar.journal.dispatch"));
        final var capability = new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY,
                Bytes.utf8("snapshot-capability"), 1, dispatch.capability());
        final var destination = new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("snapshot-dest"), 1,
                new DestinationProfileSemantic(AdapterKind.PULSAR, CanonicalTargetPartition.decode(vector(
                        "target-compatibility-vectors.properties", "pulsar.target")).resource(), 8,
                        TargetPartitionPolicy.EXPLICIT_ONLY, TargetPartitionHashInput.DELAY_MESSAGE_ID, List.of(5),
                        capability.ref(), 3, 60_000, bytes(32, 1), 20_000, 10_000, 10_000, 1,
                        Bytes.utf8("snapshot-dest"), 86_400_000, 172_800_000, 2, bytes(32, 2)));
        final var retry = new RetryPolicySemantic(Bytes.utf8("snapshot-retry"), 1, 10, 100, 3, 60_000,
                UncertainPolicy.HOLD_FOR_EVIDENCE, 0, DlqExportMode.NOT_CONFIGURED, 0, 0, 0, 0, false, bytes(32, 3));
        final var trust = new PayloadProofTrustSetSemantic(1,
                List.of(new PayloadProofVerifierKey(1, bytes(32, 4), 0, 100)));
        final var source = new KafkaSourcePosition(new ShardId(new RouteIncarnation(bytes(16, 5)), 0),
                "snapshot-cluster", new UUID(1, 2), 10, null, 100);
        final var accounting = TargetQuotaAccounting.decode(
                vector("target-quota-accounting-vectors.properties", "accounting"));
        final var stamp = new TargetQuotaMutation(1, source, bytes(32, 6));
        final var root = TargetQuotaIncarnation.allocate(new TargetQuotaScope(source.shardId(), bytes(32, 7), null),
                accounting, bytes(16, 8), stamp, (prior, next) -> {});
        final var bookkeeping = new TargetQuotaBookkeeping(root.identity(), bytes(32, 7), accounting,
                new TargetQuotaBookkeeping.Inventory(2, 0, 0), 1, stamp);
        final var proof = new TargetCheckpointRootVerifier.RootProof(new StoreMetadata(2, source.shardId(),
                bytes(16, 9), bytes(32, 10)), source, 1, root, bookkeeping,
                new TargetQuotaAggregate(source.shardId(), root.identity().accountingIncarnation(),
                        TargetQuotaUsage.empty(), 1, stamp));
        final var builder = new TargetCheckpointDependencies.Builder(proof);
        builder.profile(destination.ref());
        builder.retryUse(new TargetCheckpointDependencies.RetryUse(retry.ref(), source));
        builder.trust(trust.ref());
        return new Fixture(builder.finish(List.of()), destination, capability, retry, trust, source);
    }

    private static BoundedReadBudget budget(int records) {
        return new BoundedReadBudget(records, 1 << 20, 60_000_000_000L, System::nanoTime);
    }

    private static byte[] bytes(int length, int value) {
        final byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] vector(String file, String key) throws Exception {
        final var properties = new Properties();
        try (var input = TargetCheckpointSemanticSnapshotTest.class.getResourceAsStream("/ndip3/" + file)) {
            properties.load(input);
        }
        return HexFormat.of().parseHex(properties.getProperty(key));
    }
}
