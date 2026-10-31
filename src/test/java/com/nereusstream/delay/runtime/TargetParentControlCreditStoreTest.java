package com.nereusstream.delay.runtime;

import static com.nereusstream.delay.runtime.TargetProtocolStoreTestFixture.budget;
import static com.nereusstream.delay.runtime.TargetProtocolStoreTestFixture.bytes;
import static com.nereusstream.delay.runtime.TargetProtocolStoreTestFixture.guard;
import static com.nereusstream.delay.runtime.TargetProtocolStoreTestFixture.source;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nereusstream.delay.assessment.DataResetManifest;
import com.nereusstream.delay.ownership.TargetParentControlCreditMutationFactory;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real Native commits and reopen; external tenant/physical funding and Owner protection remain fixture authorities. */
class TargetParentControlCreditStoreTest {
    @TempDir Path directory;
    private final TargetStoreBackend.WriteLimits limits = new TargetStoreBackend.WriteLimits(64, 2 << 20);

    @Test
    void publicSourceActivationFundsIndependentCanonicalPayerAndSurvivesPhysicalReopen() throws Exception {
        final var fixture = new TargetProtocolStoreTestFixture();
        final var config = ShardStoreConfig.defaults(directory);
        final var activationAt = source(fixture.earlier, fixture.earlier.offset() + 1, 100);
        final var fundedAt = source(activationAt, activationAt.offset() + 1, 101);
        final byte[] parentBytes;
        final byte[] activationBytes;
        final byte[] initialBytes;
        final byte[] firstBytes;
        final byte[] positionBytes;
        final TargetQuotaUsage finalUsage;
        final TargetParentControlCreditMutationFactory.Prepared input;
        final TargetQuotaIncarnation owner;
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.createTarget(config, fixture.scope.shard(), resources)) {
            final var initialized = fixture.bootstrap(store, limits);
            owner = initialized.root();
            final var activation = activation(fixture, store, 22);
            final var activations = new TargetProtocolActivationStore(initialized.backend(), fixture.scope,
                    fixture.lineage, 2, 64);
            final long beforeActivation = store.latestSequenceNumber();
            final var pending = activations.prepareFirst(budget(), activation.control(), activation.marker(),
                    activationAt, activation.authority());
            assertEquals(beforeActivation, store.latestSequenceNumber());
            assertNull(store.get(ColumnFamily.META, TargetProtocolActivationRecord.key(ProtocolTuple.targetParentControlCredit())));
            assertEquals(ApplyStatus.APPLIED, activations.commit(pending, (a, b, c) -> guard()).applyStatus());
            assertAppliedPair(store, activation.marker(), activationAt, owner, ApplyStatus.APPLIED, StableCode.OK);

            final var view = initialized.backend().prepareRead(budget(), reader ->
                    TargetParentControlCreditVerifier.readView(reader, fixture.scope, fixture.lineage)).value();
            final var expected = TargetQuotaGrant.decode(fixture.rootRequest.next().canonicalBytes());
            assertNotSame(expected, view.actualGrant().grant());
            assertArrayEquals(expected.canonicalBytes(), view.actualGrant().grant().canonicalBytes());
            final var parent = parent(fixture, view, expected, 0x91);
            input = parent.prepared();
            final var parents = new TargetParentControlCreditStore(initialized.backend(), fixture.scope,
                    fixture.lineage, 2, 64);
            final var beforeFunding = usage(initialized.backend());
            final long beforePrepare = store.latestSequenceNumber();
            final var prepared = parents.prepareFirst(budget(), input.control(), input.mutation(), fundedAt,
                    parent.authority());
            assertEquals(beforePrepare, store.latestSequenceNumber());
            assertNull(store.get(ColumnFamily.META, parent.definition().key()));
            final var outcome = parents.commit(prepared, (a, b, c) -> guard());
            assertEquals(ApplyStatus.APPLIED, outcome.applyStatus());
            assertEquals(StableCode.OK, outcome.stableCode());
            final var first = assertAppliedPair(store, input.mutation(), fundedAt, owner, ApplyStatus.APPLIED, StableCode.OK);
            final var position = result(store, positionKey(fundedAt));
            parentBytes = store.get(ColumnFamily.META, parent.definition().key());
            final var credit = TargetParentControlCredit.decodeForStore(parent.definition().key(),
                    TargetValueEnvelope.decode(parentBytes, TargetParentControlCredit.VALUE_TYPE).payload(),
                    fixture.scope, fixture.lineage);
            assertNotSame(credit.definition().expectedPayerGrant(), credit.actualPayerGrant().grant());
            assertEquals(parent.definition().funded(), credit.remaining());
            assertEquals(CapacityVector.empty(), credit.allocated());
            final var expectedDebit = new TargetQuotaUsage(credit.ownContribution().resources()
                    .add(first.recordCharge()).add(position.recordCharge()), 0, 0, 0, 0);
            assertEquals(beforeFunding.add(expectedDebit), usage(initialized.backend()));
            finalUsage = usage(initialized.backend());
            activationBytes = store.get(ColumnFamily.META, TargetProtocolActivationRecord.key(ProtocolTuple.targetParentControlCredit()));
            initialBytes = store.get(ColumnFamily.META, KeyCodec.metaFixed(10));
            firstBytes = store.get(ColumnFamily.DEDUPE, firstKey(input.mutation()));
            positionBytes = store.get(ColumnFamily.DEDUPE, positionKey(fundedAt));
            assertEquals(3, store.shardMutationSequence());
        }
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, fixture.scope.shard(), resources)) {
            final var reopened = TargetStoreBootstrap.reopen(store, fixture.scope, limits, budget(), (a, b) -> guard());
            assertArrayEquals(owner.canonicalBytes(), reopened.root().canonicalBytes());
            assertEquals(3, store.shardMutationSequence());
            assertArrayEquals(fundedAt.canonicalBytes(), store.appliedShardLogPosition().canonicalBytes());
            assertEquals(finalUsage, usage(reopened.backend()));
            final var body = TargetParentControlCreditBody.decode(input.mutation().canonicalBody());
            final byte[] key = body.request().definition().key();
            assertArrayEquals(parentBytes, store.get(ColumnFamily.META, key));
            assertArrayEquals(activationBytes, store.get(ColumnFamily.META,
                    TargetProtocolActivationRecord.key(ProtocolTuple.targetParentControlCredit())));
            assertArrayEquals(initialBytes, store.get(ColumnFamily.META, KeyCodec.metaFixed(10)));
            assertArrayEquals(firstBytes, store.get(ColumnFamily.DEDUPE, firstKey(input.mutation())));
            assertArrayEquals(positionBytes, store.get(ColumnFamily.DEDUPE, positionKey(fundedAt)));
            final var restored = TargetParentControlCredit.decodeForStore(key,
                    TargetValueEnvelope.decode(parentBytes, TargetParentControlCredit.VALUE_TYPE).payload(),
                    fixture.scope, fixture.lineage);
            assertNotSame(restored.definition().expectedPayerGrant(), restored.actualPayerGrant().grant());
            assertEquals(restored.definition().funded(), restored.allocated().add(restored.remaining()));
            assertAppliedPair(store, input.mutation(), fundedAt, reopened.root(), ApplyStatus.APPLIED, StableCode.OK);
        }
    }

    @Test
    void missingPendingWrongPurposeAndRejectedActivationRetainOnlyRejectedSourceResults() throws Exception {
        for (var mode : List.of("missing", "pending", "wrong-purpose", "rejected")) {
            final var fixture = new TargetProtocolStoreTestFixture();
            final var config = ShardStoreConfig.defaults(directory.resolve(mode));
            try (var resources = new SharedRocksDbResources(config);
                    var store = ShardStore.createTarget(config, fixture.scope.shard(), resources)) {
                final var initialized = fixture.bootstrap(store, limits);
                final var at = source(fixture.earlier, fixture.earlier.offset() + 1, 100);
                final var activations = new TargetProtocolActivationStore(initialized.backend(), fixture.scope,
                        fixture.lineage, 2, 64);
                KafkaSourcePosition parentAt = at;
                if (!mode.equals("missing")) {
                    final var activation = activation(fixture, store, mode.equals("wrong-purpose") ? 19 : 22);
                    final var authority = mode.equals("rejected")
                            ? new TargetProtocolActivationStore.Authority(fixture.registrations,
                                    activation.authority().keys(), fixture.actor, prepared -> false,
                                    (a, b, c, d) -> { throw new AssertionError("denied activation acquired inputs"); })
                            : activation.authority();
                    final long before = store.latestSequenceNumber();
                    final var prepared = activations.prepareFirst(budget(), activation.control(), activation.marker(), at, authority);
                    assertEquals(before, store.latestSequenceNumber());
                    if (!mode.equals("pending")) {
                        final var applied = activations.commit(prepared, (a, b, c) -> guard());
                        assertEquals(mode.equals("rejected") ? ApplyStatus.REJECTED : ApplyStatus.APPLIED, applied.applyStatus());
                        assertAppliedPair(store, activation.marker(), at, initialized.root(),
                                mode.equals("rejected") ? ApplyStatus.REJECTED : ApplyStatus.APPLIED,
                                mode.equals("rejected") ? StableCode.UNAUTHORIZED_SYSTEM_MUTATION : StableCode.OK);
                    }
                    parentAt = source(at, at.offset() + 1, 101);
                }
                assertNull(store.get(ColumnFamily.META, TargetProtocolActivationRecord.key(ProtocolTuple.targetParentControlCredit())));
                final var view = initialized.backend().prepareRead(budget(), reader ->
                        TargetParentControlCreditVerifier.readView(reader, fixture.scope, fixture.lineage)).value();
                final var parent = parent(fixture, view,
                        TargetQuotaGrant.decode(fixture.rootRequest.next().canonicalBytes()), 0x92);
                final var parents = new TargetParentControlCreditStore(initialized.backend(), fixture.scope,
                        fixture.lineage, 2, 64);
                final var beforeUsage = usage(initialized.backend());
                final long before = store.latestSequenceNumber();
                final var prepared = parents.prepareFirst(budget(), parent.prepared().control(), parent.prepared().mutation(),
                        parentAt, parent.authority());
                assertEquals(before, store.latestSequenceNumber());
                assertNull(store.get(ColumnFamily.META, parent.definition().key()));
                final var rejected = parents.commit(prepared, (a, b, c) -> guard());
                assertEquals(ApplyStatus.REJECTED, rejected.applyStatus());
                assertEquals(StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION, rejected.stableCode());
                assertNull(store.get(ColumnFamily.META, parent.definition().key()));
                final var first = assertAppliedPair(store, parent.prepared().mutation(), parentAt, initialized.root(),
                        ApplyStatus.REJECTED, StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION);
                final var position = result(store, positionKey(parentAt));
                final var resultDebit = new TargetQuotaUsage(first.recordCharge().add(position.recordCharge()), 0, 0, 0, 0);
                assertEquals(beforeUsage.add(resultDebit), usage(initialized.backend()));
                assertArrayEquals(parentAt.canonicalBytes(), store.appliedShardLogPosition().canonicalBytes());

                final long after = store.latestSequenceNumber();
                final var replay = new TargetSystemReplayStore(initialized.backend(), fixture.scope, fixture.lineage, 2, 64);
                final var replayed = replay.commit(replay.prepareIfPresent(budget(), parent.prepared().mutation(), parentAt).orElseThrow(),
                        (a, b, c) -> guard(), (a, b) -> guard());
                assertArrayEquals(rejected.encode(), replayed.encode());
                assertEquals(after, store.latestSequenceNumber());
                assertEquals(beforeUsage.add(resultDebit), usage(initialized.backend()));
                assertArrayEquals(first.canonicalBytes(), result(store, first.key()).canonicalBytes());
                assertNull(store.get(ColumnFamily.META, parent.definition().key()));
            }
        }
    }

    @Test
    void sameSourceActivationIsNotPriorParentPermission() throws Exception {
        final var fixture = new TargetProtocolStoreTestFixture();
        final var config = ShardStoreConfig.defaults(directory);
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.createTarget(config, fixture.scope.shard(), resources)) {
            final var initialized = fixture.bootstrap(store, limits);
            final var at = source(fixture.earlier, fixture.earlier.offset() + 1, 100);
            final var activation = activation(fixture, store, 22);
            final var activations = new TargetProtocolActivationStore(initialized.backend(), fixture.scope, fixture.lineage, 2, 64);
            activations.commit(activations.prepareFirst(budget(), activation.control(), activation.marker(), at,
                    activation.authority()), (a, b, c) -> guard());
            final var view = initialized.backend().prepareRead(budget(), reader ->
                    TargetParentControlCreditVerifier.readView(reader, fixture.scope, fixture.lineage)).value();
            final var parent = parent(fixture, view, TargetQuotaGrant.decode(fixture.rootRequest.next().canonicalBytes()), 0x93);
            final long before = store.latestSequenceNumber();
            final var denied = assertThrows(CommandResolutionException.class, () -> initialized.backend().prepareRead(
                    budget(), reader -> TargetParentControlCreditVerifier.verifyFirstApplication(reader, fixture.scope,
                            fixture.lineage, parent.prepared().control(), parent.prepared().mutation(), at, parent.authority())));
            assertEquals(StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION, denied.stableCode());
            final var parents = new TargetParentControlCreditStore(initialized.backend(), fixture.scope, fixture.lineage, 2, 64);
            assertThrows(IllegalStateException.class, () -> parents.prepareFirst(budget(), parent.prepared().control(),
                    parent.prepared().mutation(), at, parent.authority()));
            assertEquals(before, store.latestSequenceNumber());
            assertNull(store.get(ColumnFamily.META, parent.definition().key()));
        }
    }

    private Activation activation(TargetProtocolStoreTestFixture fixture, ShardStore store, int version) {
        final var base = ArtifactGenerationSet.current(1, PulsarSourceLock.digest(), bytes(32, 0xb1));
        final var reset = reset(fixture, base);
        final var descriptor = new TargetProtocolActivationDescriptor(base, reset.manifestDigest(), bytes(32, 0xb2),
                "1".repeat(40), "2".repeat(40), bytes(32, 0xb3), bytes(32, 0xb4), version);
        final var declaration = ProtocolCapabilityDeclaration.parentControlCreditTarget("fixture-worker", bytes(32, 0xb5),
                List.of(base.clientCommandTuple(), base.systemMutationTuple(), descriptor.tuple()), List.of(base),
                List.of(descriptor), 1, bytes(32, 0xb6));
        final var readers = new TargetProtocolReaderSet(descriptor, List.of(new TargetProtocolReaderSet.Reader(1, declaration)));
        final var initial = TargetInitialControlSnapshot.decode(TargetValueEnvelope.decode(
                store.get(ColumnFamily.META, KeyCodec.metaFixed(10)), TargetInitialControlSnapshot.VALUE_TYPE).payload());
        final var payload = TargetProtocolActivatePayload.forRoot(initial, descriptor, readers.digest(), fixture.snapshotKeys.getPrivate());
        final var ref = new ControlRef(bytes(32, 0xb7), PreparedControlOperation.requestHash(
                ControlOperationKind.ACTIVATE_TARGET_PROTOCOL, payload.operationRequest()), 0);
        final var body = new TargetProtocolActivationBody(fixture.scope.shard(), 500, ref, payload);
        final var marker = SystemMutation.signed(body.shard(), SystemMutationType.APPLY_SHARD_CONTROL, 500,
                body.logicalIdentity(), body.canonicalBytes(), AuthorIdentity.control(fixture.actor.actorIdHash(),
                        fixture.actor.roleSet().digest(), fixture.actor.tenantResourceScopeHash()).canonicalBytes(),
                1, fixture.keys.getPrivate());
        final var target = new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(body.shard()),
                marker.systemMutationId(), marker.mutationHash());
        final var control = PreparedControlOperation.prepare(ref.operationId(), ControlOperationKind.ACTIVATE_TARGET_PROTOCOL,
                new ControlAuthor(fixture.actor.actorIdHash(), fixture.actor.roleSet().digest(), fixture.actor.tenantResourceScopeHash()),
                payload.operationRequest(), List.of(target), 1, 400, 1, fixture.keys.getPrivate());
        fixture.registrations.register(control);
        final var evidence = new TargetProtocolRootIssuerEvidence(initial, fixture.snapshotKeys.getPublic().getEncoded());
        final var authority = new TargetProtocolActivationStore.Authority(fixture.registrations,
                (key, at) -> key == 1 ? fixture.keys.getPublic() : null, fixture.actor, prepared -> true,
                (prepared, input, at, original) -> new TargetProtocolActivationStore.Inputs(readers, reset,
                        fixture.snapshotKeys.getPublic(), () -> {}, evidence, (root, authorization, position) -> {
                            assertArrayEquals(initial.canonicalBytes(), root.initial().canonicalBytes());
                            return fixture.snapshotKeys.getPublic();
                        }));
        return new Activation(control, marker, authority);
    }

    private DataResetManifest reset(TargetProtocolStoreTestFixture fixture, ArtifactGenerationSet base) {
        final var declaration = new ProtocolCapabilityDeclaration("fixture-worker", bytes(32, 0xb5),
                List.of(base.clientCommandTuple(), base.systemMutationTuple()), base, 1, bytes(32, 0xb6));
        final var worker = new DataResetManifest.WorkerCapability("fixture-worker", bytes(32, 0xb5), bytes(32, 0xb6),
                declaration, bytes(32, 0xb8));
        final var time = new TrustedUtcIntervalEvidence(50, 51, TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("fixture-reset-clock"), 1, 50, 50, bytes(32, 0xb9), 0, null);
        return DataResetManifest.create(new DataResetManifest.ManifestScope("fixture", "fixture-deployment",
                        fixture.scope.tenantScope(), bytes(32, 0xba), new ShardSubject(fixture.scope.shard())),
                "1".repeat(40), 1, base,
                List.of(new DataResetManifest.ResourceIncarnation("ROCKSDB", "fixture-store", bytes(32, 0xbb), true, bytes(32, 0xbc))),
                bytes(32, 0xbd), new DataResetManifest.ObligationZeroProof(0, 0, 0, bytes(32, 0xbe)), List.of(worker),
                time, new DataResetManifest.ActivationWindow(60, 80), 1, fixture.snapshotKeys.getPrivate());
    }

    private Parent parent(TargetProtocolStoreTestFixture fixture, TargetParentControlCreditVerifier.View view,
            TargetQuotaGrant expected, int operation) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.PENDING_PAYLOAD_BYTES.ordinal()] = 32;
        final var funded = new CapacityVector(values);
        final var definition = new TargetParentControlCredit.Definition(view.actualRoot(), expected,
                new CapacityGrant(CapacityGrantKind.NON_OUTCOME_CONTROL, bytes(32, 0xc1), 1, funded),
                Bytes.sha256(view.actualRoot().allocation().canonicalBytes()), view.priorCut().source(), funded, 2);
        final var actor = new ControlAuthorizationContext(bytes(32, 0xc2),
                ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR, ControlRole.TENANT_POLICY_ADMINISTRATOR), bytes(32, 0xc3));
        final var prepared = new TargetParentControlCreditMutationFactory(1, fixture.keys.getPrivate(), 1, 2 << 20)
                .prepare(bytes(32, operation), new TargetParentControlCreditRequest(definition), actor, ignored -> true, 400, 500);
        fixture.registrations.register(prepared.control());
        final var capacity = new TargetParentControlCreditVerifier.CapacityAuthority() {
            @Override public void requireCurrent() {}
            @Override public void requireAuthorized(TargetParentControlCredit.Definition actual,
                    TargetParentControlCreditVerifier.View selected, SourcePosition source, TargetStoreBackend.Mutation complete) {
                assertEquals(definition, actual);
                assertEquals(view.actualRoot().identity(), selected.actualRoot().identity());
                assertTrue(complete.quota().counters().nextAggregate().usage().resources().covers(funded));
            }
        };
        return new Parent(definition, prepared, new TargetParentControlCreditVerifier.Authority(fixture.registrations,
                (key, at) -> key == 1 ? fixture.keys.getPublic() : null, actor, ignored -> true, capacity));
    }

    private TargetQuotaUsage usage(TargetStoreBackend backend) {
        return backend.prepareRead(budget(), reader -> reader.aggregate().usage()).value();
    }

    private static TargetResultRecord assertAppliedPair(ShardStore store, SystemMutation input, SourcePosition source,
            TargetQuotaIncarnation owner, ApplyStatus status, StableCode code) {
        final var first = result(store, firstKey(input));
        final var position = result(store, positionKey(source));
        first.requireOwner(owner);
        position.requireOwner(owner);
        position.requireFirst(first);
        final var outcome = SystemMutationResult.decode(first.typedPayload());
        assertEquals(status, outcome.applyStatus());
        assertEquals(code, outcome.stableCode());
        assertArrayEquals(input.systemMutationId(), outcome.mutationId());
        assertArrayEquals(input.mutationHash(), outcome.mutationHash());
        assertArrayEquals(source.canonicalBytes(), outcome.appliedSourcePosition());
        assertEquals(TargetResultRecord.Kind.SYSTEM, first.kind());
        assertEquals(TargetResultRecord.Kind.POSITION_SYSTEM, position.kind());
        return first;
    }

    private static TargetResultRecord result(ShardStore store, byte[] key) {
        return TargetResultRecord.decode(TargetValueEnvelope.decode(store.get(ColumnFamily.DEDUPE, key),
                TargetResultRecord.VALUE_TYPE).payload());
    }

    private static byte[] firstKey(SystemMutation input) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, input.systemMutationId());
    }

    private static byte[] positionKey(SourcePosition source) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, source.canonicalBytes());
    }

    private record Activation(PreparedControlOperation control, SystemMutation marker, TargetProtocolActivationStore.Authority authority) {}
    private record Parent(TargetParentControlCredit.Definition definition, TargetParentControlCreditMutationFactory.Prepared prepared,
            TargetParentControlCreditVerifier.Authority authority) {}
}
