package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.CommandResolutionException;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.ValueEnvelope;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetQuotaGrantControlTest {
    private static final List<String> CASES = List.of(
            "target.initial", "shard.initialTransfer", "target.replaceTransfer", "shard.replace", "target.zero");
    private final Properties vectors = load();
    private final TargetQuotaGrantActivation initial = activation("target.initial");
    private final KafkaSourcePosition source =
            (KafkaSourcePosition) initial.mutation().source();
    private final TargetQuotaScope scope = initial.grant().scope();
    private final TargetQuotaUsage limit = TargetQuotaUsage.decode(hex("limit"));
    private final KeyPair key = fixedKey();
    private final ControlAuthorizationContext actor = new ControlAuthorizationContext(
            repeat(32, 0xa1), ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR), repeat(32, 0xa2));
    private final InMemoryControlTargetRegistrationAuthority registrations =
            new InMemoryControlTargetRegistrationAuthority();

    @TempDir
    private Path temporary;

    @Test
    void allRequestBodyMutationAndActivationBranchesMatchIndependentVectors() {
        for (String name : CASES) {
            final var request = request(name);
            assertArrayEquals(hex(name + ".request"), request.canonicalBytes());
            assertArrayEquals(hex(name + ".outer"), request.operationRequest().canonicalBytes());
            assertEquals(request.operationRequest(), ControlOperationRequest.decode(hex(name + ".outer")));
            final var body = body(request, repeat(32, 0x72), 500);
            assertArrayEquals(hex(name + ".body"), body.canonicalBytes());
            assertArrayEquals(hex(name + ".ref"), body.controlRef().canonicalBytes());
            assertArrayEquals(hex(name + ".logical"), body.logicalIdentity());
            assertArrayEquals(hex(name + ".semantic"), body.semanticHash());
            final var mutation = signed(body, actor, key, 1);
            assertArrayEquals(hex(name + ".mutationHash"), mutation.mutationHash());
            assertArrayEquals(hex(name + ".mutationId"), mutation.systemMutationId());
            assertArrayEquals(hex(name + ".envelope"), mutation.canonicalEnvelope());
            assertTrue(mutation.verifySignature(key.getPublic()));
            final var result = activation(name);
            final var expected = new TargetQuotaGrantActivation(
                    request,
                    body.controlRef(),
                    new TargetQuotaMutation(
                            request.next().version(),
                            at(request.next().version()),
                            Bytes.sha256(mutation.canonicalEnvelope())),
                    mutation.systemMutationId(),
                    mutation.mutationHash());
            assertArrayEquals(hex(name + ".activation"), expected.canonicalBytes());
            assertArrayEquals(hex(name + ".key"), result.key());
        }
    }

    @Test
    void activationBindsTheFullSignedEnvelopeNotJustTheSemanticMutationHash() {
        final var mutation = signed(body(request("target.initial"), repeat(32, 0x72), 500), actor, key, 1);
        assertArrayEquals(
                Bytes.sha256(mutation.canonicalEnvelope()), initial.mutation().mutationDigest());
        assertFalse(Arrays.equals(mutation.mutationHash(), initial.mutation().mutationDigest()));
        final var resigned = signed(body(request("target.initial"), repeat(32, 0x72), 500), actor, key, 2);
        assertArrayEquals(mutation.mutationHash(), resigned.mutationHash());
        assertFalse(Arrays.equals(
                Bytes.sha256(resigned.canonicalEnvelope()), initial.mutation().mutationDigest()));
    }

    @Test
    void completePreparedRegistrationAndFactorySignTheExactNewBranch() {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        assertEquals(value.prepared, PreparedControlOperation.decode(value.prepared.canonicalBytes()));
        assertTrue(value.prepared.verifySignature(key.getPublic()));
        final var target = value.prepared.targets().getFirst();
        final var factory = ControlSystemMutationFactory.sign(
                value.prepared,
                target,
                source.shardId(),
                500,
                value.mutation.canonicalBody(),
                AuthorIdentity.control(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                        .canonicalBytes(),
                1,
                key.getPrivate());
        assertArrayEquals(value.mutation.canonicalEnvelope(), factory.canonicalEnvelope());
        registrations.register(value.prepared);
        registrations.validateMutation(value.prepared, target, factory);
        final var change = verify(value, at(1), emptyView(), authority((request, view, position) -> {
            assertEquals(request.request().next().scope(), scope);
            assertArrayEquals(value.prepared.operationId(), request.controlRef().operationId());
            assertArrayEquals(value.prepared.requestHash(), request.controlRef().requestHash());
            assertEquals(TargetQuotaUsage.empty(), view.usage(scope));
        }));
        assertArrayEquals(initial.canonicalBytes(), change.after().canonicalBytes());
        change.requireCurrent(emptyView());
    }

    @Test
    void reducedGrantCanActivateAboveCurrentUsageWithoutChangingThatUsage() {
        final var value = prepare(request("target.zero"), repeat(32, 0x73), actor, key, 1);
        registrations.register(value.prepared);
        final var view = liveView(initial, limit);
        final var calls = new AtomicInteger();
        final var change = verify(value, at(2), view, authority((request, snapshot, position) -> {
            calls.incrementAndGet();
            assertSame(view, snapshot);
            assertEquals(limit, snapshot.usage(scope));
            assertTrue(request.request().next().limit().isZero());
        }));
        assertEquals(1, calls.get());
        assertTrue(change.after().grant().limit().isZero());
        assertSame(view, change.before());
        assertEquals(limit, view.usage(scope));
        assertEquals(2, change.after().mutation().sequence());
        change.requireCurrent(view);
    }

    @Test
    void capacityDenialTransientFailureAndFatalErrorProduceNoActivationPlan() {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        registrations.register(value.prepared);
        final var view = emptyView();
        assertThrows(
                IllegalStateException.class,
                () -> verify(value, at(1), view, authority((r, v, s) -> {
                    throw new IllegalStateException("tenant cuts or donor drain are unproven");
                })));
        assertThrows(
                IllegalArgumentException.class,
                () -> verify(value, at(1), view, authority((r, v, s) -> {
                    throw new IllegalArgumentException("recipient placement cannot reserve the physical envelope");
                })));
        assertThrows(
                AssertionError.class,
                () -> verify(value, at(1), view, authority((r, v, s) -> {
                    throw new AssertionError("capacity backend failed");
                })));
        assertEquals(null, view.grant());
        assertEquals(0, view.sequence());
        assertTrue(view.aggregate().usage().isZero());
    }

    @Test
    void exactPriorArtifactCannotBeReplacedByTheSameVersionAndScope() {
        final var value = prepare(request("target.zero"), repeat(32, 0x73), actor, key, 1);
        registrations.register(value.prepared);
        final var changed = new TargetQuotaGrant(
                scope,
                initial.grant().grantId(),
                1,
                initial.grant().accounting(),
                TargetQuotaUsage.empty(),
                7,
                repeat(32, 0xaa));
        final var priorRequest = new TargetQuotaGrantControlRequest(changed, null, null);
        final var priorBody = body(priorRequest, repeat(32, 0x74), 500);
        final var priorMutation = signed(priorBody, actor, key, 1);
        final var altered = new TargetQuotaGrantActivation(
                priorRequest,
                priorBody.controlRef(),
                new TargetQuotaMutation(1, at(1), Bytes.sha256(priorMutation.canonicalEnvelope())),
                priorMutation.systemMutationId(),
                priorMutation.mutationHash());
        assertThrows(
                CommandResolutionException.class,
                () -> verify(value, at(2), liveView(altered, limit), neverCapacity()));
        assertThrows(CommandResolutionException.class, () -> verify(value, at(2), emptyView(), neverCapacity()));
        final var first = prepare(request("target.initial"), repeat(32, 0x75), actor, key, 1);
        registrations.register(first.prepared);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(first, at(2), liveView(initial, limit), neverCapacity()));
    }

    @Test
    void storeReadSetGuardsInitialAbsencePriorGrantUsageAndSource() {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        registrations.register(value.prepared);
        final var change = verify(value, at(1), emptyView(), authority((r, v, s) -> {}));
        assertThrows(IllegalStateException.class, () -> change.requireCurrent(liveView(initial, limit)));
        assertThrows(
                IllegalStateException.class,
                () -> change.requireCurrent(new TargetQuotaGrantControlVerifier.View(
                        null, emptyView().aggregate(), null, 1, at(1))));
        final var next = prepare(request("target.zero"), repeat(32, 0x73), actor, key, 1);
        registrations.register(next.prepared);
        final var second = verify(next, at(2), liveView(initial, limit), authority((r, v, s) -> {}));
        final var less = new TargetQuotaUsage(CapacityVector.empty(), 1, 0, 0, 2);
        assertThrows(IllegalStateException.class, () -> second.requireCurrent(liveView(initial, less)));
    }

    @Test
    void repeatedOrOlderSourceAndExhaustedSequenceAreNotDedupeSuccess() {
        final var value = prepare(request("target.zero"), repeat(32, 0x73), actor, key, 1);
        registrations.register(value.prepared);
        assertThrows(
                IllegalStateException.class, () -> verify(value, at(1), liveView(initial, limit), neverCapacity()));
        assertThrows(
                IllegalStateException.class, () -> verify(value, at(0), liveView(initial, limit), neverCapacity()));
        final var exhausted = new TargetQuotaGrantControlVerifier.View(
                initial,
                liveView(initial, limit).aggregate(),
                liveView(initial, limit).total(),
                -1,
                at(2));
        assertThrows(IllegalStateException.class, () -> verify(value, at(3), exhausted, neverCapacity()));
    }

    @Test
    void viewRejectsFutureStampsSamePositionMetadataAndDigestDisagreement() {
        final var view = liveView(initial, limit);
        assertThrows(
                IllegalStateException.class,
                () -> new TargetQuotaGrantControlVerifier.View(initial, view.aggregate(), view.total(), 1, at(0)));
        final var changedStamp = new TargetQuotaMutation(1, at(1), repeat(32, 0x66));
        final var changedAggregate =
                new TargetQuotaAggregate(source.shardId(), repeat(16, 0x22), limit, 1, changedStamp);
        assertThrows(
                IllegalStateException.class,
                () -> new TargetQuotaGrantControlVerifier.View(initial, changedAggregate, null, 1, at(1)));
        final var wrongMetadata = new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                source.offset(),
                null,
                999);
        assertThrows(
                IllegalStateException.class,
                () -> new TargetQuotaGrantControlVerifier.View(initial, view.aggregate(), null, 1, wrongMetadata));
        final var earlierSequenceLaterSource = new TargetQuotaMutation(1, at(3), repeat(32, 0x66));
        final var later = activation("target.zero");
        assertThrows(
                IllegalStateException.class,
                () -> new TargetQuotaGrantControlVerifier.View(
                        later,
                        new TargetQuotaAggregate(
                                source.shardId(), repeat(16, 0x22), limit, 1, earlierSequenceLaterSource),
                        null,
                        4,
                        at(4)));
    }

    @Test
    void sourceRouteAndTenantMustBeAuthenticatedBeforeCapacityAuthority() {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        registrations.register(value.prepared);
        final var base = neverCapacity();
        final var wrongTenant = new TargetQuotaGrantControlVerifier.Authority(
                registrations,
                base.keys(),
                (scope, position) -> scope.requireRoute(position.shardId(), repeat(32, 0x45)),
                base.capacity(),
                actor,
                prepared -> true);
        assertThrows(IllegalStateException.class, () -> verify(value, at(1), emptyView(), wrongTenant));
        final var wrongResource = new TargetQuotaGrantControlVerifier.Authority(
                registrations,
                base.keys(),
                (scope, position) -> {
                    if (!source.sameSourceIdentity(position)) {
                        throw new IllegalStateException("source resource differs from the Route registry");
                    }
                },
                base.capacity(),
                actor,
                prepared -> true);
        final var foreign = new KafkaSourcePosition(
                source.shardId(), source.authenticatedClusterId(), UUID.randomUUID(), 10, null, 100);
        assertThrows(IllegalStateException.class, () -> verify(value, foreign, emptyView(), wrongResource));
        final var wrongShard = new KafkaSourcePosition(
                new ShardId(source.shardId().routeIncarnation(), 4),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                10,
                null,
                100);
        assertThrows(CommandResolutionException.class, () -> verify(value, wrongShard, emptyView(), neverCapacity()));
    }

    @Test
    void missingOrUntrustedSignaturesAndScopeProofCannotAuthorizeAGrant() throws Exception {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        registrations.register(value.prepared);
        final var base = neverCapacity();
        final var unavailableKey = new TargetQuotaGrantControlVerifier.Authority(
                registrations, (version, position) -> null, base.routes(), base.capacity(), actor, prepared -> true);
        assertThrows(CommandResolutionException.class, () -> verify(value, at(1), emptyView(), unavailableKey));
        final var other = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var wrongKey = new TargetQuotaGrantControlVerifier.Authority(
                registrations,
                (version, position) -> other.getPublic(),
                base.routes(),
                base.capacity(),
                actor,
                prepared -> true);
        assertThrows(CommandResolutionException.class, () -> verify(value, at(1), emptyView(), wrongKey));
        final var scopeDenied = new TargetQuotaGrantControlVerifier.Authority(
                registrations, base.keys(), base.routes(), base.capacity(), actor, prepared -> false);
        assertThrows(CommandResolutionException.class, () -> verify(value, at(1), emptyView(), scopeDenied));
    }

    @Test
    void registeredActorRoleSetAndResourceScopeMustMatchTheAuthenticatedContext() {
        for (var bad : List.of(
                new ControlAuthorizationContext(repeat(32, 0xa3), actor.roleSet(), actor.tenantResourceScopeHash()),
                new ControlAuthorizationContext(
                        actor.actorIdHash(),
                        ControlRoleSet.of(ControlRole.TENANT_POLICY_ADMINISTRATOR),
                        actor.tenantResourceScopeHash()),
                new ControlAuthorizationContext(actor.actorIdHash(), actor.roleSet(), repeat(32, 0xa3)))) {
            final var value = prepare(request("target.initial"), repeat(32, 0x72), bad, key, 1);
            final var separate = new InMemoryControlTargetRegistrationAuthority();
            separate.register(value.prepared);
            final var base = neverCapacity();
            final var context = new TargetQuotaGrantControlVerifier.Authority(
                    separate, base.keys(), base.routes(), base.capacity(), actor, prepared -> true);
            assertThrows(CommandResolutionException.class, () -> verify(value, at(1), emptyView(), context));
        }
        final var tenantOnly = new ControlAuthorizationContext(
                actor.actorIdHash(),
                ControlRoleSet.of(ControlRole.TENANT_POLICY_ADMINISTRATOR),
                actor.tenantResourceScopeHash());
        final var value = prepare(request("target.initial"), repeat(32, 0x72), tenantOnly, key, 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationAuthorization.authorize(value.prepared, tenantOnly, prepared -> true));
    }

    @Test
    void exactRegistrationAndBothSigningLayersAreRequired() throws Exception {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        assertThrows(CommandResolutionException.class, () -> verify(value, at(1), emptyView(), neverCapacity()));
        registrations.register(value.prepared);
        final var changedPrepared = PreparedControlOperation.prepare(
                value.prepared.operationId(),
                value.prepared.kind(),
                value.prepared.author(),
                value.prepared.request(),
                value.prepared.targets(),
                2,
                400,
                1,
                key.getPrivate());
        assertThrows(
                CommandResolutionException.class,
                () -> verify(new Prepared(changedPrepared, value.mutation), at(1), emptyView(), neverCapacity()));
        final var attacker = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var forgedMutation = signed(body(request("target.initial"), repeat(32, 0x72), 500), actor, attacker, 1);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(new Prepared(value.prepared, forgedMutation), at(1), emptyView(), neverCapacity()));
        final var forgedPrepared = PreparedControlOperation.prepare(
                value.prepared.operationId(),
                value.prepared.kind(),
                value.prepared.author(),
                value.prepared.request(),
                value.prepared.targets(),
                1,
                400,
                1,
                attacker.getPrivate());
        assertThrows(
                CommandResolutionException.class,
                () -> verify(new Prepared(forgedPrepared, value.mutation), at(1), emptyView(), neverCapacity()));
    }

    @Test
    void mutationRegistrationRejectsDifferentDeadlineBodyAndAuthor() {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        final var laterDeadline = signed(body(request("target.initial"), repeat(32, 0x72), 501), actor, key, 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> value.prepared.validateTargetMutation(
                        value.prepared.targets().getFirst(), laterDeadline));
        registrations.register(value.prepared);
        final var otherAuthor =
                new ControlAuthorizationContext(repeat(32, 0xa3), actor.roleSet(), actor.tenantResourceScopeHash());
        final var changedAuthor = signed(body(request("target.initial"), repeat(32, 0x72), 500), otherAuthor, key, 1);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(new Prepared(value.prepared, changedAuthor), at(1), emptyView(), neverCapacity()));
        final var otherOperation = signed(body(request("target.initial"), repeat(32, 0x73), 500), actor, key, 1);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(new Prepared(value.prepared, otherOperation), at(1), emptyView(), neverCapacity()));
    }

    @Test
    void acceptedSourceUsesTheSignedMutationWindowAndRetainedSourceKeys() {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        registrations.register(value.prepared);
        final var position = new KafkaSourcePosition(
                source.shardId(), source.authenticatedClusterId(), source.nativeTopicUuid(), 10, null, 450);
        final var seen = new ArrayList<SourcePosition>();
        final var base = authority((r, v, s) -> {});
        final var retained = new TargetQuotaGrantControlVerifier.Authority(
                registrations,
                (version, at) -> {
                    seen.add(at);
                    return version == 1 ? key.getPublic() : null;
                },
                base.routes(),
                base.capacity(),
                actor,
                p -> true);
        final var change = verify(value, position, emptyView(), retained);
        assertEquals(2, seen.size());
        assertArrayEquals(
                position.canonicalBytes(), change.after().mutation().source().canonicalBytes());
        final var expired = new KafkaSourcePosition(
                source.shardId(), source.authenticatedClusterId(), source.nativeTopicUuid(), 10, null, 501);
        assertEquals(
                StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                assertThrows(
                                CommandResolutionException.class,
                                () -> verify(value, expired, emptyView(), neverCapacity()))
                        .stableCode());
    }

    @Test
    void transferReferencesAreFullPolicyBoundAndCannotReferToThePublishingOperation() {
        final var replace = request("target.replaceTransfer");
        assertEquals(7, replace.transfer().tenantPolicyVersion());
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaGrantControlRequest(
                        replace.next(),
                        replace.prior(),
                        new QuotaTransferPlanRef(repeat(32, 0xb4), repeat(32, 0xb5), 8, repeat(32, 0xb6))));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaGrantControlRequest(
                        replace.next(),
                        replace.prior(),
                        new QuotaTransferPlanRef(repeat(32, 0xb4), new byte[32], 7, repeat(32, 0xb6))));
        assertThrows(
                IllegalArgumentException.class,
                () -> body(replace, replace.transfer().controlOperationId(), 500));
        final var value = prepare(replace, repeat(32, 0x73), actor, key, 1);
        registrations.register(value.prepared);
        final var calls = new AtomicInteger();
        verify(value, at(2), liveView(initial, limit), authority((r, v, s) -> {
            assertEquals(replace.transfer(), r.request().transfer());
            calls.incrementAndGet();
        }));
        assertEquals(1, calls.get());
    }

    @Test
    void initialAndReplacementPresenceVersionAndScopeCannotBeRelabeled() {
        final var first = request("target.initial");
        assertThrows(
                IllegalStateException.class,
                () -> new TargetQuotaGrantControlRequest(first.next(), first.next(), null));
        assertThrows(
                IllegalStateException.class,
                () -> new TargetQuotaGrantControlRequest(request("target.zero").next(), null, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlOperationRequest(ControlOperationKind.PUBLISH_QUOTA_GRANT, first));
        final var body = body(first, repeat(32, 0x72), 500);
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaGrantControlBody(
                        source.shardId(),
                        500,
                        new ControlRef(repeat(32, 0x72), body.controlRef().requestHash(), 1),
                        first));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaGrantControlBody(
                        new ShardId(source.shardId().routeIncarnation(), 4), 500, body.controlRef(), first));
    }

    @Test
    void preparedOperationHasOneExactSourceShardAtIndexZero() {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        for (var targets : List.of(
                List.of(new ControlTargetRef(
                        1,
                        ControlTargetKind.SHARD,
                        new ShardSubject(source.shardId()),
                        value.mutation.systemMutationId(),
                        value.mutation.mutationHash())),
                List.of(new ControlTargetRef(
                        0, ControlTargetKind.SHARD, new ShardSubject(source.shardId()), null, null)),
                List.of(
                        value.prepared.targets().getFirst(),
                        new ControlTargetRef(
                                1,
                                ControlTargetKind.SHARD,
                                new ShardSubject(source.shardId()),
                                value.mutation.systemMutationId(),
                                value.mutation.mutationHash())))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> PreparedControlOperation.prepare(
                            repeat(32, 0x72),
                            value.prepared.kind(),
                            value.prepared.author(),
                            value.prepared.request(),
                            targets,
                            1,
                            400,
                            1,
                            key.getPrivate()));
        }
    }

    @Test
    void closedRequestBodyAndActivationRejectMissingDuplicateAndUnknownFields() {
        for (var entry : List.of(
                new DecodeCase("target.replaceTransfer.request", TargetQuotaGrantControlRequest::decode),
                new DecodeCase("target.replaceTransfer.body", TargetQuotaGrantControlBody::decode),
                new DecodeCase("target.replaceTransfer.activation", TargetQuotaGrantActivation::decode))) {
            final byte[] raw = hex(entry.name);
            assertThrows(IllegalArgumentException.class, () -> entry.decoder.apply(Arrays.copyOf(raw, raw.length - 1)));
            assertThrows(IllegalArgumentException.class, () -> entry.decoder.apply(Bytes.concat(raw, uint(31, 1))));
            assertThrows(IllegalArgumentException.class, () -> entry.decoder.apply(Bytes.concat(raw, uint(1, 1))));
        }
        final byte[] body = hex("target.replaceTransfer.body");
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaGrantControlBody.decode(rewrite(body, 12, uint(12, 3))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaGrantControlBody.decode(rewrite(body, 13, field(13, new byte[32]))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaGrantControlBody.decode(rewrite(body, 14, uint(14, 1))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaGrantControlRequest.decode(
                        new byte[TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaGrantControlBody.decode(
                        new byte[TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaGrantActivation.decode(new byte[TargetQuotaGrantActivation.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void storeKeysAndMutationIdentityAreBoundToTheCompleteActivationScope() {
        for (String name : CASES) {
            final var value = activation(name);
            assertArrayEquals(
                    value.canonicalBytes(),
                    TargetQuotaGrantActivation.decodeForStore(
                                    value.key(), value.canonicalBytes(), source.shardId(), scope.tenantScope())
                            .canonicalBytes());
            final byte[] wrong = value.key();
            wrong[wrong.length - 1] ^= 1;
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetQuotaGrantActivation.decodeForStore(
                            wrong, value.canonicalBytes(), source.shardId(), scope.tenantScope()));
            assertThrows(
                    IllegalStateException.class,
                    () -> TargetQuotaGrantActivation.decodeForStore(
                            value.key(), value.canonicalBytes(), source.shardId(), repeat(32, 0x45)));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetQuotaGrantActivation(
                        initial.request(),
                        initial.controlRef(),
                        initial.mutation(),
                        repeat(32, 0x80),
                        initial.systemMutationHash()));
        assertNotEquals(
                initial.key().length, activation("shard.initialTransfer").key().length);
    }

    @Test
    void maximumBothScopeBranchesIncludeUnsignedPriorVersionAndFullPulsarSource() {
        for (boolean target : List.of(true, false)) {
            final var scope = target ? this.scope : this.scope.shardScope();
            final long[] values = new long[CapacityDimension.COUNT];
            Arrays.fill(values, 0, 15, Long.MAX_VALUE);
            if (!target) {
                Arrays.fill(values, 50, 55, Long.MAX_VALUE);
            }
            final var limit = new TargetQuotaUsage(
                    new CapacityVector(values),
                    target ? 1 : Long.MAX_VALUE,
                    target ? 64 : Long.MAX_VALUE,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE);
            final var accounting = new TargetQuotaAccounting(
                    repeat(32, 0xbb), Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
            final var next = new TargetQuotaGrant(scope, repeat(32, 0x99), -1, accounting, limit, -1, repeat(32, 0xaa));
            final var prior =
                    new TargetQuotaGrant(scope, repeat(32, 0x99), -2, accounting, limit, -1, repeat(32, 0xaa));
            final var request = new TargetQuotaGrantControlRequest(
                    next, prior, new QuotaTransferPlanRef(repeat(32, 0xb4), repeat(32, 0xb5), -1, repeat(32, 0xb6)));
            final var body = body(request, repeat(32, 0x72), Long.MAX_VALUE);
            final var mutation = signed(body, actor, key, 1);
            final var position = new PulsarSourcePosition(
                    source.shardId(),
                    repeat(32, 0x77),
                    "x".repeat(1 << 20),
                    -1,
                    -1,
                    0,
                    1,
                    PulsarSourcePosition.EntryKind.NON_BATCH,
                    Long.MAX_VALUE);
            final var activation = new TargetQuotaGrantActivation(
                    request,
                    body.controlRef(),
                    new TargetQuotaMutation(-1, position, Bytes.sha256(mutation.canonicalEnvelope())),
                    mutation.systemMutationId(),
                    mutation.mutationHash());
            final String prefix = "maximum." + (target ? "target" : "shard");
            maximum(prefix + ".request", request.canonicalBytes(), TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES);
            maximum(prefix + ".body", body.canonicalBytes(), TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES);
            maximum(
                    prefix + ".activation",
                    activation.canonicalBytes(),
                    TargetQuotaGrantActivation.MAX_CANONICAL_BYTES);
            assertEquals(
                    -1,
                    TargetQuotaGrantActivation.decode(activation.canonicalBytes())
                            .grant()
                            .version());
            assertEquals(
                    -2,
                    TargetQuotaGrantControlBody.decode(body.canonicalBytes())
                            .request()
                            .prior()
                            .version());
            assertTrue(mutation.verifySignature(key.getPublic()));
        }
    }

    @Test
    void legacySemanticAndNvReadersRejectNewBranchesWithValidIntegrity() {
        assertThrows(IllegalArgumentException.class, () -> ApplyShardControlBody.decode(hex("target.initial.body")));
        final var mutation =
                SystemMutation.decodeEnvelope(hex("target.initial.envelope"), hex("target.initial.logical"));
        assertTrue(mutation.verifySignature(key.getPublic()));
        assertThrows(IllegalArgumentException.class, () -> SystemMutation.decodeFrame(mutation.encodeFrame()));
        final byte[] prefix = ByteBuffer.allocate(8)
                .putShort((short) 0x4e56)
                .put((byte) TargetQuotaGrantActivation.VALUE_TYPE)
                .put((byte) 1)
                .putInt(initial.canonicalBytes().length)
                .array();
        final byte[] bytes = Bytes.concat(prefix, initial.canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueEnvelope.decodeAny(Bytes.concat(bytes, Bytes.crc32cbe(bytes))));
    }

    @Test
    void activeLaneStoreRejectsTheNewGrantAndPreservesTheRejectionAcrossReopen() {
        final var value = prepare(request("target.initial"), repeat(32, 0x72), actor, key, 1);
        final var config = ShardStoreConfig.defaults(temporary.resolve("old-store"));
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.open(config, source.shardId(), resources)) {
            final var shard = new DelayShard(store, DelayShardConfig.defaults());
            final var result = shard.applySystemMutation(value.mutation, at(1), key.getPublic());
            assertEquals(ApplyStatus.REJECTED, result.applyStatus());
            assertEquals(StableCode.STALE_SYSTEM_MUTATION, result.stableCode());
            assertEquals(null, store.get(ColumnFamily.META, initial.key()));
            assertEquals(result, shard.getSystemMutationResult(value.mutation.systemMutationId()));
        }
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.open(config, source.shardId(), resources)) {
            final var shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(
                    ApplyStatus.REJECTED,
                    shard.getSystemMutationResult(value.mutation.systemMutationId())
                            .applyStatus());
            assertEquals(null, store.get(ColumnFamily.META, initial.key()));
        }
    }

    private TargetQuotaGrantControlVerifier.Change verify(
            final Prepared prepared,
            final SourcePosition source,
            final TargetQuotaGrantControlVerifier.View view,
            final TargetQuotaGrantControlVerifier.Authority authority) {
        return TargetQuotaGrantControlVerifier.verifyFirstApplication(
                prepared.prepared, prepared.mutation, source, view, authority);
    }

    private TargetQuotaGrantControlVerifier.Authority authority(
            final TargetQuotaGrantControlVerifier.CapacityAuthority capacity) {
        return new TargetQuotaGrantControlVerifier.Authority(
                registrations,
                (version, position) -> version == 1 ? key.getPublic() : null,
                (scope, position) -> {
                    scope.requireRoute(source.shardId(), this.scope.tenantScope());
                    if (!source.sameSourceIdentity(position)) {
                        throw new IllegalStateException("foreign source resource");
                    }
                },
                capacity,
                actor,
                prepared -> true);
    }

    private TargetQuotaGrantControlVerifier.Authority neverCapacity() {
        return authority((r, v, s) -> {
            throw new AssertionError("rejected before capacity authority");
        });
    }

    private TargetQuotaGrantControlVerifier.View emptyView() {
        return new TargetQuotaGrantControlVerifier.View(
                null, TargetQuotaAggregate.genesis(source.shardId(), repeat(16, 0x22)), null, 0, null);
    }

    private TargetQuotaGrantControlVerifier.View liveView(
            final TargetQuotaGrantActivation grant, final TargetQuotaUsage usage) {
        final var aggregate = new TargetQuotaAggregate(source.shardId(), repeat(16, 0x22), usage, 1, grant.mutation());
        final var total = new TargetQuotaTotal(grant.grant().scope(), usage, 1, grant.mutation());
        return new TargetQuotaGrantControlVerifier.View(
                grant,
                aggregate,
                total,
                grant.mutation().sequence(),
                grant.mutation().source());
    }

    private TargetQuotaGrantControlRequest request(final String name) {
        return TargetQuotaGrantControlRequest.decode(hex(name + ".request"));
    }

    private TargetQuotaGrantActivation activation(final String name) {
        return TargetQuotaGrantActivation.decode(hex(name + ".activation"));
    }

    private TargetQuotaGrantControlBody body(
            final TargetQuotaGrantControlRequest request, final byte[] operation, final long until) {
        return new TargetQuotaGrantControlBody(
                request.next().scope().shard(),
                until,
                new ControlRef(
                        operation,
                        PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                        0),
                request);
    }

    private SystemMutation signed(
            final TargetQuotaGrantControlBody body,
            final ControlAuthorizationContext actor,
            final KeyPair keys,
            final int version) {
        return SystemMutation.signed(
                body.shard(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                body.retryUntil(),
                body.logicalIdentity(),
                body.canonicalBytes(),
                AuthorIdentity.control(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                        .canonicalBytes(),
                version,
                keys.getPrivate());
    }

    private Prepared prepare(
            final TargetQuotaGrantControlRequest request,
            final byte[] operation,
            final ControlAuthorizationContext actor,
            final KeyPair keys,
            final int version) {
        final var mutation = signed(body(request, operation, 500), actor, keys, version);
        final var target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(source.shardId()),
                mutation.systemMutationId(),
                mutation.mutationHash());
        final var prepared = PreparedControlOperation.prepare(
                operation,
                request.operationKind(),
                new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash()),
                request.operationRequest(),
                List.of(target),
                1,
                400,
                Integer.toUnsignedLong(version),
                keys.getPrivate());
        return new Prepared(prepared, mutation);
    }

    private record Prepared(PreparedControlOperation prepared, SystemMutation mutation) {}

    private record DecodeCase(String name, Function<byte[], ?> decoder) {}

    private KafkaSourcePosition at(final long sequence) {
        return new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                9 + sequence,
                null,
                99 + sequence);
    }

    private KeyPair fixedKey() {
        try {
            final var factory = KeyFactory.getInstance("Ed25519");
            return new KeyPair(
                    factory.generatePublic(new X509EncodedKeySpec(Bytes.concat(
                            HexFormat.of().parseHex("302a300506032b6570032100"), hex("target.initial.public")))),
                    factory.generatePrivate(new PKCS8EncodedKeySpec(
                            Bytes.concat(HexFormat.of().parseHex("302e020100300506032b657004220420"), hex("seed")))));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void maximum(final String name, final byte[] raw, final int bound) {
        assertEquals(Integer.parseInt(vectors.getProperty(name + ".length")), raw.length);
        assertEquals(vectors.getProperty(name + ".sha256"), HexFormat.of().formatHex(Bytes.sha256(raw)));
        assertTrue(raw.length <= bound);
    }

    private static byte[] rewrite(final byte[] encoded, final int number, final byte[] replacement) {
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final var fields = new ArrayList<CanonicalProtobuf.Reader.Field>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return CanonicalProtobuf.message(out -> {
            boolean written = false;
            for (var field : fields) {
                if (!written && field.number() >= number) {
                    out.writeBytes(replacement);
                    written = true;
                }
                if (field.number() == number) {
                    continue;
                }
                if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(out, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(out, field.number(), field.rawValue());
                }
            }
            if (!written) {
                out.writeBytes(replacement);
            }
        });
    }

    private static byte[] uint(final int field, final long value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.uint64Bits(out, field, value));
    }

    private static byte[] field(final int field, final byte[] value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, field, value));
    }

    private static byte[] repeat(final int count, final int value) {
        final byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private byte[] hex(final String key) {
        return HexFormat.of().parseHex(vectors.getProperty(key));
    }

    private static Properties load() {
        final var result = new Properties();
        try (var input =
                TargetQuotaGrantControlTest.class.getResourceAsStream("/ndip3/target-quota-grant-vectors.properties")) {
            result.load(input);
            return result;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
