package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.CommandResolutionException;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.runtime.TargetMembershipAuthority;
import com.nereusstream.delay.runtime.TargetMembershipControlVerifier;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetMembershipControlTest {
    @TempDir
    private Path temporary;

    private final Properties vectors = load("target-membership-control-vectors.properties");
    private final Properties compat = load("target-compatibility-vectors.properties");
    private final Properties prior = load("target-binding-channel-vectors.properties");
    private final KafkaSourcePosition source = (KafkaSourcePosition) TargetSourcePosition.decode(hex(prior, "source"));
    private final CanonicalTargetPartition physical = CanonicalTargetPartition.decode(hex(compat, "pulsar.target"));
    private final TargetDispatchCompatibility dispatch =
            TargetDispatchCompatibility.decode(hex(compat, "pulsar.journal.dispatch"));
    private final TargetControlScope controls = TargetControlScope.decode(hex(compat, "scope.shared"));
    private final ProfileSemanticEnvelope capability =
            new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap"), 1, dispatch.capability());
    private final ProfileSemanticEnvelope destination = new ProfileSemanticEnvelope(
            ProfileKind.DESTINATION,
            Bytes.utf8("member"),
            1,
            new DestinationProfileSemantic(
                    AdapterKind.PULSAR,
                    physical.resource(),
                    8,
                    TargetPartitionPolicy.EXPLICIT_ONLY,
                    TargetPartitionHashInput.DELAY_MESSAGE_ID,
                    List.of(5),
                    capability.ref(),
                    3,
                    60000,
                    repeated(32, 0xAA),
                    20000,
                    10000,
                    10000,
                    1,
                    Bytes.utf8("member"),
                    86400000,
                    172800000,
                    2,
                    repeated(32, 0xBB)));
    private final TargetMembershipPolicy policy = policy(destination.ref(), dispatch, controls);
    private final ControlRoleSet roles =
            ControlRoleSet.of(ControlRole.TENANT_POLICY_ADMINISTRATOR, ControlRole.PLATFORM_OPERATOR);
    private final ControlAuthorizationContext actor = actor(roles);
    private final KeyPair key = key();
    private final InMemoryControlTargetRegistrationAuthority registrations =
            new InMemoryControlTargetRegistrationAuthority();

    @Test
    void fourPoliciesAndBothControlBodiesMatchIndependentHashesAndWire() {
        for (String name : List.of("kafka.baseline", "kafka.receipt", "pulsar.baseline", "pulsar.journal")) {
            final var contract = TargetDispatchCompatibility.decode(hex(compat, name + ".dispatch"));
            final var scope = new TargetControlScope(
                    contract.target(), source.shardId(), controls.controls(), controls.permits());
            final var value = policy(
                    new ProfileRef(Bytes.utf8("dest"), 1, repeated(32, 0x61), ProfileKind.DESTINATION),
                    contract,
                    scope);
            assertArrayEquals(hex(vectors, name + ".policy"), value.canonicalBytes());
            assertArrayEquals(hex(vectors, name + ".key"), value.encodedKey());
            assertEquals(
                    value,
                    TargetMembershipPolicy.decodeForStore(
                            value.encodedKey(), value.canonicalBytes(), source.shardId()));
        }
        final var policy = TargetMembershipPolicy.decode(hex(vectors, "pulsar.journal.policy"));
        final byte[] reg = TargetMembershipGrant.prepareRegistration(
                policy.tenantScope(),
                policy.memberProfile(),
                dispatch,
                dispatch,
                controls,
                policy.digest(),
                repeated(32, 0x72));
        assertArrayEquals(hex(vectors, "registration"), reg);
        for (String label : List.of("issue", "close")) {
            final var request = label.equals("issue")
                    ? TargetMembershipControlRequest.issue(policy, reg)
                    : TargetMembershipControlRequest.close(policy, repeated(32, 0x76), reason());
            final var body = body(request, repeated(32, label.equals("issue") ? 0x72 : 0x75));
            assertArrayEquals(hex(vectors, label + ".request"), request.canonicalBytes());
            assertArrayEquals(
                    hex(vectors, label + ".outer"), request.operationRequest().canonicalBytes());
            assertArrayEquals(
                    hex(vectors, label + ".requestHash"), body.controlRef().requestHash());
            assertArrayEquals(hex(vectors, label + ".ref"), body.controlRef().canonicalBytes());
            assertArrayEquals(hex(vectors, label + ".body"), body.canonicalBytes());
            assertArrayEquals(hex(vectors, label + ".semanticHash"), body.semanticHash());
            assertArrayEquals(hex(vectors, label + ".logical"), body.logicalIdentity());
            assertArrayEquals(
                    hex(vectors, label + ".mutationHash"),
                    SystemMutation.computeMutationHash(
                            source.shardId(), SystemMutationType.APPLY_SHARD_CONTROL, 500, body.canonicalBytes()));
            assertArrayEquals(
                    hex(vectors, label + ".mutationId"),
                    SystemMutation.computeSystemMutationId(
                            source.shardId(),
                            SystemMutationType.APPLY_SHARD_CONTROL,
                            body.logicalIdentity(),
                            hex(vectors, label + ".mutationHash")));
            assertArrayEquals(
                    body.canonicalBytes(),
                    TargetMembershipControlBody.decode(body.canonicalBytes()).canonicalBytes());
            assertEquals(
                    request.operationRequest(),
                    ControlOperationRequest.decode(request.operationRequest().canonicalBytes()));
        }
    }

    @Test
    void approvedRegisteredSignedIssueMaterializesExactSourceGrantAndClosePreservesHistory() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, 1);
        registrations.register(issue.prepared);
        final var granted = verify(issue, at(6), ref -> null, authority(policy, actor, true, true));
        assertEquals(TargetMembershipControlVerifier.Action.GRANT, granted.action());
        final var grant = granted.after().grant();
        assertArrayEquals(issue.mutation.mutationHash(), grant.sourceMutationDigest());
        assertArrayEquals(at(6).canonicalBytes(), grant.activationSource().canonicalBytes());
        assertDoesNotThrow(() -> policy.requireGrant(grant));
        final var close = prepare(
                TargetMembershipControlRequest.close(policy, grant.digest(), reason()),
                repeated(32, 0x75),
                actor,
                key,
                1);
        registrations.register(close.prepared);
        final var closed = verify(close, at(8), ref -> granted.after(), authority(policy, actor, true, true));
        assertEquals(TargetMembershipControlVerifier.Action.CLOSE, closed.action());
        assertEquals(granted.after(), closed.before());
        assertEquals(grant, closed.after().grant());
        assertEquals(at(8), closed.after().closedAt());
        assertTrue(closed.after().allowsFirstBinding(at(7)));
        final var repeated = verify(close, at(9), ref -> closed.after(), authority(policy, actor, true, true));
        assertEquals(TargetMembershipControlVerifier.Action.ALREADY_CLOSED, repeated.action());
        assertEquals(closed.after(), repeated.after());
    }

    @Test
    void exactImmutableTargetRegistrationAndBothSignaturesAreMandatory() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, 1);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(issue, at(6), ref -> null, authority(policy, actor, true, true)));
        registrations.register(issue.prepared);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(issue, at(6), ref -> null, authority(policy, actor, true, false)));
        final var untrusted = prepare(issue(policy), repeated(32, 0x72), actor, key(), 1);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(untrusted, at(6), ref -> null, authority(policy, actor, true, true)));
        final var unknownVersion = prepare(issue(policy), repeated(32, 0x72), actor, key, 2);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(unknownVersion, at(6), ref -> null, authority(policy, actor, true, true)));
        assertThrows(IllegalArgumentException.class, () -> registrations.register(untrusted.prepared));
    }

    @Test
    void registeredBodyHashCannotHideAMutationAuthorReplacement() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, 1);
        registrations.register(issue.prepared);
        final byte[] replacedAuthor = AuthorIdentity.control(
                        repeated(32, 0x79), roles.digest(), actor.tenantResourceScopeHash())
                .canonicalBytes();
        final var replacement = SystemMutation.signed(
                source.shardId(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                500,
                issue.mutation.logicalOperationIdentity(),
                issue.mutation.canonicalBody(),
                replacedAuthor,
                1,
                key.getPrivate());
        assertArrayEquals(issue.mutation.systemMutationId(), replacement.systemMutationId());
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                assertThrows(
                                CommandResolutionException.class,
                                () -> verify(
                                        new Prepared(issue.prepared, replacement),
                                        at(6),
                                        ref -> null,
                                        authority(policy, actor, true, true)))
                        .stableCode());
    }

    @Test
    void retainedControlSigningKeyVersionsKeepUnsignedBitIdentity() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, -1);
        registrations.register(issue.prepared);
        assertEquals(0xffff_ffffL, issue.prepared.signingKeyVersion());
        assertEquals(-1, issue.mutation.signingKeyVersion());
        final var original = authority(policy, actor, true, true);
        final var retained = new TargetMembershipControlVerifier.Authority(
                registrations,
                original.policies(),
                (version, at) -> version == -1 ? key.getPublic() : null,
                original.profiles(),
                actor,
                original.scopeProof());
        assertEquals(
                TargetMembershipControlVerifier.Action.GRANT,
                verify(issue, at(6), ref -> null, retained).action());
        final var unavailable = new TargetMembershipControlVerifier.Authority(
                registrations, original.policies(), retained.keys(), original.profiles(), actor, prepared -> {
                    throw new IllegalStateException("scope view unavailable");
                });
        assertThrows(IllegalStateException.class, () -> verify(issue, at(6), ref -> null, unavailable));
    }

    @Test
    void onlyAnExactApprovedPolicyCanEstablishCompletenessAndSharedCredentialPermission() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, 1);
        registrations.register(issue.prepared);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(issue, at(6), ref -> null, authority(null, actor, true, true)));
        for (var scope : List.of(
                new TargetControlScope(physical.id(), source.shardId(), List.of(), controls.permits()),
                new TargetControlScope(physical.id(), source.shardId(), controls.controls(), List.of()))) {
            final var forged = policy(destination.ref(), dispatch, scope);
            final var request = issue(forged);
            assertThrows(IllegalArgumentException.class, () -> policy.requireRegistration(request.value()));
            final var otherRegistrations = new InMemoryControlTargetRegistrationAuthority();
            final var prepared = prepare(request, repeated(32, 0x72), actor, key, 1);
            otherRegistrations.register(prepared.prepared);
            final var original = authority(policy, actor, true, true);
            final var authority = new TargetMembershipControlVerifier.Authority(
                    otherRegistrations,
                    original.policies(),
                    original.keys(),
                    original.profiles(),
                    actor,
                    original.scopeProof());
            assertThrows(CommandResolutionException.class, () -> verify(prepared, at(6), ref -> null, authority));
        }
    }

    @Test
    void actorRolesResourceProofAndPolicyScopeCannotBeSubstituted() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, 1);
        registrations.register(issue.prepared);
        assertThrows(
                IllegalArgumentException.class,
                () -> verify(issue, at(6), ref -> null, authority(policy, actor, false, true)));
        for (ControlRole missing : List.of(ControlRole.TENANT_POLICY_ADMINISTRATOR, ControlRole.PLATFORM_OPERATOR)) {
            final var lowActor = actor(ControlRoleSet.of(missing));
            final var low = prepare(issue(policy), repeated(32, 0x72), lowActor, key, 1);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ControlOperationAuthorization.authorize(low.prepared, lowActor, p -> true));
        }
        final var foreignActor =
                new ControlAuthorizationContext(repeated(32, 0x79), roles, actor.tenantResourceScopeHash());
        assertThrows(
                IllegalArgumentException.class,
                () -> verify(issue, at(6), ref -> null, authority(policy, foreignActor, true, true)));
        final var wrongScope = new TargetMembershipPolicy(
                policy.tenantScope(),
                policy.memberProfile(),
                policy.requiredDispatchRef(),
                dispatch,
                controls,
                repeated(32, 0x78));
        final var prepared = prepare(issue(wrongScope), repeated(32, 0x72), actor, key, 1);
        final var separate = new InMemoryControlTargetRegistrationAuthority();
        separate.register(prepared.prepared);
        final var a = authority(wrongScope, actor, true, true);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(
                        prepared,
                        at(6),
                        ref -> null,
                        new TargetMembershipControlVerifier.Authority(
                                separate, a.policies(), a.keys(), a.profiles(), actor, a.scopeProof())));
    }

    @Test
    void exactProfilesAndPhysicalTargetRemainRequiredAndCatalogFailuresPropagate() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, 1);
        registrations.register(issue.prepared);
        final var a = authority(policy, actor, true, true);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(
                        issue,
                        at(6),
                        ref -> null,
                        new TargetMembershipControlVerifier.Authority(
                                registrations, a.policies(), a.keys(), catalog(false), actor, a.scopeProof())));
        assertThrows(
                IllegalStateException.class,
                () -> verify(
                        issue,
                        at(6),
                        ref -> null,
                        new TargetMembershipControlVerifier.Authority(
                                registrations,
                                (ref, source, kind) -> {
                                    throw new IllegalStateException("policy view unavailable");
                                },
                                a.keys(),
                                a.profiles(),
                                actor,
                                a.scopeProof())));
        final var wrong = CanonicalTargetPartition.decode(hex(compat, "kafka.target"));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipControlVerifier.verifyFirstApplication(
                        issue.prepared, issue.mutation, at(6), wrong, ref -> null, a));
    }

    @Test
    void closureRequiresExactHistoricalGrantAndComparableSourceAndCannotEraseEarlierClosure() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, 1);
        registrations.register(issue.prepared);
        final var grant = verify(issue, at(6), ref -> null, authority(policy, actor, true, true))
                .after();
        final var close = prepare(
                TargetMembershipControlRequest.close(policy, grant.grant().digest(), reason()),
                repeated(32, 0x75),
                actor,
                key,
                1);
        registrations.register(close.prepared);
        assertThrows(
                CommandResolutionException.class,
                () -> verify(close, at(8), ref -> null, authority(policy, actor, true, true)));
        assertThrows(
                CommandResolutionException.class,
                () -> verify(close, at(6), ref -> grant, authority(policy, actor, true, true)));
        final var foreign =
                new KafkaSourcePosition(source.shardId(), source.authenticatedClusterId(), UUID.randomUUID(), 8, 4, 90);
        assertThrows(
                IllegalArgumentException.class,
                () -> verify(close, foreign, ref -> grant, authority(policy, actor, true, true)));
        final var closedLater = new TargetMembershipAuthority.AppliedGrant(grant.grant(), at(10));
        assertThrows(
                IllegalArgumentException.class,
                () -> verify(close, at(8), ref -> closedLater, authority(policy, actor, true, true)));
        final var expired = new KafkaSourcePosition(
                source.shardId(), source.authenticatedClusterId(), source.nativeTopicUuid(), 8, 4, 501);
        assertEquals(
                StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                assertThrows(
                                CommandResolutionException.class,
                                () -> verify(close, expired, ref -> grant, authority(policy, actor, true, true)))
                        .stableCode());
    }

    @Test
    void requestOperationSourceIndexAndBodyPreconditionsCannotBeRelabeled() {
        final var request = issue(policy);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlOperationRequest(ControlOperationKind.CLOSE_TARGET_MEMBERSHIP, request));
        assertThrows(IllegalArgumentException.class, () -> body(request, repeated(32, 0x75)));
        final var hash = PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest());
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMembershipControlBody(
                        source.shardId(), 500, new ControlRef(repeated(32, 0x72), hash, 1), request));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMembershipControlBody(
                        new ShardId(source.shardId().routeIncarnation(), 4),
                        500,
                        new ControlRef(repeated(32, 0x72), hash, 0),
                        request));
        final var issue = prepare(request, repeated(32, 0x72), actor, key, 1);
        final var wrongTarget = new ControlTargetRef(
                1,
                ControlTargetKind.SHARD,
                new ShardSubject(source.shardId()),
                issue.mutation.systemMutationId(),
                issue.mutation.mutationHash());
        assertThrows(
                IllegalArgumentException.class,
                () -> PreparedControlOperation.prepare(
                        repeated(32, 0x72),
                        request.operationKind(),
                        author(actor),
                        request.operationRequest(),
                        List.of(wrongTarget),
                        1,
                        500,
                        1,
                        key.getPrivate()));
        final byte[] close = hex(vectors, "close.body");
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipControlBody.decode(rewrite(close, 14, uint(14, 1))));
        assertThrows(
                IllegalArgumentException.class, () -> TargetMembershipControlBody.decode(rewrite(close, 14, null)));
    }

    @Test
    void malformedUnknownAndOversizedPolicyAndControlFieldsFailClosed() {
        final byte[] encoded = policy.canonicalBytes();
        for (int n = 1; n <= 9; n++) {
            final int field = n;
            assertThrows(
                    IllegalArgumentException.class, () -> TargetMembershipPolicy.decode(rewrite(encoded, field, null)));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipPolicy.decode(new byte[TargetMembershipPolicy.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class, () -> TargetMembershipPolicy.decode(rewrite(encoded, 7, uint(7, 2))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipPolicy.decode(Bytes.concat(encoded, uint(10, 1))));
        for (String label : List.of("issue", "close")) {
            final byte[] body = hex(vectors, label + ".body");
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetMembershipControlBody.decode(rewrite(body, 12, uint(12, 2))));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetMembershipControlBody.decode(rewrite(body, 13, field(13, new byte[32]))));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetMembershipControlBody.decode(Bytes.concat(body, uint(16, 1))));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipControlBody.decode(
                        new byte[TargetMembershipControlBody.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipControlRequest.decode(
                        ControlOperationKind.GRANT_TARGET_MEMBERSHIP,
                        new byte[TargetMembershipControlRequest.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void maximumPolicyRequestAndSourceBodyMatchIndependentBounds() {
        final byte[] token = new byte[32];
        for (int n = 0; n < 32; n++) {
            token[n] = (byte) n;
        }
        final var evidence = BrokerResourceIdentity.pulsar(
                new PulsarBrokerResourceIdentity("x".repeat(256), token, "x".repeat(1 << 20), Long.MAX_VALUE));
        final var cap = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.PULSAR_BROKER_DEDUP,
                7,
                evidence,
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                repeated(32, 0x22),
                repeated(32, 0x33),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE);
        final var dispatch = new TargetDispatchCompatibility(
                physical.id(),
                repeated(32, 0xAA),
                Integer.MAX_VALUE,
                cap,
                repeated(32, 0xBB),
                Long.MAX_VALUE,
                Long.MAX_VALUE);
        final var groups = new ArrayList<TargetControlScope.ControlGroup>();
        final var permits = new ArrayList<byte[]>();
        for (int n = 1; n <= 32; n++) {
            final byte[] group = new byte[32];
            group[31] = (byte) n;
            groups.add(new TargetControlScope.ControlGroup(TargetControlScope.GroupKind.BINDING_SEND_CONTROL, group));
            final byte[] permit = new byte[32];
            permit[31] = (byte) (n + 32);
            permits.add(permit);
        }
        final var controls = new TargetControlScope(physical.id(), source.shardId(), groups, permits);
        final var policy = policy(
                new ProfileRef(Bytes.utf8("x".repeat(256)), -1, repeated(32, 0x61), ProfileKind.DESTINATION),
                dispatch,
                controls);
        final var request = TargetMembershipControlRequest.issue(
                policy,
                TargetMembershipGrant.prepareRegistration(
                        policy.tenantScope(),
                        policy.memberProfile(),
                        dispatch,
                        dispatch,
                        controls,
                        policy.digest(),
                        repeated(32, 0x72)));
        final var ref = new ControlRef(
                repeated(32, 0x72),
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body = new TargetMembershipControlBody(source.shardId(), Long.MAX_VALUE, ref, request);
        checkMaximum("policy", policy.canonicalBytes(), TargetMembershipPolicy.MAX_CANONICAL_BYTES);
        checkMaximum("request", request.canonicalBytes(), TargetMembershipControlRequest.MAX_CANONICAL_BYTES);
        checkMaximum("body", body.canonicalBytes(), TargetMembershipControlBody.MAX_CANONICAL_BYTES);
        assertEquals(policy, TargetMembershipPolicy.decode(policy.canonicalBytes()));
        assertArrayEquals(
                body.canonicalBytes(),
                TargetMembershipControlBody.decode(body.canonicalBytes()).canonicalBytes());
    }

    @Test
    void legacySemanticReaderRejectsNewControlKindsAndNv23DespiteValidEnvelope() {
        for (String label : List.of("issue", "close")) {
            assertThrows(
                    IllegalArgumentException.class, () -> ApplyShardControlBody.decode(hex(vectors, label + ".body")));
        }
        final byte[] value = hex(vectors, "policy.value");
        assertEquals(TargetMembershipPolicy.VALUE_TYPE, Byte.toUnsignedInt(value[2]));
        assertEquals(Bytes.crc32c(value, 0, value.length - 4), Bytes.readU32be(value, value.length - 4));
        assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(value));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipPolicy.decodeForStore(
                        TargetKeyCodec.membershipGrant(policy.digest()), policy.canonicalBytes(), source.shardId()));
    }

    @Test
    void activeLaneStoreRejectsTargetMembershipMutationWithoutInstallingNewState() {
        final var issue = prepare(issue(policy), repeated(32, 0x72), actor, key, 1);
        final var config = ShardStoreConfig.defaults(temporary.resolve("lane-store"));
        final var prospective =
                TargetMembershipGrant.fromRegistration(issue(policy).value(), issue.mutation.mutationHash(), at(6));
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.open(config, source.shardId(), resources)) {
            final var shard = new DelayShard(store, DelayShardConfig.defaults());
            final var result = shard.applySystemMutation(issue.mutation, at(6), key.getPublic());
            assertEquals(ApplyStatus.REJECTED, result.applyStatus());
            assertEquals(StableCode.STALE_SYSTEM_MUTATION, result.stableCode());
            assertEquals(null, store.get(ColumnFamily.META, policy.encodedKey()));
            assertEquals(null, store.get(ColumnFamily.META, prospective.encodedKey()));
            assertEquals(result, shard.getSystemMutationResult(issue.mutation.systemMutationId()));
        }
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.open(config, source.shardId(), resources)) {
            final var recovered = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(
                    ApplyStatus.REJECTED,
                    recovered
                            .getSystemMutationResult(issue.mutation.systemMutationId())
                            .applyStatus());
            assertEquals(null, store.get(ColumnFamily.META, policy.encodedKey()));
            assertEquals(null, store.get(ColumnFamily.META, prospective.encodedKey()));
        }
    }

    @Test
    void policyAndRequestBytesAreImmutableAndProviderModeCannotBeRemoved() {
        final byte[] before = policy.canonicalBytes();
        for (byte[] returned : List.of(
                policy.tenantScope(),
                policy.requiredDispatchRef(),
                policy.controlResourceScope(),
                policy.digest(),
                policy.canonicalBytes())) {
            returned[0] ^= 1;
        }
        assertArrayEquals(before, policy.canonicalBytes());
        final var request = issue(policy);
        final byte[] bytes = request.canonicalBytes();
        request.value()[0] ^= 1;
        assertArrayEquals(bytes, request.canonicalBytes());
        assertNotEquals(request, TargetMembershipControlRequest.close(policy, repeated(32, 0x76), reason()));
        assertThrows(IllegalArgumentException.class, () -> TargetMembershipPolicy.decode(rewrite(before, 7, null)));
    }

    private void checkMaximum(final String name, final byte[] value, final int bound) {
        assertEquals(Integer.parseInt(vectors.getProperty("maximum." + name + ".length")), value.length);
        assertArrayEquals(hex(vectors, "maximum." + name + ".sha256"), Bytes.sha256(value));
        assertTrue(value.length <= bound);
    }

    private TargetMembershipControlVerifier.Change verify(
            final Prepared value,
            final SourcePosition source,
            final TargetMembershipAuthority membership,
            final TargetMembershipControlVerifier.Authority authority) {
        return TargetMembershipControlVerifier.verifyFirstApplication(
                value.prepared, value.mutation, source, physical, membership, authority);
    }

    private TargetMembershipControlVerifier.Authority authority(
            final TargetMembershipPolicy approved,
            final ControlAuthorizationContext actor,
            final boolean scope,
            final boolean trustedKey) {
        return new TargetMembershipControlVerifier.Authority(
                registrations,
                (ref, position, operation) -> approved,
                (version, position) -> version == 1 && trustedKey ? key.getPublic() : null,
                catalog(true),
                actor,
                p -> scope);
    }

    private ProfileCatalog catalog(final boolean available) {
        return new ProfileCatalog() {
            @Override
            public ProfileSemanticEnvelope resolve(final ProfileRef ref) {
                if (!available) {
                    return null;
                }
                return ref.equals(destination.ref()) ? destination : ref.equals(capability.ref()) ? capability : null;
            }

            @Override
            public CredentialBinding resolveBinding(final ProfileRef ref, final long generation) {
                throw new AssertionError("membership issuance must not depend on a private credential");
            }

            @Override
            public CredentialBindingHead resolveHead(final ProfileRef ref) {
                throw new AssertionError("membership issuance must not depend on a private credential");
            }

            @Override
            public CredentialBindingProtection resolveProtection(final ProfileRef ref, final long generation) {
                throw new AssertionError("membership issuance must not depend on a private credential");
            }
        };
    }

    private Prepared prepare(
            final TargetMembershipControlRequest request,
            final byte[] operation,
            final ControlAuthorizationContext actor,
            final KeyPair key,
            final int version) {
        final var body = body(request, operation);
        final byte[] author = AuthorIdentity.control(
                        actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                .canonicalBytes();
        final var mutation = SystemMutation.signed(
                source.shardId(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                500,
                body.logicalIdentity(),
                body.canonicalBytes(),
                author,
                version,
                key.getPrivate());
        final var target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(source.shardId()),
                mutation.systemMutationId(),
                mutation.mutationHash());
        final var prepared = PreparedControlOperation.prepare(
                operation,
                request.operationKind(),
                author(actor),
                request.operationRequest(),
                List.of(target),
                1,
                400,
                Integer.toUnsignedLong(version),
                key.getPrivate());
        assertEquals(prepared, PreparedControlOperation.decode(prepared.canonicalBytes()));
        final var factory = ControlSystemMutationFactory.sign(
                prepared, target, source.shardId(), 500, body.canonicalBytes(), author, version, key.getPrivate());
        assertArrayEquals(mutation.canonicalEnvelope(), factory.canonicalEnvelope());
        return new Prepared(prepared, factory);
    }

    private record Prepared(PreparedControlOperation prepared, SystemMutation mutation) {}

    private TargetMembershipControlRequest issue(final TargetMembershipPolicy policy) {
        return TargetMembershipControlRequest.issue(
                policy,
                TargetMembershipGrant.prepareRegistration(
                        policy.tenantScope(),
                        policy.memberProfile(),
                        dispatch,
                        policy.offered(),
                        policy.controls(),
                        policy.digest(),
                        repeated(32, 0x72)));
    }

    private TargetMembershipControlBody body(final TargetMembershipControlRequest request, final byte[] operation) {
        return new TargetMembershipControlBody(
                source.shardId(),
                500,
                new ControlRef(
                        operation,
                        PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                        0),
                request);
    }

    private static TargetMembershipPolicy policy(
            final ProfileRef profile, final TargetDispatchCompatibility dispatch, final TargetControlScope scope) {
        return new TargetMembershipPolicy(
                repeated(32, 0x70), profile, dispatch.digest(), dispatch, scope, repeated(32, 0x74));
    }

    private ControlAuthorizationContext actor(final ControlRoleSet roles) {
        return new ControlAuthorizationContext(repeated(32, 0x77), roles, repeated(32, 0x74));
    }

    private static ControlAuthor author(final ControlAuthorizationContext actor) {
        return new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash());
    }

    private KafkaSourcePosition at(final long offset) {
        return new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                offset,
                source.leaderEpoch(),
                source.brokerLogAppendTimeEpochMs());
    }

    private static ControlReason reason() {
        return new ControlReason(ControlReasonKind.POLICY_CHANGE, null, null);
    }

    private static KeyPair key() {
        try {
            return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] repeated(final int n, final int value) {
        final byte[] b = new byte[n];
        Arrays.fill(b, (byte) value);
        return b;
    }

    private static byte[] field(final int n, final byte[] b) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, n, b));
    }

    private static byte[] uint(final int n, final long value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.uint64Bits(out, n, value));
    }

    private static byte[] rewrite(final byte[] value, final int number, final byte[] replacement) {
        return CanonicalProtobuf.message(out -> {
            for (var f : QueryCodecSupport.read(value, "test rewrite")) {
                if (f.number() == number) {
                    if (replacement != null) {
                        out.writeBytes(replacement);
                    }
                } else if (f.wireType() == 2) {
                    CanonicalProtobuf.bytes(out, f.number(), f.rawValue());
                } else {
                    CanonicalProtobuf.uint64Bits(out, f.number(), f.unsignedValue());
                }
            }
        });
    }

    private static byte[] hex(final Properties p, final String key) {
        return HexFormat.of().parseHex(p.getProperty(key));
    }

    private static Properties load(final String name) {
        try (var in = TargetMembershipControlTest.class.getResourceAsStream("/ndip3/" + name)) {
            final var p = new Properties();
            p.load(in);
            return p;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
