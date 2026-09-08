package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetQuotaMetadataRecordsTest {
    private final CanonicalTargetPartition physical =
            CanonicalTargetPartition.decode(raw("target-compatibility", "pulsar.target"));
    private final TargetMembershipGrant membership =
            TargetMembershipGrant.decode(raw("target-membership", "pulsar.baseline.grant"));
    private final TargetMembershipPolicy policy =
            TargetMembershipPolicy.decode(raw("target-membership-control", "pulsar.baseline.policy"));
    private final KafkaSourcePosition source = (KafkaSourcePosition) membership.activationSource();
    private final TargetQuotaAccounting accounting = new TargetQuotaAccounting(bytes(32, 0xbb), 32, 24, 40, 64);
    private final TargetQuotaScope scope =
            new TargetQuotaScope(source.shardId(), membership.tenantScope(), physical.id());
    private final TargetQuotaIncarnation owner = allocate(scope, 1, 0);
    private final TargetQuotaGrantActivation activation = activate(owner);

    @Test
    void fiveSharedRolesUseTheInitialOriginAndExactIndependentRecordBytes() {
        for (var role : TargetQuotaMetadataRecords.SharedRole.values()) {
            final var value = shared(role, owner, activation);
            assertFee(value);
            assertEquals(0, value.contribution().targets());
            assertEquals(0, value.contribution().executionDomains());
            assertEquals(0, value.contribution().strictOrderDomains());
            assertEquals(0, value.contribution().accountingIncarnations());
            final var data = view(value, owner, activation);
            final var calls = new AtomicInteger();
            value.requireCurrent(key -> {
                calls.incrementAndGet();
                return data.get(Bytes.hex(key));
            });
            assertEquals(value.readSetSize(), calls.get());
            assertEquals(role == TargetQuotaMetadataRecords.SharedRole.IDENTITY ? 3 : 4, calls.get());
            assertTrue(calls.get() <= TargetQuotaMetadataRecords.MAX_READ_RECORDS);
        }
    }

    @Test
    void everySharedReadSetDependencyIsMandatoryAndItsTypeAndBytesAreExact() {
        final var value = shared(TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_GRANT, owner, activation);
        final var data = view(value, owner, activation);
        for (var key : List.copyOf(data.keySet())) {
            final var original = data.remove(key);
            assertThrows(IllegalStateException.class, () -> value.requireCurrent(k -> data.get(Bytes.hex(k))));
            data.put(key, new TargetQuotaMetadataRecords.Stored(original.valueType() + 1, original.canonicalPayload()));
            assertThrows(IllegalStateException.class, () -> value.requireCurrent(k -> data.get(Bytes.hex(k))));
            final byte[] altered = original.canonicalPayload();
            altered[altered.length - 1] ^= 1;
            data.put(key, new TargetQuotaMetadataRecords.Stored(original.valueType(), altered));
            assertThrows(IllegalStateException.class, () -> value.requireCurrent(k -> data.get(Bytes.hex(k))));
            data.put(key, original);
        }
        value.requireCurrent(k -> data.get(Bytes.hex(k)));
    }

    @Test
    void readFailuresPropagateAndReturnedBytesCannotMutateThePlan() {
        final var value = shared(TargetQuotaMetadataRecords.SharedRole.IDENTITY, owner, activation);
        final byte[] key = value.key(), payload = value.canonicalPayload();
        key[0] ^= 1;
        payload[0] ^= 1;
        final var data = view(value, owner, activation);
        value.requireCurrent(k -> {
            final var result = data.get(Bytes.hex(k));
            k[0] ^= 1;
            result.canonicalPayload()[0] ^= 1;
            return result;
        });
        assertArrayEquals(physical.canonicalBytes(), value.canonicalPayload());
        assertThrows(
                IllegalStateException.class,
                () -> value.requireCurrent(k -> {
                    throw new IllegalStateException("unproven read");
                }));
        assertThrows(
                AssertionError.class,
                () -> value.requireCurrent(k -> {
                    throw new AssertionError("failed Store");
                }));
    }

    @Test
    void sharedMetadataCannotMoveToANewerIncarnationOfTheSameTarget() {
        final var replacement = allocate(scope, 2, 1);
        for (var role : TargetQuotaMetadataRecords.SharedRole.values()) {
            assertThrows(IllegalArgumentException.class, () -> shared(role, replacement, activation));
        }
        final var foreign = allocate(scope.forTarget(new TargetPartitionId(bytes(32, 0x42))), 1, 0);
        assertThrows(
                IllegalArgumentException.class,
                () -> shared(TargetQuotaMetadataRecords.SharedRole.IDENTITY, foreign, activate(foreign)));
        final var foreignTenant = allocate(new TargetQuotaScope(scope.shard(), bytes(32, 0xab), scope.target()), 1, 0);
        assertThrows(
                IllegalArgumentException.class,
                () -> shared(
                        TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_GRANT,
                        foreignTenant,
                        activate(foreignTenant)));
        assertThrows(
                IllegalArgumentException.class,
                () -> shared(
                        TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_POLICY,
                        foreignTenant,
                        activate(foreignTenant)));
    }

    @Test
    void grantDownToZeroAndDescriptorDrainKeepSharedRecordChargesButInvalidateOldViews() {
        final var drained = owner.drain(stamp(2, 1), (p, n) -> {});
        final var zeroGrant = new TargetQuotaGrant(
                scope, activation.grant().grantId(), 2, accounting, TargetQuotaUsage.empty(), 1, bytes(32, 0x88));
        final var zero =
                activation(new TargetQuotaGrantControlRequest(zeroGrant, activation.grant(), null), owner, stamp(3, 2));
        final var before = shared(TargetQuotaMetadataRecords.SharedRole.DISPATCH, owner, activation);
        final var after = shared(TargetQuotaMetadataRecords.SharedRole.DISPATCH, drained, zero);
        assertEquals(before.contribution(), after.contribution());
        final var current = view(after, drained, zero);
        after.requireCurrent(k -> current.get(Bytes.hex(k)));
        assertThrows(IllegalStateException.class, () -> before.requireCurrent(k -> current.get(Bytes.hex(k))));
    }

    @Test
    void membershipSourceMustStrictlyFollowAllocationAndUseTheSamePhysicalSource() {
        final var laterOwner = allocate(scope, 1, source.offset());
        assertThrows(
                IllegalArgumentException.class,
                () -> shared(TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_GRANT, laterOwner, activate(laterOwner)));
        final var foreignSource = new KafkaSourcePosition(
                source.shardId(), source.authenticatedClusterId(), java.util.UUID.randomUUID(), 0, null, 100);
        final var foreignOwner = TargetQuotaIncarnation.allocate(
                scope,
                accounting,
                bytes(16, 0xcc),
                new TargetQuotaMutation(1, foreignSource, bytes(32, 0x66)),
                (p, n) -> {});
        assertThrows(
                IllegalArgumentException.class,
                () -> shared(
                        TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_GRANT, foreignOwner, activate(foreignOwner)));
    }

    @Test
    void closedQueueAndStrictDomainOwnCardinalityWithoutCountingDependencies() {
        final var domains = List.of(
                domain(0, TargetDomainState.Lifecycle.ACTIVE),
                domain(1, TargetDomainState.Lifecycle.DRAINING),
                domain(2, TargetDomainState.Lifecycle.VACANT));
        final var queue = new TargetQueueState(
                physical.id(),
                1,
                1,
                TargetQueueState.AdmissionState.CLOSED,
                owner.identity().accountingIncarnation(),
                0,
                domains);
        final var queued = TargetQuotaMetadataRecords.queue(
                owner, physical, 3, TargetKeyCodec.state(physical.id()), queue.canonicalBytes());
        assertFee(queued);
        assertEquals(1, queued.contribution().targets());
        assertEquals(2, queued.contribution().executionDomains());
        assertEquals(0, queued.contribution().accountingIncarnations());
        assertEquals(3, queued.readSetSize());
        final var strict = new TargetOrderState(
                physical.id(),
                bytes(32, 9),
                source.shardId(),
                new TargetKeyCodec.Domain(0, 1),
                owner.identity().accountingIncarnation(),
                TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO,
                1,
                1,
                TargetOrderState.Gate.CLOSED,
                null,
                null,
                null);
        final var ordered =
                TargetQuotaMetadataRecords.strictDomain(owner, strict.encodedKey(), strict.canonicalBytes());
        assertFee(ordered);
        assertEquals(1, ordered.contribution().strictOrderDomains());
        assertEquals(2, ordered.readSetSize());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMetadataRecords.queue(owner, physical, 2, queued.key(), queued.canonicalPayload()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMetadataRecords.strictDomain(
                        allocate(scope, 2, 1), ordered.key(), ordered.canonicalPayload()));
    }

    @Test
    void channelUsesFrozenIncarnationAndChargesOnlyItsOwnStoredBytes() {
        final var old = TargetChannelIdentity.decode(raw("target-binding-channel", "channel.pulsar.baseline"));
        final var c = old.context();
        final var context = new TargetChannelIdentity.Context(
                c.sourceShard(),
                c.target(),
                c.domain(),
                owner.identity().accountingIncarnation(),
                c.dispatchCompatibilityRef(),
                c.controlScopeRef(),
                c.kind(),
                c.channelSlot(),
                c.channelGeneration(),
                c.evidenceGeneration(),
                c.resourceGuardAttestationDigest());
        final var l = old.credentialLease();
        final var lease = new CredentialUseLease(
                l.profile(),
                l.kind(),
                context.credentialHolderScope(),
                l.secretGeneration(),
                l.credentialBindingDigest(),
                l.resolvedCredentialFingerprintDigest(),
                l.issuedAt(),
                l.validUntilEpochMs(),
                l.protectionRevision());
        final var channel = new TargetChannelIdentity(context, lease);
        final var value = TargetQuotaMetadataRecords.channel(owner, channel.encodedKey(), channel.canonicalBytes());
        assertFee(value);
        assertEquals(2, value.readSetSize());
        assertEquals(0, value.contribution().executionDomains());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMetadataRecords.channel(allocate(scope, 2, 1), value.key(), value.canonicalPayload()));
        final var drained = owner.drain(stamp(2, 1), (p, n) -> {});
        assertEquals(
                value.contribution(),
                TargetQuotaMetadataRecords.channel(drained, value.key(), value.canonicalPayload())
                        .contribution());
    }

    @Test
    void nativeScopeAndSnapshotKeepIncarnationAndTheCompleteScopeReadDependency() throws Exception {
        final var old = TargetNativePolicyScope.decode(raw("target-native-policy", "pulsar.baseline.scope"));
        final var nativeScope = new TargetNativePolicyScope(
                old.authorityNamespace(),
                old.controlResourceScope(),
                source.shardId(),
                physical.id(),
                owner.identity().accountingIncarnation(),
                old.domain(),
                old.dispatchRef(),
                old.controlRef(),
                old.fixedLeadCapMs(),
                old.artifacts());
        final var disabled = TargetNativePolicySnapshot.decode(raw("target-native-policy", "disabled.snapshot"));
        final var snapshot = TargetNativePolicySnapshot.create(
                nativeScope,
                1,
                HandoffPolicyMode.DISABLED,
                0,
                1000,
                2000,
                0,
                disabled.issuedAt(),
                9,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
        final var scoped =
                TargetQuotaMetadataRecords.nativeScope(owner, nativeScope.encodedKey(), nativeScope.canonicalBytes());
        final var snap = TargetQuotaMetadataRecords.nativeSnapshot(
                owner, nativeScope, snapshot.encodedKey(), snapshot.canonicalBytes());
        assertFee(scoped);
        assertFee(snap);
        assertEquals(2, scoped.readSetSize());
        assertEquals(3, snap.readSetSize());
        final var data = view(snap, owner, null);
        put(data, TargetNativePolicyScope.VALUE_TYPE, nativeScope.encodedKey(), nativeScope.canonicalBytes());
        snap.requireCurrent(k -> data.get(Bytes.hex(k)));
        data.remove(Bytes.hex(nativeScope.encodedKey()));
        assertThrows(IllegalStateException.class, () -> snap.requireCurrent(k -> data.get(Bytes.hex(k))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMetadataRecords.nativeSnapshot(
                        owner, old, snapshot.encodedKey(), snapshot.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMetadataRecords.nativeScope(
                        allocate(scope, 2, 1), scoped.key(), scoped.canonicalPayload()));
    }

    @Test
    void wrongRoleKeyAndPayloadCannotBecomeAChargeWithTheSameOwner() {
        for (var role : TargetQuotaMetadataRecords.SharedRole.values()) {
            final var value = shared(role, owner, activation);
            final byte[] wrongKey = value.key();
            wrongKey[wrongKey.length - 1] ^= 1;
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetQuotaMetadataRecords.shared(
                            role, activation, owner, physical, wrongKey, value.canonicalPayload()));
            final byte[] wrongPayload = value.canonicalPayload();
            wrongPayload[wrongPayload.length - 1] ^= 1;
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetQuotaMetadataRecords.shared(
                            role, activation, owner, physical, value.key(), wrongPayload));
        }
        final var control = shared(TargetQuotaMetadataRecords.SharedRole.CONTROL, owner, activation);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetQuotaMetadataRecords.shared(
                        TargetQuotaMetadataRecords.SharedRole.DISPATCH,
                        activation,
                        owner,
                        physical,
                        control.key(),
                        control.canonicalPayload()));
    }

    @Test
    void denyOnlyGrantCannotAssignSharedMetadataToAnUnpublishedOrigin() {
        final var grant = new TargetQuotaGrant(
                scope, bytes(32, 0x77), 1, accounting, TargetQuotaUsage.empty(), 1, bytes(32, 0x88));
        final var denied = activation(new TargetQuotaGrantControlRequest(grant, null, null), null, owner.allocation());
        for (var role : TargetQuotaMetadataRecords.SharedRole.values()) {
            assertThrows(IllegalArgumentException.class, () -> shared(role, owner, denied));
        }
    }

    @Test
    void maximumPhysicalIdentityPaysFullBytesAndRejectsCheckedFeeOverflow() {
        final var maximum = new CanonicalTargetPartition(
                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                        "c".repeat(256), bytes(32, 0x55), "x".repeat(1 << 20), Long.MAX_VALUE)),
                0xffff_ffffL);
        final var maximumScope = scope.forTarget(maximum.id());
        final var allocated = allocate(maximumScope, 1, 0);
        final var record = TargetQuotaMetadataRecords.shared(
                TargetQuotaMetadataRecords.SharedRole.IDENTITY,
                activate(allocated),
                allocated,
                maximum,
                TargetKeyCodec.identity(maximum.id()),
                maximum.canonicalBytes());
        assertFee(record);
        assertTrue(record.contribution().resources().amount(CapacityDimension.LOGICAL_STATE_BYTES) > (1 << 20));
        final var expensive = new TargetQuotaAccounting(bytes(32, 0xbb), Long.MAX_VALUE - 4096, 24, 40, 64);
        final var charged =
                TargetQuotaIncarnation.allocate(maximumScope, expensive, bytes(16, 0xcc), stamp(1, 0), (p, n) -> {});
        assertThrows(
                ArithmeticException.class,
                () -> TargetQuotaMetadataRecords.shared(
                        TargetQuotaMetadataRecords.SharedRole.IDENTITY,
                        activate(charged),
                        charged,
                        maximum,
                        TargetKeyCodec.identity(maximum.id()),
                        maximum.canonicalBytes()));
    }

    private TargetQuotaMetadataRecords.Record shared(
            TargetQuotaMetadataRecords.SharedRole role,
            TargetQuotaIncarnation origin,
            TargetQuotaGrantActivation grant) {
        final byte[] key, payload;
        switch (role) {
            case IDENTITY -> {
                key = TargetKeyCodec.identity(physical.id());
                payload = physical.canonicalBytes();
            }
            case DISPATCH -> {
                key = membership.offered().encodedKey();
                payload = membership.offered().canonicalBytes();
            }
            case CONTROL -> {
                key = membership.controls().encodedKey();
                payload = membership.controls().canonicalBytes();
            }
            case MEMBERSHIP_GRANT -> {
                key = membership.encodedKey();
                payload = membership.canonicalBytes();
            }
            case MEMBERSHIP_POLICY -> {
                key = policy.encodedKey();
                payload = policy.canonicalBytes();
            }
            default -> throw new AssertionError(role);
        }
        return TargetQuotaMetadataRecords.shared(role, grant, origin, physical, key, payload);
    }

    private Map<String, TargetQuotaMetadataRecords.Stored> view(
            TargetQuotaMetadataRecords.Record record, TargetQuotaIncarnation origin, TargetQuotaGrantActivation grant) {
        final var data = new HashMap<String, TargetQuotaMetadataRecords.Stored>();
        put(data, record.valueType(), record.key(), record.canonicalPayload());
        put(data, TargetQuotaIncarnation.VALUE_TYPE, origin.key(), origin.canonicalBytes());
        if (grant != null) {
            put(data, TargetQuotaGrantActivation.VALUE_TYPE, grant.key(), grant.canonicalBytes());
        }
        put(
                data,
                CanonicalTargetPartition.VALUE_TYPE,
                TargetKeyCodec.identity(physical.id()),
                physical.canonicalBytes());
        return data;
    }

    private static void put(Map<String, TargetQuotaMetadataRecords.Stored> data, int type, byte[] key, byte[] payload) {
        data.put(Bytes.hex(key), new TargetQuotaMetadataRecords.Stored(type, payload));
    }

    private void assertFee(TargetQuotaMetadataRecords.Record record) {
        assertEquals(
                record.key().length + record.canonicalPayload().length + 12L + 32,
                record.contribution().resources().amount(CapacityDimension.LOGICAL_STATE_BYTES));
        for (var dimension : CapacityDimension.values()) {
            if (dimension != CapacityDimension.LOGICAL_STATE_BYTES) {
                assertEquals(0, record.contribution().resources().amount(dimension));
            }
        }
    }

    private TargetQuotaIncarnation allocate(TargetQuotaScope scope, long sequence, long offset) {
        return TargetQuotaIncarnation.allocate(
                scope, accounting, bytes(16, 0xcc), stamp(sequence, offset), (p, n) -> {});
    }

    private TargetQuotaMutation stamp(long sequence, long offset) {
        return new TargetQuotaMutation(
                sequence,
                new KafkaSourcePosition(
                        source.shardId(), source.authenticatedClusterId(), source.nativeTopicUuid(), offset, null, 100),
                bytes(32, 0x66));
    }

    private TargetQuotaGrantActivation activate(TargetQuotaIncarnation origin) {
        final var grant = new TargetQuotaGrant(
                origin.scope(), bytes(32, 0x77), 1, origin.accounting(), origin.ownContribution(), 1, bytes(32, 0x88));
        return activation(new TargetQuotaGrantControlRequest(grant, null, null), origin, origin.allocation());
    }

    private TargetQuotaGrantActivation activation(
            TargetQuotaGrantControlRequest request, TargetQuotaIncarnation origin, TargetQuotaMutation mutation) {
        final var ref = new ControlRef(
                bytes(32, 0x99),
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final byte[] mh = bytes(32, 0xaa);
        return new TargetQuotaGrantActivation(
                request,
                ref,
                mutation,
                SystemMutation.computeSystemMutationId(
                        source.shardId(),
                        SystemMutationType.APPLY_SHARD_CONTROL,
                        ref.logicalOperationIdentity(TargetQuotaGrantControlRequest.CONTROL_KIND),
                        mh),
                mh,
                origin);
    }

    private TargetDomainState domain(int slot, TargetDomainState.Lifecycle lifecycle) {
        return new TargetDomainState(
                new TargetKeyCodec.Domain(slot, 1),
                lifecycle,
                lifecycle == TargetDomainState.Lifecycle.VACANT
                        ? null
                        : membership.offered().digest(),
                lifecycle == TargetDomainState.Lifecycle.VACANT
                        ? null
                        : membership.controls().digest(),
                null,
                null,
                null);
    }

    private static byte[] bytes(int n, int value) {
        final byte[] result = new byte[n];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static byte[] raw(String name, String key) {
        final var properties = new Properties();
        try (var in =
                TargetQuotaMetadataRecordsTest.class.getResourceAsStream("/ndip3/" + name + "-vectors.properties")) {
            properties.load(in);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return HexFormat.of().parseHex(properties.getProperty(key));
    }
}
