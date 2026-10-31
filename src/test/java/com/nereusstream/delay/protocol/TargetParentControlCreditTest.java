package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.TargetParentControlCreditMutationFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TargetParentControlCreditTest {
    private final Properties vectors = load();
    private final KafkaSourcePosition origin = (KafkaSourcePosition) TargetSourcePosition.decode(hex("source"));

    @Test
    void independentlyDecodedCanonicalPayerFundsAndSurvivesRecordDecode() throws Exception {
        final var fixture = fixture();
        assertNotSame(
                fixture.definition().expectedPayerGrant(), fixture.activation().grant());
        // The input comes from the independently encoded quota fixture, not this test's Java encoder.
        assertArrayEquals(
                hex("grant.shard"), fixture.definition().expectedPayerGrant().canonicalBytes());
        assertArrayEquals(hex("grant.shard"), fixture.activation().grant().canonicalBytes());

        final var funded = fund(fixture);
        assertNotSame(
                funded.definition().expectedPayerGrant(),
                funded.actualPayerGrant().grant());
        assertEquals(1, funded.revision());
        assertTrue(funded.allocations().isEmpty());
        assertEquals(CapacityVector.empty(), funded.allocated());
        assertEquals(fixture.definition().funded(), funded.remaining());

        final var restored = TargetParentControlCredit.decode(funded.canonicalBytes());
        assertNotSame(
                restored.definition().expectedPayerGrant(),
                restored.actualPayerGrant().grant());
        assertArrayEquals(funded.canonicalBytes(), restored.canonicalBytes());
        assertArrayEquals(funded.key(), restored.key());
        assertEquals(funded.ownContribution(), restored.ownContribution());
        assertEquals(funded.futureCharge(), restored.futureCharge());
        assertEquals(funded.definition().funded(), restored.allocated().add(restored.remaining()));
        assertArrayEquals(
                funded.canonicalBytes(),
                TargetParentControlCredit.decodeForStore(
                                funded.key(), funded.canonicalBytes(), funded.scope(), funded.lineage())
                        .canonicalBytes());

        final var child = new TargetParentControlCredit.Allocation(
                new TargetSloControlScopeHeader.Reference(bytes(32, 0x81), 1, bytes(32, 0x82)),
                bytes(32, 0x83),
                payload(5),
                stamp(4, position(13, 103), 0x84));
        final var allocated = restored.allocate(child);
        final var allocatedCopy = TargetParentControlCredit.decode(allocated.canonicalBytes());
        assertEquals(2, allocatedCopy.revision());
        assertEquals(List.of(child), allocatedCopy.allocations());
        assertEquals(payload(5), allocatedCopy.allocated());
        assertEquals(payload(15), allocatedCopy.remaining());
        assertEquals(fixture.definition().funded(), allocatedCopy.allocated().add(allocatedCopy.remaining()));
        assertEquals(
                funded.ownContribution().resources(),
                allocatedCopy.ownContribution().resources().add(child.commitment()));
    }

    @Test
    void samePayerScopeDoesNotPermitDifferentGrantBytes() throws Exception {
        final var fixture = fixture();
        final var payer = fixture.activation().grant();
        final var changedAccounting = new TargetQuotaAccounting(
                bytes(32, 0xba),
                payer.accounting().recordOverheadBytes(),
                payer.accounting().kafkaEnvelopeBytes(),
                payer.accounting().pulsarEnvelopeBytes(),
                payer.accounting().minimumRecordCostBytes());
        final var changedLimit = payer.limit().add(new TargetQuotaUsage(payload(1), 0, 0, 0, 0));
        for (var changed : List.of(
                new TargetQuotaGrant(
                        payer.scope(),
                        bytes(32, 0x98),
                        1,
                        payer.accounting(),
                        payer.limit(),
                        payer.tenantPolicyVersion(),
                        payer.tenantPolicyHash()),
                new TargetQuotaGrant(
                        payer.scope(),
                        payer.grantId(),
                        1,
                        changedAccounting,
                        payer.limit(),
                        payer.tenantPolicyVersion(),
                        payer.tenantPolicyHash()),
                new TargetQuotaGrant(
                        payer.scope(),
                        payer.grantId(),
                        1,
                        payer.accounting(),
                        changedLimit,
                        payer.tenantPolicyVersion(),
                        payer.tenantPolicyHash()),
                new TargetQuotaGrant(
                        payer.scope(),
                        payer.grantId(),
                        1,
                        payer.accounting(),
                        payer.limit(),
                        payer.tenantPolicyVersion() + 1,
                        payer.tenantPolicyHash()),
                new TargetQuotaGrant(
                        payer.scope(),
                        payer.grantId(),
                        1,
                        payer.accounting(),
                        payer.limit(),
                        payer.tenantPolicyVersion(),
                        bytes(32, 0xab)))) {
            final var actual = activation(changed, fixture.activation().mutation());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetParentControlCredit.fund(
                            fixture.definition(), fixture.prepared().mutation(), actual, fixture.fundedAt()));
        }
    }

    @Test
    void definitionStillRequiresShardPayerAndOriginalPhysicalSource() throws Exception {
        final var fixture = fixture();
        final var targetPayer = TargetQuotaGrant.decode(hex("grant.target"));
        final var targetRoot = TargetQuotaIncarnation.allocate(
                targetPayer.scope(),
                targetPayer.accounting(),
                bytes(16, 0xcc),
                fixture.definition().originalRoot().allocation(),
                (prior, next) -> {});
        assertThrows(IllegalArgumentException.class, () -> definition(targetRoot, targetPayer, position(11, 101)));
        assertThrows(
                IllegalArgumentException.class,
                () -> definition(fixture.definition().originalRoot(), targetPayer, position(11, 101)));
        final var foreign = new KafkaSourcePosition(
                origin.shardId(), "other-cluster", origin.nativeTopicUuid(), 11, origin.leaderEpoch(), 101);
        assertThrows(
                IllegalArgumentException.class,
                () -> definition(
                        fixture.definition().originalRoot(),
                        fixture.definition().expectedPayerGrant(),
                        foreign));
        final var foreignActivation = activation(TargetQuotaGrant.decode(hex("grant.shard")), stamp(2, foreign, 0x72));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetParentControlCredit.fund(
                        fixture.definition(), fixture.prepared().mutation(), foreignActivation, fixture.fundedAt()));
    }

    @Test
    void fundingStillRequiresOriginalSourceOrderRetryWindowAndMutationDigest() throws Exception {
        final var fixture = fixture();
        final var input = fixture.prepared().mutation();
        final var lateActivation =
                activation(TargetQuotaGrant.decode(hex("grant.shard")), stamp(4, position(13, 103), 0x73));
        assertThrows(
                IllegalStateException.class,
                () -> TargetParentControlCredit.fund(fixture.definition(), input, lateActivation, fixture.fundedAt()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetParentControlCredit.fund(
                        fixture.definition(),
                        input,
                        fixture.activation(),
                        new TargetQuotaMutation(3, position(11, 101), Bytes.sha256(input.canonicalEnvelope()))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetParentControlCredit.fund(
                        fixture.definition(),
                        input,
                        fixture.activation(),
                        new TargetQuotaMutation(3, position(12, 501), Bytes.sha256(input.canonicalEnvelope()))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetParentControlCredit.fund(
                        fixture.definition(), input, fixture.activation(), stamp(3, position(12, 102), 0x74)));
        final var foreign = new KafkaSourcePosition(
                origin.shardId(),
                origin.authenticatedClusterId() + "-foreign",
                origin.nativeTopicUuid(),
                12,
                origin.leaderEpoch(),
                102);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetParentControlCredit.fund(
                        fixture.definition(),
                        input,
                        fixture.activation(),
                        new TargetQuotaMutation(3, foreign, Bytes.sha256(input.canonicalEnvelope()))));
    }

    @Test
    void originalControlFactoryStillRequiresBothRolesAndCoveredScope() throws Exception {
        final var fixture = fixture();
        final var request = new TargetParentControlCreditRequest(fixture.definition());
        final var factory = factory(fixture.keys());
        for (var roles : List.of(
                ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR),
                ControlRoleSet.of(ControlRole.TENANT_POLICY_ADMINISTRATOR))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> factory.prepare(bytes(32, 0x75), request, actor(roles), control -> true, 400, 500));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> factory.prepare(bytes(32, 0x75), request, actor(roles()), control -> false, 400, 500));
        assertTrue(fixture.prepared().control().verifySignature(fixture.keys().getPublic()));
        assertTrue(fixture.prepared().mutation().verifySignature(fixture.keys().getPublic()));
        final var restored = TargetParentControlCreditMutationFactory.Prepared.restore(
                PreparedControlOperation.decode(fixture.prepared().control().canonicalBytes()),
                SystemMutation.decodeTargetEnvelope(
                        fixture.prepared().mutation().canonicalEnvelope(), SystemMutationType.APPLY_SHARD_CONTROL));
        assertArrayEquals(
                fixture.prepared().mutation().canonicalEnvelope(),
                restored.mutation().canonicalEnvelope());
    }

    private Fixture fixture() throws Exception {
        final var expected = TargetQuotaGrant.decode(hex("grant.shard"));
        final var root = TargetQuotaIncarnation.allocate(
                expected.scope(),
                expected.accounting(),
                bytes(16, 0xcc),
                stamp(1, position(10, 100), 0x71),
                (prior, next) -> {});
        final var definition = definition(root, expected, position(11, 101));
        final var actual = activation(TargetQuotaGrant.decode(hex("grant.shard")), stamp(2, position(11, 101), 0x72));
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var prepared = factory(keys)
                .prepare(
                        bytes(32, 0x75),
                        new TargetParentControlCreditRequest(definition),
                        actor(roles()),
                        control -> true,
                        400,
                        500);
        final var fundedAt = new TargetQuotaMutation(
                3, position(12, 102), Bytes.sha256(prepared.mutation().canonicalEnvelope()));
        return new Fixture(definition, actual, keys, prepared, fundedAt);
    }

    private TargetParentControlCredit.Definition definition(
            TargetQuotaIncarnation root, TargetQuotaGrant expected, SourcePosition anchor) {
        return new TargetParentControlCredit.Definition(
                root,
                expected,
                new CapacityGrant(CapacityGrantKind.NON_OUTCOME_CONTROL, bytes(32, 0x76), 1, payload(20)),
                bytes(32, 0x77),
                anchor,
                payload(20),
                2);
    }

    private static TargetQuotaGrantActivation activation(TargetQuotaGrant grant, TargetQuotaMutation stamp) {
        final var request = new TargetQuotaGrantControlRequest(grant, null, null);
        final var ref = new ControlRef(
                bytes(32, 0x78),
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final byte[] hash = bytes(32, 0x79);
        final byte[] id = SystemMutation.computeSystemMutationId(
                grant.scope().shard(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                ref.logicalOperationIdentity(TargetQuotaGrantControlRequest.CONTROL_KIND),
                hash);
        return new TargetQuotaGrantActivation(request, ref, stamp, id, hash, null);
    }

    private static TargetParentControlCredit fund(Fixture fixture) {
        return TargetParentControlCredit.fund(
                fixture.definition(), fixture.prepared().mutation(), fixture.activation(), fixture.fundedAt());
    }

    private static TargetParentControlCreditMutationFactory factory(KeyPair keys) {
        return new TargetParentControlCreditMutationFactory(1, keys.getPrivate(), 1, 2 << 20);
    }

    private static ControlRoleSet roles() {
        return ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR, ControlRole.TENANT_POLICY_ADMINISTRATOR);
    }

    private static ControlAuthorizationContext actor(ControlRoleSet roles) {
        return new ControlAuthorizationContext(bytes(32, 0xa1), roles, bytes(32, 0xa2));
    }

    private KafkaSourcePosition position(long offset, long time) {
        return new KafkaSourcePosition(
                origin.shardId(),
                origin.authenticatedClusterId(),
                origin.nativeTopicUuid(),
                offset,
                origin.leaderEpoch(),
                time);
    }

    private static TargetQuotaMutation stamp(long sequence, SourcePosition source, int digest) {
        return new TargetQuotaMutation(sequence, source, bytes(32, digest));
    }

    private static CapacityVector payload(long amount) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.PENDING_PAYLOAD_BYTES.ordinal()] = amount;
        return new CapacityVector(values);
    }

    private static byte[] bytes(int length, int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private byte[] hex(String key) {
        return HexFormat.of().parseHex(vectors.getProperty(key));
    }

    private static Properties load() {
        final var values = new Properties();
        try (var stream = TargetParentControlCreditTest.class.getResourceAsStream(
                "/ndip3/target-quota-scope-vectors.properties")) {
            values.load(stream);
            return values;
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private record Fixture(
            TargetParentControlCredit.Definition definition,
            TargetQuotaGrantActivation activation,
            KeyPair keys,
            TargetParentControlCreditMutationFactory.Prepared prepared,
            TargetQuotaMutation fundedAt) {}
}
