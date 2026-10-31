package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class TargetParentControlCreditActivationContractTest {
    @Test
    void parentPurposeHasItsOwnClosedReaderMatrix() {
        final var parent = descriptor(22);
        assertEquals(ProtocolTuple.targetParentControlCredit(), parent.tuple());
        assertTrue(parent.supportsParentControlCredit());
        assertTrue(parent.extendedRootAuthorization());
        assertEquals(63, parent.nativeMaximum());
        assertEquals(6, parent.checkpointSchema());
        assertFalse(parent.supportsSourceHistoryRecords());
        assertFalse(parent.supportsDuplicatePositionReclaim());
        assertFalse(parent.supportsMigrationLogicalFirstArchive());
        assertFalse(parent.supportsMigrationFullSourceArchive());
        assertEquals(0, parent.maximumMigrationArchiveSourceFrameBytes());
        final var fields = TargetCompatibilityCodec.read(parent.canonicalBytes(),
                TargetProtocolActivationDescriptor.MAX_BYTES, 12, false, "descriptor");
        final var matrix = TargetCompatibilityCodec.read(QueryCodecSupport.bytes(fields.get(6), 7),
                1024, 64, false, "Parent matrix");
        assertFalse(matrix.stream().anyMatch(field -> field.number() >= 44 && field.number() <= 50));
        assertEquals(parent, TargetProtocolActivationDescriptor.decode(parent.canonicalBytes()));
        final byte[] future = parent.canonicalBytes();
        future[1] = 23;
        assertThrows(IllegalArgumentException.class, () -> TargetProtocolActivationDescriptor.decode(future));
    }

    @Test
    void onlyExplicitVersionSixDeclaresParentReadersAndItsTwelveSlots() {
        final var parent = descriptor(22);
        final var tuples = List.of(parent.tuple());
        final var bases = List.of(parent.base());
        assertThrows(IllegalArgumentException.class, () -> new ProtocolCapabilityDeclaration(
                "worker", bytes(32, 1), tuples, bases, List.of(parent), 1, bytes(32, 2)));
        assertThrows(IllegalArgumentException.class, () -> ProtocolCapabilityDeclaration.extendedTarget(
                "worker", bytes(32, 1), tuples, bases, List.of(parent), 1, bytes(32, 2)));
        assertThrows(IllegalArgumentException.class, () -> ProtocolCapabilityDeclaration.fullSourceArchiveTarget(
                "worker", bytes(32, 1), tuples, bases, List.of(parent), 1, bytes(32, 2)));
        final var declaration = capability(List.of(parent));
        final var decoded = ProtocolCapabilityDeclaration.decode(declaration.canonicalBytes());
        assertEquals(6, decoded.version());
        assertTrue(decoded.supports(parent));
        assertFalse(decoded.supports(ProtocolTuple.targetScopeControlObligation()));
        assertFalse(decoded.supports(ProtocolTuple.targetScopeControlObligationFrozen()));
        assertFalse(decoded.supports(ProtocolTuple.targetScopeControlObligationExposure()));
        final var archiveOnly = capability(List.of(descriptor(21)));
        assertFalse(archiveOnly.supports(parent));
        assertFalse(archiveOnly.supports(parent.tuple()));
        final var twelve = new ArrayList<>(IntStream.rangeClosed(1, 4).mapToObj(
                TargetParentControlCreditActivationContractTest::descriptor).toList());
        twelve.addAll(IntStream.rangeClosed(15, 22).mapToObj(
                TargetParentControlCreditActivationContractTest::descriptor).toList());
        assertEquals(12, ProtocolCapabilityDeclaration.decode(capability(twelve).canonicalBytes())
                .targetDescriptors().size());
        final var fiveOriginal = new ArrayList<>(twelve);
        fiveOriginal.remove(descriptor(15));
        fiveOriginal.add(descriptor(5));
        assertThrows(IllegalArgumentException.class, () -> capability(fiveOriginal));
        final var nineExtended = new ArrayList<>(twelve);
        nineExtended.remove(descriptor(1));
        nineExtended.add(new TargetProtocolActivationDescriptor(parent.base(), parent.baseResetManifest(),
                bytes(32, 99), parent.workerSourceCommit(), parent.kafkaSourceCommit(),
                parent.kafkaSdkJar(), parent.pulsarSdkJar(), 22));
        assertThrows(IllegalArgumentException.class, () -> capability(nineExtended));
    }

    @Test
    void immutableInitialRootNeedsItsOwnExactSignedParentExtension() {
        final var keys = keyPair();
        final var initial = initial(keys);
        final var parent = descriptor(22);
        final var readers = new TargetProtocolReaderSet(parent,
                List.of(new TargetProtocolReaderSet.Reader(1, capability(List.of(parent)))));
        assertFalse(initial.input().protocolTuples().contains(parent.tuple()));
        assertThrows(IllegalArgumentException.class, () -> new TargetProtocolActivatePayload(
                initial.input().scope(), initial.input().lineage(), parent, readers.digest()));
        final var payload = TargetProtocolActivatePayload.forRoot(initial, parent, readers.digest(), keys.getPrivate());
        final var decoded = TargetProtocolActivatePayload.decode(payload.canonicalBytes());
        assertEquals(payload, decoded);
        assertEquals(2, decoded.schemaVersion());
        assertTrue(decoded.rootAuthorization().verifySignature(keys.getPublic()));
        assertFalse(decoded.rootAuthorization().verifySignature(keyPair().getPublic()));
        new TargetProtocolRootIssuerEvidence(initial, keys.getPublic().getEncoded()).require(decoded);
        assertThrows(IllegalArgumentException.class, () -> new TargetProtocolActivatePayload(
                initial.input().scope(), initial.input().lineage(), descriptor(21), readers.digest(),
                decoded.rootAuthorization()));
    }

    @Test
    void everyPreviousDescriptorRetainsItsFrozenCanonicalBytes() {
        for (int version = 1; version <= LEGACY_DESCRIPTOR_SHA256.length; version++) {
            final var descriptor = descriptor(version);
            assertEquals(LEGACY_DESCRIPTOR_SHA256[version - 1], Bytes.hex(Bytes.sha256(descriptor.canonicalBytes())),
                    "descriptor" + version);
            assertEquals(descriptor, TargetProtocolActivationDescriptor.decode(descriptor.canonicalBytes()));
            assertFalse(descriptor.supportsParentControlCredit());
        }
        assertEquals(56, descriptor(14).nativeMaximum());
        assertEquals(5, new TargetProtocolActivationDescriptor(descriptor(5).base(), bytes(32, 5), bytes(32, 6),
                "1".repeat(40), "2".repeat(40), bytes(32, 7), bytes(32, 8)).schemaVersion());
    }

    @Test
    void previousCapabilityDeclarationsRetainTheirFrozenCanonicalBytes() {
        final var descriptor = descriptor(1);
        final var tuples = List.of(descriptor.tuple());
        final var bases = List.of(descriptor.base());
        final var targets = List.of(descriptor);
        final var declarations = List.of(
                new ProtocolCapabilityDeclaration("worker", bytes(32, 1), tuples, 1, bytes(32, 2)),
                new ProtocolCapabilityDeclaration("worker", bytes(32, 1), tuples, bases, 1, bytes(32, 2)),
                new ProtocolCapabilityDeclaration("worker", bytes(32, 1), tuples, bases, targets, 1, bytes(32, 2)),
                ProtocolCapabilityDeclaration.extendedTarget(
                        "worker", bytes(32, 1), tuples, bases, targets, 1, bytes(32, 2)),
                ProtocolCapabilityDeclaration.fullSourceArchiveTarget(
                        "worker", bytes(32, 1), tuples, bases, targets, 1, bytes(32, 2)));
        for (int index = 0; index < declarations.size(); index++) {
            final var declaration = declarations.get(index);
            assertEquals(LEGACY_CAPABILITY_SHA256[index], Bytes.hex(Bytes.sha256(declaration.canonicalBytes())));
            assertEquals(declaration, ProtocolCapabilityDeclaration.decode(declaration.canonicalBytes()));
        }
    }

    @Test
    void migrationPolicyTwoHasAnExplicitGrantAndItsOwnSignaturePurpose() throws Exception {
        final var keys = keyPair();
        final var plan = plan(initial(keys));
        final var old = TargetMigrationRootIssuerAuthorization.signed(plan, bytes(32, 20), bytes(32, 21), 1,
                List.of(ProtocolTuple.targetObjectPayloadReclaim()), bytes(64, 22), keys.getPrivate());
        assertEquals(1, old.schemaVersion());
        assertTrue(old.verifySignature(keys.getPublic()));
        assertEquals(old, TargetMigrationRootIssuerAuthorization.decode(old.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> TargetMigrationRootIssuerAuthorization.signed(
                plan, bytes(32, 20), bytes(32, 21), 1, List.of(ProtocolTuple.targetParentControlCredit()),
                bytes(64, 22), keys.getPrivate()));
        assertThrows(IllegalArgumentException.class,
                () -> TargetMigrationRootIssuerAuthorization.signedParentControlCredit(
                plan, bytes(32, 20), bytes(32, 21), 1, List.of(ProtocolTuple.targetObjectPayloadReclaim()),
                bytes(64, 22), keys.getPrivate()));
        final var parent = TargetMigrationRootIssuerAuthorization.signedParentControlCredit(plan,
                bytes(32, 20), bytes(32, 21), 1, List.of(ProtocolTuple.targetParentControlCredit()),
                bytes(64, 22), keys.getPrivate());
        assertEquals(2, parent.schemaVersion());
        assertEquals(List.of(ProtocolTuple.targetParentControlCredit()), parent.allowedTuples());
        assertTrue(parent.verifySignature(keys.getPublic()));
        parent.requirePlan(plan);
        assertEquals(parent, TargetMigrationRootIssuerAuthorization.decode(parent.canonicalBytes()));
        final var originalPurposes = List.of(ProtocolTuple.targetObjectPayloadReclaim(),
                ProtocolTuple.targetUncommittedObjectReclaim(), ProtocolTuple.targetMigrationObjectPayloadReclaim(),
                ProtocolTuple.targetMigrationUncommittedObjectReclaim(), ProtocolTuple.targetMigrationRetireIntent(),
                ProtocolTuple.targetMigrationRetireConfirm(), ProtocolTuple.targetPayloadObligationSummary(),
                ProtocolTuple.targetMigrationLogicalFirstArchive(), ProtocolTuple.targetMigrationFullSourceArchive());
        final var allOriginal = TargetMigrationRootIssuerAuthorization.signed(plan, bytes(32, 20), bytes(32, 21), 1,
                originalPurposes, bytes(64, 22), keys.getPrivate());
        assertEquals(9, TargetMigrationRootIssuerAuthorization.decode(allOriginal.canonicalBytes())
                .allowedTuples().size());
        assertEquals(25, TargetCompatibilityCodec.read(allOriginal.canonicalBytes(),
                TargetMigrationRootIssuerAuthorization.MAX_BYTES, 25, true, "old policy").size());
        final var ten = new ArrayList<>(originalPurposes);
        ten.add(ProtocolTuple.targetParentControlCredit());
        final var full = TargetMigrationRootIssuerAuthorization.signedParentControlCredit(plan,
                bytes(32, 20), bytes(32, 21), 1, ten, bytes(64, 22), keys.getPrivate());
        assertEquals(10, TargetMigrationRootIssuerAuthorization.decode(full.canonicalBytes()).allowedTuples().size());
        assertEquals(26, TargetCompatibilityCodec.read(full.canonicalBytes(),
                TargetMigrationRootIssuerAuthorization.MAX_BYTES, 26, true, "policy2").size());
        ten.add(ProtocolTuple.targetScopeControlObligation());
        assertThrows(IllegalArgumentException.class,
                () -> TargetMigrationRootIssuerAuthorization.signedParentControlCredit(plan,
                        bytes(32, 20), bytes(32, 21), 1, ten, bytes(64, 22), keys.getPrivate()));
        final var parsed = TargetCompatibilityCodec.read(parent.canonicalBytes(),
                TargetMigrationRootIssuerAuthorization.MAX_BYTES, 26, true, "policy2");
        assertEquals(2, QueryCodecSupport.uint32(parsed.get(12), 13));
        final byte[] fields = Arrays.copyOf(parent.canonicalBytes(),
                parent.canonicalBytes().length - 35 - 67);
        final var signer = Signature.getInstance("Ed25519");
        signer.initSign(keys.getPrivate());
        signer.update(Bytes.concat(Bytes.utf8("nereus-delay-migration-root-issuer-authorization-signature-v1\0"),
                parent.digest()));
        final byte[] wrongSignature = signer.sign();
        final byte[] wrongDomain = Bytes.concat(fields, CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 16, parent.digest());
            CanonicalProtobuf.bytes(out, 17, wrongSignature);
        }));
        assertFalse(TargetMigrationRootIssuerAuthorization.decode(wrongDomain).verifySignature(keys.getPublic()));
        for (int version : new int[] {1, 3}) {
            final byte[] changed = parent.canonicalBytes();
            changed[1] = (byte) version;
            assertThrows(IllegalArgumentException.class, () -> TargetMigrationRootIssuerAuthorization.decode(changed));
        }
        final byte[] oldPurpose = Bytes.concat(fields,
                CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 16, parent.digest())),
                CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 17, parent.signature())));
        // Field13's one-byte purpose is immediately before the first repeated tuple tag.
        final int purposeOffset = fields.length - parent.originalAuthorityHistory().length - 2
                - parent.allowedTuples().getFirst().canonicalBytes().length - 2 - 1;
        oldPurpose[purposeOffset] = 1;
        assertThrows(IllegalArgumentException.class, () -> TargetMigrationRootIssuerAuthorization.decode(oldPurpose));
    }

    @Test
    void preAMigrationReaderProofNeedsBothParentPolicyAndActualMigrationDataMatrices() {
        final var parent = descriptor(22);
        final var migration = descriptor(15);
        final var parentOnly = new TargetProtocolReaderSet(parent, List.of(new TargetProtocolReaderSet.Reader(1,
                migrationCapability(List.of(parent)))));
        assertThrows(IllegalStateException.class, parentOnly::requireMigrationRootIssuerReaders);
        final var both = new TargetProtocolReaderSet(parent, List.of(new TargetProtocolReaderSet.Reader(1,
                migrationCapability(List.of(parent, migration)))));
        both.requireMigrationRootIssuerReaders();
        final var otherBinary = new TargetProtocolActivationDescriptor(migration.base(), migration.baseResetManifest(),
                migration.targetSchemaHash(), "3".repeat(40), migration.kafkaSourceCommit(),
                migration.kafkaSdkJar(), migration.pulsarSdkJar(), 15);
        final var wrongBinary = new TargetProtocolReaderSet(parent, List.of(new TargetProtocolReaderSet.Reader(1,
                migrationCapability(List.of(parent, otherBinary)))));
        assertThrows(IllegalStateException.class, wrongBinary::requireMigrationRootIssuerReaders);
    }

    @Test
    void legacyMigrationPolicyKeepsItsBytesAndOriginalSignatureDomain() throws Exception {
        final var unsigned = legacyPolicy(null);
        assertEquals("63fc31e377a6e9e10e10140e2bc396ea75d0a76de71030375b7d002258a98f03",
                Bytes.hex(unsigned.digest()));
        final var encoded = legacyPolicy(bytes(64, 13));
        assertEquals("9d5dfad98e4d12ac673c8645bd5870cafdcabfa68f72e4d27ce7c390d0fd4467",
                Bytes.hex(Bytes.sha256(encoded.canonicalBytes())));
        assertEquals(encoded, TargetMigrationRootIssuerAuthorization.decode(encoded.canonicalBytes()));
        final var keys = keyPair();
        final var signer = Signature.getInstance("Ed25519");
        signer.initSign(keys.getPrivate());
        signer.update(Bytes.concat(Bytes.utf8("nereus-delay-migration-root-issuer-authorization-signature-v1\0"),
                unsigned.digest()));
        assertTrue(legacyPolicy(signer.sign()).verifySignature(keys.getPublic()));
    }

    private static TargetMigrationRootIssuerAuthorization legacyPolicy(byte[] signature) {
        final var shard = new ShardId(new RouteIncarnation(bytes(16, 1)), 0);
        final var scope = new TargetQuotaScope(shard, bytes(32, 2), null);
        final var source = new PulsarSourcePosition(shard, bytes(32, 3), "persistent://test/default/source",
                1, 0, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 1);
        return new TargetMigrationRootIssuerAuthorization(bytes(32, 4), bytes(32, 5), bytes(32, 6), bytes(32, 7),
                scope, source, bytes(16, 8), bytes(32, 9), bytes(16, 10), bytes(32, 11), 1,
                List.of(ProtocolTuple.targetObjectPayloadReclaim()), bytes(64, 12), signature);
    }

    private static ProtocolCapabilityDeclaration migrationCapability(
            List<TargetProtocolActivationDescriptor> descriptors) {
        final var tuples = new ArrayList<>(descriptors.stream().map(
                TargetProtocolActivationDescriptor::tuple).toList());
        tuples.add(ProtocolTuple.targetMigrationActivateRootIssuer());
        return ProtocolCapabilityDeclaration.parentControlCreditTarget("worker", bytes(32, 1), tuples,
                List.of(descriptors.getFirst().base()), descriptors, 1, bytes(32, 2));
    }

    private static TargetMigrationPlan plan(TargetInitialControlSnapshot initial) {
        final var object = new TargetMigrationObjectReference(new CheckpointResource(bytes(16, 1), bytes(16, 4),
                new ProfileRef(Bytes.utf8("migration"), 1, bytes(32, 3), ProfileKind.OBJECT_STORE),
                Bytes.utf8("bucket"), Bytes.utf8("key"), Bytes.utf8("version"), 2, bytes(32, 4)));
        return new TargetMigrationPlan(initial.input().scope(), bytes(32, 8), 1, null,
                bytes(16, 2), bytes(32, 2), 1, bytes(16, 4), initial.mutation(), object,
                bytes(16, 3), bytes(32, 3), 2, bytes(16, 5), object, object, object, object, bytes(32, 9), object);
    }

    private static ProtocolCapabilityDeclaration capability(List<TargetProtocolActivationDescriptor> descriptors) {
        return ProtocolCapabilityDeclaration.parentControlCreditTarget("worker", bytes(32, 1),
                descriptors.stream().map(TargetProtocolActivationDescriptor::tuple).distinct().toList(),
                descriptors.stream().map(TargetProtocolActivationDescriptor::base).distinct().toList(),
                descriptors, 1, bytes(32, 2));
    }

    private static TargetProtocolActivationDescriptor descriptor(int version) {
        return new TargetProtocolActivationDescriptor(
                ArtifactGenerationSet.current(1, PulsarSourceLock.digest(), bytes(32, 4)),
                bytes(32, 5), bytes(32, 6), "1".repeat(40), "2".repeat(40), bytes(32, 7), bytes(32, 8), version);
    }

    private static TargetInitialControlSnapshot initial(KeyPair keys) {
        final var vectors = new Properties();
        try (var input = TargetParentControlCreditActivationContractTest.class.getResourceAsStream(
                "/ndip3/target-quota-grant-vectors.properties")) {
            vectors.load(input);
        } catch (Exception failure) { throw new AssertionError(failure); }
        final var template = TargetQuotaGrantActivation.decode(HexFormat.of().parseHex(
                vectors.getProperty("target.initial.activation"))).grant();
        final var scope = template.scope().shardScope();
        final var grant = new TargetQuotaGrant(scope, template.grantId(), 1, template.accounting(), template.limit(),
                template.tenantPolicyVersion(), template.tenantPolicyHash());
        final var source = new PulsarSourcePosition(scope.shard(), bytes(32, 15),
                "persistent://test/default/source", 1, 0, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 1);
        final var at = new TargetQuotaMutation(1, source, bytes(32, 6));
        final var request = new TargetQuotaGrantControlRequest(grant, null, null);
        final var ref = new ControlRef(bytes(32, 5),
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()), 0);
        final byte[] hash = bytes(32, 7);
        final var accepted = new TargetQuotaGrantActivation(request, ref, at,
                SystemMutation.computeSystemMutationId(scope.shard(), SystemMutationType.APPLY_SHARD_CONTROL,
                        ref.logicalOperationIdentity(TargetQuotaGrantControlRequest.CONTROL_KIND), hash), hash, null);
        final var signed = TargetControlSnapshotInput.signed(scope, bytes(16, 8), grant.digest(), source,
                at.mutationDigest(), List.of(ProtocolTuple.currentSystemMutation()), List.of(), 1, keys.getPrivate());
        return new TargetInitialControlSnapshot(signed,
                new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(), bytes(16, 9), null, null),
                at, accepted, AuthorIdentity.control(bytes(32, 10), bytes(32, 11), bytes(32, 12)).canonicalBytes());
    }

    private static KeyPair keyPair() {
        try { return KeyPairGenerator.getInstance("Ed25519").generateKeyPair(); }
        catch (Exception failure) { throw new AssertionError(failure); }
    }

    private static byte[] bytes(int count, int value) {
        final byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    // Independently encoded from the frozen descriptor1..21 fields with Python protobuf primitives and SHA-256.
    private static final String[] LEGACY_DESCRIPTOR_SHA256 = {
        "b7c55f472692ea508b925a0582df44e4104c8123ae108447cb8d40e1744ffcd0",
        "ed814a083b0a364b8b56df0da2ffde49787d86424852e576d39921e96f4ff06d",
        "11fb476f429e40cdb354e31e581db1daec633699b3c657960d61a5d99abf6c00",
        "46784eb0a73a560678b99bb5f74934190f97fd84505da1b2f3104a01140c19ef",
        "07fe0690de6b2e33c19eb8dac1f713d79e74e36f8c087b8e1f69c985e1e4018f",
        "d71aa503ac5795e1d8f17044db3ba321798bcd937b1821fa9a7f3e829bf7e508",
        "c5834e0574b807d2dbba139046e2a731adb495a3be1aa0bd299779b747b855dc",
        "ad5564a0cf539676ca808502e6cc2bc1dd11dcd85587959f71456e2a0da0ce4d",
        "a2cf7bf66096fe37ee0ce4dac5f0cd5e1cb599592896abf06b03d409dd17beed",
        "5934a68b41449457616b68830a631e473d8b63770206369cbcfe7133b468852c",
        "be543ddd754dd868aef08d22c55efc5d0a85c47eb93ac5eba5b3e52abb31fa96",
        "078457660351fc8580facfef35b6fff648230ba644c6a5dc517a4f3b91721fe9",
        "70c68a51fb8b19585f5a909e0c6feb7345481cc41625b2b19fc3409a8dbd190c",
        "06d6d118250bee737c089d7c950c502d230b3b592981edcabbbbd33641d0d5a7",
        "964ef84bcd10e7040964ea4798b4b90aa17f29db8ac8bca9dbe749613d721c44",
        "80ff151a4996f23387fe3d91d2dee7123ebe005e08b50e31226d5aaa12be0b16",
        "6219a0d9f9db4f30dba52a2e9a33713dca26d322f45d4b00243bb35423a55f7b",
        "69dff70000be0ae1fd8baa22a0632c41af60877d7cd9b82579850b00f385ccdf",
        "81e9c6f23f7cd3f364a13a6f3ade2ff88bc6c2fba4c4adc5fae8f155f30b7bce",
        "ec5f63fafa7e1413b948a5ad50b818a5e8602cc821fbc5bdb0d3189e5ce2c4e9",
        "e05fa9410e20d4516d1f93c43f0371e0a1a37f19b025f8b2ffc6a9406eaf32a9"
    };
    private static final String[] LEGACY_CAPABILITY_SHA256 = {
        "03041f9681ef6906037e01e18d3d1b9baeb1dbec5b510ee1439afe46227f13ca",
        "e47e65803c6b414ed6f3157808c86fd8be4a34190219fea5a8d5c5ffb4163870",
        "0f789e99d235b259d7dbbb6fb410e044f9cfcc9766a15d8ac7f1c9f08166bf54",
        "4f762f9f1ee11e6ce95482e2579525eae893c753d2ccf5f5f7aa43629e1d1fe8",
        "e277ac18a6444c9f1401d29ed2d82ce092a7fbc56d6eb64e2826b1090f600bbb"
    };
}
