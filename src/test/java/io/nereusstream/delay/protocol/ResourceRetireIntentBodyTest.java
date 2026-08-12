package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourceRetireIntentBodyTest {
    @Test
    void localStoreIdentityAndProtectionSetAreCanonical() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 17);
        final byte[] protection = protectionRef(3, Bytes.sha256(Bytes.utf8("lane-channel")), 4);
        final byte[] body = resourceBody(shard, ResourceKind.LOCAL_STORE, localStoreIdentity(shard), 9,
                protectionSet(protection));

        final ResourceRetireIntentBody decoded = ResourceRetireIntentBody.decode(body);

        assertEquals(ResourceKind.LOCAL_STORE, decoded.resourceKind());
        assertEquals(9, decoded.expectedResourceStateVersion());
        assertEquals(1, decoded.protections().references().size());
        assertArrayEquals(Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity-v1\0"),
                decoded.resource().canonicalBytes()), decoded.resource().identityHash());
        assertArrayEquals(SystemMutation.computeResourceRetireLogicalIdentity(ResourceKind.LOCAL_STORE,
                decoded.resource().identityHash(), 9),
                SystemMutation.computeResourceRetireLogicalIdentity(decoded.resourceKind(),
                decoded.resource().identityHash(), decoded.expectedResourceStateVersion()));
    }

    @Test
    void preservesUnsignedResourceStateVersionBits() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 25);
        final long expectedVersion = Long.MIN_VALUE;
        final byte[] body = resourceBody(shard, ResourceKind.LOCAL_STORE, localStoreIdentity(shard), expectedVersion,
                protectionSet());

        final ResourceRetireIntentBody decoded = ResourceRetireIntentBody.decode(body);

        assertEquals(expectedVersion, decoded.expectedResourceStateVersion());
        assertArrayEquals(SystemMutation.computeResourceRetireLogicalIdentity(ResourceKind.LOCAL_STORE,
                decoded.resource().identityHash(), expectedVersion),
                SystemMutation.computeResourceRetireLogicalIdentity(decoded.resourceKind(),
                        decoded.resource().identityHash(), decoded.expectedResourceStateVersion()));
    }

    @Test
    void preservesUnsignedProtectionGenerationBits() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 26);
        final long protectionGeneration = Long.MIN_VALUE;
        final byte[] protection = protectionRef(3, Bytes.sha256(Bytes.utf8("high-bit-protection")),
                protectionGeneration);
        final ResourceRetireIntentBody decoded = ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.LOCAL_STORE, localStoreIdentity(shard), 1, protectionSet(protection)));

        assertEquals(protectionGeneration, decoded.protections().references().get(0).protectionGeneration());
    }

    @Test
    void preservesUnsignedResourceIdentityProfileAndExternalGenerationBits() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 27);
        final byte[] payload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profileRef(Long.MIN_VALUE, ProfileKindV1.OBJECT_STORE));
            CanonicalProtobuf.bytes(output, 2, Bytes.utf8("container"));
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8("object-key"));
            CanonicalProtobuf.bytes(output, 4, Bytes.utf8("version-1"));
            CanonicalProtobuf.uint64Bits(output, 6, 12);
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(Bytes.utf8("payload")));
        });
        final ResourceRetireIntentBody payloadBody = ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.PAYLOAD_OBJECT, branch(ResourceKind.PAYLOAD_OBJECT, payload), 1,
                protectionSet()));
        assertArrayEquals(payload, branch(payloadBody.resource()));

        final byte[] kafka = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("cluster"));
            CanonicalProtobuf.bytes(output, 2, new byte[16]);
            CanonicalProtobuf.bytes(output, 3, new byte[16]);
            CanonicalProtobuf.uint32(output, 4, 3);
            CanonicalProtobuf.uint32(output, 5, 4);
            CanonicalProtobuf.uint64Bits(output, 6, Long.MIN_VALUE);
        });
        final ResourceRetireIntentBody kafkaBody = ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.KAFKA_RECEIPT_SLOT, branch(ResourceKind.KAFKA_RECEIPT_SLOT, kafka), 1,
                protectionSet()));
        assertArrayEquals(kafka, branch(kafkaBody.resource()));

        final PulsarBrokerResourceIdentityV1 broker = new PulsarBrokerResourceIdentityV1(
                "cluster", new byte[32], "persistent://tenant/topic", Long.MIN_VALUE);
        final byte[] pulsar = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, broker.canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, 5);
            CanonicalProtobuf.uint64Bits(output, 3, -1L);
        });
        final ResourceRetireIntentBody pulsarBody = ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.PULSAR_JOURNAL_GENERATION,
                branch(ResourceKind.PULSAR_JOURNAL_GENERATION, pulsar), 1, protectionSet()));
        assertArrayEquals(pulsar, branch(pulsarBody.resource()));

        PublishAdmissionBody.validateBrokerResourceIdentity(broker.canonicalBytes());
    }

    @Test
    void payloadObjectWithoutOptionalEtagUsesLengthAndHashFieldsSixAndSeven() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 18);
        final byte[] payload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profileRef());
            CanonicalProtobuf.bytes(output, 2, Bytes.utf8("container"));
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8("object-key"));
            CanonicalProtobuf.bytes(output, 4, Bytes.utf8("version-1"));
            CanonicalProtobuf.uint32(output, 6, 12);
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(Bytes.utf8("payload")));
        });

        final ResourceRetireIntentBody decoded = ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.PAYLOAD_OBJECT, branch(ResourceKind.PAYLOAD_OBJECT, payload), 1,
                protectionSet()));

        assertEquals(ResourceKind.PAYLOAD_OBJECT, decoded.resourceKind());
        assertArrayEquals(payload, branch(decoded.resource()));
    }

    @Test
    void objectResourceIdentitiesUseObjectStoreProfilesAndAllowZeroLengthObjects() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 28);
        final byte[] payload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profileRef(1, ProfileKindV1.OBJECT_STORE));
            CanonicalProtobuf.bytes(output, 2, Bytes.utf8("container"));
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8("empty-object"));
            CanonicalProtobuf.bytes(output, 4, Bytes.utf8("version-1"));
            CanonicalProtobuf.uint64(output, 6, 0);
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(new byte[0]));
        });
        final ResourceRetireIntentBody payloadBody = ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.PAYLOAD_OBJECT, branch(ResourceKind.PAYLOAD_OBJECT, payload), 1,
                protectionSet()));
        assertArrayEquals(payload, branch(payloadBody.resource()));

        final byte[] checkpoint = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(16, 31));
            CanonicalProtobuf.bytes(output, 2, bytes(16, 32));
            CanonicalProtobuf.bytes(output, 3, profileRef(1, ProfileKindV1.OBJECT_STORE));
            CanonicalProtobuf.bytes(output, 4, Bytes.utf8("container"));
            CanonicalProtobuf.bytes(output, 5, Bytes.utf8("empty-manifest"));
            CanonicalProtobuf.bytes(output, 6, Bytes.utf8("version-1"));
            CanonicalProtobuf.uint64(output, 7, 0);
            CanonicalProtobuf.bytes(output, 8, Bytes.sha256(new byte[0]));
        });
        final ResourceRetireIntentBody checkpointBody = ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.CHECKPOINT, branch(ResourceKind.CHECKPOINT, checkpoint), 1,
                protectionSet()));
        assertArrayEquals(checkpoint, branch(checkpointBody.resource()));

        final byte[] wrongProfile = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profileRef(1, ProfileKindV1.DESTINATION));
            CanonicalProtobuf.bytes(output, 2, Bytes.utf8("container"));
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8("object"));
            CanonicalProtobuf.bytes(output, 4, Bytes.utf8("version-1"));
            CanonicalProtobuf.uint64(output, 6, 0);
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(new byte[0]));
        });
        assertThrows(IllegalArgumentException.class, () -> ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.PAYLOAD_OBJECT, branch(ResourceKind.PAYLOAD_OBJECT, wrongProfile), 1,
                protectionSet())));
    }

    @Test
    void branchAndProtectionDigestMismatchesAreRejected() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        final byte[] localStore = localStoreIdentity(shard);
        final byte[] wrongBranch = branch(ResourceKind.PAYLOAD_OBJECT, localStore);
        assertThrows(IllegalArgumentException.class, () -> ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.LOCAL_STORE, wrongBranch, 1, protectionSet())));

        final byte[] ref = protectionRef(3, Bytes.sha256(Bytes.utf8("resource")), 1);
        final byte[] mismatchedProtectionSet = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, ref);
            CanonicalProtobuf.bytes(output, 2, new byte[32]);
        });
        assertThrows(IllegalArgumentException.class, () -> ResourceRetireIntentBody.decode(resourceBody(shard,
                ResourceKind.LOCAL_STORE, localStore, 1, mismatchedProtectionSet)));
    }

    @Test
    void protectionRefConstructorRequiresCanonicalSourceAndKindSpecificFields() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 29);
        final byte[] source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 3, null, 1_000)
                .canonicalBytes();
        final byte[] resourceId = Bytes.sha256(Bytes.utf8("source-protection"));
        final byte[] canonical = protectionRef(2, resourceId, 7, source, new byte[0], new byte[0], new byte[0]);

        final ResourceRetireIntentBody.ProtectionRef ref = new ResourceRetireIntentBody.ProtectionRef(
                2, resourceId, 7, source, new byte[0], new byte[0], new byte[0], canonical);
        assertArrayEquals(source, ref.minimumSourcePosition());
        assertArrayEquals(canonical, ref.canonicalBytes());

        assertThrows(IllegalArgumentException.class, () -> new ResourceRetireIntentBody.ProtectionRef(
                2, resourceId, 7, Bytes.concat(source, new byte[]{0}), new byte[0], new byte[0], new byte[0],
                canonical));
        assertThrows(IllegalArgumentException.class, () -> new ResourceRetireIntentBody.ProtectionRef(
                3, resourceId, 7, source, new byte[0], new byte[0], new byte[0],
                protectionRef(3, resourceId, 7, source, new byte[0], new byte[0], new byte[0])));
        assertThrows(IllegalArgumentException.class, () -> new ResourceRetireIntentBody.ProtectionRef(
                2, resourceId, 7, source, new byte[0], new byte[0], new byte[0], new byte[]{1}));
    }

    @Test
    void exactIdentityAndProtectionSetConstructorsRequireCanonicalDigestsAndOrdering() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 30);
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject(shard));
            CanonicalProtobuf.bytes(output, 2, new byte[16]);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("db")));
            CanonicalProtobuf.bytes(output, 4, Bytes.sha256(Bytes.utf8("root-policy")));
        });
        final byte[] identity = branch(ResourceKind.LOCAL_STORE, branch);
        final byte[] identityHash = Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity-v1\0"), identity);
        final ResourceRetireIntentBody.ExactResourceIdentity exact =
                new ResourceRetireIntentBody.ExactResourceIdentity(ResourceKind.LOCAL_STORE, identity, identityHash);
        assertArrayEquals(identity, exact.canonicalBytes());
        assertArrayEquals(identityHash, exact.identityHash());
        assertThrows(IllegalArgumentException.class, () ->
                new ResourceRetireIntentBody.ExactResourceIdentity(ResourceKind.LOCAL_STORE, identity, new byte[32]));
        assertThrows(IllegalArgumentException.class, () ->
                new ResourceRetireIntentBody.ExactResourceIdentity(ResourceKind.LOCAL_STORE,
                        Bytes.concat(identity, new byte[]{0}), identityHash));

        final byte[] first = protectionRef(3, Bytes.sha256(Bytes.utf8("first")), 1);
        final byte[] second = protectionRef(3, Bytes.sha256(Bytes.utf8("second")), 2);
        final List<byte[]> refs = new ArrayList<>(List.of(first, second));
        refs.sort((left, right) -> Bytes.hex(left).compareTo(Bytes.hex(right)));
        final byte[] protectionSet = protectionSet(refs.toArray(byte[][]::new));
        final List<ResourceRetireIntentBody.ProtectionRef> decodedRefs = new ArrayList<>();
        for (byte[] ref : refs) {
            decodedRefs.add(ResourceRetireIntentBody.decode(resourceBody(shard, ResourceKind.LOCAL_STORE, identity, 1,
                    protectionSet(ref))).protections().references().get(0));
        }
        decodedRefs.sort((left, right) -> Bytes.hex(left.canonicalBytes())
                .compareTo(Bytes.hex(right.canonicalBytes())));
        final ResourceRetireIntentBody.ProtectionSet valid = new ResourceRetireIntentBody.ProtectionSet(
                decodedRefs, protectionSet, Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"),
                        CanonicalProtobuf.message(output -> {
                            for (ResourceRetireIntentBody.ProtectionRef ref : decodedRefs) {
                                CanonicalProtobuf.bytes(output, 1, ref.canonicalBytes());
                            }
                        })));
        assertArrayEquals(protectionSet, valid.canonicalBytes());
        assertThrows(IllegalArgumentException.class, () -> new ResourceRetireIntentBody.ProtectionSet(
                List.of(decodedRefs.get(0), decodedRefs.get(0)), protectionSet, valid.digest()));
        assertThrows(IllegalArgumentException.class, () -> new ResourceRetireIntentBody.ProtectionSet(
                decodedRefs, protectionSet, new byte[32]));
    }

    private static byte[] resourceBody(final ShardId shard, final ResourceKind kind, final byte[] resource,
                                       final long expectedVersion, final byte[] protections) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject(shard));
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOURCE_RETIRE_INTENT.wireValue());
            CanonicalProtobuf.int64(output, 3, 10_000);
            CanonicalProtobuf.uint32(output, 10, kind.wireValue());
            CanonicalProtobuf.bytes(output, 11, resource);
            CanonicalProtobuf.uint64Bits(output, 12, expectedVersion);
            CanonicalProtobuf.bytes(output, 13, protections);
        });
    }

    private static byte[] localStoreIdentity(final ShardId shard) {
        return branch(ResourceKind.LOCAL_STORE, CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject(shard));
            CanonicalProtobuf.bytes(output, 2, new byte[16]);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("db")));
            CanonicalProtobuf.bytes(output, 4, Bytes.sha256(Bytes.utf8("root-policy")));
        }));
    }

    private static byte[] profileRef() {
        return profileRef(1, ProfileKindV1.OBJECT_STORE);
    }

    private static byte[] profileRef(final long version, final ProfileKindV1 kind) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("object-store"));
            CanonicalProtobuf.uint64Bits(output, 2, version);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("profile")));
            CanonicalProtobuf.uint32(output, 4, kind.wireValue());
        });
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] protectionSet(final byte[]... refs) {
        final byte[] canonicalRefs = CanonicalProtobuf.message(output -> {
            for (byte[] ref : refs) {
                CanonicalProtobuf.bytes(output, 1, ref);
            }
        });
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"), canonicalRefs);
        return CanonicalProtobuf.message(output -> {
            for (byte[] ref : refs) {
                CanonicalProtobuf.bytes(output, 1, ref);
            }
            CanonicalProtobuf.bytes(output, 2, digest);
        });
    }

    private static byte[] protectionRef(final int kind, final byte[] resourceId, final long generation) {
        return protectionRef(kind, resourceId, generation, new byte[0], new byte[0], new byte[0], new byte[0]);
    }

    private static byte[] protectionRef(final int kind, final byte[] resourceId, final long generation,
                                        final byte[] source, final byte[] lineage, final byte[] checkpoint,
                                        final byte[] manifest) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind);
            CanonicalProtobuf.bytes(output, 2, resourceId);
            CanonicalProtobuf.uint64Bits(output, 3, generation);
            if (source.length != 0) {
                CanonicalProtobuf.bytes(output, 4, source);
            }
            if (lineage.length != 0) {
                CanonicalProtobuf.bytes(output, 5, lineage);
            }
            if (checkpoint.length != 0) {
                CanonicalProtobuf.bytes(output, 6, checkpoint);
            }
            if (manifest.length != 0) {
                CanonicalProtobuf.bytes(output, 7, manifest);
            }
        });
    }

    private static byte[] branch(final ResourceKind kind, final byte[] fields) {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, kind.wireValue(), fields));
    }

    private static byte[] branch(final ResourceRetireIntentBody.ExactResourceIdentity identity) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(identity.canonicalBytes());
        return reader.next().rawValue();
    }

    private static byte[] subject(final ShardId shard) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
    }
}
