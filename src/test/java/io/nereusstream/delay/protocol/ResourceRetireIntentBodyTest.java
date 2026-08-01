package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

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

    private static byte[] resourceBody(final ShardId shard, final ResourceKind kind, final byte[] resource,
                                       final long expectedVersion, final byte[] protections) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject(shard));
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOURCE_RETIRE_INTENT.wireValue());
            CanonicalProtobuf.int64(output, 3, 10_000);
            CanonicalProtobuf.uint32(output, 10, kind.wireValue());
            CanonicalProtobuf.bytes(output, 11, resource);
            CanonicalProtobuf.uint32(output, 12, expectedVersion);
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
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("object-store"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("profile")));
            CanonicalProtobuf.uint32(output, 4, 1);
        });
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
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind);
            CanonicalProtobuf.bytes(output, 2, resourceId);
            CanonicalProtobuf.uint32(output, 3, generation);
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
