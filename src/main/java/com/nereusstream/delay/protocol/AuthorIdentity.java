package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical closed AuthorIdentity oneof used by signed System Mutations. */
public final class AuthorIdentity {
    private static final int HASH_LENGTH = 32;

    public enum Kind {
        OWNER,
        CONTROL,
        FENCE,
        SERVICE
    }

    private final Kind kind;
    private final byte[] first;
    private final byte[] second;
    private final long generation;
    private final byte[] digest;

    private AuthorIdentity(
            final Kind kind, final byte[] first, final byte[] second, final long generation, final byte[] digest) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.first = nonEmpty(first, "author identity first field");
        this.second = nonEmpty(second, "author identity second field");
        if (generation == 0) {
            throw new IllegalArgumentException("author identity generation/epoch must be nonzero");
        }
        this.generation = generation;
        this.digest = digest == null ? new byte[0] : fixed(digest, "author identity digest");
        if (kind == Kind.OWNER && this.digest.length == 0) {
            throw new IllegalArgumentException("OwnerIdentity requires a lease fencing digest");
        }
        if (kind == Kind.CONTROL && this.digest.length == 0) {
            throw new IllegalArgumentException("ControlAuthor requires a tenant resource scope hash");
        }
    }

    public static AuthorIdentity owner(
            final byte[] deploymentId,
            final byte[] workerRunId,
            final long ownerEpoch,
            final byte[] leaseFencingDigest) {
        return new AuthorIdentity(Kind.OWNER, deploymentId, workerRunId, ownerEpoch, leaseFencingDigest);
    }

    public static AuthorIdentity control(
            final byte[] operationActorIdHash, final byte[] roleSetHash, final byte[] tenantResourceScopeHash) {
        return new AuthorIdentity(
                Kind.CONTROL,
                fixed(operationActorIdHash, "operationActorIdHash"),
                fixed(roleSetHash, "authenticatedRoleSetHash"),
                1,
                fixed(tenantResourceScopeHash, "tenantResourceScopeHash"));
    }

    public static AuthorIdentity fence(final byte[] writerId, final long configGeneration) {
        return new AuthorIdentity(Kind.FENCE, writerId, new byte[] {1}, configGeneration, null);
    }

    public static AuthorIdentity service(
            final byte[] serviceId, final byte[] serviceRunId, final long configGeneration) {
        return new AuthorIdentity(Kind.SERVICE, serviceId, serviceRunId, configGeneration, null);
    }

    public Kind kind() {
        return kind;
    }

    public byte[] first() {
        return Bytes.copy(first);
    }

    public byte[] second() {
        return Bytes.copy(second);
    }

    public long generation() {
        return generation;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    /** Returns the Registry nested OwnerIdentity value for the OWNER branch. */
    public OwnerIdentity asOwnerIdentity() {
        if (kind != Kind.OWNER) {
            throw new IllegalStateException("author identity is not an Owner");
        }
        return new OwnerIdentity(first, second, generation, digest);
    }

    public byte[] canonicalBytes() {
        final byte[] body =
                switch (kind) {
                    case OWNER ->
                        CanonicalProtobuf.message(output -> {
                            CanonicalProtobuf.bytes(output, 1, first);
                            CanonicalProtobuf.bytes(output, 2, second);
                            CanonicalProtobuf.uint64Bits(output, 3, generation);
                            CanonicalProtobuf.bytes(output, 4, digest);
                        });
                    case CONTROL ->
                        CanonicalProtobuf.message(output -> {
                            CanonicalProtobuf.bytes(output, 1, first);
                            CanonicalProtobuf.bytes(output, 2, second);
                            CanonicalProtobuf.bytes(output, 3, digest);
                        });
                    case FENCE ->
                        CanonicalProtobuf.message(output -> {
                            CanonicalProtobuf.bytes(output, 1, first);
                            CanonicalProtobuf.uint64Bits(output, 2, generation);
                        });
                    case SERVICE ->
                        CanonicalProtobuf.message(output -> {
                            CanonicalProtobuf.bytes(output, 1, first);
                            CanonicalProtobuf.bytes(output, 2, second);
                            CanonicalProtobuf.uint64Bits(output, 3, generation);
                        });
                };
        final int field =
                switch (kind) {
                    case OWNER -> 1;
                    case CONTROL -> 2;
                    case FENCE -> 3;
                    case SERVICE -> 4;
                };
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, field, body));
    }

    public void requireFor(final SystemMutationType mutationType) {
        final Kind expected =
                switch (Objects.requireNonNull(mutationType, "mutationType")) {
                    case APPLY_SHARD_CONTROL, REPLAY_DEAD_LETTER, RESOLVE_UNCERTAIN -> Kind.CONTROL;
                    case TIME_FENCE -> Kind.FENCE;
                    case PUBLISH_ADMISSION, PUBLISH_OUTCOME, EXPIRE_GENERATION, CLAIM_RESULT -> Kind.OWNER;
                    case EVIDENCE_RESOLUTION, RESOURCE_RETIRE_INTENT, RESOURCE_DELETE_CONFIRMED, DLQ_EXPORT_RESULT ->
                        Kind.SERVICE;
                };
        if (kind != expected) {
            throw new IllegalArgumentException("author identity branch does not match System Mutation type");
        }
    }

    public static AuthorIdentity decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = readAll(new CanonicalProtobuf.Reader(encoded));
        if (outer.size() != 1 || outer.get(0).wireType() != 2) {
            throw new IllegalArgumentException("AuthorIdentity must contain exactly one tagged branch");
        }
        final int branch = outer.get(0).number();
        final List<CanonicalProtobuf.Reader.Field> fields =
                readAll(new CanonicalProtobuf.Reader(outer.get(0).rawValue()));
        final AuthorIdentity result =
                switch (branch) {
                    case 1 -> decodeOwner(fields);
                    case 2 -> decodeControl(fields);
                    case 3 -> decodeFence(fields);
                    case 4 -> decodeService(fields);
                    default -> throw new IllegalArgumentException("unknown AuthorIdentity branch: " + branch);
                };
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical AuthorIdentity");
        }
        return result;
    }

    private static AuthorIdentity decodeOwner(final List<CanonicalProtobuf.Reader.Field> fields) {
        if (fields.size() != 4) {
            throw new IllegalArgumentException("OwnerIdentity fields are incomplete or unknown");
        }
        return owner(
                bytes(fields.get(0), 1),
                bytes(fields.get(1), 2),
                positive(fields.get(2), 3),
                fixed(bytes(fields.get(3), 4), "leaseFencingDigest"));
    }

    private static AuthorIdentity decodeControl(final List<CanonicalProtobuf.Reader.Field> fields) {
        if (fields.size() != 3) {
            throw new IllegalArgumentException("ControlAuthor fields are incomplete or unknown");
        }
        return control(
                fixed(bytes(fields.get(0), 1), "operationActorIdHash"),
                fixed(bytes(fields.get(1), 2), "authenticatedRoleSetHash"),
                fixed(bytes(fields.get(2), 3), "tenantResourceScopeHash"));
    }

    private static AuthorIdentity decodeFence(final List<CanonicalProtobuf.Reader.Field> fields) {
        if (fields.size() != 2) {
            throw new IllegalArgumentException("FenceWriter fields are incomplete or unknown");
        }
        return fence(bytes(fields.get(0), 1), positive(fields.get(1), 2));
    }

    private static AuthorIdentity decodeService(final List<CanonicalProtobuf.Reader.Field> fields) {
        if (fields.size() != 3) {
            throw new IllegalArgumentException("ServiceWriter fields are incomplete or unknown");
        }
        return service(bytes(fields.get(0), 1), bytes(fields.get(1), 2), positive(fields.get(2), 3));
    }

    private static long positive(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() == 0) {
            throw new IllegalArgumentException("invalid AuthorIdentity numeric field " + number);
        }
        return field.unsignedValue();
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid AuthorIdentity bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof AuthorIdentity that)) {
            return false;
        }
        return kind == that.kind
                && generation == that.generation
                && Arrays.equals(first, that.first)
                && Arrays.equals(second, that.second)
                && Arrays.equals(digest, that.digest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(first), Arrays.hashCode(second), generation, Arrays.hashCode(digest));
    }
}
