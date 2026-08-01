package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Canonical semantic parser for {@code RESOURCE_RETIRE_INTENT_V1}.
 *
 * <p>The parser validates the closed resource identity and protection-set
 * shapes before a shard is allowed to persist a retire intent. It does not
 * perform an external delete or claim that a Recovery Floor has released a
 * protection.</p>
 */
public final class ResourceRetireIntentBody {
    private static final int HASH_LENGTH = 32;
    private static final int INCARNATION_LENGTH = 16;

    private final ResourceKind resourceKind;
    private final ExactResourceIdentity resource;
    private final long expectedResourceStateVersion;
    private final ProtectionSet protections;

    private ResourceRetireIntentBody(final ResourceKind resourceKind, final ExactResourceIdentity resource,
                                      final long expectedResourceStateVersion, final ProtectionSet protections) {
        this.resourceKind = Objects.requireNonNull(resourceKind, "resourceKind");
        this.resource = Objects.requireNonNull(resource, "resource");
        if (resource.kind() != resourceKind) {
            throw new IllegalArgumentException("resource kind does not match identity branch");
        }
        if (expectedResourceStateVersion < 0) {
            throw new IllegalArgumentException("expected resource state version must be non-negative");
        }
        this.expectedResourceStateVersion = expectedResourceStateVersion;
        this.protections = Objects.requireNonNull(protections, "protections");
    }

    public static ResourceRetireIntentBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.RESOURCE_RETIRE_INTENT, canonicalBody);
        final ResourceKind kind = ResourceKind.fromWire(unsigned(field(fields, 10), 10));
        final ExactResourceIdentity resource = ExactResourceIdentity.decode(kind, nested(field(fields, 11), 11));
        final long expectedVersion = unsigned(field(fields, 12), 12);
        final ProtectionSet protections = ProtectionSet.decode(nested(field(fields, 13), 13));
        return new ResourceRetireIntentBody(kind, resource, expectedVersion, protections);
    }

    public ResourceKind resourceKind() {
        return resourceKind;
    }

    public ExactResourceIdentity resource() {
        return resource;
    }

    public long expectedResourceStateVersion() {
        return expectedResourceStateVersion;
    }

    public ProtectionSet protections() {
        return protections;
    }

    /** Closed ExactResourceIdentityV1 projection. */
    public record ExactResourceIdentity(ResourceKind kind, byte[] canonicalBytes, byte[] identityHash) {
        public ExactResourceIdentity {
            Objects.requireNonNull(kind, "kind");
            if (canonicalBytes == null || canonicalBytes.length == 0) {
                throw new IllegalArgumentException("resource identity must not be empty");
            }
            Bytes.requireLength(identityHash, HASH_LENGTH, "resource identity hash");
            canonicalBytes = Bytes.copy(canonicalBytes);
            identityHash = Bytes.copy(identityHash);
        }

        @Override
        public byte[] canonicalBytes() {
            return Bytes.copy(canonicalBytes);
        }

        @Override
        public byte[] identityHash() {
            return Bytes.copy(identityHash);
        }

        private static ExactResourceIdentity decode(final ResourceKind kind, final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> outer = read(encoded, "ExactResourceIdentity");
            if (outer.size() != 1 || outer.get(0).wireType() != 2
                    || outer.get(0).number() != kind.wireValue()) {
                throw new IllegalArgumentException("resource identity branch does not match resource kind");
            }
            final byte[] branch = outer.get(0).rawValue();
            validateBranch(kind, branch);
            final byte[] canonical = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output,
                    kind.wireValue(), branch));
            if (!Arrays.equals(encoded, canonical)) {
                throw new IllegalArgumentException("non-canonical ExactResourceIdentity");
            }
            return new ExactResourceIdentity(kind, canonical,
                    Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity-v1\0"), canonical));
        }

        private static void validateBranch(final ResourceKind kind, final byte[] branch) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(branch, kind + " resource");
            switch (kind) {
                case PAYLOAD_OBJECT -> {
                    requireCount(fields, 6, 7, kind);
                    if (fields.size() == 6) {
                        requireNumbers(fields, new int[]{1, 2, 3, 4, 6, 7}, kind);
                    } else {
                        requireExact(fields, 7, kind);
                    }
                    profileRef(nested(field(fields, 1), 1));
                    nonEmpty(bytes(field(fields, 2), 2), 2);
                    nonEmpty(bytes(field(fields, 3), 3), 3);
                    nonEmpty(bytes(field(fields, 4), 4), 4);
                    if (fields.size() == 7) {
                        nonEmpty(bytes(field(fields, 5), 5), 5);
                    }
                    positive(unsigned(field(fields, 6), 6), 6);
                    fixed(bytes(field(fields, 7), 7), HASH_LENGTH, 7);
                }
                case CHECKPOINT -> {
                    requireExact(fields, 8, kind);
                    fixed(bytes(field(fields, 1), 1), INCARNATION_LENGTH, 1);
                    fixed(bytes(field(fields, 2), 2), INCARNATION_LENGTH, 2);
                    profileRef(nested(field(fields, 3), 3));
                    nonEmpty(bytes(field(fields, 4), 4), 4);
                    nonEmpty(bytes(field(fields, 5), 5), 5);
                    nonEmpty(bytes(field(fields, 6), 6), 6);
                    positive(unsigned(field(fields, 7), 7), 7);
                    fixed(bytes(field(fields, 8), 8), HASH_LENGTH, 8);
                }
                case DLQ_EXPORT_OBJECT -> {
                    requireExact(fields, 4, kind);
                    fixed(bytes(field(fields, 1), 1), HASH_LENGTH, 1);
                    PublishAdmissionBody.validateBrokerResourceIdentity(nested(field(fields, 2), 2));
                    nonEmpty(bytes(field(fields, 3), 3), 3);
                    fixed(bytes(field(fields, 4), 4), HASH_LENGTH, 4);
                }
                case KAFKA_RECEIPT_SLOT -> {
                    requireExact(fields, 6, kind);
                    nonEmpty(bytes(field(fields, 1), 1), 1);
                    fixed(bytes(field(fields, 2), 2), INCARNATION_LENGTH, 2);
                    fixed(bytes(field(fields, 3), 3), INCARNATION_LENGTH, 3);
                    nonNegative(unsigned(field(fields, 4), 4), 4);
                    nonNegative(unsigned(field(fields, 5), 5), 5);
                    positive(unsigned(field(fields, 6), 6), 6);
                }
                case PULSAR_JOURNAL_GENERATION -> {
                    requireExact(fields, 3, kind);
                    PublishAdmissionBody.validateBrokerResourceIdentity(nested(field(fields, 1), 1));
                    nonNegative(unsigned(field(fields, 2), 2), 2);
                    positive(unsigned(field(fields, 3), 3), 3);
                }
                case LANE_CHANNEL -> {
                    requireExact(fields, 1, kind);
                    PublishAdmissionBody.decodeChannelIdentity(nested(field(fields, 1), 1));
                }
                case LOCAL_STORE -> {
                    requireExact(fields, 4, kind);
                    shardSubject(nested(field(fields, 1), 1));
                    fixed(bytes(field(fields, 2), 2), INCARNATION_LENGTH, 2);
                    fixed(bytes(field(fields, 3), 3), HASH_LENGTH, 3);
                    fixed(bytes(field(fields, 4), 4), HASH_LENGTH, 4);
                }
            }
        }
    }

    /** Canonical ProtectionSetV1 projection. */
    public record ProtectionSet(List<ProtectionRef> references, byte[] canonicalBytes, byte[] digest) {
        public ProtectionSet {
            Objects.requireNonNull(references, "references");
            Objects.requireNonNull(canonicalBytes, "canonicalBytes");
            Bytes.requireLength(digest, HASH_LENGTH, "protection set digest");
            references = List.copyOf(references);
            canonicalBytes = Bytes.copy(canonicalBytes);
            digest = Bytes.copy(digest);
        }

        @Override
        public byte[] canonicalBytes() {
            return Bytes.copy(canonicalBytes);
        }

        @Override
        public byte[] digest() {
            return Bytes.copy(digest);
        }

        private static ProtectionSet decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ProtectionSet");
            if (fields.isEmpty() || fields.get(fields.size() - 1).number() != 2) {
                throw new IllegalArgumentException("ProtectionSet must contain a digest field");
            }
            final List<ProtectionRef> refs = new ArrayList<>();
            for (CanonicalProtobuf.Reader.Field field : fields) {
                if (field.number() == 1) {
                    refs.add(ProtectionRef.decode(field.rawValue()));
                } else if (field.number() != 2) {
                    throw new IllegalArgumentException("unknown ProtectionSet field " + field.number());
                }
            }
            final byte[] canonicalRefs = CanonicalProtobuf.message(output -> {
                for (ProtectionRef ref : refs) {
                    CanonicalProtobuf.bytes(output, 1, ref.canonicalBytes());
                }
            });
            final byte[] digest = fixed(bytes(field(fields, 2), 2), HASH_LENGTH, 2);
            final byte[] expected = Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"), canonicalRefs);
            if (!Bytes.constantTimeEquals(digest, expected)) {
                throw new IllegalArgumentException("ProtectionSet digest mismatch");
            }
            final List<ProtectionRef> sorted = new ArrayList<>(refs);
            sorted.sort(Comparator.comparing(ref -> Bytes.hex(ref.canonicalBytes())));
            if (!refs.equals(sorted)) {
                throw new IllegalArgumentException("ProtectionSet references are not canonical-byte sorted");
            }
            for (int index = 1; index < refs.size(); index++) {
                if (Arrays.equals(refs.get(index - 1).canonicalBytes(), refs.get(index).canonicalBytes())) {
                    throw new IllegalArgumentException("ProtectionSet contains duplicate references");
                }
            }
            final byte[] canonical = CanonicalProtobuf.message(output -> {
                for (ProtectionRef ref : refs) {
                    CanonicalProtobuf.bytes(output, 1, ref.canonicalBytes());
                }
                CanonicalProtobuf.bytes(output, 2, digest);
            });
            if (!Arrays.equals(encoded, canonical)) {
                throw new IllegalArgumentException("non-canonical ProtectionSet");
            }
            return new ProtectionSet(refs, canonical, digest);
        }
    }

    /** Canonical ProtectionRefV1 projection. */
    public record ProtectionRef(int protectionKind, byte[] protectedResourceId, long protectionGeneration,
                                byte[] minimumSourcePosition, byte[] recoveryLineageId, byte[] checkpointId,
                                byte[] manifestHash, byte[] canonicalBytes) {
        public ProtectionRef {
            if (protectionKind < 1 || protectionKind > 6) {
                throw new IllegalArgumentException("unknown protection kind");
            }
            Bytes.requireLength(protectedResourceId, HASH_LENGTH, "protected resource id");
            if (protectionGeneration < 0) {
                throw new IllegalArgumentException("protection generation must be non-negative");
            }
            Objects.requireNonNull(minimumSourcePosition, "minimumSourcePosition");
            Objects.requireNonNull(recoveryLineageId, "recoveryLineageId");
            Objects.requireNonNull(checkpointId, "checkpointId");
            Objects.requireNonNull(manifestHash, "manifestHash");
            Objects.requireNonNull(canonicalBytes, "canonicalBytes");
            protectedResourceId = Bytes.copy(protectedResourceId);
            minimumSourcePosition = Bytes.copy(minimumSourcePosition);
            recoveryLineageId = Bytes.copy(recoveryLineageId);
            checkpointId = Bytes.copy(checkpointId);
            manifestHash = Bytes.copy(manifestHash);
            canonicalBytes = Bytes.copy(canonicalBytes);
        }

        @Override
        public byte[] protectedResourceId() {
            return Bytes.copy(protectedResourceId);
        }

        @Override
        public byte[] minimumSourcePosition() {
            return Bytes.copy(minimumSourcePosition);
        }

        @Override
        public byte[] recoveryLineageId() {
            return Bytes.copy(recoveryLineageId);
        }

        @Override
        public byte[] checkpointId() {
            return Bytes.copy(checkpointId);
        }

        @Override
        public byte[] manifestHash() {
            return Bytes.copy(manifestHash);
        }

        @Override
        public byte[] canonicalBytes() {
            return Bytes.copy(canonicalBytes);
        }

        private static ProtectionRef decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ProtectionRef");
            if (fields.size() < 3 || fields.size() > 7) {
                throw new IllegalArgumentException("ProtectionRef fields are incomplete");
            }
            final int kind = Math.toIntExact(unsigned(field(fields, 1), 1));
            final byte[] resourceId = fixed(bytes(field(fields, 2), 2), HASH_LENGTH, 2);
            final long generation = unsigned(field(fields, 3), 3);
            final byte[] source = optionalBytes(fields, 4);
            final byte[] lineage = optionalBytes(fields, 5);
            final byte[] checkpoint = optionalBytes(fields, 6);
            final byte[] manifest = optionalBytes(fields, 7);
            if (kind == 1) {
                if (source.length == 0 || lineage.length != INCARNATION_LENGTH
                        || checkpoint.length != INCARNATION_LENGTH || manifest.length != HASH_LENGTH) {
                    throw new IllegalArgumentException("Recovery Floor protection is incomplete");
                }
                SourcePositionCodec.decode(source);
            } else if (kind == 2 || kind == 4) {
                if (source.length == 0 || lineage.length != 0 || checkpoint.length != 0 || manifest.length != 0) {
                    throw new IllegalArgumentException("time-bound protection fields are invalid");
                }
                SourcePositionCodec.decode(source);
            } else if (source.length != 0 || lineage.length != 0 || checkpoint.length != 0 || manifest.length != 0) {
                throw new IllegalArgumentException("non-time-bound protection carries source fields");
            }
            final byte[] canonical = CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, kind);
                CanonicalProtobuf.bytes(output, 2, resourceId);
                CanonicalProtobuf.uint32(output, 3, generation);
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
            if (!Arrays.equals(encoded, canonical)) {
                throw new IllegalArgumentException("non-canonical ProtectionRef");
            }
            return new ProtectionRef(kind, resourceId, generation, source, lineage, checkpoint, manifest, canonical);
        }
    }

    private static void profileRef(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ProfileRef");
        requireExact(fields, 4, "ProfileRef");
        nonEmpty(bytes(field(fields, 1), 1), 1);
        positive(unsigned(field(fields, 2), 2), 2);
        fixed(bytes(field(fields, 3), 3), HASH_LENGTH, 3);
        final long kind = unsigned(field(fields, 4), 4);
        if (kind < 1 || kind > 4) {
            throw new IllegalArgumentException("invalid ProfileRef kind");
        }
    }

    private static void shardSubject(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ShardSubject");
        requireExact(fields, 2, "ShardSubject");
        fixed(bytes(field(fields, 1), 1), RouteIncarnation.LENGTH, 1);
        nonNegative(unsigned(field(fields, 2), 2), 2);
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return fields;
    }

    private static void requireExact(final List<CanonicalProtobuf.Reader.Field> fields, final int count,
                                     final Object name) {
        if (fields.size() != count) {
            throw new IllegalArgumentException(name + " fields are incomplete or unknown");
        }
        for (int index = 0; index < count; index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException(name + " fields are out of order");
            }
        }
    }

    private static void requireCount(final List<CanonicalProtobuf.Reader.Field> fields, final int minimum,
                                     final int maximum, final Object name) {
        if (fields.size() < minimum || fields.size() > maximum) {
            throw new IllegalArgumentException(name + " fields are incomplete or unknown");
        }
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int[] numbers,
                                       final Object name) {
        if (fields.size() != numbers.length) {
            throw new IllegalArgumentException(name + " fields are incomplete or unknown");
        }
        for (int index = 0; index < numbers.length; index++) {
            if (fields.get(index).number() != numbers[index]) {
                throw new IllegalArgumentException(name + " fields are out of order");
            }
        }
    }

    private static CanonicalProtobuf.Reader.Field field(final List<CanonicalProtobuf.Reader.Field> fields,
                                                        final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        throw new IllegalArgumentException("missing nested field " + number);
    }

    private static byte[] optionalBytes(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return bytes(field, number);
            }
        }
        return new byte[0];
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        return bytes(field, number);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid nested bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final byte[] value, final int length, final int number) {
        Bytes.requireLength(value, length, "nested field " + number);
        return value;
    }

    private static byte[] nonEmpty(final byte[] value, final int number) {
        if (value.length == 0) {
            throw new IllegalArgumentException("nested field " + number + " must not be empty");
        }
        return value;
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid nested uint field " + number);
        }
        return field.unsignedValue();
    }

    private static long positive(final long value, final int number) {
        if (value <= 0) {
            throw new IllegalArgumentException("nested field " + number + " must be positive");
        }
        return value;
    }

    private static long nonNegative(final long value, final int number) {
        if (value < 0) {
            throw new IllegalArgumentException("nested field " + number + " must be non-negative");
        }
        return value;
    }
}
