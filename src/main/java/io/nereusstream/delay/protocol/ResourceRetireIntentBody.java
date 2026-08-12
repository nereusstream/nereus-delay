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
        this.expectedResourceStateVersion = expectedResourceStateVersion;
        this.protections = Objects.requireNonNull(protections, "protections");
    }

    public static ResourceRetireIntentBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.RESOURCE_RETIRE_INTENT, canonicalBody);
        final ResourceKind kind = ResourceKind.fromWire(unsigned(field(fields, 10), 10));
        final ExactResourceIdentity resource = decodeResourceIdentity(kind, nested(field(fields, 11), 11));
        final long expectedVersion = rawUnsigned(field(fields, 12), 12);
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

    /**
     * Validates that every source-bearing protection reference belongs to the
     * shard applying this retire intent. The protection-set codec can validate
     * canonical Source Position bytes, but only the applying shard knows the
     * ownership boundary that those positions must satisfy.
     */
    public void validateProtectionSourceShard(final ShardId expectedShard) {
        Objects.requireNonNull(expectedShard, "expectedShard");
        for (ProtectionRef reference : protections.references()) {
            if (reference.minimumSourcePosition().length == 0) {
                continue;
            }
            final SourcePosition sourcePosition = SourcePositionCodec.decode(reference.minimumSourcePosition());
            if (!expectedShard.equals(sourcePosition.shardId())) {
                throw new IllegalArgumentException("protection source position belongs to another shard");
            }
        }
    }

    /** Decodes one closed identity outside the enclosing retire body. */
    public static ExactResourceIdentity decodeResourceIdentity(final ResourceKind kind, final byte[] encoded) {
        Objects.requireNonNull(kind, "kind");
        return ExactResourceIdentity.decode(kind, encoded);
    }

    /**
     * Checks optional provider-returned version/etag fields against identity branches that expose them.
     * Other resource branches retain those fields for the authenticated adapter to interpret.
     */
    public static void validateExternalDeleteIdentity(final ResourceKind kind, final byte[] encoded,
                                                       final byte[] observedImmutableVersion,
                                                       final byte[] observedEtag) {
        validateExternalDeleteIdentity(kind, encoded, observedImmutableVersion, observedEtag, null);
    }

    /**
     * Checks provider-returned identity fields for a specific delete outcome.
     * A {@code DELETED} payload/checkpoint must carry the immutable version
     * that was actually deleted; an empty response cannot prove that the
     * exact pinned object was removed.  The legacy overload above remains a
     * shape-only compatibility seam for callers that do not yet carry the
     * outcome tag.
     */
    public static void validateExternalDeleteIdentity(final ResourceKind kind, final byte[] encoded,
                                                       final byte[] observedImmutableVersion,
                                                       final byte[] observedEtag,
                                                       final ResourceDeleteConfirmedBody.DeleteOutcome outcome) {
        final ExactResourceIdentity identity = decodeResourceIdentity(kind, encoded);
        Objects.requireNonNull(observedImmutableVersion, "observedImmutableVersion");
        Objects.requireNonNull(observedEtag, "observedEtag");
        final CanonicalProtobuf.Reader.Field branchField = new CanonicalProtobuf.Reader(identity.canonicalBytes()).next();
        final List<CanonicalProtobuf.Reader.Field> fields = read(branchField.rawValue(), kind + " resource");
        if (kind == ResourceKind.PAYLOAD_OBJECT) {
            final byte[] exactVersion = bytes(field(fields, 4), 4);
            if (outcome == ResourceDeleteConfirmedBody.DeleteOutcome.DELETED
                    && observedImmutableVersion.length == 0) {
                throw new IllegalArgumentException("DELETED payload evidence must carry immutable version");
            }
            if (observedImmutableVersion.length != 0
                    && !Arrays.equals(exactVersion, observedImmutableVersion)) {
                throw new IllegalArgumentException("delete evidence immutable version does not match payload identity");
            }
            final byte[] exactEtag = optionalBytes(fields, 5);
            if (outcome == ResourceDeleteConfirmedBody.DeleteOutcome.DELETED
                    && exactEtag.length != 0 && observedEtag.length == 0) {
                throw new IllegalArgumentException("DELETED payload evidence must carry pinned etag");
            }
            if (observedEtag.length != 0
                    && (exactEtag.length == 0 || !Arrays.equals(exactEtag, observedEtag))) {
                throw new IllegalArgumentException("delete evidence etag does not match payload identity");
            }
        } else if (kind == ResourceKind.CHECKPOINT) {
            final byte[] exactVersion = bytes(field(fields, 6), 6);
            if (outcome == ResourceDeleteConfirmedBody.DeleteOutcome.DELETED
                    && observedImmutableVersion.length == 0) {
                throw new IllegalArgumentException("DELETED checkpoint evidence must carry immutable version");
            }
            if (observedImmutableVersion.length != 0
                    && !Arrays.equals(exactVersion, observedImmutableVersion)) {
                throw new IllegalArgumentException("delete evidence immutable version does not match checkpoint identity");
            }
            if (observedEtag.length != 0) {
                throw new IllegalArgumentException("checkpoint identity has no etag field");
            }
        }
    }

    /** Closed ExactResourceIdentityV1 projection. */
    public record ExactResourceIdentity(ResourceKind kind, byte[] canonicalBytes, byte[] identityHash) {
        public ExactResourceIdentity {
            Objects.requireNonNull(kind, "kind");
            if (canonicalBytes == null || canonicalBytes.length == 0) {
                throw new IllegalArgumentException("resource identity must not be empty");
            }
            Bytes.requireLength(identityHash, HASH_LENGTH, "resource identity hash");
            final List<CanonicalProtobuf.Reader.Field> outer = read(canonicalBytes, "ExactResourceIdentity");
            if (outer.size() != 1 || outer.get(0).wireType() != 2
                    || outer.get(0).number() != kind.wireValue()) {
                throw new IllegalArgumentException("resource identity branch does not match resource kind");
            }
            final byte[] branch = outer.get(0).rawValue();
            validateBranch(kind, branch);
            final byte[] expectedCanonical = canonicalBytes(kind, branch);
            if (!Arrays.equals(canonicalBytes, expectedCanonical)) {
                throw new IllegalArgumentException("non-canonical ExactResourceIdentity");
            }
            final byte[] expectedHash = Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity-v1\0"),
                    expectedCanonical);
            if (!Bytes.constantTimeEquals(identityHash, expectedHash)) {
                throw new IllegalArgumentException("resource identity hash does not match canonical identity");
            }
            canonicalBytes = expectedCanonical;
            identityHash = expectedHash;
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
            final byte[] canonical = canonicalBytes(kind, branch);
            if (!Arrays.equals(encoded, canonical)) {
                throw new IllegalArgumentException("non-canonical ExactResourceIdentity");
            }
            return new ExactResourceIdentity(kind, canonical,
                    Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity-v1\0"), canonical));
        }

        private static byte[] canonicalBytes(final ResourceKind kind, final byte[] branch) {
            return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, kind.wireValue(), branch));
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
                    profileRef(nested(field(fields, 1), 1), ProfileKindV1.OBJECT_STORE);
                    nonEmpty(bytes(field(fields, 2), 2), 2);
                    nonEmpty(bytes(field(fields, 3), 3), 3);
                    nonEmpty(bytes(field(fields, 4), 4), 4);
                    if (fields.size() == 7) {
                        nonEmpty(bytes(field(fields, 5), 5), 5);
                    }
                    nonNegative(unsigned(field(fields, 6), 6), 6);
                    fixed(bytes(field(fields, 7), 7), HASH_LENGTH, 7);
                }
                case CHECKPOINT -> {
                    // This branch is byte-identical to the closed checkpoint
                    // resource codec. Reuse it so the retirement identity
                    // cannot weaken its non-zero IDs or Object Store Profile
                    // fence while adding GC-specific meaning around it.
                    CheckpointResourceV1.decode(branch);
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
                    nonZeroRaw(rawUnsigned(field(fields, 6), 6), 6);
                }
                case PULSAR_JOURNAL_GENERATION -> {
                    requireExact(fields, 3, kind);
                    PublishAdmissionBody.validateBrokerResourceIdentity(nested(field(fields, 1), 1));
                    nonNegative(unsigned(field(fields, 2), 2), 2);
                    nonZeroRaw(rawUnsigned(field(fields, 3), 3), 3);
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
            final Comparator<ProtectionRef> comparator = Comparator.comparing(ref -> Bytes.hex(ref.canonicalBytes()));
            for (int index = 1; index < references.size(); index++) {
                final int order = comparator.compare(references.get(index - 1), references.get(index));
                if (order > 0) {
                    throw new IllegalArgumentException("ProtectionSet references are not canonical-byte sorted");
                }
                if (order == 0) {
                    throw new IllegalArgumentException("ProtectionSet contains duplicate references");
                }
            }
            final byte[] canonicalRefs = canonicalReferences(references);
            final byte[] expectedDigest = Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"), canonicalRefs);
            if (!Bytes.constantTimeEquals(digest, expectedDigest)) {
                throw new IllegalArgumentException("ProtectionSet digest mismatch");
            }
            final byte[] expectedCanonical = canonicalBytes(references, expectedDigest);
            if (!Arrays.equals(canonicalBytes, expectedCanonical)) {
                throw new IllegalArgumentException("non-canonical ProtectionSet");
            }
            canonicalBytes = expectedCanonical;
            digest = expectedDigest;
        }

        @Override
        public byte[] canonicalBytes() {
            return Bytes.copy(canonicalBytes);
        }

        @Override
        public byte[] digest() {
            return Bytes.copy(digest);
        }

        public static ProtectionSet decodeCanonical(final byte[] encoded) {
            return decode(encoded);
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
            final byte[] canonicalRefs = canonicalReferences(refs);
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
            final byte[] canonical = canonicalBytes(refs, digest);
            if (!Arrays.equals(encoded, canonical)) {
                throw new IllegalArgumentException("non-canonical ProtectionSet");
            }
            return new ProtectionSet(refs, canonical, digest);
        }

        private static byte[] canonicalReferences(final List<ProtectionRef> references) {
            return CanonicalProtobuf.message(output -> {
                for (ProtectionRef ref : references) {
                    CanonicalProtobuf.bytes(output, 1, ref.canonicalBytes());
                }
            });
        }

        private static byte[] canonicalBytes(final List<ProtectionRef> references, final byte[] digest) {
            return CanonicalProtobuf.message(output -> {
                for (ProtectionRef ref : references) {
                    CanonicalProtobuf.bytes(output, 1, ref.canonicalBytes());
                }
                CanonicalProtobuf.bytes(output, 2, digest);
            });
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
            Objects.requireNonNull(minimumSourcePosition, "minimumSourcePosition");
            Objects.requireNonNull(recoveryLineageId, "recoveryLineageId");
            Objects.requireNonNull(checkpointId, "checkpointId");
            Objects.requireNonNull(manifestHash, "manifestHash");
            Objects.requireNonNull(canonicalBytes, "canonicalBytes");
            if (protectionKind == 1) {
                if (minimumSourcePosition.length == 0 || recoveryLineageId.length != INCARNATION_LENGTH
                        || checkpointId.length != INCARNATION_LENGTH || manifestHash.length != HASH_LENGTH) {
                    throw new IllegalArgumentException("Recovery Floor protection is incomplete");
                }
                minimumSourcePosition = SourcePositionCodec.decode(minimumSourcePosition).canonicalBytes();
            } else if (protectionKind == 2 || protectionKind == 4) {
                if (minimumSourcePosition.length == 0 || recoveryLineageId.length != 0
                        || checkpointId.length != 0 || manifestHash.length != 0) {
                    throw new IllegalArgumentException("time-bound protection fields are invalid");
                }
                minimumSourcePosition = SourcePositionCodec.decode(minimumSourcePosition).canonicalBytes();
            } else if (minimumSourcePosition.length != 0 || recoveryLineageId.length != 0
                    || checkpointId.length != 0 || manifestHash.length != 0) {
                throw new IllegalArgumentException("non-time-bound protection carries source fields");
            }
            protectedResourceId = Bytes.copy(protectedResourceId);
            minimumSourcePosition = Bytes.copy(minimumSourcePosition);
            recoveryLineageId = Bytes.copy(recoveryLineageId);
            checkpointId = Bytes.copy(checkpointId);
            manifestHash = Bytes.copy(manifestHash);
            final byte[] expectedCanonical = canonicalBytes(protectionKind, protectedResourceId,
                    protectionGeneration, minimumSourcePosition, recoveryLineageId, checkpointId, manifestHash);
            if (!Arrays.equals(canonicalBytes, expectedCanonical)) {
                throw new IllegalArgumentException("ProtectionRef canonical bytes do not match fields");
            }
            canonicalBytes = expectedCanonical;
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
            final long kindValue = unsigned(field(fields, 1), 1);
            if (kindValue > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("ProtectionRef kind exceeds local runtime range");
            }
            final int kind = (int) kindValue;
            final byte[] resourceId = fixed(bytes(field(fields, 2), 2), HASH_LENGTH, 2);
            final long generation = rawUnsigned(field(fields, 3), 3);
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
            final byte[] canonical = canonicalBytes(kind, resourceId, generation, source, lineage, checkpoint,
                    manifest);
            if (!Arrays.equals(encoded, canonical)) {
                throw new IllegalArgumentException("non-canonical ProtectionRef");
            }
            return new ProtectionRef(kind, resourceId, generation, source, lineage, checkpoint, manifest, canonical);
        }

        private static byte[] canonicalBytes(final int kind, final byte[] resourceId, final long generation,
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
    }

    private static void profileRef(final byte[] encoded, final ProfileKindV1 expectedKind) {
        final ProfileRefV1 profile = ProfileRefV1.decode(encoded);
        if (profile.profileKind() != expectedKind) {
            throw new IllegalArgumentException("resource identity ProfileRef kind does not match resource branch");
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

    private static long rawUnsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid nested uint field " + number);
        }
        return field.unsignedValue();
    }

    private static long nonZeroRaw(final long value, final int number) {
        if (value == 0) {
            throw new IllegalArgumentException("nested field " + number + " must be non-zero");
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
