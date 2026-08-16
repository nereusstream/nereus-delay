package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical semantic parser for {@code RESOURCE_DELETE_CONFIRMED_V1}. */
public final class ResourceDeleteConfirmedBody {
    private static final int HASH_LENGTH = 32;

    private final RetireIntentRef intent;
    private final DeleteOutcome outcome;
    private final ExternalDeleteEvidence evidence;
    private final TrustedUtcIntervalEvidence confirmedAt;

    private ResourceDeleteConfirmedBody(final RetireIntentRef intent, final DeleteOutcome outcome,
                                        final ExternalDeleteEvidence evidence,
                                        final TrustedUtcIntervalEvidence confirmedAt) {
        this.intent = Objects.requireNonNull(intent, "intent");
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.evidence = Objects.requireNonNull(evidence, "evidence");
        if (evidence.outcome() != outcome
                || !Bytes.constantTimeEquals(intent.resourceIdentityHash(), evidence.resourceIdentityHash())) {
            throw new IllegalArgumentException("delete confirmation intent/evidence identity does not match");
        }
        this.confirmedAt = Objects.requireNonNull(confirmedAt, "confirmedAt");
        this.confirmedAt.requireEarliestAtLeast(this.evidence.observedAt().latestEpochMs());
    }

    public static ResourceDeleteConfirmedBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.RESOURCE_DELETE_CONFIRMED, canonicalBody);
        final RetireIntentRef intent = RetireIntentRef.decode(nested(field(fields, 10), 10));
        final DeleteOutcome outcome = DeleteOutcome.fromWire(unsigned(field(fields, 11), 11));
        final ExternalDeleteEvidence evidence = ExternalDeleteEvidence.decode(nested(field(fields, 12), 12));
        final TrustedUtcIntervalEvidence confirmedAt = TrustedUtcIntervalEvidence.decode(
                nested(field(fields, 13), 13));
        return new ResourceDeleteConfirmedBody(intent, outcome, evidence, confirmedAt);
    }

    public RetireIntentRef intent() {
        return intent;
    }

    public DeleteOutcome outcome() {
        return outcome;
    }

    public ExternalDeleteEvidence evidence() {
        return evidence;
    }

    public TrustedUtcIntervalEvidence confirmedAt() {
        return confirmedAt;
    }

    public enum DeleteOutcome {
        DELETED(1),
        ALREADY_ABSENT(2);

        private final int wireValue;

        DeleteOutcome(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        public static DeleteOutcome fromWire(final long value) {
            for (DeleteOutcome outcome : values()) {
                if (outcome.wireValue == value) {
                    return outcome;
                }
            }
            throw new IllegalArgumentException("unknown delete outcome: " + value);
        }
    }

    /** Exact reference to the already applied retire intent. */
    public record RetireIntentRef(byte[] mutationId, byte[] mutationHash, byte[] resourceIdentityHash,
                                  long expectedResourceStateVersion) {
        public RetireIntentRef {
            Bytes.requireLength(mutationId, HASH_LENGTH, "retire mutationId");
            Bytes.requireLength(mutationHash, HASH_LENGTH, "retire mutationHash");
            Bytes.requireLength(resourceIdentityHash, HASH_LENGTH, "retire resourceIdentityHash");
            mutationId = Bytes.copy(mutationId);
            mutationHash = Bytes.copy(mutationHash);
            resourceIdentityHash = Bytes.copy(resourceIdentityHash);
        }

        @Override
        public byte[] mutationId() {
            return Bytes.copy(mutationId);
        }

        @Override
        public byte[] mutationHash() {
            return Bytes.copy(mutationHash);
        }

        @Override
        public byte[] resourceIdentityHash() {
            return Bytes.copy(resourceIdentityHash);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, mutationId);
                CanonicalProtobuf.bytes(output, 2, mutationHash);
                CanonicalProtobuf.bytes(output, 3, resourceIdentityHash);
                CanonicalProtobuf.uint64Bits(output, 4, expectedResourceStateVersion);
            });
        }

        private static RetireIntentRef decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "RetireIntentRef");
            requireExact(fields, 4, "RetireIntentRef");
            final RetireIntentRef result = new RetireIntentRef(
                    fixed(bytes(fields.get(0), 1), HASH_LENGTH, "retire mutationId"),
                    fixed(bytes(fields.get(1), 2), HASH_LENGTH, "retire mutationHash"),
                    fixed(bytes(fields.get(2), 3), HASH_LENGTH, "retire resourceIdentityHash"),
                    rawUnsigned(fields.get(3), 4));
            if (!Arrays.equals(encoded, result.canonicalBytes())) {
                throw new IllegalArgumentException("non-canonical RetireIntentRef");
            }
            return result;
        }
    }

    /** Authenticated provider response projection; optional identity fields stay byte-exact. */
    public record ExternalDeleteEvidence(byte[] resourceIdentityHash, byte[] providerRequestIdHash,
                                          DeleteOutcome outcome, byte[] observedImmutableVersion,
                                          byte[] observedEtag, byte[] responseHash,
                                          TrustedUtcIntervalEvidence observedAt) {
        public ExternalDeleteEvidence {
            Bytes.requireLength(resourceIdentityHash, HASH_LENGTH, "delete resourceIdentityHash");
            Bytes.requireLength(providerRequestIdHash, HASH_LENGTH, "providerRequestIdHash");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(observedImmutableVersion, "observedImmutableVersion");
            Objects.requireNonNull(observedEtag, "observedEtag");
            Bytes.requireLength(responseHash, HASH_LENGTH, "delete responseHash");
            Objects.requireNonNull(observedAt, "observedAt");
            if (outcome == DeleteOutcome.ALREADY_ABSENT
                    && (observedImmutableVersion.length != 0 || observedEtag.length != 0)) {
                throw new IllegalArgumentException("ALREADY_ABSENT cannot carry observed identity fields");
            }
            resourceIdentityHash = Bytes.copy(resourceIdentityHash);
            providerRequestIdHash = Bytes.copy(providerRequestIdHash);
            observedImmutableVersion = Bytes.copy(observedImmutableVersion);
            observedEtag = Bytes.copy(observedEtag);
            responseHash = Bytes.copy(responseHash);
        }

        @Override
        public byte[] resourceIdentityHash() {
            return Bytes.copy(resourceIdentityHash);
        }

        @Override
        public byte[] providerRequestIdHash() {
            return Bytes.copy(providerRequestIdHash);
        }

        @Override
        public byte[] observedImmutableVersion() {
            return Bytes.copy(observedImmutableVersion);
        }

        @Override
        public byte[] observedEtag() {
            return Bytes.copy(observedEtag);
        }

        @Override
        public byte[] responseHash() {
            return Bytes.copy(responseHash);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, resourceIdentityHash);
                CanonicalProtobuf.bytes(output, 2, providerRequestIdHash);
                CanonicalProtobuf.uint32(output, 3, outcome.wireValue());
                if (observedImmutableVersion.length != 0) {
                    CanonicalProtobuf.bytes(output, 4, observedImmutableVersion);
                }
                if (observedEtag.length != 0) {
                    CanonicalProtobuf.bytes(output, 5, observedEtag);
                }
                CanonicalProtobuf.bytes(output, 6, responseHash);
                CanonicalProtobuf.bytes(output, 7, observedAt.canonicalBytes());
            });
        }

        private static ExternalDeleteEvidence decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ExternalDeleteEvidence");
            if (fields.size() < 5 || fields.size() > 7) {
                throw new IllegalArgumentException("ExternalDeleteEvidence fields are incomplete");
            }
            if (fields.get(0).number() != 1 || fields.get(1).number() != 2 || fields.get(2).number() != 3) {
                throw new IllegalArgumentException("ExternalDeleteEvidence required fields are out of order");
            }
            int index = 3;
            final byte[] immutableVersion = optional(fields, index, 4);
            if (immutableVersion.length != 0) {
                index++;
            }
            final byte[] etag = optional(fields, index, 5);
            if (etag.length != 0) {
                index++;
            }
            if (index + 2 != fields.size() || fields.get(index).number() != 6
                    || fields.get(index + 1).number() != 7) {
                throw new IllegalArgumentException("ExternalDeleteEvidence response fields are out of order");
            }
            final ExternalDeleteEvidence result = new ExternalDeleteEvidence(
                    fixed(bytes(fields.get(0), 1), HASH_LENGTH, "delete resourceIdentityHash"),
                    fixed(bytes(fields.get(1), 2), HASH_LENGTH, "providerRequestIdHash"),
                    DeleteOutcome.fromWire(unsigned(fields.get(2), 3)), immutableVersion, etag,
                    fixed(bytes(fields.get(index), 6), HASH_LENGTH, "delete responseHash"),
                    TrustedUtcIntervalEvidence.decode(bytes(fields.get(index + 1), 7)));
            if (!Arrays.equals(encoded, result.canonicalBytes())) {
                throw new IllegalArgumentException("non-canonical ExternalDeleteEvidence");
            }
            return result;
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return fields;
    }

    private static void requireExact(final List<CanonicalProtobuf.Reader.Field> fields, final int count,
                                     final String name) {
        if (fields.size() != count) {
            throw new IllegalArgumentException(name + " fields are incomplete or unknown");
        }
        for (int index = 0; index < count; index++) {
            if (fields.get(index).number() != index + 1) {
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
        throw new IllegalArgumentException("missing delete confirmation field " + number);
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        return bytes(field, number);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid delete confirmation bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return value;
    }

    private static byte[] optional(final List<CanonicalProtobuf.Reader.Field> fields, final int index,
                                   final int number) {
        if (index >= fields.size() || fields.get(index).number() != number) {
            return new byte[0];
        }
        return bytes(fields.get(index), number);
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid delete confirmation scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static long rawUnsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid delete confirmation scalar field " + number);
        }
        return field.unsignedValue();
    }
}
