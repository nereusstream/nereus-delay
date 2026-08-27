package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Closed sequence authority used after a Managed Attempt Journal mapping. */
public final class PulsarSequenceAuthority {
    public enum Kind {
        MANAGED_JOURNAL(1),
        PRODUCER_ASSIGNED(2);

        private final int wireValue;

        Kind(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        private static Kind fromWire(final long value) {
            for (Kind kind : values()) {
                if (kind.wireValue == value) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("unknown PulsarSequenceAuthority kind: " + value);
        }
    }

    private static final int HASH_LENGTH = 32;
    private final Kind kind;
    private final byte[] journalMappingId;
    private final long sequenceId;
    private final byte[] producerNameHash;

    private PulsarSequenceAuthority(
            final Kind kind, final byte[] journalMappingId, final long sequenceId, final byte[] producerNameHash) {
        this.kind = Objects.requireNonNull(kind, "kind");
        if (kind == Kind.MANAGED_JOURNAL) {
            Bytes.requireLength(journalMappingId, HASH_LENGTH, "journalMappingId");
            if (allZero(journalMappingId) || sequenceId < 0) {
                throw new IllegalArgumentException("managed journal sequence authority is invalid");
            }
            Bytes.requireLength(producerNameHash, HASH_LENGTH, "producerNameHash");
            if (allZero(producerNameHash)) {
                throw new IllegalArgumentException("producerNameHash must be non-zero");
            }
            this.journalMappingId = Bytes.copy(journalMappingId);
            this.sequenceId = sequenceId;
            this.producerNameHash = Bytes.copy(producerNameHash);
        } else {
            if (journalMappingId != null || sequenceId != 0 || producerNameHash != null) {
                throw new IllegalArgumentException("producer-assigned authority cannot carry managed values");
            }
            this.journalMappingId = null;
            this.sequenceId = 0;
            this.producerNameHash = null;
        }
    }

    public static PulsarSequenceAuthority managedJournal(
            final byte[] journalMappingId, final long sequenceId, final byte[] producerNameHash) {
        return new PulsarSequenceAuthority(Kind.MANAGED_JOURNAL, journalMappingId, sequenceId, producerNameHash);
    }

    public static PulsarSequenceAuthority producerAssigned() {
        return new PulsarSequenceAuthority(Kind.PRODUCER_ASSIGNED, null, 0, null);
    }

    public Kind kind() {
        return kind;
    }

    public boolean isManagedJournal() {
        return kind == Kind.MANAGED_JOURNAL;
    }

    public byte[] journalMappingId() {
        if (!isManagedJournal()) {
            throw new IllegalStateException("sequence authority is producer-assigned");
        }
        return Bytes.copy(journalMappingId);
    }

    public long sequenceId() {
        if (!isManagedJournal()) {
            throw new IllegalStateException("sequence authority is producer-assigned");
        }
        return sequenceId;
    }

    public byte[] producerNameHash() {
        if (!isManagedJournal()) {
            throw new IllegalStateException("sequence authority is producer-assigned");
        }
        return Bytes.copy(producerNameHash);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            if (isManagedJournal()) {
                CanonicalProtobuf.bytes(output, 2, journalMappingId);
                CanonicalProtobuf.uint64Bits(output, 3, sequenceId);
                CanonicalProtobuf.bytes(output, 4, producerNameHash);
            }
        });
    }

    public static PulsarSequenceAuthority decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PulsarSequenceAuthority");
        final Kind kind = Kind.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final PulsarSequenceAuthority result;
        if (kind == Kind.PRODUCER_ASSIGNED) {
            if (fields.size() != 1) {
                throw new IllegalArgumentException("producer-assigned authority has managed fields");
            }
            result = producerAssigned();
        } else {
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "PulsarSequenceAuthority");
            result = managedJournal(
                    QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                    QueryCodecSupport.uint64Bits(fields.get(2), 3),
                    QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH));
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarSequenceAuthority");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarSequenceAuthority that
                && kind == that.kind
                && sequenceId == that.sequenceId
                && Arrays.equals(journalMappingId, that.journalMappingId)
                && Arrays.equals(producerNameHash, that.producerNameHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(journalMappingId), sequenceId, Arrays.hashCode(producerNameHash));
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }
}
