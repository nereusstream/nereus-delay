package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.EvidenceCursorV1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical local projection of the mutable Store metadata required by V1.
 *
 * <p>This value records facts that belong to the physical shard DB. It is not
 * an ownership or checkpoint authority: Owner Lease and published checkpoint
 * decisions still come from their respective authenticated services.</p>
 */
public record StoreRuntimeMetadata(
        byte[] lastIngressFenceProofId,
        byte[] lastCheckpointId,
        long lastOpenedOwnerEpoch,
        boolean cleanCloseMarker,
        List<EvidenceCursorV1> evidenceCursors) {
    public static final int FENCE_PROOF_ID_LENGTH = 32;
    public static final int CHECKPOINT_ID_LENGTH = 16;
    public static final int MAX_EVIDENCE_CURSORS = 1024;
    public static final int MAX_CANONICAL_BYTES = 1 << 20;

    public StoreRuntimeMetadata {
        lastIngressFenceProofId = optionalIdentity(lastIngressFenceProofId, FENCE_PROOF_ID_LENGTH,
                "lastIngressFenceProofId");
        lastCheckpointId = optionalIdentity(lastCheckpointId, CHECKPOINT_ID_LENGTH, "lastCheckpointId");
        if (lastOpenedOwnerEpoch < 0) {
            throw new IllegalArgumentException("lastOpenedOwnerEpoch must be non-negative");
        }
        Objects.requireNonNull(evidenceCursors, "evidenceCursors");
        if (evidenceCursors.size() > MAX_EVIDENCE_CURSORS) {
            throw new IllegalArgumentException("too many evidence cursors");
        }
        final List<EvidenceCursorV1> copied = new ArrayList<>(evidenceCursors.size());
        EvidenceCursorV1 previous = null;
        for (EvidenceCursorV1 cursor : evidenceCursors) {
            Objects.requireNonNull(cursor, "evidence cursor");
            if (previous != null && previous.compareTo(cursor) >= 0) {
                throw new IllegalArgumentException("evidence cursors must be strictly sorted");
            }
            copied.add(cursor);
            previous = cursor;
        }
        evidenceCursors = List.copyOf(copied);
    }

    public static StoreRuntimeMetadata empty() {
        return new StoreRuntimeMetadata(null, null, 0, false, List.of());
    }

    @Override
    public byte[] lastIngressFenceProofId() {
        return lastIngressFenceProofId == null ? null : Bytes.copy(lastIngressFenceProofId);
    }

    @Override
    public byte[] lastCheckpointId() {
        return lastCheckpointId == null ? null : Bytes.copy(lastCheckpointId);
    }

    @Override
    public List<EvidenceCursorV1> evidenceCursors() {
        return evidenceCursors;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof StoreRuntimeMetadata that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }

    /** Returns a copy with the clean-close marker changed. */
    public StoreRuntimeMetadata withCleanCloseMarker(final boolean clean) {
        return new StoreRuntimeMetadata(lastIngressFenceProofId, lastCheckpointId, lastOpenedOwnerEpoch, clean,
                evidenceCursors);
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            if (lastIngressFenceProofId != null) {
                CanonicalProtobuf.bytes(output, 1, lastIngressFenceProofId);
            }
            if (lastCheckpointId != null) {
                CanonicalProtobuf.bytes(output, 2, lastCheckpointId);
            }
            CanonicalProtobuf.uint64(output, 3, lastOpenedOwnerEpoch);
            CanonicalProtobuf.uint32(output, 4, cleanCloseMarker ? 1 : 0);
            for (EvidenceCursorV1 cursor : evidenceCursors) {
                CanonicalProtobuf.bytes(output, 5, cursor.canonicalBytes());
            }
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("StoreRuntimeMetadata is too large");
        }
        return encoded;
    }

    public static StoreRuntimeMetadata decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length == 0 || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("invalid StoreRuntimeMetadata length");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        int index = 0;
        byte[] fenceProof = null;
        byte[] checkpoint = null;
        if (index < fields.size() && fields.get(index).number() == 1) {
            fenceProof = fixedIdentity(fields.get(index++), 1, FENCE_PROOF_ID_LENGTH, "lastIngressFenceProofId");
        }
        if (index < fields.size() && fields.get(index).number() == 2) {
            checkpoint = fixedIdentity(fields.get(index++), 2, CHECKPOINT_ID_LENGTH, "lastCheckpointId");
        }
        if (index >= fields.size() || fields.get(index).number() != 3) {
            throw new IllegalArgumentException("StoreRuntimeMetadata is missing owner epoch");
        }
        final long ownerEpoch = unsigned(fields.get(index++), 3);
        if (index >= fields.size() || fields.get(index).number() != 4) {
            throw new IllegalArgumentException("StoreRuntimeMetadata is missing clean-close marker");
        }
        final long cleanValue = unsigned(fields.get(index++), 4);
        if (cleanValue > 1) {
            throw new IllegalArgumentException("invalid clean-close marker");
        }
        final List<EvidenceCursorV1> cursors = new ArrayList<>();
        while (index < fields.size()) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index++);
            if (field.number() != 5) {
                throw new IllegalArgumentException("invalid StoreRuntimeMetadata field order");
            }
            if (cursors.size() == MAX_EVIDENCE_CURSORS) {
                throw new IllegalArgumentException("too many evidence cursors");
            }
            cursors.add(EvidenceCursorV1.decode(bytes(field, 5)));
        }
        final StoreRuntimeMetadata result = new StoreRuntimeMetadata(fenceProof, checkpoint, ownerEpoch,
                cleanValue == 1, cursors);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical StoreRuntimeMetadata");
        }
        return result;
    }

    private static byte[] optionalIdentity(final byte[] value, final int length, final String name) {
        if (value == null) {
            return null;
        }
        Bytes.requireLength(value, length, name);
        boolean nonZero = false;
        for (byte current : value) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    private static byte[] fixedIdentity(final CanonicalProtobuf.Reader.Field field, final int number,
                                        final int length, final String name) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, name);
        return optionalIdentity(value, length, name);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid StoreRuntimeMetadata bytes field " + number);
        }
        return field.rawValue();
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid StoreRuntimeMetadata integer field " + number);
        }
        return field.unsignedValue();
    }
}
