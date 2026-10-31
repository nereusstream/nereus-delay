package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetMigrationRetirementInventory;

/** Operation-specific immutable identity; published objects and pending prefixes cannot change deletion contracts. */
public record CheckpointRetirementResourceIdentity(
        CheckpointResource published, CheckpointUploadIntent pending, boolean artifact) {
    public CheckpointRetirementResourceIdentity {
        if ((published == null) == (pending == null)
                || pending != null && (pending.state() != CheckpointUploadState.PENDING_UPLOAD || artifact)) {
            throw new IllegalArgumentException("one exact published checkpoint or original pending upload is required");
        }
    }

    public static CheckpointRetirementResourceIdentity published(CheckpointResource exact) {
        return new CheckpointRetirementResourceIdentity(exact, null, false);
    }

    public static CheckpointRetirementResourceIdentity pending(CheckpointUploadIntent exact) {
        return new CheckpointRetirementResourceIdentity(null, exact, false);
    }

    public static CheckpointRetirementResourceIdentity artifact(CheckpointResource exact) {
        return new CheckpointRetirementResourceIdentity(exact, null, true);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.uint32(out, 2, published == null ? 2 : artifact ? 3 : 1);
            CanonicalProtobuf.bytes(out, 3, published == null ? pending.canonicalBytes() : published.canonicalBytes());
        });
    }

    public static CheckpointRetirementResourceIdentity decode(byte[] bytes) {
        if (bytes == null
                || bytes.length == 0
                || bytes.length > TargetMigrationRetirementInventory.Resource.MAX_MATERIAL_BYTES) {
            throw new IllegalArgumentException("bounded exact checkpoint retirement identity required");
        }
        final var f = QueryCodecSupport.read(bytes, "checkpoint retirement identity");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3}, "checkpoint retirement identity");
        if (QueryCodecSupport.uint32(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unsupported checkpoint retirement identity");
        }
        final int kind = QueryCodecSupport.uint32(f.get(1), 2);
        final var value = kind == 1
                ? published(CheckpointResource.decode(QueryCodecSupport.bytes(f.get(2), 3)))
                : kind == 2
                        ? pending(CheckpointUploadIntent.decode(QueryCodecSupport.bytes(f.get(2), 3)))
                        : kind == 3 ? artifact(CheckpointResource.decode(QueryCodecSupport.bytes(f.get(2), 3))) : null;
        if (value == null) {
            throw new IllegalArgumentException("unknown published/pending checkpoint contract");
        }
        QueryCodecSupport.requireCanonical(bytes, value.canonicalBytes(), "checkpoint retirement identity");
        return value;
    }
}
