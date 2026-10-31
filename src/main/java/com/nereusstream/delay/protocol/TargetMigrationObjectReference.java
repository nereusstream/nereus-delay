package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Full immutable checkpoint-object identity. The referenced content is independently read and authenticated. */
public record TargetMigrationObjectReference(CheckpointResource resource) {
    /** Existing Target recovery/install inline-reference ceiling; this does not cap the object's contents. */
    public static final int MAX_CANONICAL_BYTES = TargetCatalogCapacityOwnerRef.MAX_BYTES;

    public TargetMigrationObjectReference {
        Objects.requireNonNull(resource, "exact immutable object");
        if (resource.manifestLength() <= 0
                || resource.manifestLength() == Long.MAX_VALUE
                || resource.canonicalBytes().length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("migration requires a finite complete immutable object reference");
        }
    }

    public byte[] canonicalBytes() {
        return resource.canonicalBytes();
    }

    public byte[] digest() {
        return Bytes.sha256(canonicalBytes());
    }

    public void requireContent(byte[] content) {
        Objects.requireNonNull(content, "complete original object");
        if (resource.manifestLength() != content.length
                || !Arrays.equals(resource.manifestSha256(), Bytes.sha256(content))) {
            throw new IllegalArgumentException("migration object differs from its exact length/hash");
        }
    }

    public static TargetMigrationObjectReference decode(byte[] encoded) {
        TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "migration object reference");
        final var value = new TargetMigrationObjectReference(CheckpointResource.decode(encoded));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration object reference");
        return value;
    }
}
