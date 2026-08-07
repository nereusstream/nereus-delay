package io.nereusstream.delay.store;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Hard limits for one checkpoint manifest and its physical file inventory.
 *
 * <p>Production restore/upload paths must receive an activated finite value.
 * The unbounded value exists only for the older embedded compatibility
 * overloads.</p>
 */
public record CheckpointManifestLimits(
        int maxFiles,
        long maxTotalFileBytes,
        long maxIndividualFileBytes,
        int maxPathBytes,
        int maxManifestBytes,
        int maxEvidenceCursors,
        int maxObjectIdentityBytes) {
    public CheckpointManifestLimits {
        if (maxFiles <= 0 || maxTotalFileBytes <= 0 || maxIndividualFileBytes <= 0
                || maxPathBytes <= 0 || maxManifestBytes <= 0 || maxEvidenceCursors <= 0
                || maxObjectIdentityBytes <= 0 || maxIndividualFileBytes > maxTotalFileBytes) {
            throw new IllegalArgumentException("checkpoint manifest limits must be positive and ordered");
        }
    }

    /** Compatibility value for callers that predate explicit manifest limits. */
    public static CheckpointManifestLimits unbounded() {
        return new CheckpointManifestLimits(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
                Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public void validateManifest(final CheckpointManifest manifest) {
        Objects.requireNonNull(manifest, "manifest");
        if (manifest.files().size() > maxFiles) {
            throw new IllegalArgumentException("checkpoint manifest file count exceeds configured bound");
        }
        if (manifest.evidenceCursors().size() > maxEvidenceCursors) {
            throw new IllegalArgumentException("checkpoint manifest evidence cursor count exceeds configured bound");
        }
        long totalBytes = 0;
        for (CheckpointManifest.FileEntry file : manifest.files()) {
            validateFile(file.name(), file.length());
            if (file.objectKey().length > maxObjectIdentityBytes
                    || file.objectVersion().length > maxObjectIdentityBytes
                    || (file.etag() != null && file.etag().length > maxObjectIdentityBytes)) {
                throw new IllegalArgumentException("checkpoint file object identity exceeds configured bound");
            }
            totalBytes = Math.addExact(totalBytes, file.length());
        }
        if (totalBytes > maxTotalFileBytes) {
            throw new IllegalArgumentException("checkpoint total file bytes exceed configured bound");
        }
        if (manifest.canonicalJsonBytes().length > maxManifestBytes) {
            throw new IllegalArgumentException("checkpoint manifest bytes exceed configured bound");
        }
    }

    public void validateFile(final String name, final long length) {
        Objects.requireNonNull(name, "name");
        if (name.getBytes(StandardCharsets.UTF_8).length > maxPathBytes) {
            throw new IllegalArgumentException("checkpoint file path exceeds configured bound: " + name);
        }
        if (length < 0 || length > maxIndividualFileBytes) {
            throw new IllegalArgumentException("checkpoint file length exceeds configured bound: " + name);
        }
    }
}
