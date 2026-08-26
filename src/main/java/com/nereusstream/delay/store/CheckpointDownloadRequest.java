package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import java.util.Objects;

/** Exact catalog manifest/resource pair supplied to a checkpoint downloader. */
public record CheckpointDownloadRequest(CheckpointManifest manifest, CheckpointResource resource) {
    public CheckpointDownloadRequest {
        manifest = Objects.requireNonNull(manifest, "manifest");
        resource = Objects.requireNonNull(resource, "resource");
        if (!Bytes.constantTimeEquals(manifest.recoveryLineageId(), resource.recoveryLineageId())
                || !Bytes.constantTimeEquals(manifest.checkpointId(), resource.checkpointId())
                || resource.manifestLength() != manifest.canonicalJsonBytes().length
                || !Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())) {
            throw new IllegalArgumentException("checkpoint manifest/resource identity differs");
        }
    }
}
