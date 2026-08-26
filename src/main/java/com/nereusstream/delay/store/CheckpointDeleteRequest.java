package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CheckpointResource;
import java.util.Objects;

/** Exact manifest/resource identity supplied to a checkpoint delete adapter. */
public record CheckpointDeleteRequest(CheckpointManifest manifest, CheckpointResource resource) {
    public CheckpointDeleteRequest {
        manifest = Objects.requireNonNull(manifest, "manifest");
        resource = Objects.requireNonNull(resource, "resource");
        new CheckpointDownloadRequest(manifest, resource);
    }
}
