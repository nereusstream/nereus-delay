package com.nereusstream.delay.store;

import java.nio.file.Path;

/** Provider boundary that materializes one exact checkpoint into a new directory. */
@FunctionalInterface
public interface CheckpointDownloadAdapter {
    Path download(CheckpointDownloadRequest request, Path targetDirectory);
}
