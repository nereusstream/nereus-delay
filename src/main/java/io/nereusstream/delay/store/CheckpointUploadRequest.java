package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;

import java.nio.file.Path;
import java.util.Objects;

/** Immutable input passed to a checkpoint Object Store adapter. */
public record CheckpointUploadRequest(
        CheckpointUploadIntentV1 intent,
        CheckpointManifest manifest,
        Path checkpointDirectory,
        byte[] manifestBytes) {
    public CheckpointUploadRequest {
        intent = Objects.requireNonNull(intent, "intent");
        manifest = Objects.requireNonNull(manifest, "manifest");
        checkpointDirectory = Objects.requireNonNull(checkpointDirectory, "checkpointDirectory");
        manifestBytes = Bytes.copy(Objects.requireNonNull(manifestBytes, "manifestBytes"));
        if (intent.state() != io.nereusstream.delay.protocol.CheckpointUploadStateV1.PENDING_UPLOAD) {
            throw new IllegalArgumentException("checkpoint upload request requires PENDING_UPLOAD intent");
        }
        if (!java.util.Arrays.equals(manifestBytes, manifest.canonicalJsonBytes())) {
            throw new IllegalArgumentException("checkpoint upload request manifest bytes are not canonical");
        }
    }

    @Override
    public byte[] manifestBytes() {
        return Bytes.copy(manifestBytes);
    }
}
