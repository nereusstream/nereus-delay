package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CheckpointResourceV1;

/**
 * Object Store upload boundary for one immutable checkpoint manifest and its
 * complete RocksDB file set.  Implementations own provider authentication and
 * publication evidence; the coordinator owns local fencing and CAS ordering.
 */
@FunctionalInterface
public interface CheckpointUploadAdapter {
    CheckpointResourceV1 upload(CheckpointUploadRequest request);
}
