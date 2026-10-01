package com.nereusstream.delay.store;

import org.rocksdb.RocksDBException;

/** Real-K1 smoke bridge for a native write that commits before reporting failure. */
public final class TargetStoreBackendFailureSmokeBridge {
    private TargetStoreBackendFailureSmokeBridge() {}

    public static void failNextCommitAfterNativeWrite(final TargetStoreBackend backend) {
        backend.injectNextNativeWriteForTesting((db, writeOptions, batch) -> {
            db.write(writeOptions, batch);
            throw new RocksDBException("synthetic Target native write response loss");
        });
    }
}
