package com.nereusstream.delay.store;

import org.rocksdb.RocksDBException;

/** Test-only bridge for exercising native write failures through an active Target runtime. */
public final class TargetStoreBackendFailureTestBridge {
    private TargetStoreBackendFailureTestBridge() {}

    public static void failNextCommitAfterNativeWrite(final TargetStoreBackend backend) {
        backend.injectNextNativeWriteForTesting((db, writeOptions, batch) -> {
            db.write(writeOptions, batch);
            throw new RocksDBException("synthetic Target native write response loss");
        });
    }
}
