package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;

/** Original managed Journal generation; closing it does not release durable Journal/UNKNOWN obligations. */
public interface OwnedTargetJournal {
    TargetWorkerShardRuntime journalWorker();
    PulsarAttemptJournal journal();
    TargetOwnedBackendGroup ownedBackendGroup();
    boolean readyForJournal();
    boolean settleClose();
    boolean closed();
}
