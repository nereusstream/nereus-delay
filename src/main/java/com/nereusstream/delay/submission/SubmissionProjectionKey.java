package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.AdapterKind;
import java.util.Objects;

/** Selects the closed outcome projection for one prepared branch and adapter. */
public record SubmissionProjectionKey(PreparedSubmissionBranch branch, AdapterKind adapterKind) {
    public SubmissionProjectionKey {
        Objects.requireNonNull(branch, "branch");
        Objects.requireNonNull(adapterKind, "adapterKind");
        if (branch == PreparedSubmissionBranch.NATIVE && adapterKind != AdapterKind.PULSAR) {
            throw new IllegalArgumentException("native submissions are Pulsar-only");
        }
    }
}
