package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TerminalPolicySemantic;

/**
 * Source-position-pinned lookup for an immutable Retry Policy semantic value.
 *
 * <p>A catalog returns {@code null} when the exact reference is not visible at
 * the supplied Source Position. Implementations may be backed by a local
 * recovery snapshot or an external authority; this interface does not infer
 * activation from a later or different policy version.</p>
 */
@FunctionalInterface
public interface RetryPolicyCatalog {
    RetryPolicySemantic resolve(RetryPolicyRef reference, SourcePosition sourcePosition);

    /**
     * Exact original publication, including optional terminal content. Legacy providers have no such content;
     * absence cannot authorize payload release. A later configuration must never fill in this value.
     */
    default Publication resolvePublication(RetryPolicyRef reference, SourcePosition sourcePosition) {
        final var semantic = resolve(reference, sourcePosition);
        return semantic == null ? null : new Publication(semantic, null);
    }

    /** Content addressing authenticates the terminal bytes through the original, independently authorized retry use. */
    record Publication(RetryPolicySemantic retry, TerminalPolicySemantic terminal) {
        public Publication {
            java.util.Objects.requireNonNull(retry, "retry");
            if (terminal != null) { terminal.requireBinding(retry); }
        }
    }
}
