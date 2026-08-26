package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.SourcePosition;

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
}
