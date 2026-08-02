package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RetryPolicySemanticV1;
import io.nereusstream.delay.protocol.SourcePosition;

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
    RetryPolicySemanticV1 resolve(RetryPolicyRefV1 reference, SourcePosition sourcePosition);
}
