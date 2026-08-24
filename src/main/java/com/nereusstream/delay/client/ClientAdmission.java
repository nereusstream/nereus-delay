package com.nereusstream.delay.client;

import com.nereusstream.delay.protocol.PreparedSubmissionV1;

/** Optional bounded client-side admission hook; it runs before transport ownership. */
@FunctionalInterface
public interface ClientAdmission {
    void admit(PreparedSubmissionV1 submission);
}
