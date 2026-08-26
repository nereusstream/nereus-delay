package com.nereusstream.delay.client;

import com.nereusstream.delay.protocol.PreparedSubmission;

/** Optional bounded client-side admission hook; it runs before transport ownership. */
@FunctionalInterface
public interface ClientAdmission {
    void admit(PreparedSubmission submission);
}
