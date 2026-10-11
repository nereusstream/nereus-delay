package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteSnapshot;

/** Pre-ownership admission on the exact resolved managed Source route; never authorizes a native target send. */
@FunctionalInterface
public interface ManagedSourceIngressAdmission {
    void requireAllowed(PreparedCommand command, RouteSnapshot route, int frameBytes);
}
