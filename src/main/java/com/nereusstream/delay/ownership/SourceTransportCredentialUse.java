package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Local use bound by the actual Worker to its accepted assignment, Owner and native client generation. */
public final class SourceTransportCredentialUse {
    private final LocalBrokerMtlsResolver.SourceReceipt receipt;
    private final TargetSourceApplyRuntime runtime;
    private final SourceAssignment assignment;
    private final OwnerIdentity owner;
    private final Object nativeGeneration;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final LongSupplier clock;
    SourceTransportCredentialUse(LocalBrokerMtlsResolver.SourceReceipt receipt, TargetSourceApplyRuntime runtime,
            SourceAssignment assignment, OwnerIdentity owner, Object nativeGeneration,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier clock) {
        this.receipt = Objects.requireNonNull(receipt); this.runtime = runtime; this.assignment = assignment;
        this.owner = owner; this.nativeGeneration = Objects.requireNonNull(nativeGeneration);
        this.time = Objects.requireNonNull(time); this.clock = Objects.requireNonNull(clock);
    }
    public void require(SourceTransportCredentialAuthorization.Operation operation, Object actualGeneration) {
        if (actualGeneration != nativeGeneration) { throw new IllegalStateException("Source native credential generation changed"); }
        receipt.requireOperation(operation, time.get());
        runtime.requireLocalSourceTransportOwner(assignment, owner, clock.getAsLong());
    }
    @Override public String toString() { return "BoundSourceTransportCredentialUse"; }
}
