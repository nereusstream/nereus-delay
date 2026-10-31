package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetSloObservationEventBody;
import java.util.Arrays;
import java.util.Objects;

/** Common admission boundary immediately before an actual Source SDK submission. */
public final class TargetScopeExposureSource {
    private TargetScopeExposureSource() {}

    /** A frozen SDK submission bound; it grants no physical Source, retention or UTC authority. */
    public record CopyContract(long maximumCopies, byte[] digest) {
        public CopyContract {
            if (maximumCopies <= 0 || maximumCopies == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Source copy bound must be finite and positive");
            }
            Bytes.requireLength(digest, 32, "Source copy contract digest");
            boolean assigned = false;
            for (byte value : digest) {
                assigned |= value != 0;
            }
            if (!assigned) {
                throw new IllegalArgumentException("Source copy contract digest must be assigned");
            }
            digest = digest.clone();
        }

        @Override
        public byte[] digest() {
            return digest.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof CopyContract that
                    && maximumCopies == that.maximumCopies
                    && Arrays.equals(digest, that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(maximumCopies, Arrays.hashCode(digest));
        }
    }

    @FunctionalInterface
    public interface Issuer {
        /** Durable acquisition occurs outside producer and SDK handoff locks. */
        TargetSloControlScopeAuthority.ExposurePermit acquire(SystemMutation mutation, CopyContract contract);
    }

    public static boolean requiresExposure(SystemMutation mutation) {
        final var exact = Objects.requireNonNull(mutation, "mutation");
        return TargetSloControlScopeAuthority.reference(exact).isPresent()
                || exact.type() == SystemMutationType.TARGET_SLO_OBSERVATION
                        && TargetSloObservationEventBody.decode(exact.canonicalBody())
                                        .writerEpoch()
                                != null;
    }

    public static TargetSloControlScopeAuthority.ExposurePermit acquire(
            SystemMutation mutation, CopyContract contract, Issuer issuer) {
        if (!requiresExposure(mutation)) {
            return null;
        }
        if (issuer == null || contract == null) {
            throw new IllegalStateException("actual Source exposure requires its issuer and frozen SDK copy contract");
        }
        return Objects.requireNonNull(issuer.acquire(mutation, contract), "actual Source exposure permit");
    }
}
