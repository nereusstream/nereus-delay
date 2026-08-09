package io.nereusstream.delay.adapter;

import java.util.Objects;

/**
 * Local timing guard for the managed Pulsar destination adapter.
 *
 * <p>The immutable Destination/Delivery Capability Profile remains the
 * authority for whether a certified handoff may be used.  This value is only
 * the adapter-side fail-closed seam: the default policy accepts ordinary
 * managed sends, while an explicitly constructed handoff policy accepts one
 * fixed {@code deliverAt - handoffLead} relationship and nothing else.</p>
 */
public record PulsarDestinationTimingPolicy(Mode mode, long handoffLeadMs) {
    public PulsarDestinationTimingPolicy {
        Objects.requireNonNull(mode, "mode");
        if (handoffLeadMs < 0 || (mode == Mode.ORDINARY_MANAGED && handoffLeadMs != 0)
                || (mode == Mode.CERTIFIED_HANDOFF && handoffLeadMs <= 0)) {
            throw new IllegalArgumentException("invalid Pulsar destination timing policy");
        }
    }

    /** Returns the conservative managed-send policy. */
    public static PulsarDestinationTimingPolicy ordinaryManaged() {
        return new PulsarDestinationTimingPolicy(Mode.ORDINARY_MANAGED, 0);
    }

    /** Returns a fixed-lead policy for an upstream certified Pulsar handoff. */
    public static PulsarDestinationTimingPolicy certifiedHandoff(final long handoffLeadMs) {
        return new PulsarDestinationTimingPolicy(Mode.CERTIFIED_HANDOFF, handoffLeadMs);
    }

    /** Validates the exact timing relationship before transport ownership. */
    public void validate(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        final long actionAt = request.actionAtEpochMs();
        final long deliverAt = request.deliverAtEpochMs();
        if (mode == Mode.ORDINARY_MANAGED) {
            if (actionAt != deliverAt) {
                throw new IllegalArgumentException("ordinary Pulsar managed timing requires actionAt=deliverAt");
            }
            return;
        }
        final long expectedActionAt;
        try {
            expectedActionAt = Math.subtractExact(deliverAt, handoffLeadMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Pulsar handoff timing underflows deliverAt", overflow);
        }
        if (expectedActionAt < 0 || actionAt != expectedActionAt) {
            throw new IllegalArgumentException("Pulsar handoff actionAt does not match the fixed lead");
        }
    }

    public enum Mode {
        ORDINARY_MANAGED,
        CERTIFIED_HANDOFF
    }
}
