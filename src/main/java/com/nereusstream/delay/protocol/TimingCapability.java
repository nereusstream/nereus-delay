package com.nereusstream.delay.protocol;

/** Bit registry for immutable destination timing capabilities. */
public final class TimingCapability {
    public static final int ORDINARY_MANAGED = 0x01;
    public static final int PULSAR_NATIVE_MANAGED_HANDOFF = 0x02;
    /** Source-compatible alias for the renamed native managed-handoff capability. */
    public static final int PULSAR_GUARDED_HANDOFF = PULSAR_NATIVE_MANAGED_HANDOFF;

    public static final int PULSAR_AUTO_FAST = 0x04;
    public static final int VALID_MASK = ORDINARY_MANAGED | PULSAR_NATIVE_MANAGED_HANDOFF | PULSAR_AUTO_FAST;

    private TimingCapability() {}

    public static void requireValid(final int bits) {
        if ((bits & ~VALID_MASK) != 0 || (bits & ORDINARY_MANAGED) == 0) {
            throw new IllegalArgumentException("invalid timing capability bits");
        }
    }

    public static boolean includes(final int bits, final int capability) {
        if (capability <= 0 || (capability & ~VALID_MASK) != 0) {
            throw new IllegalArgumentException("invalid timing capability bit");
        }
        return (bits & capability) == capability;
    }
}
