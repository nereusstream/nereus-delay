package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Closed value stored at one {@code meta_cf/LANE} key.
 *
 * <p>The active branch deliberately carries the current local active-state
 * adapter bytes while the richer ActiveLaneStateV1 projection is completed.
 * The branch boundary itself is already durable and closed: a terminal guard
 * is never decoded as an active record and cannot be reopened by a normal lane
 * mutation.</p>
 */
public final class LaneRecordEnvelopeV1 {
    private static final byte[] ACTIVE_ADAPTER_DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-active-lane-adapter-v1\0");
    private static final int HASH_LENGTH = 32;

    public enum Kind {
        ACTIVE_LANE(1),
        TERMINAL_GUARD(2);

        private final int wireValue;

        Kind(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        private static Kind fromWire(final long value) {
            for (Kind kind : values()) {
                if (kind.wireValue == value) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("unknown LaneRecordKindV1: " + value);
        }
    }

    private final Kind kind;
    private final byte[] activeStateBytes;
    private final LaneTerminalGuardV1 terminalGuard;

    private LaneRecordEnvelopeV1(final Kind kind, final byte[] activeStateBytes,
                                 final LaneTerminalGuardV1 terminalGuard) {
        this.kind = Objects.requireNonNull(kind, "kind");
        if (kind == Kind.ACTIVE_LANE) {
            Objects.requireNonNull(activeStateBytes, "activeStateBytes");
            if (activeStateBytes.length == 0) {
                throw new IllegalArgumentException("activeStateBytes must not be empty");
            }
            if (terminalGuard != null) {
                throw new IllegalArgumentException("active lane cannot carry a terminal guard");
            }
            this.activeStateBytes = Bytes.copy(activeStateBytes);
        } else {
            if (activeStateBytes != null || terminalGuard == null) {
                throw new IllegalArgumentException("terminal lane must carry only a terminal guard");
            }
            this.activeStateBytes = null;
        }
        this.terminalGuard = terminalGuard;
    }

    public static LaneRecordEnvelopeV1 active(final byte[] activeStateBytes) {
        return new LaneRecordEnvelopeV1(Kind.ACTIVE_LANE, activeStateBytes, null);
    }

    public static LaneRecordEnvelopeV1 terminal(final LaneTerminalGuardV1 guard) {
        return new LaneRecordEnvelopeV1(Kind.TERMINAL_GUARD, null,
                Objects.requireNonNull(guard, "guard"));
    }

    public Kind kind() {
        return kind;
    }

    public boolean isActive() {
        return kind == Kind.ACTIVE_LANE;
    }

    public byte[] activeStateBytes() {
        if (!isActive()) {
            throw new IllegalStateException("lane value is terminal");
        }
        return Bytes.copy(activeStateBytes);
    }

    public LaneTerminalGuardV1 terminalGuard() {
        if (isActive()) {
            throw new IllegalStateException("lane value is active");
        }
        return terminalGuard;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            if (isActive()) {
                CanonicalProtobuf.bytes(output, 10, activeAdapterBytes());
            } else {
                CanonicalProtobuf.bytes(output, 11, terminalGuard.canonicalBytes());
            }
        });
    }

    public static LaneRecordEnvelopeV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "LaneRecordEnvelopeV1");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("invalid LaneRecordEnvelopeV1 field order");
        }
        final Kind kind = Kind.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final LaneRecordEnvelopeV1 result;
        if (kind == Kind.ACTIVE_LANE) {
            if (fields.get(1).number() != 10) {
                throw new IllegalArgumentException("active lane must use field 10");
            }
            result = active(decodeActiveAdapter(QueryCodecSupport.nested(fields.get(1), 10)));
        } else {
            if (fields.get(1).number() != 11) {
                throw new IllegalArgumentException("terminal lane must use field 11");
            }
            result = terminal(LaneTerminalGuardV1.decode(QueryCodecSupport.nested(fields.get(1), 11)));
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneRecordEnvelopeV1");
        return result;
    }

    private byte[] activeAdapterBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, activeStateBytes);
            CanonicalProtobuf.bytes(output, 2, activeAdapterDigest(activeStateBytes));
        });
    }

    private static byte[] decodeActiveAdapter(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ActiveLaneAdapterV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2}, "ActiveLaneAdapterV1");
        final byte[] state = QueryCodecSupport.bytes(fields.get(0), 1);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        if (!Bytes.constantTimeEquals(digest, activeAdapterDigest(state))) {
            throw new IllegalArgumentException("active lane adapter digest mismatch");
        }
        final byte[] canonical = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, state);
            CanonicalProtobuf.bytes(output, 2, digest);
        });
        QueryCodecSupport.requireCanonical(encoded, canonical, "ActiveLaneAdapterV1");
        return state;
    }

    private static byte[] activeAdapterDigest(final byte[] state) {
        return Bytes.sha256(ACTIVE_ADAPTER_DIGEST_DOMAIN, state);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneRecordEnvelopeV1 that && kind == that.kind
                && Arrays.equals(activeStateBytes, that.activeStateBytes)
                && Objects.equals(terminalGuard, that.terminalGuard);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(activeStateBytes), terminalGuard);
    }
}
