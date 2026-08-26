package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Closed value stored at one {@code meta_cf/LANE} key.
 *
 * <p>The typed active branch carries the Registry {@link ActiveLaneState}
 * directly. The old adapter sub-message remains readable for databases
 * written before the typed cutover; it is never confused with malformed typed
 * state because the two encodings use different wire types for field 1. The
 * branch boundary itself is closed: a terminal guard is never decoded as an
 * active record and cannot be reopened by a normal lane mutation.</p>
 */
public final class LaneRecordEnvelope {
    private static final byte[] ACTIVE_ADAPTER_DIGEST_DOMAIN = Bytes.utf8("nereus-delay-active-lane-adapter\0");
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
            throw new IllegalArgumentException("unknown LaneRecordKind: " + value);
        }
    }

    private final Kind kind;
    private final byte[] activeStateBytes;
    private final boolean typedActiveState;
    private final LaneTerminalGuard terminalGuard;

    private LaneRecordEnvelope(
            final Kind kind,
            final byte[] activeStateBytes,
            final LaneTerminalGuard terminalGuard,
            final boolean typedActiveState) {
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
            this.typedActiveState = typedActiveState;
            if (typedActiveState) {
                ActiveLaneState.decode(this.activeStateBytes);
            }
        } else {
            if (activeStateBytes != null || terminalGuard == null) {
                throw new IllegalArgumentException("terminal lane must carry only a terminal guard");
            }
            this.activeStateBytes = null;
            this.typedActiveState = false;
        }
        this.terminalGuard = terminalGuard;
    }

    public static LaneRecordEnvelope active(final byte[] activeStateBytes) {
        return new LaneRecordEnvelope(Kind.ACTIVE_LANE, activeStateBytes, null, false);
    }

    /**
     * Constructs the typed ACTIVE branch from the closed Registry state.
     *
     * <p>The byte-array overload remains for reopening databases written by
     * the legacy {@code LaneRecord} adapter. New callers that have the full
     * immutable Profile/tuple/certificate inputs should use this overload so
     * the branch cannot silently carry a non-canonical state.</p>
     */
    public static LaneRecordEnvelope active(final ActiveLaneState state) {
        return new LaneRecordEnvelope(
                Kind.ACTIVE_LANE, Objects.requireNonNull(state, "state").canonicalBytes(), null, true);
    }

    public static LaneRecordEnvelope terminal(final LaneTerminalGuard guard) {
        return new LaneRecordEnvelope(Kind.TERMINAL_GUARD, null, Objects.requireNonNull(guard, "guard"), false);
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

    /**
     * Decodes the typed Registry ACTIVE state. Legacy adapter bytes are
     * intentionally not guessed or upgraded: callers must supply the missing
     * immutable Profile and tuple inputs before a typed state can be written.
     */
    public ActiveLaneState activeState() {
        if (!isActive()) {
            throw new IllegalStateException("lane value is terminal");
        }
        if (!typedActiveState) {
            throw new IllegalArgumentException("lane value is a legacy adapter, not ActiveLaneState");
        }
        return ActiveLaneState.decode(activeStateBytes);
    }

    /**
     * Returns the typed state when this ACTIVE branch contains one; legacy
     * adapter bytes produce an empty result without weakening decode checks.
     */
    public Optional<ActiveLaneState> typedActiveState() {
        if (!isActive()) {
            return Optional.empty();
        }
        if (!typedActiveState) {
            return Optional.empty();
        }
        try {
            return Optional.of(ActiveLaneState.decode(activeStateBytes));
        } catch (IllegalArgumentException malformedTypedState) {
            return Optional.empty();
        }
    }

    public LaneTerminalGuard terminalGuard() {
        if (isActive()) {
            throw new IllegalStateException("lane value is active");
        }
        return terminalGuard;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            if (isActive()) {
                CanonicalProtobuf.bytes(output, 10, typedActiveState ? activeStateBytes : activeAdapterBytes());
            } else {
                CanonicalProtobuf.bytes(output, 11, terminalGuard.canonicalBytes());
            }
        });
    }

    public static LaneRecordEnvelope decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "LaneRecordEnvelope");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("invalid LaneRecordEnvelope field order");
        }
        final Kind kind = Kind.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final LaneRecordEnvelope result;
        if (kind == Kind.ACTIVE_LANE) {
            if (fields.get(1).number() != 10) {
                throw new IllegalArgumentException("active lane must use field 10");
            }
            final byte[] active = QueryCodecSupport.nested(fields.get(1), 10);
            final var activeFields = QueryCodecSupport.read(active, "ActiveLaneStateOrAdapter");
            if (activeFields.isEmpty()) {
                throw new IllegalArgumentException("active lane state must not be empty");
            }
            // ActiveLaneState field 1 is a varint version. The legacy
            // adapter's field 1 is length-delimited state bytes. This lets
            // malformed typed state fail closed instead of being accepted as
            // an opaque legacy payload.
            result = activeFields.get(0).wireType() == 0
                    ? active(ActiveLaneState.decode(active))
                    : active(decodeActiveAdapter(active));
        } else {
            if (fields.get(1).number() != 11) {
                throw new IllegalArgumentException("terminal lane must use field 11");
            }
            result = terminal(LaneTerminalGuard.decode(QueryCodecSupport.nested(fields.get(1), 11)));
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneRecordEnvelope");
        return result;
    }

    private byte[] activeAdapterBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, activeStateBytes);
            CanonicalProtobuf.bytes(output, 2, activeAdapterDigest(activeStateBytes));
        });
    }

    private static byte[] decodeActiveAdapter(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ActiveLaneAdapter");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "ActiveLaneAdapter");
        final byte[] state = QueryCodecSupport.bytes(fields.get(0), 1);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        if (!Bytes.constantTimeEquals(digest, activeAdapterDigest(state))) {
            throw new IllegalArgumentException("active lane adapter digest mismatch");
        }
        final byte[] canonical = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, state);
            CanonicalProtobuf.bytes(output, 2, digest);
        });
        QueryCodecSupport.requireCanonical(encoded, canonical, "ActiveLaneAdapter");
        return state;
    }

    private static byte[] activeAdapterDigest(final byte[] state) {
        return Bytes.sha256(ACTIVE_ADAPTER_DIGEST_DOMAIN, state);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneRecordEnvelope that
                && kind == that.kind
                && Arrays.equals(activeStateBytes, that.activeStateBytes)
                && Objects.equals(terminalGuard, that.terminalGuard);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(activeStateBytes), terminalGuard);
    }
}
