package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable content addressed by the original Retry Policy's terminalPolicyDigest.
 * This value defines semantics only; it does not prove that readers, references or retention have closed.
 */
public record TerminalPolicySemantic(
        long minimumPayloadRetentionMs,
        TerminalTimeBasis terminalTimeBasis,
        PostTerminalPrimaryPayloadRead postTerminalPrimaryPayloadRead,
        ReplayMode replayMode) {
    public static final int SCHEMA_VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = 4096;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-terminal-policy-semantic\0");

    public enum TerminalTimeBasis {
        RETAINED_OWNER_SOURCE_PERSISTENCE
    }

    public enum PostTerminalPrimaryPayloadRead {
        METADATA_ONLY
    }

    public enum ReplayMode {
        DISABLED
    }

    public TerminalPolicySemantic {
        if (minimumPayloadRetentionMs < 0 || minimumPayloadRetentionMs == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "terminal payload retention requires an explicit finite nonnegative duration");
        }
        Objects.requireNonNull(terminalTimeBasis, "terminalTimeBasis");
        Objects.requireNonNull(postTerminalPrimaryPayloadRead, "postTerminalPrimaryPayloadRead");
        Objects.requireNonNull(replayMode, "replayMode");
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, SCHEMA_VERSION);
            // All fields, including zero retention, are mandatory. Enum values are closed, not Java ordinals.
            CanonicalProtobuf.int64(out, 2, minimumPayloadRetentionMs);
            CanonicalProtobuf.uint32(out, 3, 1);
            CanonicalProtobuf.uint32(out, 4, 1);
            CanonicalProtobuf.uint32(out, 5, 1);
        });
    }

    public byte[] digest() {
        return Bytes.sha256(DOMAIN, canonicalBytes());
    }

    /** Content binding only. The Retry Policy must independently be authenticated at every original Source use. */
    public void requireBinding(RetryPolicySemantic retry) {
        if (!Arrays.equals(digest(), Objects.requireNonNull(retry, "retry").terminalPolicyDigest())) {
            throw new IllegalArgumentException(
                    "terminal policy content differs from the original frozen Retry Policy digest");
        }
    }

    /** The first inline release branch has no export contract; dynamic historical obligations still require closure. */
    public void requireInlineReleasePolicy(RetryPolicySemantic retry) {
        requirePrimaryReleasePolicy(retry);
    }

    /** The current finite metadata-only/no-replay contract has no DLQ export release branch. */
    public void requirePrimaryReleasePolicy(RetryPolicySemantic retry) {
        requireBinding(retry);
        if (retry.dlqExportMode() != DlqExportMode.NOT_CONFIGURED) {
            throw new IllegalArgumentException(
                    "inline payload release does not support a Retry Policy with DLQ export");
        }
    }

    /**
     * Arithmetic on an independently authenticated original RETAINED transition. This is Source persistence time,
     * not local apply time or a clock/fence proof that the returned deadline has passed.
     */
    public long retentionDeadline(TargetQuotaMutation retainedTransition) {
        final var source =
                Objects.requireNonNull(retainedTransition, "retainedTransition").source();
        TargetSourcePosition.requireBounded(source);
        final long start = source.brokerPersistenceTimeEpochMs();
        if (start < 0) {
            throw new IllegalArgumentException("original RETAINED Source has no persistence timestamp");
        }
        final long deadline;
        try {
            deadline = Math.addExact(start, minimumPayloadRetentionMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("terminal payload retention deadline overflows", overflow);
        }
        if (deadline == Long.MAX_VALUE) {
            throw new IllegalArgumentException("terminal payload retention deadline is not finite");
        }
        return deadline;
    }

    public static TerminalPolicySemantic decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "terminal policy semantic");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5}, "terminal policy semantic");
        if (QueryCodecSupport.uint32(f.get(0), 1) != SCHEMA_VERSION
                || QueryCodecSupport.uint32(f.get(2), 3) != 1
                || QueryCodecSupport.uint32(f.get(3), 4) != 1
                || QueryCodecSupport.uint32(f.get(4), 5) != 1) {
            throw new IllegalArgumentException("unsupported terminal policy schema or semantics");
        }
        final var value = new TerminalPolicySemantic(
                QueryCodecSupport.uint(f.get(1), 2),
                TerminalTimeBasis.RETAINED_OWNER_SOURCE_PERSISTENCE,
                PostTerminalPrimaryPayloadRead.METADATA_ONLY,
                ReplayMode.DISABLED);
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "terminal policy semantic");
        return value;
    }
}
