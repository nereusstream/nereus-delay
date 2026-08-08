package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable, hash-addressed retry policy semantic value from Registry §5.1.1.
 *
 * <p>This class validates only the policy bytes and their deterministic
 * arithmetic. It does not publish, activate or resolve a policy in Oxia.</p>
 */
public final class RetryPolicySemanticV1 {
    public static final int ENVELOPE_VERSION = 1;
    public static final int JITTER_ALGORITHM_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final String HASH_DOMAIN = "nereus-delay-retry-policy-semantic-v1\0";

    private final byte[] policyId;
    private final long version;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final int maxPublishAdmissions;
    private final long maxRetryDurationMs;
    private final UncertainPolicyV1 uncertainPolicy;
    private final int maxUncertainRetries;
    private final DlqExportModeV1 dlqExportMode;
    private final long dlqInitialBackoffMs;
    private final long dlqMaxBackoffMs;
    private final int dlqMaxAttempts;
    private final long dlqMaxRetryDurationMs;
    private final boolean dlqAllowPossibleDuplicate;
    private final byte[] terminalPolicyDigest;
    private final byte[] semanticHash;

    public RetryPolicySemanticV1(final byte[] policyId, final long version, final long initialBackoffMs,
                                 final long maxBackoffMs, final int maxPublishAdmissions,
                                 final long maxRetryDurationMs, final UncertainPolicyV1 uncertainPolicy,
                                 final int maxUncertainRetries, final DlqExportModeV1 dlqExportMode,
                                 final long dlqInitialBackoffMs, final long dlqMaxBackoffMs,
                                 final int dlqMaxAttempts, final long dlqMaxRetryDurationMs,
                                 final boolean dlqAllowPossibleDuplicate, final byte[] terminalPolicyDigest) {
        this.policyId = nonEmpty(policyId, "policyId");
        if (version == 0) {
            throw new IllegalArgumentException("retry policy version must be nonzero");
        }
        if (initialBackoffMs < 0 || maxBackoffMs < initialBackoffMs) {
            throw new IllegalArgumentException("invalid retry backoff range");
        }
        if (maxPublishAdmissions <= 0 || maxRetryDurationMs <= 0) {
            throw new IllegalArgumentException("retry admission/duration bounds must be positive");
        }
        checkedBackoffBudget(maxBackoffMs, maxPublishAdmissions, "retry");
        this.version = version;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.maxPublishAdmissions = maxPublishAdmissions;
        this.maxRetryDurationMs = maxRetryDurationMs;
        this.uncertainPolicy = Objects.requireNonNull(uncertainPolicy, "uncertainPolicy");
        if (uncertainPolicy == UncertainPolicyV1.BOUNDED_RETRY_POSSIBLE_DUPLICATE
                ? maxUncertainRetries <= 0 || maxUncertainRetries >= maxPublishAdmissions
                : maxUncertainRetries != 0) {
            throw new IllegalArgumentException("uncertain retry budget does not match policy");
        }
        this.maxUncertainRetries = maxUncertainRetries;
        this.dlqExportMode = Objects.requireNonNull(dlqExportMode, "dlqExportMode");
        validateDlq(dlqExportMode, dlqInitialBackoffMs, dlqMaxBackoffMs, dlqMaxAttempts,
                dlqMaxRetryDurationMs, dlqAllowPossibleDuplicate);
        this.dlqInitialBackoffMs = dlqInitialBackoffMs;
        this.dlqMaxBackoffMs = dlqMaxBackoffMs;
        this.dlqMaxAttempts = dlqMaxAttempts;
        this.dlqMaxRetryDurationMs = dlqMaxRetryDurationMs;
        this.dlqAllowPossibleDuplicate = dlqAllowPossibleDuplicate;
        Bytes.requireLength(terminalPolicyDigest, HASH_LENGTH, "terminalPolicyDigest");
        this.terminalPolicyDigest = Bytes.copy(terminalPolicyDigest);
        this.semanticHash = computeSemanticHash();
    }

    public byte[] policyId() {
        return Bytes.copy(policyId);
    }

    public long version() {
        return version;
    }

    public long initialBackoffMs() {
        return initialBackoffMs;
    }

    public long maxBackoffMs() {
        return maxBackoffMs;
    }

    public int maxPublishAdmissions() {
        return maxPublishAdmissions;
    }

    public long maxRetryDurationMs() {
        return maxRetryDurationMs;
    }

    public UncertainPolicyV1 uncertainPolicy() {
        return uncertainPolicy;
    }

    public int maxUncertainRetries() {
        return maxUncertainRetries;
    }

    public DlqExportModeV1 dlqExportMode() {
        return dlqExportMode;
    }

    public long dlqInitialBackoffMs() {
        return dlqInitialBackoffMs;
    }

    public long dlqMaxBackoffMs() {
        return dlqMaxBackoffMs;
    }

    public int dlqMaxAttempts() {
        return dlqMaxAttempts;
    }

    public long dlqMaxRetryDurationMs() {
        return dlqMaxRetryDurationMs;
    }

    public boolean dlqAllowPossibleDuplicate() {
        return dlqAllowPossibleDuplicate;
    }

    public byte[] terminalPolicyDigest() {
        return Bytes.copy(terminalPolicyDigest);
    }

    public byte[] semanticHash() {
        return Bytes.copy(semanticHash);
    }

    public RetryPolicyRefV1 ref() {
        return new RetryPolicyRefV1(policyId, version, semanticHash);
    }

    /** Rejects a FIFO binding when this policy can authorize uncertain retry. */
    public void validateFor(final OrderingMode orderingMode) {
        Objects.requireNonNull(orderingMode, "orderingMode");
        if (orderingMode == OrderingMode.DELIVERY_TIME_FIFO
                && uncertainPolicy == UncertainPolicyV1.BOUNDED_RETRY_POSSIBLE_DUPLICATE) {
            throw new IllegalArgumentException("possible-duplicate retry requires BEST_EFFORT ordering");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ENVELOPE_VERSION);
            CanonicalProtobuf.bytes(output, 2, policyId);
            CanonicalProtobuf.uint64Bits(output, 3, version);
            CanonicalProtobuf.uint64(output, 4, initialBackoffMs);
            CanonicalProtobuf.uint64(output, 5, maxBackoffMs);
            CanonicalProtobuf.uint32(output, 6, maxPublishAdmissions);
            CanonicalProtobuf.uint64(output, 7, maxRetryDurationMs);
            CanonicalProtobuf.uint32(output, 8, uncertainPolicy.wireValue());
            CanonicalProtobuf.uint32(output, 9, maxUncertainRetries);
            CanonicalProtobuf.uint32(output, 10, dlqExportMode.wireValue());
            CanonicalProtobuf.uint64(output, 11, dlqInitialBackoffMs);
            CanonicalProtobuf.uint64(output, 12, dlqMaxBackoffMs);
            CanonicalProtobuf.uint32(output, 13, dlqMaxAttempts);
            CanonicalProtobuf.uint64(output, 14, dlqMaxRetryDurationMs);
            CanonicalProtobuf.uint32(output, 15, dlqAllowPossibleDuplicate ? 1 : 0);
            CanonicalProtobuf.uint32(output, 16, JITTER_ALGORITHM_VERSION);
            CanonicalProtobuf.bytes(output, 17, terminalPolicyDigest);
            CanonicalProtobuf.bytes(output, 18, semanticHash);
        });
    }

    public static RetryPolicySemanticV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "RetryPolicySemanticV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 11, 12, 13, 14, 15, 16, 17, 18}, "RetryPolicySemanticV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != ENVELOPE_VERSION
                || QueryCodecSupport.uint(fields.get(15), 16) != JITTER_ALGORITHM_VERSION) {
            throw new IllegalArgumentException("unsupported RetryPolicySemanticV1 version");
        }
        final RetryPolicySemanticV1 result = new RetryPolicySemanticV1(
                QueryCodecSupport.bytes(fields.get(1), 2), QueryCodecSupport.uint64Bits(fields.get(2), 3),
                QueryCodecSupport.uint(fields.get(3), 4), QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.uint32(fields.get(5), 6), QueryCodecSupport.uint(fields.get(6), 7),
                UncertainPolicyV1.fromWire(QueryCodecSupport.uint(fields.get(7), 8)),
                QueryCodecSupport.uint32(fields.get(8), 9),
                DlqExportModeV1.fromWire(QueryCodecSupport.uint(fields.get(9), 10)),
                QueryCodecSupport.uint(fields.get(10), 11), QueryCodecSupport.uint(fields.get(11), 12),
                QueryCodecSupport.uint32(fields.get(12), 13), QueryCodecSupport.uint(fields.get(13), 14),
                QueryCodecSupport.bool(fields.get(14), 15), QueryCodecSupport.fixed(fields.get(16), 17, HASH_LENGTH));
        if (!Bytes.constantTimeEquals(result.semanticHash, QueryCodecSupport.fixed(fields.get(17), 18, HASH_LENGTH))) {
            throw new IllegalArgumentException("RetryPolicySemanticV1 semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RetryPolicySemanticV1");
        return result;
    }

    private byte[] computeSemanticHash() {
        final byte[] semanticFields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ENVELOPE_VERSION);
            CanonicalProtobuf.uint64(output, 4, initialBackoffMs);
            CanonicalProtobuf.uint64(output, 5, maxBackoffMs);
            CanonicalProtobuf.uint32(output, 6, maxPublishAdmissions);
            CanonicalProtobuf.uint64(output, 7, maxRetryDurationMs);
            CanonicalProtobuf.uint32(output, 8, uncertainPolicy.wireValue());
            CanonicalProtobuf.uint32(output, 9, maxUncertainRetries);
            CanonicalProtobuf.uint32(output, 10, dlqExportMode.wireValue());
            CanonicalProtobuf.uint64(output, 11, dlqInitialBackoffMs);
            CanonicalProtobuf.uint64(output, 12, dlqMaxBackoffMs);
            CanonicalProtobuf.uint32(output, 13, dlqMaxAttempts);
            CanonicalProtobuf.uint64(output, 14, dlqMaxRetryDurationMs);
            CanonicalProtobuf.uint32(output, 15, dlqAllowPossibleDuplicate ? 1 : 0);
            CanonicalProtobuf.uint32(output, 16, JITTER_ALGORITHM_VERSION);
            CanonicalProtobuf.bytes(output, 17, terminalPolicyDigest);
        });
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), Bytes.lp32(policyId), Bytes.u64beBits(version), semanticFields);
    }

    private static void validateDlq(final DlqExportModeV1 mode, final long initialBackoff,
                                    final long maxBackoff, final int maxAttempts, final long maxDuration,
                                    final boolean allowPossibleDuplicate) {
        if (mode == DlqExportModeV1.NOT_CONFIGURED) {
            if (initialBackoff != 0 || maxBackoff != 0 || maxAttempts != 0 || maxDuration != 0
                    || allowPossibleDuplicate) {
                throw new IllegalArgumentException("disabled DLQ policy must use zero bounds");
            }
            return;
        }
        if (initialBackoff < 0 || maxBackoff < initialBackoff || maxAttempts <= 0 || maxDuration <= 0) {
            throw new IllegalArgumentException("invalid DLQ retry bounds");
        }
        checkedBackoffBudget(maxBackoff, maxAttempts, "DLQ");
    }

    private static void checkedBackoffBudget(final long maxBackoff, final int attempts, final String name) {
        try {
            Math.multiplyExact(maxBackoff, attempts);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(name + " backoff budget overflows", exception);
        }
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RetryPolicySemanticV1 that && version == that.version
                && initialBackoffMs == that.initialBackoffMs && maxBackoffMs == that.maxBackoffMs
                && maxPublishAdmissions == that.maxPublishAdmissions
                && maxRetryDurationMs == that.maxRetryDurationMs && uncertainPolicy == that.uncertainPolicy
                && maxUncertainRetries == that.maxUncertainRetries && dlqExportMode == that.dlqExportMode
                && dlqInitialBackoffMs == that.dlqInitialBackoffMs && dlqMaxBackoffMs == that.dlqMaxBackoffMs
                && dlqMaxAttempts == that.dlqMaxAttempts && dlqMaxRetryDurationMs == that.dlqMaxRetryDurationMs
                && dlqAllowPossibleDuplicate == that.dlqAllowPossibleDuplicate
                && Arrays.equals(policyId, that.policyId) && Arrays.equals(terminalPolicyDigest, that.terminalPolicyDigest)
                && Arrays.equals(semanticHash, that.semanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(policyId), version, initialBackoffMs, maxBackoffMs,
                maxPublishAdmissions, maxRetryDurationMs, uncertainPolicy, maxUncertainRetries, dlqExportMode,
                dlqInitialBackoffMs, dlqMaxBackoffMs, dlqMaxAttempts, dlqMaxRetryDurationMs,
                dlqAllowPossibleDuplicate, Arrays.hashCode(terminalPolicyDigest), Arrays.hashCode(semanticHash));
    }
}
