package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Receipt for a registered System Control Operation, separate from Command receipts. */
public final class ControlOperationReceiptV1 {
    public static final int RECEIPT_VERSION = 1;

    private final byte[] operationId;
    private final byte[] requestHash;
    private final byte[] authenticatedScopeHash;
    private final byte[] targetSnapshotHash;
    private final long operationRevision;
    private final TrustedUtcIntervalEvidence registeredAt;
    private final long queryUntilEpochMs;
    private final byte[] receiptPayloadDigest;

    private ControlOperationReceiptV1(final byte[] operationId, final byte[] requestHash,
                                      final byte[] authenticatedScopeHash, final byte[] targetSnapshotHash,
                                      final long operationRevision, final TrustedUtcIntervalEvidence registeredAt,
                                      final long queryUntilEpochMs, final byte[] receiptPayloadDigest) {
        this.operationId = requireNonZero(operationId, "operationId");
        Bytes.requireLength(requestHash, 32, "requestHash");
        Bytes.requireLength(authenticatedScopeHash, 32, "authenticatedScopeHash");
        Bytes.requireLength(targetSnapshotHash, 32, "targetSnapshotHash");
        this.requestHash = Bytes.copy(requestHash);
        this.authenticatedScopeHash = Bytes.copy(authenticatedScopeHash);
        this.targetSnapshotHash = Bytes.copy(targetSnapshotHash);
        if (operationRevision <= 0) {
            throw new IllegalArgumentException("operation revision must be positive");
        }
        this.operationRevision = operationRevision;
        this.registeredAt = Objects.requireNonNull(registeredAt, "registeredAt");
        if (queryUntilEpochMs < registeredAt.latestEpochMs()) {
            throw new IllegalArgumentException("control query boundary precedes registration evidence");
        }
        this.queryUntilEpochMs = queryUntilEpochMs;
        Bytes.requireLength(receiptPayloadDigest, 32, "receiptPayloadDigest");
        this.receiptPayloadDigest = Bytes.copy(receiptPayloadDigest);
    }

    public static ControlOperationReceiptV1 create(final byte[] operationId, final byte[] requestHash,
                                                   final byte[] authenticatedScopeHash,
                                                   final byte[] targetSnapshotHash, final long operationRevision,
                                                   final TrustedUtcIntervalEvidence registeredAt,
                                                   final long queryUntilEpochMs) {
        final byte[] fields = canonicalFields(operationId, requestHash, authenticatedScopeHash, targetSnapshotHash,
                operationRevision, registeredAt, queryUntilEpochMs);
        return new ControlOperationReceiptV1(operationId, requestHash, authenticatedScopeHash, targetSnapshotHash,
                operationRevision, registeredAt, queryUntilEpochMs, Bytes.sha256(fields));
    }

    public static ControlOperationReceiptV1 decodeFrame(final byte[] frame) {
        final ReceiptFrame.Decoded decoded = ReceiptFrame.decode(frame);
        if (decoded.kind() != ReceiptKind.CONTROL_OPERATION) {
            throw new IllegalArgumentException("receipt frame is not CONTROL_OPERATION");
        }
        return decodePayload(decoded.payload());
    }

    public static ControlOperationReceiptV1 decodePayload(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ControlOperationReceiptV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
                "ControlOperationReceiptV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != RECEIPT_VERSION) {
            throw new IllegalArgumentException("unsupported ControlOperationReceiptV1 version");
        }
        final byte[] operationId = QueryCodecSupport.fixed(fields.get(1), 2, 32);
        final byte[] requestHash = QueryCodecSupport.fixed(fields.get(2), 3, 32);
        final byte[] scopeHash = QueryCodecSupport.fixed(fields.get(3), 4, 32);
        final byte[] targetHash = QueryCodecSupport.fixed(fields.get(4), 5, 32);
        final long operationRevision = QueryCodecSupport.uint(fields.get(5), 6);
        final TrustedUtcIntervalEvidence registeredAt = TrustedUtcIntervalEvidence.decode(
                QueryCodecSupport.nested(fields.get(6), 7));
        final long queryUntil = QueryCodecSupport.uint(fields.get(7), 8);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(8), 9, 32);
        final ControlOperationReceiptV1 result = new ControlOperationReceiptV1(operationId, requestHash, scopeHash,
                targetHash, operationRevision, registeredAt, queryUntil, digest);
        final byte[] expected = Bytes.sha256(canonicalFields(operationId, requestHash, scopeHash, targetHash,
                operationRevision, registeredAt, queryUntil));
        if (!Bytes.constantTimeEquals(digest, expected)) {
            throw new IllegalArgumentException("ControlOperationReceipt digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.payload(), "ControlOperationReceiptV1");
        return result;
    }

    public byte[] operationId() {
        return Bytes.copy(operationId);
    }

    public byte[] requestHash() {
        return Bytes.copy(requestHash);
    }

    public byte[] authenticatedScopeHash() {
        return Bytes.copy(authenticatedScopeHash);
    }

    public byte[] targetSnapshotHash() {
        return Bytes.copy(targetSnapshotHash);
    }

    public long operationRevision() {
        return operationRevision;
    }

    public TrustedUtcIntervalEvidence registeredAt() {
        return registeredAt;
    }

    public long queryUntilEpochMs() {
        return queryUntilEpochMs;
    }

    public byte[] receiptPayloadDigest() {
        return Bytes.copy(receiptPayloadDigest);
    }

    public byte[] payload() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, operationId);
            CanonicalProtobuf.bytes(output, 3, requestHash);
            CanonicalProtobuf.bytes(output, 4, authenticatedScopeHash);
            CanonicalProtobuf.bytes(output, 5, targetSnapshotHash);
            CanonicalProtobuf.uint64(output, 6, operationRevision);
            CanonicalProtobuf.bytes(output, 7, registeredAt.canonicalBytes());
            CanonicalProtobuf.int64(output, 8, queryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 9, receiptPayloadDigest);
        });
    }

    public byte[] frame() {
        return ReceiptFrame.encode(ReceiptKind.CONTROL_OPERATION, payload());
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ControlOperationReceiptV1 that)) {
            return false;
        }
        return operationRevision == that.operationRevision && queryUntilEpochMs == that.queryUntilEpochMs
                && Arrays.equals(operationId, that.operationId) && Arrays.equals(requestHash, that.requestHash)
                && Arrays.equals(authenticatedScopeHash, that.authenticatedScopeHash)
                && Arrays.equals(targetSnapshotHash, that.targetSnapshotHash)
                && registeredAt.canonicalBytes().length == that.registeredAt.canonicalBytes().length
                && Arrays.equals(registeredAt.canonicalBytes(), that.registeredAt.canonicalBytes())
                && Arrays.equals(receiptPayloadDigest, that.receiptPayloadDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(operationId), Arrays.hashCode(requestHash),
                Arrays.hashCode(authenticatedScopeHash), Arrays.hashCode(targetSnapshotHash), operationRevision,
                Arrays.hashCode(registeredAt.canonicalBytes()), queryUntilEpochMs,
                Arrays.hashCode(receiptPayloadDigest));
    }

    private static byte[] canonicalFields(final byte[] operationId, final byte[] requestHash,
                                          final byte[] authenticatedScopeHash, final byte[] targetSnapshotHash,
                                          final long operationRevision, final TrustedUtcIntervalEvidence registeredAt,
                                          final long queryUntilEpochMs) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, operationId);
            CanonicalProtobuf.bytes(output, 3, requestHash);
            CanonicalProtobuf.bytes(output, 4, authenticatedScopeHash);
            CanonicalProtobuf.bytes(output, 5, targetSnapshotHash);
            CanonicalProtobuf.uint64(output, 6, operationRevision);
            CanonicalProtobuf.bytes(output, 7, registeredAt.canonicalBytes());
            CanonicalProtobuf.int64(output, 8, queryUntilEpochMs);
        });
    }

    private static byte[] requireNonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        boolean nonZero = false;
        for (byte current : value) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }
}
