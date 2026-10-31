package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetMessageRecord;
import java.util.Arrays;
import java.util.Objects;

/** Compact original ordinary eligibility facts, captured from the actual accepted Message birth batch. */
public record TargetSloMessageEligibility(
        TargetMessageLocator locator,
        long stateVersion,
        long deliverAt,
        long expireAt,
        NativeDeliveryPolicy nativePolicy,
        SourcePosition source,
        byte[] messageDigest) {
    public static final int MAX_CANONICAL_BYTES =
            TargetMessageLocator.MAX_CANONICAL_BYTES + TargetSourcePosition.MAX_CANONICAL_BYTES + 128;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-message-eligibility\0");

    public TargetSloMessageEligibility {
        Objects.requireNonNull(locator);
        Objects.requireNonNull(nativePolicy);
        TargetSourcePosition.requireBounded(source);
        messageDigest = TargetCompatibilityCodec.assigned(messageDigest, 32, "original accepted Message digest");
        if (stateVersion == 0
                || deliverAt < 0
                || expireAt < deliverAt
                || nativePolicy != NativeDeliveryPolicy.FORBID
                || !locator.messageId().routingId().shardId().equals(source.shardId())) {
            throw new IllegalArgumentException("ordinary eligibility requires original complete Message/Source facts");
        }
    }

    @Override
    public byte[] messageDigest() {
        return Bytes.copy(messageDigest);
    }

    public static TargetSloMessageEligibility from(TargetMessageRecord message) {
        if (message.runtime().admissionsUsed() != 0) {
            throw new IllegalArgumentException("eligibility cannot be first created from an admitted retry");
        }
        return new TargetSloMessageEligibility(
                message.locator(),
                message.stateVersion(),
                message.deliverAtEpochMs(),
                message.expireAtEpochMs(),
                message.nativeDeliveryPolicy(),
                message.scheduleSource(),
                message.digest());
    }

    public SloSampleStart start(TargetSloObservationPolicy.Declaration declaration) {
        if (declaration.objective().name() != SloObjectiveName.DUE_ADMISSION_LAG
                || declaration.path() != SloPath.ORDINARY_MANAGED) {
            throw new IllegalArgumentException("ordinary eligibility changed original declaration/path");
        }
        return SloAuthoritativeStartFactory.dueAdmission(
                declaration.objective(),
                locator.messageId(),
                Integer.toUnsignedLong(locator.generation()),
                SloPath.ORDINARY_MANAGED,
                deliverAt,
                digest());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, locator.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 3, stateVersion);
            CanonicalProtobuf.uint64Bits(out, 4, deliverAt);
            CanonicalProtobuf.uint64Bits(out, 5, expireAt);
            CanonicalProtobuf.uint32(out, 6, nativePolicy.wireValue());
            CanonicalProtobuf.bytes(out, 7, source.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, messageDigest);
        });
    }

    public byte[] digest() {
        return Bytes.sha256(DOMAIN, fields());
    }

    public byte[] canonicalBytes() {
        return Bytes.concat(fields(), CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 9, digest())));
    }

    public static TargetSloMessageEligibility decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 9, false, "Target original Message eligibility");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, "Target original Message eligibility");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown Target eligibility schema");
        }
        final var value = new TargetSloMessageEligibility(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.uint64Bits(f.get(2), 3),
                QueryCodecSupport.uint64Bits(f.get(3), 4),
                QueryCodecSupport.uint64Bits(f.get(4), 5),
                NativeDeliveryPolicy.fromWire(QueryCodecSupport.uint32(f.get(5), 6)),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                QueryCodecSupport.fixed(f.get(7), 8, 32));
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(8), 9, 32))) {
            throw new IllegalArgumentException("Target eligibility digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target original Message eligibility");
        return value;
    }
}
