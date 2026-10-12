package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Registered Control request for one Source-owned channel slot; it never rewrites retained channel identities. */
public final class TargetChannelControlRequest implements ControlOperationRequestBranch {
    public static final int CREATE_CONTROL_KIND = 24;
    public static final int RENEW_CONTROL_KIND = 25;
    public static final int STOP_CONTROL_KIND = 26;
    public static final int MAX_CANONICAL_BYTES =
            TargetChannelIdentity.MAX_CANONICAL_BYTES + TargetQuotaClaimCharge.MAX_OWNER_BYTES + 160;

    private final ControlOperationKind operation;
    private final TargetChannelIdentity channel;
    private final byte[] expectedPriorDigest;
    private final OwnerIdentity owner;
    private final ControlReason reason;

    private TargetChannelControlRequest(
            final ControlOperationKind operation, final TargetChannelIdentity channel,
            final byte[] expectedPriorDigest, final OwnerIdentity owner, final ControlReason reason) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.owner = Objects.requireNonNull(owner, "owner");
        if (owner.canonicalBytes().length > TargetQuotaClaimCharge.MAX_OWNER_BYTES) {
            throw new IllegalArgumentException("channel Control Owner identity exceeds its bound");
        }
        this.expectedPriorDigest = expectedPriorDigest == null ? null
                : TargetCompatibilityCodec.assigned(expectedPriorDigest, 32, "priorChannelDigest");
        this.reason = reason;
        switch (operation) {
            case CREATE_TARGET_CHANNEL -> {
                if (expectedPriorDigest != null || reason != null || channel.context().channelGeneration() != 1) {
                    throw new IllegalArgumentException("channel creation requires a fresh slot and generation one");
                }
            }
            case RENEW_TARGET_CHANNEL -> {
                if (expectedPriorDigest == null || reason != null || channel.context().channelGeneration() == 1) {
                    throw new IllegalArgumentException("channel renewal requires an exact predecessor");
                }
            }
            case STOP_TARGET_CHANNEL_NEW_WORK -> {
                if (expectedPriorDigest == null || reason == null
                        || !Arrays.equals(expectedPriorDigest, channel.digest())) {
                    throw new IllegalArgumentException("channel stop must retain the exact current identity");
                }
            }
            default -> throw new IllegalArgumentException("operation is not a Target channel lifecycle request");
        }
    }

    public static TargetChannelControlRequest create(final TargetChannelIdentity channel, final OwnerIdentity owner) {
        return new TargetChannelControlRequest(ControlOperationKind.CREATE_TARGET_CHANNEL, channel, null, owner, null);
    }

    public static TargetChannelControlRequest renew(
            final TargetChannelIdentity channel, final byte[] priorDigest, final OwnerIdentity owner) {
        return new TargetChannelControlRequest(ControlOperationKind.RENEW_TARGET_CHANNEL, channel,
                Objects.requireNonNull(priorDigest, "priorDigest"), owner, null);
    }

    public static TargetChannelControlRequest stop(
            final TargetChannelIdentity channel, final OwnerIdentity owner, final ControlReason reason) {
        return new TargetChannelControlRequest(ControlOperationKind.STOP_TARGET_CHANNEL_NEW_WORK,
                channel, channel.digest(), owner, Objects.requireNonNull(reason, "reason"));
    }

    public ControlOperationKind operationKind() { return operation; }
    public TargetChannelIdentity channel() { return channel; }
    public OwnerIdentity owner() { return owner; }
    public byte[] expectedPriorDigest() {
        return expectedPriorDigest == null ? null : Bytes.copy(expectedPriorDigest);
    }
    public ControlReason reason() { return reason; }

    public int controlKind() {
        return switch (operation) {
            case CREATE_TARGET_CHANNEL -> CREATE_CONTROL_KIND;
            case RENEW_TARGET_CHANNEL -> RENEW_CONTROL_KIND;
            case STOP_TARGET_CHANNEL_NEW_WORK -> STOP_CONTROL_KIND;
            default -> throw new IllegalStateException("unknown channel lifecycle operation");
        };
    }

    public ControlOperationRequest operationRequest() {
        return new ControlOperationRequest(operation, this);
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, channel.canonicalBytes());
            if (expectedPriorDigest != null) { CanonicalProtobuf.bytes(out, 3, expectedPriorDigest); }
            CanonicalProtobuf.bytes(out, 4, owner.canonicalBytes());
            if (reason != null) { CanonicalProtobuf.bytes(out, 5, reason.canonicalBytes()); }
        });
    }

    public static TargetChannelControlRequest decode(final ControlOperationKind operation, final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false,
                "Target channel Control request");
        final int[] required = switch (operation) {
            case CREATE_TARGET_CHANNEL -> new int[] {1, 2, 4};
            case RENEW_TARGET_CHANNEL -> new int[] {1, 2, 3, 4};
            case STOP_TARGET_CHANNEL_NEW_WORK -> new int[] {1, 2, 3, 4, 5};
            default -> throw new IllegalArgumentException("unknown channel lifecycle operation");
        };
        QueryCodecSupport.requireNumbers(fields, required, "Target channel Control request");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown channel Control request version");
        }
        final var channel = TargetChannelIdentity.decode(QueryCodecSupport.bytes(fields.get(1), 2));
        final boolean create = operation == ControlOperationKind.CREATE_TARGET_CHANNEL;
        final byte[] ownerBytes = QueryCodecSupport.bytes(fields.get(create ? 2 : 3), 4);
        if (ownerBytes.length > TargetQuotaClaimCharge.MAX_OWNER_BYTES) {
            throw new IllegalArgumentException("channel Control Owner exceeds its bound");
        }
        final var result = new TargetChannelControlRequest(operation, channel,
                create ? null : QueryCodecSupport.fixed(fields.get(2), 3, 32), OwnerIdentity.decode(ownerBytes),
                operation == ControlOperationKind.STOP_TARGET_CHANNEL_NEW_WORK
                        ? ControlReason.decode(QueryCodecSupport.bytes(fields.getLast(), 5)) : null);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target channel Control request");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetChannelControlRequest that && operation == that.operation
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
}
