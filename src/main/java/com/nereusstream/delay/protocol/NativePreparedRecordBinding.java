package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Binds the AUTO_FAST logical record context into the native delivery ID.
 *
 * <p>{@link NativePreparedRecordContext} deliberately stays outside the
 * public native outcome envelope. The native delivery ID is therefore the
 * collision-resistant join between that context and every physical record
 * field already committed by {@link NativePreparedDelivery}. This prevents
 * one prepared envelope from being paired with a different Delay identity or
 * artifact generation at the physical-send boundary.</p>
 */
public final class NativePreparedRecordBinding {
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-native-record-binding\0");

    private NativePreparedRecordBinding() {}

    public static byte[] derive(
            final NativePreparedRecordContext context,
            final ProfileRef destination,
            final ProfileRef capability,
            final PulsarBrokerResourceIdentity target,
            final int physicalPartition,
            final byte[] inlinePayload,
            final PulsarMetadata metadata,
            final Long eventTimeEpochMs,
            final long deliverAtEpochMs,
            final NativeDeliveryPolicy policy,
            final DeliveryContract contract,
            final HandoffPolicySnapshot handoffPolicySnapshot,
            final NativeCapabilitySnapshot capabilitySnapshot) {
        final NativePreparedRecordContext exactContext = Objects.requireNonNull(context, "context");
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, exactContext.canonicalBytes());
            CanonicalProtobuf.bytes(
                    output,
                    2,
                    Objects.requireNonNull(destination, "destination").canonicalBytes());
            CanonicalProtobuf.bytes(
                    output, 3, Objects.requireNonNull(capability, "capability").canonicalBytes());
            CanonicalProtobuf.bytes(
                    output, 4, Objects.requireNonNull(target, "target").canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 5, physicalPartition);
            CanonicalProtobuf.bytes(output, 6, Objects.requireNonNull(inlinePayload, "inlinePayload"));
            CanonicalProtobuf.bytes(
                    output, 7, Objects.requireNonNull(metadata, "metadata").canonicalBytes());
            if (eventTimeEpochMs != null) {
                CanonicalProtobuf.int64(output, 8, eventTimeEpochMs);
            }
            CanonicalProtobuf.int64(output, 9, deliverAtEpochMs);
            CanonicalProtobuf.uint32(
                    output, 10, Objects.requireNonNull(policy, "policy").wireValue());
            CanonicalProtobuf.uint32(
                    output, 11, Objects.requireNonNull(contract, "contract").wireValue());
            CanonicalProtobuf.bytes(
                    output,
                    12,
                    Objects.requireNonNull(handoffPolicySnapshot, "handoffPolicySnapshot")
                            .canonicalBytes());
            CanonicalProtobuf.bytes(
                    output,
                    13,
                    Objects.requireNonNull(capabilitySnapshot, "capabilitySnapshot")
                            .canonicalBytes());
        });
        return Bytes.sha256(DOMAIN, fields);
    }

    public static byte[] derive(final NativePreparedRecordContext context, final NativePreparedDelivery prepared) {
        final NativePreparedDelivery exact = Objects.requireNonNull(prepared, "prepared");
        return derive(
                context,
                exact.destination(),
                exact.capability(),
                exact.target(),
                exact.physicalPartition(),
                exact.inlinePayload(),
                exact.metadata(),
                exact.eventTimeEpochMs(),
                exact.deliverAtEpochMs(),
                exact.nativeDeliveryPolicy(),
                exact.deliveryContract(),
                exact.handoffPolicySnapshot(),
                exact.capabilitySnapshot());
    }

    public static void requireExact(final NativePreparedRecordContext context, final NativePreparedDelivery prepared) {
        if (!Arrays.equals(derive(context, prepared), prepared.nativeDeliveryId())) {
            throw new IllegalArgumentException("AUTO_FAST record context is not bound to the native delivery ID");
        }
    }
}
