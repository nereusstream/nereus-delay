package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Registry-shaped Schedule intent used by managed transport. {@link ScheduleIntent} is the compact embedded API
 * representation; it is selected explicitly and is not a numbered project revision.
 */
public final class CanonicalScheduleIntent {
    public static final int QUOTA_ACCOUNTING_VERSION = 1;

    private final ProfileRef profile;
    private final RetryPolicyRef retryPolicy;
    private final long deliverAtEpochMs;
    private final long expireAtEpochMs;
    private final DeliveryMode deliveryMode;
    private final OrderingMode orderingMode;
    private final NativeDeliveryPolicy nativeDeliveryPolicy;
    private final byte[] orderingKey;
    private final byte[] inlinePayload;
    private final CommittedPayloadDescriptor committedPayload;
    private final AdapterMetadata adapterMetadata;
    private final byte[] businessKey;
    private final Long eventTimeEpochMs;
    private final boolean legacyPolicyDefault;

    private CanonicalScheduleIntent(
            final ProfileRef profile,
            final RetryPolicyRef retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final byte[] orderingKey,
            final byte[] inlinePayload,
            final CommittedPayloadDescriptor committedPayload,
            final AdapterMetadata adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs,
            final NativeDeliveryPolicy nativeDeliveryPolicy,
            final boolean payloadRequired,
            final boolean legacyPolicyDefault) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKind.DESTINATION) {
            throw new IllegalArgumentException("Schedule profile must have DESTINATION kind");
        }
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        if (deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid Schedule delivery window");
        }
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.expireAtEpochMs = expireAtEpochMs;
        this.deliveryMode = Objects.requireNonNull(deliveryMode, "deliveryMode");
        if (deliveryMode != DeliveryMode.MANAGED) {
            throw new IllegalArgumentException("unsupported delivery mode");
        }
        this.orderingMode = Objects.requireNonNull(orderingMode, "orderingMode");
        this.nativeDeliveryPolicy = Objects.requireNonNull(nativeDeliveryPolicy, "nativeDeliveryPolicy");
        this.orderingKey = Bytes.copy(Objects.requireNonNull(orderingKey, "orderingKey"));
        if ((inlinePayload == null) == (committedPayload == null)
                && (payloadRequired || inlinePayload != null || committedPayload != null)) {
            throw new IllegalArgumentException("Schedule must select exactly one payload branch");
        }
        this.inlinePayload = inlinePayload == null ? null : Bytes.copy(inlinePayload);
        this.committedPayload = committedPayload;
        this.adapterMetadata = Objects.requireNonNull(adapterMetadata, "adapterMetadata");
        requireNativePolicyCompatibility(this.nativeDeliveryPolicy, this.orderingMode, this.adapterMetadata);
        this.businessKey = businessKey == null ? null : nonEmpty(businessKey, "businessKey");
        if (eventTimeEpochMs != null && eventTimeEpochMs < 0) {
            throw new IllegalArgumentException("eventTime must be non-negative");
        }
        this.eventTimeEpochMs = eventTimeEpochMs;
        this.legacyPolicyDefault = legacyPolicyDefault;
    }

    /** Creates an ordinary Schedule intent with exactly one payload branch. */
    public static CanonicalScheduleIntent create(
            final ProfileRef profile,
            final RetryPolicyRef retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final byte[] orderingKey,
            final byte[] inlinePayload,
            final CommittedPayloadDescriptor committedPayload,
            final AdapterMetadata adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs,
            final NativeDeliveryPolicy nativeDeliveryPolicy) {
        return new CanonicalScheduleIntent(
                profile,
                retryPolicy,
                deliverAtEpochMs,
                expireAtEpochMs,
                deliveryMode,
                orderingMode,
                orderingKey,
                inlinePayload,
                committedPayload,
                adapterMetadata,
                businessKey,
                eventTimeEpochMs,
                nativeDeliveryPolicy,
                true,
                false);
    }

    /** Required-policy overload with the policy adjacent to the ordering contract. */
    public static CanonicalScheduleIntent create(
            final ProfileRef profile,
            final RetryPolicyRef retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final NativeDeliveryPolicy nativeDeliveryPolicy,
            final byte[] orderingKey,
            final byte[] inlinePayload,
            final CommittedPayloadDescriptor committedPayload,
            final AdapterMetadata adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs) {
        return create(
                profile,
                retryPolicy,
                deliverAtEpochMs,
                expireAtEpochMs,
                deliveryMode,
                orderingMode,
                orderingKey,
                inlinePayload,
                committedPayload,
                adapterMetadata,
                businessKey,
                eventTimeEpochMs,
                nativeDeliveryPolicy);
    }

    /** Source migration overload; new writers should pass the explicit policy. */
    public static CanonicalScheduleIntent create(
            final ProfileRef profile,
            final RetryPolicyRef retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final byte[] orderingKey,
            final byte[] inlinePayload,
            final CommittedPayloadDescriptor committedPayload,
            final AdapterMetadata adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs) {
        return new CanonicalScheduleIntent(
                profile,
                retryPolicy,
                deliverAtEpochMs,
                expireAtEpochMs,
                deliveryMode,
                orderingMode,
                orderingKey,
                inlinePayload,
                committedPayload,
                adapterMetadata,
                businessKey,
                eventTimeEpochMs,
                NativeDeliveryPolicy.FORBID,
                true,
                true);
    }

    /** Creates the PrepareLargeSchedule form, which deliberately has no payload branch. */
    public static CanonicalScheduleIntent forPrepare(
            final ProfileRef profile,
            final RetryPolicyRef retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final byte[] orderingKey,
            final AdapterMetadata adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs,
            final NativeDeliveryPolicy nativeDeliveryPolicy) {
        return new CanonicalScheduleIntent(
                profile,
                retryPolicy,
                deliverAtEpochMs,
                expireAtEpochMs,
                deliveryMode,
                orderingMode,
                orderingKey,
                null,
                null,
                adapterMetadata,
                businessKey,
                eventTimeEpochMs,
                nativeDeliveryPolicy,
                false,
                false);
    }

    /** Required-policy overload with the policy adjacent to the ordering contract. */
    public static CanonicalScheduleIntent forPrepare(
            final ProfileRef profile,
            final RetryPolicyRef retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final NativeDeliveryPolicy nativeDeliveryPolicy,
            final byte[] orderingKey,
            final AdapterMetadata adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs) {
        return forPrepare(
                profile,
                retryPolicy,
                deliverAtEpochMs,
                expireAtEpochMs,
                deliveryMode,
                orderingMode,
                orderingKey,
                adapterMetadata,
                businessKey,
                eventTimeEpochMs,
                nativeDeliveryPolicy);
    }

    /** Source migration overload; new writers should pass the explicit policy. */
    public static CanonicalScheduleIntent forPrepare(
            final ProfileRef profile,
            final RetryPolicyRef retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final byte[] orderingKey,
            final AdapterMetadata adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs) {
        return new CanonicalScheduleIntent(
                profile,
                retryPolicy,
                deliverAtEpochMs,
                expireAtEpochMs,
                deliveryMode,
                orderingMode,
                orderingKey,
                null,
                null,
                adapterMetadata,
                businessKey,
                eventTimeEpochMs,
                NativeDeliveryPolicy.FORBID,
                false,
                true);
    }

    public ProfileRef profile() {
        return profile;
    }

    public RetryPolicyRef retryPolicy() {
        return retryPolicy;
    }

    public long deliverAtEpochMs() {
        return deliverAtEpochMs;
    }

    public long expireAtEpochMs() {
        return expireAtEpochMs;
    }

    public DeliveryMode deliveryMode() {
        return deliveryMode;
    }

    public OrderingMode orderingMode() {
        return orderingMode;
    }

    public NativeDeliveryPolicy nativeDeliveryPolicy() {
        return nativeDeliveryPolicy;
    }

    /** True only for the source-compatible overload that omitted field 14. */
    public boolean legacyPolicyDefault() {
        return legacyPolicyDefault;
    }

    public byte[] orderingKey() {
        return Bytes.copy(orderingKey);
    }

    public boolean hasInlinePayload() {
        return inlinePayload != null;
    }

    public byte[] inlinePayload() {
        if (inlinePayload == null) {
            throw new IllegalStateException("Schedule intent has no inline payload");
        }
        return Bytes.copy(inlinePayload);
    }

    public CommittedPayloadDescriptor committedPayload() {
        if (committedPayload == null) {
            throw new IllegalStateException("Schedule intent has no committed payload");
        }
        return committedPayload;
    }

    public AdapterMetadata adapterMetadata() {
        return adapterMetadata;
    }

    public byte[] businessKey() {
        return businessKey == null ? null : Bytes.copy(businessKey);
    }

    public Long eventTimeEpochMs() {
        return eventTimeEpochMs;
    }

    public boolean hasPayloadBranch() {
        return inlinePayload != null || committedPayload != null;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, retryPolicy.canonicalBytes());
            CanonicalProtobuf.int64(output, 3, deliverAtEpochMs);
            CanonicalProtobuf.int64(output, 4, expireAtEpochMs);
            CanonicalProtobuf.uint32(output, 5, deliveryMode.wireValue());
            CanonicalProtobuf.uint32(output, 6, orderingMode == OrderingMode.BEST_EFFORT ? 1 : 2);
            CanonicalProtobuf.bytes(output, 7, orderingKey);
            if (inlinePayload != null) {
                CanonicalProtobuf.bytes(output, 8, inlinePayload);
            } else if (committedPayload != null) {
                CanonicalProtobuf.bytes(output, 9, committedPayload.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 10, adapterMetadata.canonicalBytes());
            if (businessKey != null) {
                CanonicalProtobuf.bytes(output, 11, businessKey);
            }
            if (eventTimeEpochMs != null) {
                CanonicalProtobuf.int64(output, 12, eventTimeEpochMs);
            }
            CanonicalProtobuf.uint32(output, 13, QUOTA_ACCOUNTING_VERSION);
            if (!legacyPolicyDefault) {
                CanonicalProtobuf.uint32(output, 14, nativeDeliveryPolicy.wireValue());
            }
        });
    }

    public static CanonicalScheduleIntent decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "CanonicalScheduleIntent");
        if (fields.size() < 9 || fields.size() > 14) {
            throw new IllegalArgumentException("CanonicalScheduleIntent has invalid field count");
        }
        int index = 0;
        final ProfileRef profile = ProfileRef.decode(QueryCodecSupport.nested(fields.get(index++), 1));
        final RetryPolicyRef retry = RetryPolicyRef.decode(QueryCodecSupport.nested(fields.get(index++), 2));
        final long deliverAt = QueryCodecSupport.uint(fields.get(index++), 3);
        final long expireAt = QueryCodecSupport.uint(fields.get(index++), 4);
        final DeliveryMode delivery = DeliveryMode.fromWire(QueryCodecSupport.uint(fields.get(index++), 5));
        final OrderingMode ordering = OrderingMode.fromWire(QueryCodecSupport.uint(fields.get(index++), 6));
        final byte[] orderingKey = QueryCodecSupport.bytes(fields.get(index++), 7);
        byte[] inline = null;
        CommittedPayloadDescriptor committed = null;
        if (index < fields.size() && fields.get(index).number() == 8) {
            inline = QueryCodecSupport.bytes(fields.get(index++), 8);
        } else if (index < fields.size() && fields.get(index).number() == 9) {
            committed = CommittedPayloadDescriptor.decode(QueryCodecSupport.nested(fields.get(index++), 9));
        }
        final AdapterMetadata metadata = AdapterMetadata.decode(QueryCodecSupport.nested(fields.get(index++), 10));
        byte[] business = null;
        if (index < fields.size() && fields.get(index).number() == 11) {
            business = QueryCodecSupport.bytes(fields.get(index++), 11);
        }
        Long eventTime = null;
        if (index < fields.size() && fields.get(index).number() == 12) {
            eventTime = QueryCodecSupport.uint(fields.get(index++), 12);
        }
        if (index >= fields.size()
                || fields.get(index).number() != 13
                || QueryCodecSupport.uint(fields.get(index), 13) != QUOTA_ACCOUNTING_VERSION) {
            throw new IllegalArgumentException("CanonicalScheduleIntent quota accounting version is invalid");
        }
        index++;
        final boolean legacyPolicyDefault;
        final NativeDeliveryPolicy nativeDeliveryPolicy;
        if (index == fields.size()) {
            // The pre-NDIP shape omitted field 14. It is readable only as a
            // migration value; all new writers use the explicit field.
            legacyPolicyDefault = true;
            nativeDeliveryPolicy = NativeDeliveryPolicy.FORBID;
        } else {
            if (fields.get(index).number() != 14 || ++index != fields.size()) {
                throw new IllegalArgumentException("CanonicalScheduleIntent native delivery policy is invalid");
            }
            legacyPolicyDefault = false;
            nativeDeliveryPolicy = NativeDeliveryPolicy.fromWire(QueryCodecSupport.uint(fields.get(index - 1), 14));
        }
        final CanonicalScheduleIntent result;
        if (inline != null || committed != null) {
            result = legacyPolicyDefault
                    ? create(
                            profile,
                            retry,
                            deliverAt,
                            expireAt,
                            delivery,
                            ordering,
                            orderingKey,
                            inline,
                            committed,
                            metadata,
                            business,
                            eventTime)
                    : create(
                            profile,
                            retry,
                            deliverAt,
                            expireAt,
                            delivery,
                            ordering,
                            orderingKey,
                            inline,
                            committed,
                            metadata,
                            business,
                            eventTime,
                            nativeDeliveryPolicy);
        } else {
            result = legacyPolicyDefault
                    ? forPrepare(
                            profile,
                            retry,
                            deliverAt,
                            expireAt,
                            delivery,
                            ordering,
                            orderingKey,
                            metadata,
                            business,
                            eventTime)
                    : forPrepare(
                            profile,
                            retry,
                            deliverAt,
                            expireAt,
                            delivery,
                            ordering,
                            orderingKey,
                            metadata,
                            business,
                            eventTime,
                            nativeDeliveryPolicy);
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CanonicalScheduleIntent");
        return result;
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static void requireNativePolicyCompatibility(
            final NativeDeliveryPolicy policy, final OrderingMode orderingMode, final AdapterMetadata adapterMetadata) {
        if (policy != NativeDeliveryPolicy.FORBID
                && (adapterMetadata.kind() != AdapterMetadata.Kind.PULSAR
                        || orderingMode != OrderingMode.BEST_EFFORT)) {
            throw new IllegalArgumentException("native delivery policy requires Pulsar BEST_EFFORT Schedule metadata");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CanonicalScheduleIntent that
                && deliverAtEpochMs == that.deliverAtEpochMs
                && expireAtEpochMs == that.expireAtEpochMs
                && deliveryMode == that.deliveryMode
                && orderingMode == that.orderingMode
                && nativeDeliveryPolicy == that.nativeDeliveryPolicy
                && profile.equals(that.profile)
                && retryPolicy.equals(that.retryPolicy)
                && Arrays.equals(orderingKey, that.orderingKey)
                && Arrays.equals(inlinePayload, that.inlinePayload)
                && Objects.equals(committedPayload, that.committedPayload)
                && adapterMetadata.equals(that.adapterMetadata)
                && Arrays.equals(businessKey, that.businessKey)
                && Objects.equals(eventTimeEpochMs, that.eventTimeEpochMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                profile,
                retryPolicy,
                deliverAtEpochMs,
                expireAtEpochMs,
                deliveryMode,
                orderingMode,
                nativeDeliveryPolicy,
                Arrays.hashCode(orderingKey),
                Arrays.hashCode(inlinePayload),
                committedPayload,
                adapterMetadata,
                Arrays.hashCode(businessKey),
                eventTimeEpochMs);
    }
}
