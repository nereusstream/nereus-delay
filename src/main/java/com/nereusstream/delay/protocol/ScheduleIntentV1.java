package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Registry-shaped Schedule intent.  This is the canonical command payload;
 * the older {@link ScheduleIntent} remains a compatibility adapter for the
 * embedded pre-Registry API.
 */
public final class ScheduleIntentV1 {
    public static final int QUOTA_ACCOUNTING_VERSION = 1;

    private final ProfileRefV1 profile;
    private final RetryPolicyRefV1 retryPolicy;
    private final long deliverAtEpochMs;
    private final long expireAtEpochMs;
    private final DeliveryMode deliveryMode;
    private final OrderingMode orderingMode;
    private final byte[] orderingKey;
    private final byte[] inlinePayload;
    private final CommittedPayloadDescriptorV1 committedPayload;
    private final AdapterMetadataV1 adapterMetadata;
    private final byte[] businessKey;
    private final Long eventTimeEpochMs;

    private ScheduleIntentV1(
            final ProfileRefV1 profile,
            final RetryPolicyRefV1 retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final byte[] orderingKey,
            final byte[] inlinePayload,
            final CommittedPayloadDescriptorV1 committedPayload,
            final AdapterMetadataV1 adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs,
            final boolean payloadRequired) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKindV1.DESTINATION) {
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
            throw new IllegalArgumentException("unsupported V1 delivery mode");
        }
        this.orderingMode = Objects.requireNonNull(orderingMode, "orderingMode");
        this.orderingKey = Bytes.copy(Objects.requireNonNull(orderingKey, "orderingKey"));
        if ((inlinePayload == null) == (committedPayload == null)
                && (payloadRequired || inlinePayload != null || committedPayload != null)) {
            throw new IllegalArgumentException("Schedule must select exactly one payload branch");
        }
        this.inlinePayload = inlinePayload == null ? null : Bytes.copy(inlinePayload);
        this.committedPayload = committedPayload;
        this.adapterMetadata = Objects.requireNonNull(adapterMetadata, "adapterMetadata");
        this.businessKey = businessKey == null ? null : nonEmpty(businessKey, "businessKey");
        if (eventTimeEpochMs != null && eventTimeEpochMs < 0) {
            throw new IllegalArgumentException("eventTime must be non-negative");
        }
        this.eventTimeEpochMs = eventTimeEpochMs;
    }

    /** Creates an ordinary Schedule intent with exactly one payload branch. */
    public static ScheduleIntentV1 create(
            final ProfileRefV1 profile,
            final RetryPolicyRefV1 retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final byte[] orderingKey,
            final byte[] inlinePayload,
            final CommittedPayloadDescriptorV1 committedPayload,
            final AdapterMetadataV1 adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs) {
        return new ScheduleIntentV1(
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
                true);
    }

    /** Creates the PrepareLargeSchedule form, which deliberately has no payload branch. */
    public static ScheduleIntentV1 forPrepare(
            final ProfileRefV1 profile,
            final RetryPolicyRefV1 retryPolicy,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final DeliveryMode deliveryMode,
            final OrderingMode orderingMode,
            final byte[] orderingKey,
            final AdapterMetadataV1 adapterMetadata,
            final byte[] businessKey,
            final Long eventTimeEpochMs) {
        return new ScheduleIntentV1(
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
                false);
    }

    public ProfileRefV1 profile() {
        return profile;
    }

    public RetryPolicyRefV1 retryPolicy() {
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

    public CommittedPayloadDescriptorV1 committedPayload() {
        if (committedPayload == null) {
            throw new IllegalStateException("Schedule intent has no committed payload");
        }
        return committedPayload;
    }

    public AdapterMetadataV1 adapterMetadata() {
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
        });
    }

    public static ScheduleIntentV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ScheduleIntentV1");
        if (fields.size() < 9 || fields.size() > 13) {
            throw new IllegalArgumentException("ScheduleIntentV1 has invalid field count");
        }
        int index = 0;
        final ProfileRefV1 profile = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 1));
        final RetryPolicyRefV1 retry = RetryPolicyRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 2));
        final long deliverAt = QueryCodecSupport.uint(fields.get(index++), 3);
        final long expireAt = QueryCodecSupport.uint(fields.get(index++), 4);
        final DeliveryMode delivery = DeliveryMode.fromWire(QueryCodecSupport.uint(fields.get(index++), 5));
        final OrderingMode ordering = OrderingMode.fromWire(QueryCodecSupport.uint(fields.get(index++), 6));
        final byte[] orderingKey = QueryCodecSupport.bytes(fields.get(index++), 7);
        byte[] inline = null;
        CommittedPayloadDescriptorV1 committed = null;
        if (index < fields.size() && fields.get(index).number() == 8) {
            inline = QueryCodecSupport.bytes(fields.get(index++), 8);
        } else if (index < fields.size() && fields.get(index).number() == 9) {
            committed = CommittedPayloadDescriptorV1.decode(QueryCodecSupport.nested(fields.get(index++), 9));
        }
        final AdapterMetadataV1 metadata = AdapterMetadataV1.decode(QueryCodecSupport.nested(fields.get(index++), 10));
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
                || QueryCodecSupport.uint(fields.get(index), 13) != QUOTA_ACCOUNTING_VERSION
                || ++index != fields.size()) {
            throw new IllegalArgumentException("ScheduleIntentV1 quota accounting version is invalid");
        }
        final ScheduleIntentV1 result;
        if (inline != null || committed != null) {
            result = create(
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
                    eventTime);
        } else {
            result = forPrepare(
                    profile,
                    retry,
                    deliverAt,
                    expireAt,
                    delivery,
                    ordering,
                    orderingKey,
                    metadata,
                    business,
                    eventTime);
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ScheduleIntentV1");
        return result;
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
        return other instanceof ScheduleIntentV1 that
                && deliverAtEpochMs == that.deliverAtEpochMs
                && expireAtEpochMs == that.expireAtEpochMs
                && deliveryMode == that.deliveryMode
                && orderingMode == that.orderingMode
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
                Arrays.hashCode(orderingKey),
                Arrays.hashCode(inlinePayload),
                committedPayload,
                adapterMetadata,
                Arrays.hashCode(businessKey),
                eventTimeEpochMs);
    }
}
