package com.nereusstream.delay.protocol;

/** Closed client action projection for a stable error. */
public enum Retryability {
    NEVER(1),
    RETRY_EXACT_BYTES(2),
    RETRY_EXACT_BYTES_AFTER_RETRY_AT(3),
    NEW_PREPARATION_REQUIRED(4),
    RETRY_EXACT_BYTES_AFTER_EXTERNAL_CHANGE(5),
    REREAD_AFTER_REPAIR(6);

    private final int wireValue;

    Retryability(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static Retryability fromWire(final long value) {
        for (Retryability retryability : values()) {
            if (retryability.wireValue == value) {
                return retryability;
            }
        }
        throw new IllegalArgumentException("unknown Retryability: " + value);
    }

    public static Retryability forCode(final StableCode code) {
        return switch (code) {
            case CLIENT_CLOSED,
                    SDK_BACKPRESSURE_NOT_SUBMITTED,
                    BROKER_DEFINITIVE_NOT_PERSISTED,
                    ENQUEUE_RESULT_UNCERTAIN,
                    NATIVE_ENQUEUE_RESULT_UNCERTAIN,
                    PRODUCER_OWNERSHIP_UNKNOWN -> RETRY_EXACT_BYTES;
            case SHARD_TRANSITIONING, OBJECT_NOT_READY_RETRYABLE, OBJECT_STORE_UNAVAILABLE_RETRYABLE ->
                RETRY_EXACT_BYTES_AFTER_RETRY_AT;
            case INVALID_COMMAND,
                    INVALID_PREPARED_COMMAND,
                    PAYLOAD_TOO_LARGE,
                    INVALID_METADATA,
                    UNSUPPORTED_DELIVERY_MODE,
                    ROUTE_SNAPSHOT_UNAVAILABLE,
                    PREPARED_COMMAND_EXPIRED,
                    AUTO_FAST_PREREQUISITE_UNAVAILABLE,
                    NATIVE_PREPARED_SUBMISSION_EXPIRED,
                    PREPARED_SUBMISSION_MISMATCH,
                    NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED,
                    NOT_FOUND,
                    VERSION_CONFLICT,
                    RESERVATION_NOT_COMMITTED,
                    INVALID_DELIVERY_WINDOW,
                    UNAUTHORIZED,
                    DESTINATION_NOT_ALLOWED,
                    HARD_QUOTA_EXCEEDED,
                    ROUTE_NOT_ACTIVE,
                    COMMAND_ID_CONFLICT,
                    COMMAND_RETRY_WINDOW_EXPIRED,
                    DELAY_MESSAGE_ID_CONFLICT,
                    DELAY_MESSAGE_ID_EXPIRED,
                    INGRESS_ROUTE_MISMATCH,
                    PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION,
                    PROFILE_DEPRECATED_FOR_NEW_USE,
                    LANE_CLOSED,
                    LANE_TERMINALLY_CLOSED,
                    ORDERING_DOMAIN_BROKEN,
                    DESTINATION_LANE_LIMIT_EXCEEDED,
                    ORDERING_CAPABILITY_UNAVAILABLE,
                    SERVER_INVALID_COMMAND,
                    SERVER_PAYLOAD_TOO_LARGE,
                    SERVER_INVALID_METADATA,
                    SERVER_UNSUPPORTED_DELIVERY_MODE,
                    RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION,
                    UNACTIVATED_PROTOCOL_VERSION,
                    UNACTIVATED_SYSTEM_PROTOCOL_VERSION,
                    UNAUTHORIZED_SYSTEM_MUTATION,
                    SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                    STALE_SYSTEM_MUTATION,
                    ADMISSION_CAPACITY_GATED,
                    QUARANTINED_SOURCE_RECORD,
                    PAYLOAD_OBJECT_CONFLICT,
                    PAYLOAD_COMMIT_CONFLICT,
                    PAYLOAD_PROOF_INVALID,
                    PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION,
                    RESERVATION_EXPIRED,
                    RESERVATION_ABANDONED,
                    PAYLOAD_RESERVATION_CLOSED,
                    OBJECT_IDENTITY_CONFLICT,
                    PAYLOAD_RESERVATION_ABANDONED,
                    RESOURCE_INCARNATION_MISMATCH,
                    SOURCE_INCARNATION_MISMATCH,
                    REPLAY_WINDOW_EXPIRED,
                    CREDENTIAL_EQUIVALENCE_NOT_PROVEN -> NEW_PREPARATION_REQUIRED;
            case BROKER_RESOURCE_UNCERTIFIED,
                    CAPABILITY_UNAVAILABLE,
                    FENCE_STALLED_CAPACITY,
                    CREDENTIAL_BINDING_DRIFT -> RETRY_EXACT_BYTES_AFTER_EXTERNAL_CHANGE;
            case SHARD_UNAVAILABLE,
                    INTEGRITY_ERROR,
                    PULSAR_EVIDENCE_DIVERGENCE,
                    SOURCE_GAP,
                    UNSUPPORTED_ACTIVATED_PROTOCOL -> REREAD_AFTER_REPAIR;
            default -> NEVER;
        };
    }
}
