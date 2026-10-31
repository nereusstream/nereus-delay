package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PayloadObjectResource;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord;
import com.nereusstream.delay.protocol.TargetUncommittedObjectClosure;

/**
 * The original deployment's payload upload/provider history, not another local ledger or service.
 * Every method operates on finite already authenticated original reservation/Profile/holder/epoch inputs.
 * Admission and CLOSED must be durable and mutually exclusive across processes, retries and old handles.
 * The implementation authenticates original Prepare acceptance/namespace and the actual holder/epoch;
 * today's registration cannot ratify a historical writer protocol or an unknown earlier PUT.
 */
public interface PayloadUploadOperationHistory {
    /** Persist coverage of this exact capability before it can be exposed or used to bypass the service. */
    void admitCapability(
            PayloadReservationReceipt receipt, OpaquePayloadUploadHandle handle, CredentialUseLease credential);

    /**
     * Durably register the actual operation before any provider ownership. Returned handle owns its original
     * admission loan/operation; re-registration after CLOSED is forbidden even if the process map is empty.
     */
    Upload admitUpload(
            PayloadReservationReceipt receipt, OpaquePayloadUploadHandle handle, CredentialUseLease credential);

    interface Upload {
        byte[] operationId();

        OwnerIdentity holder();

        void requireCurrent();
        /** Persist provider ownership/UNKNOWN before dispatch; method entry or local exit is not remote terminal. */
        void beforePut();
        /** Actual successful HTTP/provider response, including immutable version; persist before acknowledging it. */
        void putApplied(byte[] immutableVersion, byte[] etag, String providerRequestId, byte[] publicResponse);
        /** An actual definite conflict/no-create response; no assumption that an earlier operation is terminal. */
        void putNotCreated(int status, String providerRequestId, byte[] publicResponse);
        /** Preserve original durable UNKNOWN and late-response responsibility; do not infer completion from failure. */
        void putUncertain(Throwable failure);
        /** Only this local invocation exited; durable operation/history survives failed close/process loss. */
        void localExited();
    }

    /**
     * Authenticate the actual APPLIED intent; close uploads/commit/old capabilities durably, complete every
     * original remote PUT or prove real server fencing, authenticate coverage since original first admission,
     * and persist the complete immutable terminal inventory BEFORE returning or allowing any DELETE.
     * Missing/late/unknown operations or an oversized complete inventory must throw, never return a partial list.
     */
    TargetUncommittedObjectClosure closeAndFreeze(
            TargetPayloadObjectRetiringRecord intent, long maximumBytes, Runnable originalHeld);

    /**
     * Local recheck of the original qualified hold and exact persisted closed inventory, including old deleted
     * versions.
     */
    void requireClosed(TargetPayloadObjectRetiringRecord intent, TargetUncommittedObjectClosure closure);

    /** Original protocol's finite upper bound for each complete public deletion history, reserved before DELETE. */
    int maximumDeleteHistoryBytes();

    /**
     * Persist this actual exact deletion outcome and return its complete public history. A failure keeps UNKNOWN;
     * the history must recover a lost response and late I/O, not compensate with absence/404 or SDK shutdown.
     * Retaining an actual old response must remain possible after the current admission loan expires; it grants no new
     * I/O.
     */
    TargetUncommittedObjectClosure.Deletion recordDeleted(
            TargetPayloadObjectRetiringRecord intent,
            TargetUncommittedObjectClosure closure,
            PayloadObjectResource version,
            byte[] requestIdHash,
            byte[] responseHash,
            byte[] actualResponse);

    /** Durable final custody before a hot confirmation can escape; CLOSED cannot reopen after Source RELEASED. */
    void retainTerminal(TargetPayloadObjectRetiringRecord intent, TargetUncommittedObjectClosure terminal);
}
