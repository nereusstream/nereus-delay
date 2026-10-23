package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.PayloadObjectResource;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.TargetPayloadReference;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import java.util.Arrays;
import java.util.Objects;

/** Exact provider I/O data. Neither a request nor an observation grants Native retirement authority. */
public final class PayloadObjectDelete {
    private PayloadObjectDelete() {}

    /**
     * Binds the immutable committed object to its already applied Native retirement intent.
     * The caller must authenticate that intent and hold actual read/upload/commit exclusion across I/O.
     * The Source stamp is data to correlate with that authority, not proof that an intent was applied.
     */
    public record Request(TargetQuotaPayloadOwner expectedOwner, CanonicalPayloadCommitProof proof,
            TargetQuotaMutation intentAt) {
        public Request {
            Objects.requireNonNull(expectedOwner);
            Objects.requireNonNull(proof);
            Objects.requireNonNull(intentAt).requireSourceApplied();
            if (expectedOwner.kind() != TargetQuotaPayloadOwner.Kind.OBJECT
                    || expectedOwner.phase() != TargetQuotaPayloadOwner.Phase.RETAINED
                    || expectedOwner.committedPayload() == null) {
                throw new IllegalArgumentException("committed payload deletion requires its exact retained Native owner");
            }
            final var reference = TargetPayloadReference.requireBounded(expectedOwner.committedPayload());
            final var fromProof = TargetPayloadReference.requireBounded(new PayloadReference(
                    proof.objectStoreProfileHash(), proof.container(), proof.objectKey(), proof.immutableObjectVersion(),
                    proof.etag(), proof.length(), proof.payloadSha256(), proof.reservationId(), proof.proofId()));
            if (proof.objectStoreProfile().profileKind() != ProfileKind.OBJECT_STORE
                    || !reference.equals(fromProof) || !expectedOwner.messageId().equals(proof.delayMessageId())
                    || !Arrays.equals(expectedOwner.tenantScope(), proof.tenantRoutingScope())
                    || !expectedOwner.primaryIdentity().shard().equals(intentAt.source().shardId())) {
                throw new IllegalArgumentException("payload deletion changed original Native owner/reservation/commit proof");
            }
            intentAt.requireAfter(expectedOwner.mutation());
        }

        public ProfileRef profile() { return proof.objectStoreProfile(); }
        public PayloadReference reference() { return expectedOwner.committedPayload(); }

        public PayloadObjectResource resource() {
            final var reference = reference();
            return new PayloadObjectResource(profile(), reference.container(), reference.objectKey(),
                    reference.immutableObjectVersion(), reference.etag(), reference.length(), reference.payloadSha256());
        }

        public byte[] digest() {
            return Bytes.sha256(Bytes.utf8("nereus-delay-native-payload-delete-request\0"),
                    Bytes.lp32(resource().exactResourceCanonicalBytes()), Bytes.lp32(expectedOwner.canonicalBytes()),
                    Bytes.lp32(proof.canonicalBytes()),
                    Bytes.lp32(intentAt.canonicalBytes()));
        }

        @Override public boolean equals(Object other) {
            return other instanceof Request that && proof.equals(that.proof) && intentAt.equals(that.intentAt)
                    && Arrays.equals(expectedOwner.canonicalBytes(), that.expectedOwner.canonicalBytes());
        }
        @Override public int hashCode() { return Arrays.hashCode(digest()); }
    }

    /**
     * A hot, non-deserializable witness minted only after this adapter receives an exact S3 DELETE response.
     * It does not certify no resurrection, absence after a lost response, or independent Source approval.
     */
    public static final class Observation {
        private final Request request;
        private final byte[] providerRequestIdHash;
        private final byte[] providerResponseHash;

        private Observation(Request request, String providerRequestId, String secondaryId) {
            this.request = request;
            providerRequestIdHash = Bytes.sha256(Bytes.utf8("nereus-delay-native-payload-delete-provider-request\0"),
                    request.digest(), Bytes.lp32(Bytes.utf8(providerRequestId)));
            providerResponseHash = Bytes.sha256(Bytes.utf8("nereus-delay-native-payload-delete-provider-response\0"),
                    request.digest(), Bytes.u32be(204), Bytes.lp32(request.reference().immutableObjectVersion()),
                    Bytes.lp32(Bytes.utf8(providerRequestId)), Bytes.lp32(Bytes.utf8(secondaryId)));
        }

        public Request request() { return request; }
        public byte[] providerRequestIdHash() { return Bytes.copy(providerRequestIdHash); }
        public byte[] providerResponseHash() { return Bytes.copy(providerResponseHash); }

        public void requireRequest(Request expected) {
            if (!request.equals(Objects.requireNonNull(expected))) {
                throw new IllegalArgumentException("payload deletion observation belongs to another object/intent");
            }
        }
    }

    // Only provider adapters in this package may mint an observation. Local backends have no success fallback.
    static Observation s3Applied(Request request, String requestId, String secondaryId) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalStateException("S3 payload deletion omitted provider request ID");
        }
        return new Observation(Objects.requireNonNull(request), requestId, Objects.requireNonNull(secondaryId));
    }
}
