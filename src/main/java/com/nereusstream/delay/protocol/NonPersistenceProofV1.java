package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical proof that a Producer-owned request was definitely not persisted. */
public final class NonPersistenceProofV1 {
    public static final int ADAPTER_PROOF_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int ATTEMPT_ID_LENGTH = 16;

    private final NonPersistenceProofKindV1 kind;
    private final byte[] physicalEnqueueAttemptId;
    private final byte[] preparedHash;
    private final BrokerResourceIdentityV1 brokerResource;
    private final byte[] brokerRequestSha256;
    private final byte[] authenticatedResponseSha256;
    private final byte[] proofDigest;

    private NonPersistenceProofV1(
            final NonPersistenceProofKindV1 kind,
            final byte[] physicalEnqueueAttemptId,
            final byte[] preparedHash,
            final BrokerResourceIdentityV1 brokerResource,
            final byte[] brokerRequestSha256,
            final byte[] authenticatedResponseSha256,
            final byte[] proofDigest) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.physicalEnqueueAttemptId =
                optionalFixed(physicalEnqueueAttemptId, ATTEMPT_ID_LENGTH, "physicalEnqueueAttemptId");
        this.preparedHash = fixed(preparedHash, "preparedHash");
        this.brokerResource = brokerResource;
        this.brokerRequestSha256 = optionalFixed(brokerRequestSha256, HASH_LENGTH, "brokerRequestSha256");
        this.authenticatedResponseSha256 =
                optionalFixed(authenticatedResponseSha256, HASH_LENGTH, "authenticatedResponseSha256");
        validatePresence(
                kind,
                this.physicalEnqueueAttemptId,
                brokerResource,
                this.brokerRequestSha256,
                this.authenticatedResponseSha256);
        this.proofDigest = fixed(proofDigest, "proofDigest");
    }

    public static NonPersistenceProofV1 create(
            final NonPersistenceProofKindV1 kind,
            final byte[] physicalEnqueueAttemptId,
            final byte[] preparedHash,
            final BrokerResourceIdentityV1 brokerResource,
            final byte[] brokerRequestSha256,
            final byte[] authenticatedResponseSha256) {
        final byte[] fields = canonicalFields(
                kind,
                physicalEnqueueAttemptId,
                preparedHash,
                brokerResource,
                brokerRequestSha256,
                authenticatedResponseSha256);
        return new NonPersistenceProofV1(
                kind,
                physicalEnqueueAttemptId,
                preparedHash,
                brokerResource,
                brokerRequestSha256,
                authenticatedResponseSha256,
                Bytes.sha256(fields));
    }

    public static NonPersistenceProofV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "NonPersistenceProofV1");
        if (fields.size() < 4 || fields.size() > 8) {
            throw new IllegalArgumentException("NonPersistenceProofV1 fields are incomplete or unknown");
        }
        final NonPersistenceProofKindV1 kind =
                NonPersistenceProofKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        if (QueryCodecSupport.uint(fields.get(1), 2) != ADAPTER_PROOF_VERSION) {
            throw new IllegalArgumentException("unsupported adapter proof version");
        }
        int index = 2;
        byte[] attempt = null;
        if (fields.get(index).number() == 3) {
            attempt = QueryCodecSupport.fixed(fields.get(index), 3, ATTEMPT_ID_LENGTH);
            requireNonZero(attempt, "physicalEnqueueAttemptId");
            index++;
        }
        if (fields.get(index).number() != 4) {
            throw new IllegalArgumentException("NonPersistenceProof prepared hash is missing");
        }
        final byte[] preparedHash = QueryCodecSupport.fixed(fields.get(index), 4, HASH_LENGTH);
        index++;
        BrokerResourceIdentityV1 resource = null;
        if (index < fields.size() && fields.get(index).number() == 5) {
            resource = BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(index), 5));
            index++;
        }
        byte[] requestHash = null;
        if (index < fields.size() && fields.get(index).number() == 6) {
            requestHash = QueryCodecSupport.fixed(fields.get(index), 6, HASH_LENGTH);
            index++;
        }
        byte[] responseHash = null;
        if (index < fields.size() && fields.get(index).number() == 7) {
            responseHash = QueryCodecSupport.fixed(fields.get(index), 7, HASH_LENGTH);
            index++;
        }
        if (index >= fields.size() || fields.get(index).number() != 8 || index != fields.size() - 1) {
            throw new IllegalArgumentException("NonPersistenceProof digest is missing or out of order");
        }
        final byte[] digest = QueryCodecSupport.fixed(fields.get(index), 8, HASH_LENGTH);
        final NonPersistenceProofV1 result =
                new NonPersistenceProofV1(kind, attempt, preparedHash, resource, requestHash, responseHash, digest);
        if (!Bytes.constantTimeEquals(
                digest,
                Bytes.sha256(canonicalFields(kind, attempt, preparedHash, resource, requestHash, responseHash)))) {
            throw new IllegalArgumentException("NonPersistenceProof digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NonPersistenceProofV1");
        return result;
    }

    public NonPersistenceProofKindV1 kind() {
        return kind;
    }

    public byte[] physicalEnqueueAttemptId() {
        return physicalEnqueueAttemptId == null ? null : Bytes.copy(physicalEnqueueAttemptId);
    }

    public byte[] preparedHash() {
        return Bytes.copy(preparedHash);
    }

    public BrokerResourceIdentityV1 brokerResource() {
        return brokerResource;
    }

    public byte[] brokerRequestSha256() {
        return brokerRequestSha256 == null ? null : Bytes.copy(brokerRequestSha256);
    }

    public byte[] authenticatedResponseSha256() {
        return authenticatedResponseSha256 == null ? null : Bytes.copy(authenticatedResponseSha256);
    }

    public byte[] proofDigest() {
        return Bytes.copy(proofDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.uint32(output, 2, ADAPTER_PROOF_VERSION);
            if (physicalEnqueueAttemptId != null) {
                CanonicalProtobuf.bytes(output, 3, physicalEnqueueAttemptId);
            }
            CanonicalProtobuf.bytes(output, 4, preparedHash);
            if (brokerResource != null) {
                CanonicalProtobuf.bytes(output, 5, brokerResource.canonicalBytes());
            }
            if (brokerRequestSha256 != null) {
                CanonicalProtobuf.bytes(output, 6, brokerRequestSha256);
            }
            if (authenticatedResponseSha256 != null) {
                CanonicalProtobuf.bytes(output, 7, authenticatedResponseSha256);
            }
            CanonicalProtobuf.bytes(output, 8, proofDigest);
        });
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof NonPersistenceProofV1 that)) {
            return false;
        }
        return kind == that.kind
                && Objects.equals(brokerResource, that.brokerResource)
                && Arrays.equals(physicalEnqueueAttemptId, that.physicalEnqueueAttemptId)
                && Arrays.equals(preparedHash, that.preparedHash)
                && Arrays.equals(brokerRequestSha256, that.brokerRequestSha256)
                && Arrays.equals(authenticatedResponseSha256, that.authenticatedResponseSha256)
                && Arrays.equals(proofDigest, that.proofDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                kind,
                Arrays.hashCode(physicalEnqueueAttemptId),
                Arrays.hashCode(preparedHash),
                brokerResource,
                Arrays.hashCode(brokerRequestSha256),
                Arrays.hashCode(authenticatedResponseSha256),
                Arrays.hashCode(proofDigest));
    }

    private static byte[] canonicalFields(
            final NonPersistenceProofKindV1 kind,
            final byte[] attempt,
            final byte[] preparedHash,
            final BrokerResourceIdentityV1 resource,
            final byte[] requestHash,
            final byte[] responseHash) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.uint32(output, 2, ADAPTER_PROOF_VERSION);
            if (attempt != null) {
                CanonicalProtobuf.bytes(output, 3, attempt);
            }
            CanonicalProtobuf.bytes(output, 4, preparedHash);
            if (resource != null) {
                CanonicalProtobuf.bytes(output, 5, resource.canonicalBytes());
            }
            if (requestHash != null) {
                CanonicalProtobuf.bytes(output, 6, requestHash);
            }
            if (responseHash != null) {
                CanonicalProtobuf.bytes(output, 7, responseHash);
            }
        });
    }

    private static void validatePresence(
            final NonPersistenceProofKindV1 kind,
            final byte[] attempt,
            final BrokerResourceIdentityV1 resource,
            final byte[] requestHash,
            final byte[] responseHash) {
        final boolean brokerEvidence = resource != null || requestHash != null || responseHash != null;
        switch (kind) {
            case LOCAL_BEFORE_PRODUCER_OWNERSHIP, LIBRARY_CERTIFIED_PRE_OWNERSHIP_CANCEL -> {
                if (kind == NonPersistenceProofKindV1.LIBRARY_CERTIFIED_PRE_OWNERSHIP_CANCEL && attempt == null) {
                    throw new IllegalArgumentException("certified pre-ownership cancel requires physical attempt ID");
                }
                if (brokerEvidence) {
                    throw new IllegalArgumentException("pre-ownership proof cannot carry Broker evidence");
                }
            }
            case KAFKA_DEFINITIVE_REJECTION -> {
                requireBrokerEvidence(
                        attempt, resource, requestHash, responseHash, BrokerResourceIdentityV1.Kind.KAFKA);
            }
            case PULSAR_GUARD_REJECTION -> {
                requireBrokerEvidence(
                        attempt, resource, requestHash, responseHash, BrokerResourceIdentityV1.Kind.PULSAR);
            }
        }
    }

    private static void requireBrokerEvidence(
            final byte[] attempt,
            final BrokerResourceIdentityV1 resource,
            final byte[] requestHash,
            final byte[] responseHash,
            final BrokerResourceIdentityV1.Kind expectedKind) {
        if (attempt == null
                || resource == null
                || resource.kind() != expectedKind
                || requestHash == null
                || responseHash == null) {
            throw new IllegalArgumentException("Broker rejection proof requires matching complete evidence");
        }
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] optionalFixed(final byte[] value, final int length, final String name) {
        if (value == null) {
            return null;
        }
        Bytes.requireLength(value, length, name);
        requireNonZero(value, name);
        return Bytes.copy(value);
    }

    private static void requireNonZero(final byte[] value, final String name) {
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
