package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Final deterministic Pulsar record projection created after Journal mapping. */
public final class PulsarPreparedRecord {
    public static final int SCHEMA_GENERATION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int FINAL_RESERVED_PROPERTY_COUNT = 9;
    private static final String HASH_DOMAIN = "nereus-delay-pulsar-prepared-record\0";

    private final PulsarRecordTemplate template;
    private final byte[] recordTemplateHash;
    private final ResolvedPayload resolvedPayload;
    private final PulsarSequenceAuthority sequenceAuthority;
    private final ExternalDeliveryIdentity externalIdentity;
    private final byte[] preparedIdentityHash;
    private final List<PulsarMetadata.Property> finalReservedProperties;
    private final byte[] artifactGenerationSetDigest;

    public PulsarPreparedRecord(
            final PulsarRecordTemplate template,
            final byte[] recordTemplateHash,
            final ResolvedPayload resolvedPayload,
            final PulsarSequenceAuthority sequenceAuthority,
            final ExternalDeliveryIdentity externalIdentity,
            final byte[] preparedIdentityHash,
            final List<PulsarMetadata.Property> finalReservedProperties,
            final byte[] artifactGenerationSetDigest) {
        this.template = Objects.requireNonNull(template, "template");
        this.recordTemplateHash = fixed(recordTemplateHash, "recordTemplateHash");
        if (!Arrays.equals(this.recordTemplateHash, template.recordTemplateHash())) {
            throw new IllegalArgumentException("Pulsar record template hash mismatch");
        }
        this.resolvedPayload = Objects.requireNonNull(resolvedPayload, "resolvedPayload");
        if (resolvedPayload.length() != template.payload().length()
                || !Arrays.equals(resolvedPayload.sha256(), template.payload().payloadSha256())) {
            throw new IllegalArgumentException("resolved payload does not match template commitment");
        }
        this.sequenceAuthority = Objects.requireNonNull(sequenceAuthority, "sequenceAuthority");
        this.externalIdentity = Objects.requireNonNull(externalIdentity, "externalIdentity");
        if (template.deliveryContract().isNative()) {
            if (sequenceAuthority.kind() != PulsarSequenceAuthority.Kind.PRODUCER_ASSIGNED
                    || externalIdentity.kind() != ExternalDeliveryIdentity.Kind.NATIVE_DELIVERY) {
                throw new IllegalArgumentException("native record requires producer-assigned/native identity branches");
            }
        } else if (sequenceAuthority.kind() != PulsarSequenceAuthority.Kind.MANAGED_JOURNAL
                || externalIdentity.kind() != ExternalDeliveryIdentity.Kind.PUBLISH_ATTEMPT) {
            throw new IllegalArgumentException("managed record requires journal/publish-attempt branches");
        }
        this.preparedIdentityHash = fixed(preparedIdentityHash, "preparedIdentityHash");
        this.finalReservedProperties = finalProperties(finalReservedProperties);
        final List<PulsarMetadata.Property> expectedProperties = PulsarReservedProperties.all(
                template.reservedMetadata(), template.reservedMetadata().publishAttemptId(), preparedIdentityHash);
        if (!this.finalReservedProperties.equals(expectedProperties)) {
            throw new IllegalArgumentException("final reserved properties do not match the closed registry");
        }
        this.artifactGenerationSetDigest = fixed(artifactGenerationSetDigest, "artifactGenerationSetDigest");
        if (!Arrays.equals(this.artifactGenerationSetDigest, template.artifactGenerationSetDigest())) {
            throw new IllegalArgumentException("prepared record artifact generation digest mismatch");
        }
    }

    public PulsarRecordTemplate template() {
        return template;
    }

    public byte[] recordTemplateHash() {
        return Bytes.copy(recordTemplateHash);
    }

    public ResolvedPayload resolvedPayload() {
        return resolvedPayload;
    }

    public PulsarSequenceAuthority sequenceAuthority() {
        return sequenceAuthority;
    }

    public ExternalDeliveryIdentity externalIdentity() {
        return externalIdentity;
    }

    public byte[] preparedIdentityHash() {
        return Bytes.copy(preparedIdentityHash);
    }

    public List<PulsarMetadata.Property> finalReservedProperties() {
        return finalReservedProperties;
    }

    public byte[] artifactGenerationSetDigest() {
        return Bytes.copy(artifactGenerationSetDigest);
    }

    /**
     * Deterministic bounded-admission charge for the final record. The
     * canonical envelope is deliberately charged rather than only the
     * business payload: it covers the exact metadata, identity and artifact
     * commitments that remain live until physical completion is observed.
     */
    public long physicalByteCharge() {
        return canonicalBytes().length;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
            CanonicalProtobuf.bytes(output, 2, template.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, recordTemplateHash);
            CanonicalProtobuf.bytes(output, 4, resolvedPayload.canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, sequenceAuthority.canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, externalIdentity.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, preparedIdentityHash);
            for (PulsarMetadata.Property property : finalReservedProperties) {
                CanonicalProtobuf.bytes(output, 8, property.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 9, artifactGenerationSetDigest);
        });
    }

    public byte[] preparedRecordHash() {
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), canonicalBytes());
    }

    public static PulsarPreparedRecord decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PulsarPreparedRecord", true);
        if (fields.size() != 17) {
            throw new IllegalArgumentException("PulsarPreparedRecord must contain nine reserved properties");
        }
        int index = 0;
        if (QueryCodecSupport.uint(fields.get(index), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported PulsarPreparedRecord generation");
        }
        index++;
        final PulsarRecordTemplate template =
                PulsarRecordTemplate.decode(QueryCodecSupport.nested(fields.get(index++), 2));
        final byte[] templateHash = QueryCodecSupport.fixed(fields.get(index++), 3, HASH_LENGTH);
        final ResolvedPayload payload = ResolvedPayload.decode(QueryCodecSupport.nested(fields.get(index++), 4));
        final PulsarSequenceAuthority authority =
                PulsarSequenceAuthority.decode(QueryCodecSupport.nested(fields.get(index++), 5));
        final ExternalDeliveryIdentity identity =
                ExternalDeliveryIdentity.decode(QueryCodecSupport.nested(fields.get(index++), 6));
        final byte[] preparedIdentityHash = QueryCodecSupport.fixed(fields.get(index++), 7, HASH_LENGTH);
        final List<PulsarMetadata.Property> reserved = new ArrayList<>();
        while (index < fields.size() - 1 && fields.get(index).number() == 8) {
            reserved.add(PulsarMetadata.Property.decodeReserved(QueryCodecSupport.nested(fields.get(index++), 8)));
        }
        if (reserved.size() != FINAL_RESERVED_PROPERTY_COUNT) {
            throw new IllegalArgumentException("PulsarPreparedRecord reserved property count is not nine");
        }
        final byte[] artifactDigest = QueryCodecSupport.fixed(fields.get(index++), 9, HASH_LENGTH);
        if (index != fields.size()) {
            throw new IllegalArgumentException("PulsarPreparedRecord field order mismatch");
        }
        final PulsarPreparedRecord result = new PulsarPreparedRecord(
                template, templateHash, payload, authority, identity, preparedIdentityHash, reserved, artifactDigest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarPreparedRecord");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarPreparedRecord that
                && template.equals(that.template)
                && Arrays.equals(recordTemplateHash, that.recordTemplateHash)
                && resolvedPayload.equals(that.resolvedPayload)
                && sequenceAuthority.equals(that.sequenceAuthority)
                && externalIdentity.equals(that.externalIdentity)
                && Arrays.equals(preparedIdentityHash, that.preparedIdentityHash)
                && finalReservedProperties.equals(that.finalReservedProperties)
                && Arrays.equals(artifactGenerationSetDigest, that.artifactGenerationSetDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                template,
                Arrays.hashCode(recordTemplateHash),
                resolvedPayload,
                sequenceAuthority,
                externalIdentity,
                Arrays.hashCode(preparedIdentityHash),
                finalReservedProperties,
                Arrays.hashCode(artifactGenerationSetDigest));
    }

    private static List<PulsarMetadata.Property> finalProperties(final List<PulsarMetadata.Property> values) {
        Objects.requireNonNull(values, "finalReservedProperties");
        if (values.size() != FINAL_RESERVED_PROPERTY_COUNT) {
            throw new IllegalArgumentException("exactly nine reserved Pulsar properties are required");
        }
        final List<PulsarMetadata.Property> copy = new ArrayList<>(values);
        for (PulsarMetadata.Property property : copy) {
            Objects.requireNonNull(property, "reserved property");
        }
        String previous = null;
        for (PulsarMetadata.Property property : copy) {
            if (!property.key().startsWith(PulsarReservedProperties.PREFIX)) {
                throw new IllegalArgumentException("final properties must use the reserved Nereus prefix");
            }
            if (previous != null && previous.equals(property.key())) {
                throw new IllegalArgumentException("final reserved properties must be unique");
            }
            previous = property.key();
        }
        return Collections.unmodifiableList(copy);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
