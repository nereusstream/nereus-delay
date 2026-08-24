package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 branch for resolving one uncertain publish attempt. */
public final class ResolveUncertainRequestV1 implements ControlOperationRequestBranchV1 {
    private final UncertainResolutionKindV1 resolutionKind;
    private final PublishEvidenceV1 evidence;
    private final boolean allowPossibleDuplicate;
    private final boolean allowPossibleDeliveryTerminal;
    private final AcknowledgementSetV1 acknowledgements;

    public ResolveUncertainRequestV1(
            final UncertainResolutionKindV1 resolutionKind,
            final PublishEvidenceV1 evidence,
            final boolean allowPossibleDuplicate,
            final boolean allowPossibleDeliveryTerminal,
            final AcknowledgementSetV1 acknowledgements) {
        this.resolutionKind = Objects.requireNonNull(resolutionKind, "resolutionKind");
        this.evidence = evidence;
        this.allowPossibleDuplicate = allowPossibleDuplicate;
        this.allowPossibleDeliveryTerminal = allowPossibleDeliveryTerminal;
        this.acknowledgements = Objects.requireNonNull(acknowledgements, "acknowledgements");
        validateMatrix();
    }

    public UncertainResolutionKindV1 resolutionKind() {
        return resolutionKind;
    }

    public PublishEvidenceV1 evidence() {
        return evidence;
    }

    public boolean allowPossibleDuplicate() {
        return allowPossibleDuplicate;
    }

    public boolean allowPossibleDeliveryTerminal() {
        return allowPossibleDeliveryTerminal;
    }

    public AcknowledgementSetV1 acknowledgements() {
        return acknowledgements;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, resolutionKind.wireValue());
            if (evidence != null) {
                CanonicalProtobuf.bytes(output, 2, evidence.canonicalBytes());
            }
            CanonicalProtobuf.uint32(output, 3, allowPossibleDuplicate ? 1 : 0);
            CanonicalProtobuf.uint32(output, 4, allowPossibleDeliveryTerminal ? 1 : 0);
            CanonicalProtobuf.bytes(output, 5, acknowledgements.canonicalBytes());
        });
    }

    public static ResolveUncertainRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ResolveUncertainRequestV1");
        if (fields.size() != 4 && fields.size() != 5) {
            throw new IllegalArgumentException("ResolveUncertainRequestV1 has an unexpected field count");
        }
        if (fields.get(0).number() != 1
                || (fields.size() == 5
                        ? fields.get(1).number() != 2
                        : fields.get(1).number() != 3)
                || (fields.size() == 5
                        ? fields.get(2).number() != 3
                        : fields.get(2).number() != 4)
                || (fields.size() == 5
                        ? fields.get(3).number() != 4
                        : fields.get(3).number() != 5)
                || (fields.size() == 5 ? fields.get(4).number() != 5 : false)) {
            throw new IllegalArgumentException("ResolveUncertainRequestV1 has an unexpected field order");
        }
        final int offset = fields.size() == 5 ? 1 : 0;
        final PublishEvidenceV1 evidence =
                offset == 1 ? PublishEvidenceV1.decode(QueryCodecSupport.nested(fields.get(1), 2)) : null;
        final ResolveUncertainRequestV1 result = new ResolveUncertainRequestV1(
                UncertainResolutionKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                evidence,
                QueryCodecSupport.bool(fields.get(1 + offset), 3),
                QueryCodecSupport.bool(fields.get(2 + offset), 4),
                AcknowledgementSetV1.decode(QueryCodecSupport.nested(fields.get(3 + offset), 5)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ResolveUncertainRequestV1");
        return result;
    }

    private void validateMatrix() {
        switch (resolutionKind) {
            case ATTACH_PUBLISHED_EVIDENCE -> {
                requireEvidence(EvidenceVerificationStatusV1.VERIFIED_PUBLISHED);
                requireFalse(allowPossibleDuplicate, "allowPossibleDuplicate");
                requireFalse(allowPossibleDeliveryTerminal, "allowPossibleDeliveryTerminal");
            }
            case ATTACH_NOT_PUBLISHED_EVIDENCE -> {
                requireEvidence(EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED);
                requireFalse(allowPossibleDuplicate, "allowPossibleDuplicate");
                requireFalse(allowPossibleDeliveryTerminal, "allowPossibleDeliveryTerminal");
            }
            case RETRY_ALLOW_POSSIBLE_DUPLICATE -> {
                if (evidence != null) {
                    throw new IllegalArgumentException("retry resolution forbids evidence");
                }
                if (!allowPossibleDuplicate
                        || allowPossibleDeliveryTerminal
                        || !acknowledgements.has(AcknowledgementKindV1.POSSIBLE_DUPLICATE)) {
                    throw new IllegalArgumentException("possible duplicate retry acknowledgement matrix is invalid");
                }
            }
            case TERMINALIZE_POSSIBLE_DELIVERY -> {
                if (evidence != null) {
                    throw new IllegalArgumentException("terminalization forbids evidence");
                }
                if (allowPossibleDuplicate
                        || !allowPossibleDeliveryTerminal
                        || !acknowledgements.has(AcknowledgementKindV1.POSSIBLE_DELIVERY)) {
                    throw new IllegalArgumentException("possible delivery terminalization matrix is invalid");
                }
            }
        }
    }

    private void requireEvidence(final EvidenceVerificationStatusV1 expected) {
        if (evidence == null || evidence.verificationStatus() != expected) {
            throw new IllegalArgumentException("evidence verification status does not match resolution kind");
        }
    }

    private static void requireFalse(final boolean value, final String name) {
        if (value) {
            throw new IllegalArgumentException(name + " must be false for evidence attachment");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ResolveUncertainRequestV1 that
                && resolutionKind == that.resolutionKind
                && Objects.equals(evidence, that.evidence)
                && allowPossibleDuplicate == that.allowPossibleDuplicate
                && allowPossibleDeliveryTerminal == that.allowPossibleDeliveryTerminal
                && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                resolutionKind, evidence, allowPossibleDuplicate, allowPossibleDeliveryTerminal, acknowledgements);
    }
}
