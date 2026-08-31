package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PreparedPublishDescriptor;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.StableCode;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Closed local proof that a physical request stopped before library ownership. */
public final class AdapterNonSubmissionEvidence {
    public static final int BEFORE_LIBRARY_OWNERSHIP = 1;
    public static final int CURRENT_ADAPTER_CONFORMANCE_VERSION = 1;
    private static final byte[] REQUEST_HASH_DOMAIN = Bytes.utf8("nereus-delay-destination-publish-request\0");

    private AdapterNonSubmissionEvidence() {}

    /**
     * Creates a verified-not-published branch bound to one typed Admission and
     * its exact immutable destination request.
     */
    public static PublishEvidence beforeLibraryOwnership(
            final PublishAdmissionBody admission,
            final DestinationPublishRequest request,
            final byte[] preparedPublishHash,
            final StableCode code) {
        final PublishAdmissionBody exactAdmission = Objects.requireNonNull(admission, "admission");
        final DestinationPublishRequest exactRequest = Objects.requireNonNull(request, "request");
        final StableCode exactCode = Objects.requireNonNull(code, "code");
        if (exactCode == StableCode.OK) {
            throw new IllegalArgumentException("adapter non-submission requires a non-OK stable code");
        }
        requireRequestBinding(exactAdmission, exactRequest, preparedPublishHash);
        final PreparedPublishDescriptor descriptor = exactAdmission.descriptor().value();
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, descriptor.channel().canonicalBytes());
            CanonicalProtobuf.bytes(
                    output,
                    2,
                    ExternalDeliveryIdentity.publishAttempt(exactRequest.publishAttemptId())
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, preparedPublishHash);
            CanonicalProtobuf.uint32(output, 4, BEFORE_LIBRARY_OWNERSHIP);
            CanonicalProtobuf.bytes(output, 5, requestHash(exactRequest));
            CanonicalProtobuf.uint32(output, 6, CURRENT_ADAPTER_CONFORMANCE_VERSION);
            CanonicalProtobuf.uint32(output, 7, exactCode.wireValue());
        });
        return PublishEvidence.create(
                PublishEvidenceKind.ADAPTER_NON_SUBMISSION, EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED, branch);
    }

    /**
     * Requires a decoded local proof to match the exact Outcome inputs. The
     * generic envelope owner check alone is insufficient because it does not
     * bind the channel, request commitment, prepared hash or stable code.
     */
    public static void requireExactBinding(
            final PublishEvidence evidence,
            final PublishAdmissionBody admission,
            final DestinationPublishRequest request,
            final byte[] preparedPublishHash,
            final StableCode code) {
        final PublishEvidence exactEvidence = Objects.requireNonNull(evidence, "evidence");
        final PublishAdmissionBody exactAdmission = Objects.requireNonNull(admission, "admission");
        final DestinationPublishRequest exactRequest = Objects.requireNonNull(request, "request");
        final StableCode exactCode = Objects.requireNonNull(code, "code");
        requireRequestBinding(exactAdmission, exactRequest, preparedPublishHash);
        if (exactEvidence.evidenceKind() != PublishEvidenceKind.ADAPTER_NON_SUBMISSION
                || exactEvidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED) {
            throw new IllegalArgumentException("evidence is not a verified adapter non-submission proof");
        }
        exactEvidence.requireBusinessMutation(exactRequest.publishAttemptId(), false);
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(exactEvidence.branch(), "AdapterNonSubmissionEvidence");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "AdapterNonSubmissionEvidence");
        final ChannelResourceIdentity channel =
                ChannelResourceIdentity.decode(QueryCodecSupport.nested(fields.get(0), 1));
        final ExternalDeliveryIdentity identity =
                ExternalDeliveryIdentity.decode(QueryCodecSupport.nested(fields.get(1), 2));
        final byte[] branchPreparedHash = QueryCodecSupport.fixed(fields.get(2), 3, 32);
        final long kind = QueryCodecSupport.uint(fields.get(3), 4);
        final byte[] branchRequestHash = QueryCodecSupport.fixed(fields.get(4), 5, 32);
        final long conformanceVersion = QueryCodecSupport.uint(fields.get(5), 6);
        final StableCode branchCode = StableCode.fromWire(QueryCodecSupport.uint32(fields.get(6), 7));
        if (!Arrays.equals(channel.canonicalBytes(), exactAdmission.channel().canonicalBytes())
                || identity.kind() != ExternalDeliveryIdentity.Kind.PUBLISH_ATTEMPT
                || !Arrays.equals(identity.identity(), exactRequest.publishAttemptId())
                || !Arrays.equals(branchPreparedHash, preparedPublishHash)
                || !Arrays.equals(exactAdmission.preparedPublishHash(), preparedPublishHash)
                || kind != BEFORE_LIBRARY_OWNERSHIP
                || !Arrays.equals(branchRequestHash, requestHash(exactRequest))
                || conformanceVersion != CURRENT_ADAPTER_CONFORMANCE_VERSION
                || branchCode != exactCode) {
            throw new IllegalArgumentException("adapter non-submission evidence binding mismatch");
        }
    }

    /** Domain-separated commitment used by AdapterNonSubmissionEvidence field 5. */
    public static byte[] requestHash(final DestinationPublishRequest request) {
        final DestinationPublishRequest exact = Objects.requireNonNull(request, "request");
        return Bytes.sha256(REQUEST_HASH_DOMAIN, CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, exact.laneId().bytes());
            CanonicalProtobuf.bytes(output, 2, exact.laneIncarnation());
            CanonicalProtobuf.bytes(output, 3, exact.delayMessageId().bytes());
            CanonicalProtobuf.uint32Bits(output, 4, exact.generation());
            CanonicalProtobuf.bytes(output, 5, exact.publishAttemptId());
            CanonicalProtobuf.uint64(output, 6, exact.actionAtEpochMs());
            CanonicalProtobuf.uint64(output, 7, exact.deliverAtEpochMs());
            CanonicalProtobuf.bytes(output, 8, Bytes.sha256(exact.payload()));
            CanonicalProtobuf.bytes(output, 9, Bytes.sha256(exact.adapterMetadata()));
        }));
    }

    private static void requireRequestBinding(
            final PublishAdmissionBody admission,
            final DestinationPublishRequest request,
            final byte[] preparedPublishHash) {
        Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
        final PreparedPublishDescriptor descriptor = admission.descriptor().value();
        final PayloadForPublish payload = descriptor.payload();
        final boolean payloadMatches = payload.hasInlinePayload()
                ? Arrays.equals(payload.inlinePayload(), request.payload())
                : payload.length() == request.payload().length
                        && Arrays.equals(payload.payloadSha256(), Bytes.sha256(request.payload()));
        if (!descriptor.destinationLaneId().equals(request.laneId())
                || !Arrays.equals(descriptor.laneIncarnation(), request.laneIncarnation())
                || !descriptor.messageId().equals(request.delayMessageId())
                || descriptor.generation() != Integer.toUnsignedLong(request.generation())
                || !Arrays.equals(descriptor.publishAttemptId(), request.publishAttemptId())
                || descriptor.actionAtEpochMs() != request.actionAtEpochMs()
                || descriptor.deliverAtEpochMs() != request.deliverAtEpochMs()
                || !payloadMatches
                || !Arrays.equals(descriptor.businessMetadata().canonicalBytes(), request.adapterMetadata())
                || !Arrays.equals(admission.publishAttemptId(), request.publishAttemptId())
                || !Arrays.equals(admission.preparedPublishHash(), preparedPublishHash)) {
            throw new IllegalArgumentException("adapter non-submission request does not match its Admission");
        }
    }
}
