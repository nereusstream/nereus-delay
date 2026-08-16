package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.ResourceRetireIntentBody;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.ResourceRetireIntentRecord;

import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/**
 * Composes one exact checkpoint-provider result into a signed
 * {@code RESOURCE_DELETE_CONFIRMED} source-log mutation.
 *
 * <p>This class only composes authenticated evidence. It does not authorize
 * the retire intent, advance a Recovery Floor, release a Pin, fence a GC
 * owner, append to the Shard Log, or apply the resulting mutation.</p>
 */
public final class CheckpointDeleteConfirmationComposer {
    private static final byte[] RESOURCE_IDENTITY_DOMAIN =
            Bytes.utf8("nereus-delay-resource-identity-v1\0");

    private CheckpointDeleteConfirmationComposer() {
    }

    /**
     * Creates a signed confirmation for a provider result bound to one exact,
     * already-applied checkpoint retire intent.
     */
    public static SystemMutation compose(final ResourceRetireIntentRecord retireIntent,
                                         final CheckpointDeleteResult deleteResult,
                                         final TrustedUtcIntervalEvidence observedAt,
                                         final TrustedUtcIntervalEvidence confirmedAt,
                                         final long retryUntilEpochMs,
                                         final byte[] authorIdentity,
                                         final int signingKeyVersion,
                                         final PrivateKey privateKey) {
        Objects.requireNonNull(retireIntent, "retireIntent");
        Objects.requireNonNull(deleteResult, "deleteResult");
        Objects.requireNonNull(observedAt, "observedAt");
        Objects.requireNonNull(confirmedAt, "confirmedAt");
        if (retireIntent.resourceKind() != ResourceKind.CHECKPOINT) {
            throw new IllegalArgumentException("checkpoint confirmation requires a CHECKPOINT retire intent");
        }

        final CheckpointResourceV1 resource = deleteResult.resource();
        final byte[] providerIdentity = resource.exactResourceCanonicalBytes();
        if (!Arrays.equals(retireIntent.resourceIdentity(), providerIdentity)) {
            throw new IllegalArgumentException("provider checkpoint identity does not match retire intent");
        }
        final byte[] expectedIdentityHash = Bytes.sha256(RESOURCE_IDENTITY_DOMAIN, providerIdentity);
        if (!Bytes.constantTimeEquals(retireIntent.resourceIdentityHash(), expectedIdentityHash)) {
            throw new IllegalArgumentException("retire intent checkpoint identity hash is not canonical");
        }

        // The confirmation interval must prove that the confirmation happened
        // after the whole provider-observation interval, not merely after its
        // lower bound.
        confirmedAt.requireEarliestAtLeast(observedAt.latestEpochMs());
        final ResourceDeleteConfirmedBody.ExternalDeleteEvidence evidence = deleteResult.externalEvidence(
                retireIntent.resourceIdentityHash(), observedAt);
        ResourceRetireIntentBody.validateExternalDeleteIdentity(ResourceKind.CHECKPOINT,
                retireIntent.resourceIdentity(), evidence.observedImmutableVersion(), evidence.observedEtag(),
                evidence.outcome());

        final ShardId shard = SourcePositionCodec.decode(retireIntent.appliedSourcePosition()).shardId();
        final ResourceDeleteConfirmedBody.RetireIntentRef intent = new ResourceDeleteConfirmedBody.RetireIntentRef(
                retireIntent.mutationId(), retireIntent.mutationHash(), retireIntent.resourceIdentityHash(),
                retireIntent.expectedResourceStateVersion());
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shard).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOURCE_DELETE_CONFIRMED.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, intent.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, evidence.outcome().wireValue());
            CanonicalProtobuf.bytes(output, 12, evidence.canonicalBytes());
            CanonicalProtobuf.bytes(output, 13, confirmedAt.canonicalBytes());
        });

        // Parse the composed body before signing so this boundary also checks
        // the nested canonical ordering and intent/evidence identity coupling.
        ResourceDeleteConfirmedBody.decode(body);
        final byte[] canonicalAuthor = AuthorIdentity.decode(
                Objects.requireNonNull(authorIdentity, "authorIdentity")).canonicalBytes();
        return SystemMutation.signed(shard, SystemMutationType.RESOURCE_DELETE_CONFIRMED, retryUntilEpochMs,
                intent.mutationId(), body, canonicalAuthor, signingKeyVersion, privateKey);
    }
}
