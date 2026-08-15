package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/**
 * Derives the Registry logical identity for a canonical System Mutation body.
 *
 * <p>The logical identity is intentionally absent from the wire envelope. A
 * source cursor therefore has to parse the typed body before it can construct
 * the signed mutation object. Keeping that derivation in one protocol helper
 * prevents Kafka/Pulsar adapters from inventing operation-specific identity
 * rules or accepting a caller-provided identity during replay.</p>
 */
public final class SystemMutationIdentityV1 {
    private SystemMutationIdentityV1() {
    }

    /** Returns the exact registered logical identity for one canonical body. */
    public static byte[] resolve(final ShardId shardId, final SystemMutationType type,
                                 final byte[] canonicalBody) {
        final ShardId shard = Objects.requireNonNull(shardId, "shardId");
        final SystemMutationType mutationType = Objects.requireNonNull(type, "type");
        final byte[] body = Objects.requireNonNull(canonicalBody, "canonicalBody");
        final List<CanonicalProtobuf.Reader.Field> fields = SystemMutationBodyCodec.fields(mutationType, body);
        if (!shard.equals(SystemMutationBodyCodec.subjectShard(fields))) {
            throw new IllegalArgumentException("System Mutation identity body belongs to another shard");
        }
        return switch (mutationType) {
            case APPLY_SHARD_CONTROL -> {
                final ApplyShardControlBody control = ApplyShardControlBody.decode(body);
                yield control.controlRef().logicalOperationIdentity(control.controlKind());
            }
            case REPLAY_DEAD_LETTER -> ReplayDeadLetterBody.decode(body).controlRef()
                    .logicalOperationIdentity(mutationType);
            case RESOLVE_UNCERTAIN -> ResolveUncertainBody.decode(body).controlRef()
                    .logicalOperationIdentity(mutationType);
            case TIME_FENCE -> fixed(field(fields, 12), 12, SystemMutation.HASH_LENGTH);
            case PUBLISH_ADMISSION -> PublishAdmissionBody.decode(body).publishAttemptId();
            case PUBLISH_OUTCOME -> PublishOutcomeBody.decode(body).initialLogicalOperationIdentity();
            case EXPIRE_GENERATION -> {
                final DelayMessageId messageId = new DelayMessageId(
                        fixed(field(fields, 10), 10, DelayMessageId.LENGTH));
                yield SystemMutation.computeExpiryLogicalIdentity(messageId,
                        (int) unsigned(field(fields, 11), 11), unsigned(field(fields, 12), 12));
            }
            case EVIDENCE_RESOLUTION -> PublishOutcomeBody.decodeEvidenceResolution(body)
                    .evidenceResolutionLogicalOperationIdentity();
            case RESOURCE_RETIRE_INTENT -> {
                final ResourceRetireIntentBody retire = ResourceRetireIntentBody.decode(body);
                yield SystemMutation.computeResourceRetireLogicalIdentity(retire.resourceKind(),
                        retire.resource().identityHash(), retire.expectedResourceStateVersion());
            }
            case RESOURCE_DELETE_CONFIRMED -> ResourceDeleteConfirmedBody.decode(body).intent().mutationId();
            case CLAIM_RESULT -> ClaimResultBody.decode(body).claimId();
            case DLQ_EXPORT_RESULT -> DlqExportResultBody.decode(body).logicalOperationIdentity();
        };
    }

    private static CanonicalProtobuf.Reader.Field field(
            final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        throw new IllegalArgumentException("missing System Mutation body field " + number);
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid System Mutation body bytes field " + number);
        }
        final byte[] value = field.rawValue();
        Bytes.requireLength(value, length, "System Mutation body field " + number);
        return value;
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid System Mutation body scalar field " + number);
        }
        return field.unsignedValue();
    }
}
