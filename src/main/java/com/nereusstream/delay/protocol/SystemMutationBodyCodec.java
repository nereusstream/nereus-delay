package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Registry §5.3 field-shape validator for System Mutation bodies. */
public final class SystemMutationBodyCodec {
    private static final int VARIABLE = -1;

    private SystemMutationBodyCodec() {}

    /** Validates required/optional operation fields without interpreting their nested semantic objects. */
    public static void validate(final SystemMutationType type, final byte[] canonicalBody) {
        Objects.requireNonNull(type, "type");
        if(type==SystemMutationType.TARGET_RESOURCE_RECLAIM){TargetResourceReclaimBody.decode(canonicalBody);return;}
        if(type==SystemMutationType.TARGET_SLO_OBSERVATION){TargetSloObservationEventBody.decode(canonicalBody);return;}
        final List<CanonicalProtobuf.Reader.Field> fields =
                readBodyFields(Objects.requireNonNull(canonicalBody, "canonicalBody"));
        if(type==SystemMutationType.APPLY_SHARD_CONTROL&&fields.size()==10&&fields.get(4).number()==11
                &&fields.getLast().number()==16){
            final long kind=fields.get(4).unsignedValue();
            if(kind==TargetSloWriterControlRequest.REGISTER_CONTROL_KIND||kind==TargetSloWriterControlRequest.CLOSE_CONTROL_KIND){
                TargetSloWriterControlBody.decode(canonicalBody);return;
            }
            if(kind==TargetSloObservationControlRequest.ENABLE_CONTROL_KIND||kind==TargetSloObservationControlRequest.STOP_CONTROL_KIND){
                TargetSloObservationControlBody.decode(canonicalBody);return;
            }
        }
        Spec[] specs = specs(type);
        final boolean targetRetirement = (type == SystemMutationType.RESOURCE_RETIRE_INTENT
                || type == SystemMutationType.RESOURCE_DELETE_CONFIRMED)
                && fields.size() > 3 && fields.get(3).number() == 9;
        if (targetRetirement) {
            if (canonicalBody.length > TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES
                    || fields.get(3).wireType() != 0
                    || fields.get(3).unsignedValue() != TargetEvidenceRetireIntentBody.BODY_VERSION)
                throw new IllegalArgumentException("unsupported Target evidence retirement body generation");
            specs = type == SystemMutationType.RESOURCE_RETIRE_INTENT
                    ? new Spec[]{varint(9),varint(10),nested(11),varint(12),nested(13),nested(14)}
                    : new Spec[]{varint(9),nested(10),varint(11),nested(12),nested(13),nested(14),nested(15),varint(16)};
        }
        if ((type == SystemMutationType.PUBLISH_OUTCOME || type == SystemMutationType.EVIDENCE_RESOLUTION)
                && fields.size() > 3 && fields.get(3).number() == 9) {
            if (fields.get(3).wireType() != 0 || fields.get(3).unsignedValue() != PublishOutcomeBody.RECOVERY_PROOF_BODY_VERSION)
                throw new IllegalArgumentException("unsupported proof-bearing Target K2 outcome body version");
            final var versioned = new Spec[specs.length + 2];
            versioned[0] = varint(9); System.arraycopy(specs, 0, versioned, 1, specs.length);
            versioned[versioned.length - 1] = nested(19); specs = versioned;
        }
        if (type == SystemMutationType.TARGET_PUBLISH_ADMISSION
                && fields.size() > 3
                && fields.get(3).number() == 10
                && (fields.get(3).unsignedValue() == TargetPublishAdmissionBody.MATERIALIZED_BODY_VERSION
                        || fields.get(3).unsignedValue() == TargetPublishAdmissionBody.NATIVE_MATERIALIZED_BODY_VERSION
                        || fields.get(3).unsignedValue() == TargetPublishAdmissionBody.KAFKA_RECEIPT_BODY_VERSION)) {
            specs = Arrays.copyOf(specs, specs.length + 2);
            specs[specs.length - 2] = nested(22);
            specs[specs.length - 1] = nested(23);
        }
        if (type == SystemMutationType.TARGET_PUBLISH_ADMISSION && fields.size() > 3
                && fields.get(3).unsignedValue() == TargetPublishAdmissionBody.KAFKA_RECEIPT_BODY_VERSION) {
            specs = Arrays.copyOf(specs, specs.length + 1);
            specs[specs.length - 1] = nested(24);
        }
        int fieldIndex = 3;
        for (Spec spec : specs) {
            if (fieldIndex < fields.size() && fields.get(fieldIndex).number() == spec.number()) {
                validateField(fields.get(fieldIndex), spec, type, targetRetirement);
                fieldIndex++;
            } else if (spec.required()) {
                throw new IllegalArgumentException("missing System Mutation body field " + spec.number());
            }
        }
        if (fieldIndex != fields.size()) {
            throw new IllegalArgumentException("unknown or out-of-order System Mutation body field "
                    + fields.get(fieldIndex).number());
        }
        if (targetRetirement) {
            if (type == SystemMutationType.RESOURCE_RETIRE_INTENT) {
                if (fields.get(4).unsignedValue() != ResourceKind.TARGET_EXTERNAL_EVIDENCE_OBJECT.wireValue())
                    throw new IllegalArgumentException("generation8 only admits Target external evidence kind8");
                TargetEvidenceRetireIntentBody.decodeResource(fields.get(5).rawValue());
            } else TargetEvidenceRetireIntentBody.decodeResource(fields.get(8).rawValue());
        }
        if (type == SystemMutationType.TARGET_PUBLISH_ADMISSION
                && fields.get(3).unsignedValue() != TargetPublishAdmissionBody.BODY_VERSION
                && fields.get(3).unsignedValue() != TargetPublishAdmissionBody.MATERIALIZED_BODY_VERSION
                && fields.get(3).unsignedValue() != TargetPublishAdmissionBody.NATIVE_MATERIALIZED_BODY_VERSION
                && fields.get(3).unsignedValue() != TargetPublishAdmissionBody.KAFKA_RECEIPT_BODY_VERSION) {
            throw new IllegalArgumentException("unsupported Target Publish Admission body version");
        }
    }

    /** Returns the already-validated canonical body fields for an operation applier. */
    public static List<CanonicalProtobuf.Reader.Field> fields(
            final SystemMutationType type, final byte[] canonicalBody) {
        validate(type, canonicalBody);
        return List.copyOf(readAll(new CanonicalProtobuf.Reader(canonicalBody)));
    }

    /** Returns the Shard identity carried by the common body subject field. */
    public static ShardId subjectShard(final List<CanonicalProtobuf.Reader.Field> fields) {
        Objects.requireNonNull(fields, "fields");
        if (fields.isEmpty() || fields.get(0).number() != 1 || fields.get(0).wireType() != 2) {
            throw new IllegalArgumentException("System Mutation body subject field is missing");
        }
        return ShardSubject.decode(fields.get(0).rawValue()).shardId();
    }

    /** Rejects a message identity whose self-routing Shard differs from the body subject. */
    public static void requireMessageShard(
            final List<CanonicalProtobuf.Reader.Field> fields, final DelayMessageId messageId, final String operation) {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(operation, "operation");
        if (!subjectShard(fields).equals(messageId.routingId().shardId())) {
            throw new IllegalArgumentException(operation + " messageId does not belong to body shard");
        }
    }

    private static Spec[] specs(final SystemMutationType type) {
        return switch (type) {
            case TARGET_RESOURCE_RECLAIM -> throw new IllegalStateException("reclaim uses its closed typed body codec");
            case TARGET_SLO_OBSERVATION -> throw new IllegalStateException("SLO observation uses its bounded typed body codec");
            case APPLY_SHARD_CONTROL ->
                new Spec[] {nested(10), varint(11), varint(12), fixed(13, 32, true), optionalVarint(14), nested(15)};
            case REPLAY_DEAD_LETTER ->
                new Spec[] {
                    nested(10),
                    fixed(11, DelayMessageId.LENGTH, true),
                    varint(12),
                    varint(13),
                    varint(14),
                    varint(15),
                    nested(16),
                    bool(17),
                    optionalFixed(18, 32, false)
                };
            case RESOLVE_UNCERTAIN ->
                new Spec[] {
                    nested(10),
                    fixed(11, 32, true),
                    fixed(12, 16, true),
                    fixed(13, DelayMessageId.LENGTH, true),
                    varint(14),
                    fixed(15, 32, true),
                    varint(16),
                    optionalNested(17),
                    bool(18),
                    bool(19),
                    optionalFixed(20, 32, false)
                };
            case TIME_FENCE -> new Spec[] {varint(10), varint(11), fixed(12, 32, true), nested(13)};
            case PUBLISH_ADMISSION ->
                new Spec[] {
                    nested(10),
                    fixed(11, 16, true),
                    fixed(12, 32, true),
                    fixed(13, 32, true),
                    fixed(14, 16, true),
                    fixed(15, DelayMessageId.LENGTH, true),
                    varint(16),
                    fixed(17, 32, true),
                    fixed(18, 32, true),
                    nested(19),
                    fixed(20, 32, true),
                    nested(21),
                    nested(22),
                    nested(23),
                    nested(24),
                    nested(25)
                };
            case TARGET_PUBLISH_ADMISSION ->
                new Spec[] {
                    varint(10),
                    nested(11),
                    fixed(12, 16, true),
                    fixed(13, 32, true),
                    nested(14),
                    varint(15),
                    fixed(16, 32, true),
                    nested(17),
                    varint(18),
                    nested(19),
                    nested(20),
                    nested(21)
                };
            case PUBLISH_OUTCOME ->
                new Spec[] {
                    fixed(10, 32, true),
                    varint(11),
                    varint(12),
                    varint(13),
                    optionalNested(14),
                    nested(15),
                    nested(16),
                    nested(17)
                };
            case EXPIRE_GENERATION ->
                new Spec[] {fixed(10, DelayMessageId.LENGTH, true), varint(11), varint(12), nested(13)};
            case EVIDENCE_RESOLUTION ->
                new Spec[] {
                    fixed(10, 32, true),
                    nested(11),
                    nested(12),
                    varint(13),
                    varint(14),
                    varint(15),
                    nested(16),
                    nested(17),
                    nested(18)
                };
            case RESOURCE_RETIRE_INTENT -> new Spec[] {varint(10), nested(11), varint(12), nested(13)};
            case RESOURCE_DELETE_CONFIRMED -> new Spec[] {nested(10), varint(11), nested(12), nested(13)};
            case CLAIM_RESULT ->
                new Spec[] {
                    fixed(10, 32, true),
                    fixed(11, DelayMessageId.LENGTH, true),
                    varint(12),
                    fixed(13, 32, true),
                    fixed(14, 16, true),
                    nested(15),
                    varint(16),
                    varint(17),
                    nested(18),
                    nested(20)
                };
            case DLQ_EXPORT_RESULT ->
                new Spec[] {
                    fixed(10, 32, true),
                    fixed(11, DelayMessageId.LENGTH, true),
                    varint(12),
                    varint(13),
                    fixed(14, 32, true),
                    varint(15),
                    varint(16),
                    varint(17),
                    varint(18),
                    optionalNested(19),
                    nested(20),
                    nested(21),
                    nested(22),
                    varint(23),
                    varint(24)
                };
        };
    }

    private static void validateField(
            final CanonicalProtobuf.Reader.Field field, final Spec spec, final SystemMutationType type,
            final boolean targetRetirement) {
        if (field.wireType() != spec.wireType()) {
            throw new IllegalArgumentException(
                    "System Mutation body field " + spec.number() + " has the wrong wire type");
        }
        if (spec.wireType() == 0) {
            final long value = field.unsignedValue();
            final boolean rawUint64 = (type == SystemMutationType.RESOURCE_RETIRE_INTENT && field.number() == 12)
                    || (type == SystemMutationType.APPLY_SHARD_CONTROL && field.number() == 12)
                    || (type == SystemMutationType.DLQ_EXPORT_RESULT && field.number() == 13);
            if ((value < 0 && !rawUint64) || (spec.bool() && (value < 0 || value > 1))) {
                throw new IllegalArgumentException("invalid System Mutation body scalar field " + spec.number());
            }
            return;
        }
        final byte[] value = field.rawValue();
        if (spec.fixedLength() != VARIABLE) {
            Bytes.requireLength(value, spec.fixedLength(), "System Mutation body field " + spec.number());
        }
        if (spec.nested()) {
            if (value.length == 0) {
                throw new IllegalArgumentException(
                        "nested System Mutation body field " + spec.number() + " must not be empty");
            }
            if (targetRetirement) {
                if (type == SystemMutationType.RESOURCE_DELETE_CONFIRMED) {
                    if (field.number() == 10) { TargetEvidenceDeleteConfirmedBody.decodeIntent(value); return; }
                    if (field.number() == 12) { TargetEvidenceDeleteConfirmedBody.decodeEvidence(value); return; }
                    if (field.number() == 13) { TargetEvidenceDeleteConfirmedBody.decodeTime(value); return; }
                }
                if (type == SystemMutationType.RESOURCE_RETIRE_INTENT && field.number() == 13) {
                    TargetEvidenceRetireIntentBody.decodeProtections(value); return;
                }
                if (type == SystemMutationType.RESOURCE_RETIRE_INTENT && field.number() == 14
                        || type == SystemMutationType.RESOURCE_DELETE_CONFIRMED && field.number() == 15) {
                    TargetEvidenceRetirementContext.decode(value); return;
                }
                if (type == SystemMutationType.RESOURCE_RETIRE_INTENT && field.number() == 11
                        || type == SystemMutationType.RESOURCE_DELETE_CONFIRMED && field.number() == 14) {
                    TargetEvidenceRetireIntentBody.decodeResource(value); return;
                }
            }
            final boolean repeatedCanonicalFields = type == SystemMutationType.TARGET_PUBLISH_ADMISSION
                    && (field.number() == 19 || field.number() == 20);
            readAll(new CanonicalProtobuf.Reader(value, repeatedCanonicalFields));
        }
    }

    private static Spec varint(final int number) {
        return new Spec(number, 0, VARIABLE, false, false, true);
    }

    private static Spec optionalVarint(final int number) {
        return new Spec(number, 0, VARIABLE, false, false, false);
    }

    private static Spec bool(final int number) {
        return new Spec(number, 0, VARIABLE, false, true, true);
    }

    private static Spec fixed(final int number, final int length, final boolean required) {
        return new Spec(number, 2, length, false, false, required);
    }

    private static Spec optionalFixed(final int number, final int length, final boolean nested) {
        return new Spec(number, 2, length, nested, false, false);
    }

    private static Spec nested(final int number) {
        return new Spec(number, 2, VARIABLE, true, false, true);
    }

    private static Spec optionalNested(final int number) {
        return new Spec(number, 2, VARIABLE, true, false, false);
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static List<CanonicalProtobuf.Reader.Field> readBodyFields(final byte[] encoded) {
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final var fields = new ArrayList<CanonicalProtobuf.Reader.Field>();
        // Every registered operation has at most 32 top-level fields, including all explicit body generations.
        while (reader.hasRemaining()) {
            if (fields.size() == 32) throw new IllegalArgumentException("System Mutation body has excess fields");
            fields.add(reader.next());
        }
        return fields;
    }

    private record Spec(int number, int wireType, int fixedLength, boolean nested, boolean bool, boolean required) {}
}
