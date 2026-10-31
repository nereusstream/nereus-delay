package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Exact accepted Schedule/Prepare intent and Target projection; a reference digest is not membership authority. */
public final class TargetScheduleBinding {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 21;
    public static final int MAX_BODY_BYTES = TargetScheduleBody.MAX_BODY_BYTES;
    public static final int MAX_METADATA_ENTRIES = TargetScheduleBody.MAX_METADATA_ENTRIES;
    public static final int LEGACY_MAX_CANONICAL_BYTES = 2
            + 43
            + 2
            + 5
            + MAX_BODY_BYTES
            + 4
            + TargetSourcePosition.MAX_CANONICAL_BYTES
            + 34
            + 4
            + 11
            + 18
            + 6 * 34
            + 35;
    public static final int MAX_CANONICAL_BYTES = LEGACY_MAX_CANONICAL_BYTES
            + TargetMigrationRecordReference.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 82;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-schedule-binding\0");
    private static final byte[] IMPORT_DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-target-migration-schedule-binding-v2\0");
    private static final byte[] PREFIX_DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-target-migration-prefix-schedule-binding-v3\0");
    private final DelayMessageId messageId;
    private final CommandType commandType;
    private final byte[] canonicalBody;
    private final CanonicalScheduleIntent intent;
    private final SourcePosition bindingSource;
    private final TargetPartitionId target;
    private final TargetKeyCodec.Domain domain;
    private final byte[] accountingIncarnation;
    private final byte[] requiredDispatchRef;
    private final byte[] offeredDispatchRef;
    private final byte[] controlScopeRef;
    private final byte[] membershipGrantRef;
    private final byte[] nativePolicyScopeRef;
    private final byte[] orderingDomain;
    private final byte[] migrationPlanDigest;
    private final TargetMigrationRecordReference migrationRecord;
    private final TargetQuotaMutation migrationSource;
    private final byte[] digest;

    public TargetScheduleBinding(
            final DelayMessageId messageId,
            final CommandType commandType,
            final byte[] canonicalBody,
            final SourcePosition bindingSource,
            final TargetPartitionId target,
            final TargetKeyCodec.Domain domain,
            final byte[] accountingIncarnation,
            final byte[] requiredDispatchRef,
            final byte[] offeredDispatchRef,
            final byte[] controlScopeRef,
            final byte[] membershipGrantRef,
            final byte[] nativePolicyScopeRef,
            final byte[] orderingDomain) {
        this(
                messageId,
                commandType,
                canonicalBody,
                bindingSource,
                target,
                domain,
                accountingIncarnation,
                requiredDispatchRef,
                offeredDispatchRef,
                controlScopeRef,
                membershipGrantRef,
                nativePolicyScopeRef,
                orderingDomain,
                null,
                null,
                null);
    }

    private TargetScheduleBinding(
            final DelayMessageId messageId,
            final CommandType commandType,
            final byte[] canonicalBody,
            final SourcePosition bindingSource,
            final TargetPartitionId target,
            final TargetKeyCodec.Domain domain,
            final byte[] accountingIncarnation,
            final byte[] requiredDispatchRef,
            final byte[] offeredDispatchRef,
            final byte[] controlScopeRef,
            final byte[] membershipGrantRef,
            final byte[] nativePolicyScopeRef,
            final byte[] orderingDomain,
            final byte[] migrationPlanDigest,
            final TargetMigrationRecordReference migrationRecord,
            final TargetQuotaMutation migrationSource) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.commandType = Objects.requireNonNull(commandType, "commandType");
        Objects.requireNonNull(canonicalBody, "canonicalBody");
        if (canonicalBody.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("Target binding body exceeds byte bound");
        }
        this.canonicalBody = Bytes.copy(canonicalBody);
        intent = TargetScheduleBody.validate(this.canonicalBody, commandType, messageId);
        this.bindingSource = TargetSourcePosition.requireBounded(bindingSource);
        this.target = Objects.requireNonNull(target, "target");
        this.domain = Objects.requireNonNull(domain, "domain");
        if (!bindingSource.shardId().equals(messageId.routingId().shardId())
                || domain.slot() >= TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("Target binding source Shard/domain slot mismatch");
        }
        this.accountingIncarnation =
                TargetCompatibilityCodec.assigned(accountingIncarnation, 16, "accountingIncarnation");
        this.requiredDispatchRef = TargetCompatibilityCodec.assigned(requiredDispatchRef, 32, "requiredDispatchRef");
        this.offeredDispatchRef = TargetCompatibilityCodec.assigned(offeredDispatchRef, 32, "offeredDispatchRef");
        this.controlScopeRef = TargetCompatibilityCodec.assigned(controlScopeRef, 32, "controlScopeRef");
        if ((migrationPlanDigest == null) != (migrationRecord == null)
                || (migrationPlanDigest == null) != (membershipGrantRef != null)) {
            throw new IllegalArgumentException(
                    "binding requires exactly ordinary membership or explicit complete migration origin");
        }
        this.membershipGrantRef = membershipGrantRef == null
                ? null
                : TargetCompatibilityCodec.assigned(membershipGrantRef, 32, "membershipGrantRef");
        this.migrationPlanDigest = migrationPlanDigest == null
                ? null
                : TargetCompatibilityCodec.assigned(migrationPlanDigest, 32, "original imported binding plan");
        this.migrationRecord = migrationRecord;
        this.migrationSource = migrationSource;
        if (migrationSource != null) {
            migrationSource.requireSourceApplied();
            if (migrationPlanDigest == null || bindingSource.compareTo(migrationSource.source()) > 0) {
                throw new IllegalArgumentException(
                        "migration prefix binding changed actual original Source/authorization order");
            }
        }
        if (nativePolicyScopeRef != null && intent.nativeDeliveryPolicy() == NativeDeliveryPolicy.FORBID) {
            throw new IllegalArgumentException("Target binding cannot add Native authority to FORBID intent");
        }
        this.nativePolicyScopeRef = nativePolicyScopeRef == null
                ? null
                : TargetCompatibilityCodec.assigned(nativePolicyScopeRef, 32, "nativePolicyScopeRef");
        if ((intent.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) != (orderingDomain != null)) {
            throw new IllegalArgumentException("Target binding ordering identity presence mismatch");
        }
        this.orderingDomain =
                orderingDomain == null ? null : TargetCompatibilityCodec.assigned(orderingDomain, 32, "orderingDomain");
        digest = Bytes.sha256(
                schemaVersion() == 3 ? PREFIX_DIGEST_DOMAIN : imported() ? IMPORT_DIGEST_DOMAIN : DIGEST_DOMAIN,
                fields());
    }

    /** Caller authenticates the frozen mapping and full old intent; this factory does not grant membership. */
    public static TargetScheduleBinding imported(
            TargetMigrationPlan plan,
            TargetMigrationRecordReference originalRecord,
            DelayMessageId messageId,
            CommandType commandType,
            byte[] originalBody,
            SourcePosition originalBindingSource,
            TargetPartitionId target,
            TargetKeyCodec.Domain domain,
            byte[] accountingIncarnation,
            byte[] requiredDispatchRef,
            byte[] offeredDispatchRef,
            byte[] controlScopeRef,
            byte[] nativePolicyScopeRef,
            byte[] orderingDomain) {
        Objects.requireNonNull(plan);
        Objects.requireNonNull(originalRecord);
        if (!plan.scope().shard().equals(messageId.routingId().shardId())
                || originalBindingSource.compareTo(plan.baseline().source()) > 0) {
            throw new IllegalArgumentException("imported binding is outside original complete migration Source cut");
        }
        return new TargetScheduleBinding(
                messageId,
                commandType,
                originalBody,
                originalBindingSource,
                target,
                domain,
                accountingIncarnation,
                requiredDispatchRef,
                offeredDispatchRef,
                controlScopeRef,
                null,
                nativePolicyScopeRef,
                orderingDomain,
                plan.digest(),
                originalRecord,
                null);
    }
    /** Actual legacy prefix Source S after C, with the unchanged original raw frame/closure independently retained. */
    public static TargetScheduleBinding importedPrefix(
            TargetMigrationPlan plan,
            TargetMigrationRecordReference originalRecord,
            TargetQuotaMutation actualSource,
            DelayMessageId messageId,
            CommandType commandType,
            byte[] originalBody,
            SourcePosition originalBindingSource,
            TargetPartitionId target,
            TargetKeyCodec.Domain domain,
            byte[] accountingIncarnation,
            byte[] requiredDispatchRef,
            byte[] offeredDispatchRef,
            byte[] controlScopeRef,
            byte[] nativePolicyScopeRef,
            byte[] orderingDomain) {
        Objects.requireNonNull(plan);
        Objects.requireNonNull(originalRecord);
        Objects.requireNonNull(actualSource).requireAfter(plan.baseline());
        if (!plan.scope().shard().equals(messageId.routingId().shardId())) {
            throw new IllegalArgumentException("prefix binding changed original Source Shard");
        }
        return new TargetScheduleBinding(
                messageId,
                commandType,
                originalBody,
                originalBindingSource,
                target,
                domain,
                accountingIncarnation,
                requiredDispatchRef,
                offeredDispatchRef,
                controlScopeRef,
                null,
                nativePolicyScopeRef,
                orderingDomain,
                plan.digest(),
                originalRecord,
                actualSource);
    }

    public boolean imported() {
        return migrationPlanDigest != null;
    }

    public int schemaVersion() {
        return migrationSource != null ? 3 : imported() ? 2 : 1;
    }

    public byte[] migrationPlanDigest() {
        return migrationPlanDigest == null ? null : Bytes.copy(migrationPlanDigest);
    }

    public TargetMigrationRecordReference migrationRecord() {
        return migrationRecord;
    }

    public TargetQuotaMutation migrationSource() {
        return migrationSource;
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public CommandType commandType() {
        return commandType;
    }

    public byte[] canonicalBody() {
        return Bytes.copy(canonicalBody);
    }

    public CanonicalScheduleIntent intent() {
        return intent;
    }

    public SourcePosition bindingSource() {
        return bindingSource;
    }

    public TargetPartitionId target() {
        return target;
    }

    public TargetKeyCodec.Domain domain() {
        return domain;
    }

    public byte[] accountingIncarnation() {
        return Bytes.copy(accountingIncarnation);
    }

    public byte[] requiredDispatchRef() {
        return Bytes.copy(requiredDispatchRef);
    }

    public byte[] offeredDispatchRef() {
        return Bytes.copy(offeredDispatchRef);
    }

    public byte[] controlScopeRef() {
        return Bytes.copy(controlScopeRef);
    }

    public byte[] membershipGrantRef() {
        return membershipGrantRef == null ? null : Bytes.copy(membershipGrantRef);
    }

    public byte[] nativePolicyScopeRef() {
        return nativePolicyScopeRef == null ? null : Bytes.copy(nativePolicyScopeRef);
    }

    public byte[] orderingDomain() {
        return orderingDomain == null ? null : Bytes.copy(orderingDomain);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.scheduleBinding(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion());
            CanonicalProtobuf.bytes(out, 2, messageId.bytes());
            CanonicalProtobuf.uint32(out, 3, commandType.wireValue());
            CanonicalProtobuf.bytes(out, 4, canonicalBody);
            CanonicalProtobuf.bytes(out, 5, bindingSource.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, target.bytes());
            CanonicalProtobuf.uint32(out, 7, domain.slot());
            CanonicalProtobuf.uint64Bits(out, 8, domain.generation());
            CanonicalProtobuf.bytes(out, 9, accountingIncarnation);
            CanonicalProtobuf.bytes(out, 10, requiredDispatchRef);
            CanonicalProtobuf.bytes(out, 11, offeredDispatchRef);
            CanonicalProtobuf.bytes(out, 12, controlScopeRef);
            if (membershipGrantRef != null) {
                CanonicalProtobuf.bytes(out, 13, membershipGrantRef);
            }
            if (nativePolicyScopeRef != null) {
                CanonicalProtobuf.bytes(out, 14, nativePolicyScopeRef);
            }
            if (orderingDomain != null) {
                CanonicalProtobuf.bytes(out, 15, orderingDomain);
            }
            if (imported()) {
                CanonicalProtobuf.bytes(out, 17, migrationPlanDigest);
                CanonicalProtobuf.bytes(out, 18, migrationRecord.canonicalBytes());
            }
            if (migrationSource != null) {
                CanonicalProtobuf.bytes(out, 20, migrationSource.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, schemaVersion() == 3 ? 21 : imported() ? 19 : 16, digest);
        });
    }

    public static TargetScheduleBinding decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 18, false, "TargetScheduleBinding");
        final int schema = QueryCodecSupport.uint32(fields.getFirst(), 1);
        if (fields.size() < 14 || (schema != VERSION && schema != 2 && schema != 3)) {
            throw new IllegalArgumentException("missing or unknown Target binding schema");
        }
        for (int n = 0; n < (schema == 1 ? 13 : 12); n++) {
            if (fields.get(n).number() != n + 1) {
                throw new IllegalArgumentException("Target binding fields out of order");
            }
        }
        int at = schema == 1 ? 13 : 12;
        byte[] nativeScope = null;
        byte[] ordered = null;
        if (fields.get(at).number() == 14) {
            nativeScope = QueryCodecSupport.fixed(fields.get(at++), 14, 32);
        }
        if (at < fields.size() && fields.get(at).number() == 15) {
            ordered = QueryCodecSupport.fixed(fields.get(at++), 15, 32);
        }
        byte[] migration = null;
        TargetMigrationRecordReference originalRecord = null;
        TargetQuotaMutation migrationSource = null;
        if (schema >= 2) {
            if (at + 2 >= fields.size()) {
                throw new IllegalArgumentException("imported binding lacks complete raw origin");
            }
            migration = QueryCodecSupport.fixed(fields.get(at++), 17, 32);
            originalRecord = TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(fields.get(at++), 18));
        }
        if (schema == 3) {
            migrationSource = TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(at++), 20));
        }
        final int digestField = schema == 1 ? 16 : schema == 2 ? 19 : 21;
        if (at != fields.size() - 1 || fields.get(at).number() != digestField) {
            throw new IllegalArgumentException("unexpected Target binding fields");
        }
        final var result = new TargetScheduleBinding(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(1), 2, DelayMessageId.LENGTH)),
                switch (QueryCodecSupport.uint32(fields.get(2), 3)) {
                    case 1 -> CommandType.SCHEDULE;
                    case 2 -> CommandType.PREPARE_LARGE_SCHEDULE;
                    default -> throw new IllegalArgumentException("unsupported Target binding command type");
                },
                QueryCodecSupport.bytes(fields.get(3), 4),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                new TargetPartitionId(QueryCodecSupport.fixed(fields.get(5), 6, 32)),
                new TargetKeyCodec.Domain(
                        QueryCodecSupport.uint32(fields.get(6), 7), QueryCodecSupport.uint64Bits(fields.get(7), 8)),
                QueryCodecSupport.fixed(fields.get(8), 9, 16),
                QueryCodecSupport.fixed(fields.get(9), 10, 32),
                QueryCodecSupport.fixed(fields.get(10), 11, 32),
                QueryCodecSupport.fixed(fields.get(11), 12, 32),
                schema == 1 ? QueryCodecSupport.fixed(fields.get(12), 13, 32) : null,
                nativeScope,
                ordered,
                migration,
                originalRecord,
                migrationSource);
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(at), digestField, 32))) {
            throw new IllegalArgumentException("Target binding digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetScheduleBinding");
        return result;
    }

    /**
     * Static projections only; source-ordered Profile/retry activation
     * and complete membership grants are mandatory.
     */
    public void requireReferences(
            final CanonicalTargetPartition physical,
            final ProfileSemanticEnvelope destination,
            final ProfileSemanticEnvelope capability,
            final TargetDispatchCompatibility required,
            final TargetDispatchCompatibility offered,
            final TargetControlScope controls) {
        if (!intent.profile().equals(destination.ref())
                || !target.equals(physical.id())
                || !TargetDispatchCompatibility.fromProfiles(physical, destination, capability)
                        .equals(required)
                || !offered.canServe(required)
                || !Arrays.equals(required.digest(), requiredDispatchRef)
                || !Arrays.equals(offered.digest(), offeredDispatchRef)
                || !Arrays.equals(controls.digest(), controlScopeRef)
                || !controls.target().equals(target)
                || !controls.sourceShard().equals(bindingSource.shardId())) {
            throw new IllegalArgumentException("Target binding Profile/compatibility/control reference mismatch");
        }
        final var dest = (DestinationProfileSemantic) destination.body();
        final int orderingBit = intent.orderingMode() == OrderingMode.BEST_EFFORT ? 1 : 2;
        final long length = commandType == CommandType.PREPARE_LARGE_SCHEDULE
                ? PrepareLargeScheduleBody.decode(canonicalBody).expectedPayloadLength()
                : intent.hasInlinePayload()
                        ? intent.inlinePayload().length
                        : intent.committedPayload().length();
        if ((dest.allowedOrderingModeBits() & orderingBit) == 0
                || length > dest.maxPayloadBytes()
                || intent.adapterMetadata().canonicalBytes().length > dest.maxAdapterMetadataBytes()
                || (dest.adapterKind() == AdapterKind.KAFKA)
                        != (intent.adapterMetadata().kind() == AdapterMetadata.Kind.KAFKA)
                || (nativePolicyScopeRef != null
                        && !TimingCapability.includes(
                                required.capability().timingCapabilityBits(),
                                TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF))) {
            throw new IllegalArgumentException(
                    "Target binding exceeds Profile payload/metadata/ordering/Native capability");
        }
    }

    public void requireLocator(final TargetMessageLocator locator) {
        if (!messageId.equals(locator.messageId())
                || !target.equals(locator.target())
                || !domain.equals(locator.domain())
                || !Arrays.equals(accountingIncarnation, locator.accountingIncarnation())
                || intent.orderingMode() != locator.orderingMode()
                || !Arrays.equals(orderingDomain, locator.orderingDomain())
                || !Arrays.equals(digest, locator.scheduleBindingDigest())) {
            throw new IllegalArgumentException("Target binding Message locator mismatch");
        }
    }

    public void requireQueueProjection(final TargetQueueState queue) {
        if (!target.equals(queue.targetId())
                || !Arrays.equals(accountingIncarnation, queue.accountingIncarnation())
                || domain.slot() >= queue.domains().size()) {
            throw new IllegalArgumentException("Target binding queue/accounting identity mismatch");
        }
        final var bound = queue.domains().get(domain.slot());
        if (!domain.equals(bound.domain())
                || bound.lifecycle() == TargetDomainState.Lifecycle.VACANT
                || !Arrays.equals(offeredDispatchRef, bound.dispatchCompatibilityRef())
                || !Arrays.equals(controlScopeRef, bound.controlScopeRef())
                || (nativePolicyScopeRef != null
                        && !Arrays.equals(nativePolicyScopeRef, bound.nativePolicyScopeRef()))) {
            throw new IllegalArgumentException("Target binding refers to a different or released domain");
        }
    }

    /** Prepare remains the authorization source even when Commit creates the Message at a later position. */
    public void requireMessageSource(final SourcePosition messageSource) {
        TargetSourcePosition.requireBounded(messageSource);
        if (bindingSource.compareTo(messageSource) > 0) {
            throw new IllegalArgumentException("Target Message source precedes its binding authorization");
        }
    }

    public static TargetScheduleBinding decodeReferenced(final byte[] reference, final byte[] encoded) {
        final var result = decode(encoded);
        if (!Bytes.constantTimeEquals(reference, result.digest)) {
            throw new IllegalArgumentException("Target Schedule binding reference mismatch");
        }
        return result;
    }

    public static TargetScheduleBinding decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId expectedSourceShard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.encodedKey())
                || !result.bindingSource.shardId().equals(expectedSourceShard)) {
            throw new IllegalArgumentException("Target binding Store key/Shard mismatch");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetScheduleBinding that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
