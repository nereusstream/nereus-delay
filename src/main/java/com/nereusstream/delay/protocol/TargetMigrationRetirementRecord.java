package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/**
 * Permanent root-bound retirement fact; the original migration root/birth and every resource receipt remain retained.
 */
public record TargetMigrationRetirementRecord(
        TargetQuotaIncarnation payer,
        PreparedControlOperation intentControl,
        SystemMutation intentInput,
        TargetQuotaMutation intentAt,
        PreparedControlOperation confirmationControl,
        SystemMutation confirmationInput,
        TargetQuotaMutation confirmedAt) {
    public static final int VALUE_TYPE = 71;
    public static final int MAX_INTENT_PREPARED_BYTES =
            TargetMigrationRetirementIntentRequest.MAX_CANONICAL_BYTES + 2048;
    public static final int MAX_CONFIRM_PREPARED_BYTES =
            TargetMigrationRetirementConfirmRequest.MAX_CANONICAL_BYTES + 2048;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaIncarnation.MAX_CANONICAL_BYTES
            + 2
                    * (TargetMigrationRetirementIntentRequest.MAX_CANONICAL_BYTES
                            + TargetMigrationRetirementConfirmRequest.MAX_CANONICAL_BYTES)
            + 2 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 8192;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-retirement-root-v1\0");

    public enum State {
        RETIRING,
        RETIRED
    }

    public TargetMigrationRetirementRecord {
        Objects.requireNonNull(payer);
        Objects.requireNonNull(intentControl);
        Objects.requireNonNull(intentInput);
        Objects.requireNonNull(intentAt).requireSourceApplied();
        if ((confirmationControl == null) != (confirmationInput == null)
                || (confirmationInput == null) != (confirmedAt == null)) {
            throw new IllegalArgumentException(
                    "retirement confirmation requires complete exact Prepared/frame/Source fact");
        }
        final var body = TargetMigrationControlBody.decode(intentInput.canonicalBody());
        body.requireInput(intentInput);
        if (!(body.request() instanceof TargetMigrationRetirementIntentRequest intention)
                || !payer.scope().equals(intention.scope())
                || payer.identity().kind() != TargetQuotaIdentity.Kind.SHARD
                || intentControl.canonicalBytes().length > MAX_INTENT_PREPARED_BYTES
                || !intentControl.request().equals(intention.operationRequest())
                || !Arrays.equals(intentAt.mutationDigest(), Bytes.sha256(intentInput.canonicalEnvelope()))) {
            throw new IllegalArgumentException("retirement lost exact intent/Prepared/Source management payer");
        }
        ControlTargetMutationBinding.validate(
                intentControl, intentControl.targets().getFirst(), intentInput);
        intentAt.requireAfter(intention.activation());
        payer.latestMutation().requireAtOrBefore(intentAt);
        if (confirmedAt != null) {
            confirmedAt.requireSourceApplied();
            confirmedAt.requireAfter(intentAt);
            final var confirmationBody = TargetMigrationControlBody.decode(confirmationInput.canonicalBody());
            confirmationBody.requireInput(confirmationInput);
            if (!(confirmationBody.request() instanceof TargetMigrationRetirementConfirmRequest confirmation)
                    || confirmationControl.canonicalBytes().length > MAX_CONFIRM_PREPARED_BYTES
                    || !confirmationControl.request().equals(confirmation.operationRequest())
                    || !Arrays.equals(
                            confirmedAt.mutationDigest(), Bytes.sha256(confirmationInput.canonicalEnvelope()))) {
                throw new IllegalArgumentException("retirement lost exact complete confirmation/Source input");
            }
            confirmation.requireIntent(intention, intentAt);
            if (confirmation.takeoverAt() != null) {
                confirmedAt.requireAfter(confirmation.takeoverAt());
            }
            ControlTargetMutationBinding.validate(
                    confirmationControl, confirmationControl.targets().getFirst(), confirmationInput);
        }
    }

    public static TargetMigrationRetirementRecord retiring(
            TargetQuotaIncarnation payer,
            PreparedControlOperation control,
            SystemMutation input,
            TargetQuotaMutation at) {
        return new TargetMigrationRetirementRecord(payer, control, input, at, null, null, null);
    }

    public State state() {
        return confirmedAt == null ? State.RETIRING : State.RETIRED;
    }

    public boolean retired() {
        return confirmedAt != null;
    }

    public TargetMigrationRetirementIntentRequest intention() {
        return (TargetMigrationRetirementIntentRequest)
                TargetMigrationControlBody.decode(intentInput.canonicalBody()).request();
    }

    public TargetMigrationRetirementConfirmRequest confirmation() {
        return confirmationInput == null
                ? null
                : (TargetMigrationRetirementConfirmRequest)
                        TargetMigrationControlBody.decode(confirmationInput.canonicalBody())
                                .request();
    }

    public TargetQuotaMutation mutation() {
        return retired() ? confirmedAt : intentAt;
    }

    public TargetMigrationRetirementRecord confirm(
            PreparedControlOperation control, SystemMutation input, TargetQuotaMutation at) {
        if (retired()) {
            if (!Arrays.equals(confirmationControl.canonicalBytes(), control.canonicalBytes())
                    || !Arrays.equals(confirmationInput.canonicalEnvelope(), input.canonicalEnvelope())
                    || !confirmedAt.equals(at)) {
                throw new IllegalStateException("retired old domain cannot reopen or change its exact confirmation");
            }
            return this;
        }
        return new TargetMigrationRetirementRecord(payer, intentControl, intentInput, intentAt, control, input, at);
    }

    public void requireRoot(TargetMigrationRoot root) {
        intention().requireRoot(root);
        if (!payer.identity().equals(root.owner().identity())
                || !payer.accounting().equals(root.owner().accounting())
                || !payer.allocation().equals(root.owner().allocation())
                || !Arrays.equals(payer.recoveryLineage(), root.plan().newLineage())) {
            throw new IllegalStateException("retirement Native metadata changed actual original management payer");
        }
    }

    public static byte[] keyFor(byte[] migrationId) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.MIGRATION_RETIREMENT_TAG, TargetKeyCodec.KEY_FORMAT},
                TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID"));
    }

    public byte[] key() {
        return keyFor(intention().migrationId());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.uint32(out, 2, retired() ? 2 : 1);
            CanonicalProtobuf.bytes(out, 3, payer.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, intentControl.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, intentInput.canonicalEnvelope());
            CanonicalProtobuf.bytes(out, 6, intentAt.canonicalBytes());
            if (retired()) {
                CanonicalProtobuf.bytes(out, 7, confirmationControl.canonicalBytes());
                CanonicalProtobuf.bytes(out, 8, confirmationInput.canonicalEnvelope());
                CanonicalProtobuf.bytes(out, 9, confirmedAt.canonicalBytes());
            }
        });
    }

    public byte[] digest() {
        return Bytes.sha256(DOMAIN, fields());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 10, digest());
        });
    }

    public static TargetMigrationRetirementRecord decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 10, false, "migration retirement Native fact");
        final boolean retired = f.size() == 10;
        QueryCodecSupport.requireNumbers(
                f,
                retired ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} : new int[] {1, 2, 3, 4, 5, 6, 10},
                "migration retirement Native fact");
        TargetMigrationRetirementInventory.version(f.getFirst());
        if (QueryCodecSupport.uint32(f.get(1), 2) != (retired ? 2 : 1)) {
            throw new IllegalArgumentException("unknown migration retirement state");
        }
        final byte[] prepared = QueryCodecSupport.bytes(f.get(3), 4);
        final byte[] confirmed = retired ? QueryCodecSupport.bytes(f.get(6), 7) : null;
        if (prepared.length > MAX_INTENT_PREPARED_BYTES
                || confirmed != null && confirmed.length > MAX_CONFIRM_PREPARED_BYTES) {
            throw new IllegalArgumentException("retirement Prepared exceeds exact purpose cap");
        }
        final var value = new TargetMigrationRetirementRecord(
                TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                PreparedControlOperation.decode(prepared),
                SystemMutation.decodeTargetEnvelope(
                        QueryCodecSupport.bytes(f.get(4), 5), SystemMutationType.APPLY_SHARD_CONTROL),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                retired ? PreparedControlOperation.decode(confirmed) : null,
                retired
                        ? SystemMutation.decodeTargetEnvelope(
                                QueryCodecSupport.bytes(f.get(7), 8), SystemMutationType.APPLY_SHARD_CONTROL)
                        : null,
                retired ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(8), 9)) : null);
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.getLast(), 10, 32))) {
            throw new IllegalArgumentException("retirement Native digest changed");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration retirement Native fact");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationRetirementRecord v
                && Arrays.equals(canonicalBytes(), v.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest());
    }
}
