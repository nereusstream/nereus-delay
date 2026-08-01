package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Closed outer SLO event-identity union.
 *
 * <p>The branch payload is kept as canonical bytes here because the
 * objective-specific identity messages are owned by their measured component.
 * The outer branch number and canonical framing are nevertheless enforced at
 * this protocol boundary, so a payload cannot be relabelled as another
 * objective.</p>
 */
public final class SloSampleEventIdentityV1 {
    private final SloObjectiveNameV1 objective;
    private final byte[] canonicalBytes;

    public SloSampleEventIdentityV1(final SloObjectiveNameV1 objective, final byte[] canonicalBranchPayload) {
        this.objective = Objects.requireNonNull(objective, "objective");
        Objects.requireNonNull(canonicalBranchPayload, "canonicalBranchPayload");
        if (canonicalBranchPayload.length == 0) {
            throw new IllegalArgumentException("SLO event identity branch payload must not be empty");
        }
        final var nested = QueryCodecSupport.read(canonicalBranchPayload, "Slo event identity branch payload");
        final byte[] outer = CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, objective.wireValue(), canonicalBranchPayload));
        this.canonicalBytes = outer;
        // Reading the constructed value also verifies that the branch is the
        // exact oneof selected by the objective and that no unknown outer field
        // can be smuggled into the identity.
        decode(outer);
        if (nested.isEmpty()) {
            throw new IllegalArgumentException("SLO event identity branch payload is empty");
        }
    }

    private SloSampleEventIdentityV1(final SloObjectiveNameV1 objective, final byte[] canonicalBytes,
                                     final boolean alreadyCanonical) {
        this.objective = Objects.requireNonNull(objective, "objective");
        this.canonicalBytes = Bytes.copy(canonicalBytes);
        if (!alreadyCanonical) {
            throw new IllegalArgumentException("internal SLO identity constructor misuse");
        }
    }

    public SloObjectiveNameV1 objective() {
        return objective;
    }

    public byte[] canonicalBytes() {
        return Bytes.copy(canonicalBytes);
    }

    public byte[] branchPayload() {
        final var fields = QueryCodecSupport.read(canonicalBytes, "SloSampleEventIdentityV1");
        return QueryCodecSupport.nested(fields.get(0), objective.wireValue());
    }

    public static SloSampleEventIdentityV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SloSampleEventIdentityV1");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("SloSampleEventIdentityV1 must select one branch");
        }
        final var field = fields.get(0);
        final SloObjectiveNameV1 objective = SloObjectiveNameV1.fromWire(field.number());
        final byte[] branch = QueryCodecSupport.nested(field, field.number());
        // The branch itself is a canonical nested message; semantic validation
        // of its closed identity type is performed by the owning component.
        QueryCodecSupport.read(branch, "SloSampleEventIdentityV1 branch");
        final SloSampleEventIdentityV1 result = new SloSampleEventIdentityV1(objective,
                Bytes.copy(encoded), true);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloSampleEventIdentityV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloSampleEventIdentityV1 that
                && objective == that.objective
                && Arrays.equals(canonicalBytes, that.canonicalBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objective, Arrays.hashCode(canonicalBytes));
    }
}
