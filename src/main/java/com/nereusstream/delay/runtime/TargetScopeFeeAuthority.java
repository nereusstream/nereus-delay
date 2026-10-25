package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation;
import com.nereusstream.delay.protocol.*;
import java.util.Arrays;
import java.util.Objects;

/** Already-held original Scope payment materials, independent of whether the business mutation was accepted. */
public interface TargetScopeFeeAuthority {
    enum Payment { FUNDED_SCOPE, INVALID_SHARED }

    /**
     * The position is this physical occurrence, not the logical first's position. firstInput supplies
     * the matching logical body; on a hot duplicate it may be this occurrence's frame. The original
     * first-frame digest remains in first.mutation(). A cold caller resolves actualInput separately.
     * expectedScope is the unique origin selected from actual Native records, never a provider-chosen payer.
     */
    record Occurrence(SystemMutation firstInput,SystemMutation actualInput,TargetResultRecord first,
            TargetResultRecord position,TargetScopeControlObligation expectedScope){
        public Occurrence {
            Objects.requireNonNull(firstInput);Objects.requireNonNull(first);Objects.requireNonNull(position);
            if(first.kind()!=TargetResultRecord.Kind.SYSTEM||position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM)
                throw new IllegalArgumentException("Scope payment requires the actual SYSTEM first and physical POSITION");
            position.requireFirst(first);
            final var outcome=SystemMutationResult.decode(first.typedPayload());
            if(!Arrays.equals(outcome.mutationId(),firstInput.systemMutationId())
                    ||!Arrays.equals(outcome.mutationHash(),firstInput.mutationHash())
                    ||!Arrays.equals(outcome.authorIdentity(),firstInput.authorIdentity())
                    ||outcome.mutationType()!=firstInput.type()||outcome.retryUntilEpochMs()!=firstInput.retryUntilEpochMs())
                throw new IllegalArgumentException("Scope payment changed its original logical first input");
            if(actualInput!=null){
                if(!Arrays.equals(Bytes.sha256(actualInput.canonicalEnvelope()),position.mutation().mutationDigest())
                        ||!Arrays.equals(actualInput.systemMutationId(),outcome.mutationId())
                        ||!Arrays.equals(actualInput.mutationHash(),outcome.mutationHash())
                        ||!Arrays.equals(actualInput.authorIdentity(),outcome.authorIdentity())
                        ||actualInput.type()!=outcome.mutationType()||actualInput.retryUntilEpochMs()!=outcome.retryUntilEpochMs())
                    throw new IllegalArgumentException("Scope payment lost this occurrence's exact frame or logical binding");
            }
        }
    }

    /** Exact canonical input at this position, resolved from already-held Source materials when it differs. */
    default SystemMutation occurrenceInput(SystemMutation firstInput,TargetResultRecord first,TargetResultRecord position){
        if(Arrays.equals(position.mutation().mutationDigest(),Bytes.sha256(firstInput.canonicalEnvelope())))return firstInput;
        throw new TargetSourceAuthorityPreparation.InputUnavailable("this Source occurrence's original frame is unavailable");
    }

    /**
     * Original parent allocation, accounting and complete Scope resource/retention protection. Retain every
     * charged first/POS and birth/origin fact or its independently complete original Source prefix; lifetime
     * counts cannot be inferred from a pruned current image. Acquisition precedes Store views; checks are local.
     */
    default void requireScope(TargetScopeControlObligation scope){
        throw new TargetSourceAuthorityPreparation.InputUnavailable("original Scope fee protection is unavailable");
    }

    /**
     * Authenticate MEMBER/writer REGISTER and this exact Source/exposure occurrence. INVALID_SHARED requires
     * positive absence of the binding plus its original invalid-input payer protection. Missing material,
     * an exhausted funded Scope or unknown exposure must throw InputUnavailable, never fall back to shared.
     * This permission supplies no amount, remaining capacity or business-write authorization.
     */
    default Payment requireOccurrence(Occurrence occurrence){
        throw new TargetSourceAuthorityPreparation.InputUnavailable("original Source occurrence fee payer is unavailable");
    }

    void requireCurrent();

    TargetScopeFeeAuthority UNAVAILABLE=()->{
        throw new TargetSourceAuthorityPreparation.InputUnavailable("original Scope fee holds are unavailable");
    };
}
