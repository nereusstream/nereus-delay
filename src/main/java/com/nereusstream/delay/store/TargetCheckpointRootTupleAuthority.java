package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import java.util.List;
import java.util.Objects;

/** Complete original initial-Source/issuer authentication, preserving every existing lifecycle obligation. */
final class TargetCheckpointRootTupleAuthority implements TargetCheckpointLifecycleInputs.Authority {
    private final TargetCheckpointLifecycleInputs.Authority original;
    private final TargetCheckpointControlAuthority controls;
    private final TargetStoreBootstrap.InitialControlAuthority issuer;
    private final TargetCheckpointDependencies dependencies;
    private final List<TargetCheckpointDependencies.ControlImage> inputs;
    TargetCheckpointRootTupleAuthority(TargetCheckpointLifecycleInputs.Authority original,TargetCheckpointControlAuthority controls,
            TargetStoreBootstrap.InitialControlAuthority issuer,TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.ControlImage> inputs){
        this.original=Objects.requireNonNull(original);this.controls=Objects.requireNonNull(controls);
        this.issuer=issuer;this.dependencies=Objects.requireNonNull(dependencies);this.inputs=List.copyOf(inputs);
    }
    @Override public void requireRootTupleAuthorization(TargetProtocolActivationRecord record){
        if(record.rootEvidence()!=null)controls.requireRootTupleAuthorization(record,issuer,dependencies,inputs);
        else original.requireRootTupleAuthorization(record);
    }
    @Override public void requireActivation(TargetProtocolActivationRecord record){original.requireActivation(record);}
    @Override public TargetCheckpointLifecycleInputs.HistoryImage resolve(TargetResultRecord first){return original.resolve(first);}
    @Override public TargetRetirementAuthorizationHistory.Pin requireOriginal(TargetCheckpointLifecycleInputs.HistoryImage input,TargetResultRecord first){return original.requireOriginal(input,first);}
    @Override public void requireRetiring(TargetEvidenceRetiringRecord record,TargetRetirementAuthorizationHistory.Grant grant){original.requireRetiring(record,grant);}
    @Override public void requireConfirmed(TargetEvidenceConfirmedRecord record,TargetCheckpointLifecycleInputs.HistoryImage intent,
            TargetRetirementAuthorizationHistory.Grant intentGrant,TargetRetirementAuthorizationHistory.Grant confirmationGrant){original.requireConfirmed(record,intent,intentGrant,confirmationGrant);}
    @Override public TargetMigrationPayloadMaterials migrationPayloadMaterials(TargetCheckpointLifecycleInputs.HistoryImage input,TargetResultRecord first){return original.migrationPayloadMaterials(input,first);}
    @Override public void requireInlinePayloadDecision(TargetCheckpointLifecycleInputs.HistoryImage input,TargetResultRecord first){original.requireInlinePayloadDecision(input,first);}
    @Override public void requireObjectPayloadDecision(TargetCheckpointLifecycleInputs.HistoryImage input,TargetResultRecord first){original.requireObjectPayloadDecision(input,first);}
    @Override public void requireUncommittedObjectDecision(TargetCheckpointLifecycleInputs.HistoryImage input,TargetResultRecord first){original.requireUncommittedObjectDecision(input,first);}
    @Override public void requireObjectPayloadAcceptance(TargetCheckpointLifecycleInputs.HistoryImage input,TargetResultRecord first,TargetObjectPayloadOrigin origin){original.requireObjectPayloadAcceptance(input,first,origin);}
    @Override public void requireIncarnationDecision(TargetCheckpointLifecycleInputs.HistoryImage input,TargetResultRecord first){original.requireIncarnationDecision(input,first);}
    @Override public void requireNativeRetainedDecision(TargetCheckpointLifecycleInputs.HistoryImage input,TargetResultRecord first){original.requireNativeRetainedDecision(input,first);}
}
