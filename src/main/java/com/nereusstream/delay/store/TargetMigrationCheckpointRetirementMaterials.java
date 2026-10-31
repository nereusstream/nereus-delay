package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.List;
import java.util.Objects;

/** Combines complete immutable migration content with the original public Source and actual Catalog retirement authority. */
public final class TargetMigrationCheckpointRetirementMaterials implements TargetCheckpointControlAuthority.MigrationMaterials {
    private final TargetCheckpointControlAuthority.MigrationMaterials original;
    private final TargetMigrationRetirementStore.OriginalInputs retirement;
    private final OxiaSyncCheckpointPublicationBackend catalog;
    private final TargetMigrationArchiveHeld archive;
    public TargetMigrationCheckpointRetirementMaterials(TargetCheckpointControlAuthority.MigrationMaterials original,
            TargetMigrationRetirementStore.OriginalInputs retirement, OxiaSyncCheckpointPublicationBackend catalog) {
        this(original,retirement,catalog,null);
    }
    private TargetMigrationCheckpointRetirementMaterials(TargetCheckpointControlAuthority.MigrationMaterials original,
            TargetMigrationRetirementStore.OriginalInputs retirement,OxiaSyncCheckpointPublicationBackend catalog,TargetMigrationArchiveHeld archive){
        this.original=Objects.requireNonNull(original);this.retirement=Objects.requireNonNull(retirement);this.catalog=Objects.requireNonNull(catalog);this.archive=archive;
    }
    public TargetMigrationCheckpointRetirementMaterials withArchive(TargetMigrationArchiveHeld actual){
        return new TargetMigrationCheckpointRetirementMaterials(original,retirement,catalog,Objects.requireNonNull(actual));
    }
    @Override public TargetMigrationPayloadMaterials payloadMaterials(){requireCurrent();return original.payloadMaterials();}
    @Override public void requireComplete(TargetCheckpointDependencies dependencies){
        requireCurrent();original.requireComplete(dependencies);
        if(dependencies.migrationArchiveState(TargetMigrationArchiveState.headKey(dependencies.migrationRoot().plan().migrationId()))!=null)
            Objects.requireNonNull(archive,"actual full archive receiving material").requireDependencies(dependencies);
        requireCurrent();
    }
    @Override public void requireCurrent(){original.requireCurrent();retirement.requireCurrent();}
    @Override public void requireArchiveHistorical(TargetCheckpointDependencies.ControlImage input,TargetResultRecord first,
            TargetCheckpointDependencies dependencies){
        requireCurrent();Objects.requireNonNull(archive,"actual full received archive custody").requireDependencies(dependencies);requireCurrent();
    }
    @Override public java.security.PublicKey authenticateRootIssuerAuthorization(TargetMigrationRootIssuerAuthorization statement,
            TargetMigrationPlan plan,TargetMigrationActivationAttempt attempt,SourcePosition actualA){
        requireCurrent();final var issuer=original.authenticateRootIssuerAuthorization(statement,plan,attempt,actualA);requireCurrent();return issuer;
    }
    @Override public java.security.PublicKey authenticateRootTupleIssuer(TargetMigrationRoot root,TargetMigrationActivationAttempt attempt,
            TargetProtocolTupleAuthorization authorization,SourcePosition tupleSource){
        requireCurrent();final var issuer=original.authenticateRootTupleIssuer(root,attempt,authorization,tupleSource);requireCurrent();return issuer;
    }
    @Override public void requireHistorical(TargetCheckpointDependencies.ControlImage input,TargetResultRecord first,
            TargetMigrationRoot root,List<TargetMigrationOrigin> origins,List<TargetMigrationCustodyGuard> guards,
            List<TargetMigrationPrefixRecord> prefixes,TargetMigrationActivationAttempt attempt){
        requireCurrent();original.requireHistorical(input,first,root,origins,guards,prefixes,attempt);requireCurrent();
    }
    @Override public void requireRetirementHistorical(TargetCheckpointDependencies.ControlImage input,TargetResultRecord first,
            TargetMigrationRoot root,TargetMigrationRetirementRecord record){
        requireCurrent();record.requireRoot(root);
        retirement.requireIntent(root,record.intention(),record.intentInput(),record.intentAt().source());
        if(record.retired()){
            record.confirmation().requireIntent(record.intention(),record.intentAt());
            final var prior=TargetMigrationRetirementRecord.retiring(record.payer(),record.intentControl(),record.intentInput(),record.intentAt());
            retirement.requireConfirmation(root,prior,record.confirmation(),record.confirmationInput(),record.confirmedAt().source());
            catalog.requireHistoricalRetirement(record,root);
        }
        requireCurrent();
    }
}
