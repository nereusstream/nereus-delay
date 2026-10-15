package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Original trusted publishing/history inputs, not a caller-built runtime or transport permission. */
public record TargetWorkerPublishingConfiguration(
        TargetPublishAdmissionMutationFactory admissions, TargetPublishOutcomeMutationFactory outcomes,
        ArtifactGenerationSet artifacts, TargetPulsarPublishRuntime.Preparation preparation,
        TargetPulsarPublishRuntime.Payloads payloads, Supplier<BoundedReadBudget> reads,
        Supplier<TrustedUtcIntervalEvidence> time, LongSupplier ownerClock,
        TargetPublishRecoveryMaintenance.History recoveryHistory, List<Endpoint> endpoints) {
    public sealed interface Endpoint permits Managed, Baseline, KafkaReceipt {
        ChannelKind kind();
    }
    public record Managed(BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight ordinaryGate,
            BoundedDestinationPublishAdapter.TargetNativePreparedPublishPreflight nativeGate,
            TargetPulsarPublishExecutor.ResultAuthority results,
            PulsarAttemptJournal.NegativeSettlementAuthority negativeAuthority,
            Runnable protection, TargetJournalNegativeSettlementMaintenance.History history) implements Endpoint {
        public Managed {
            Objects.requireNonNull(ordinaryGate); Objects.requireNonNull(nativeGate); Objects.requireNonNull(results);
            Objects.requireNonNull(negativeAuthority); Objects.requireNonNull(protection); Objects.requireNonNull(history);
        }
        @Override public ChannelKind kind() { return ChannelKind.PULSAR_DEDUP_PRODUCER; }
    }
    public record Baseline(BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results) implements Endpoint {
        public Baseline { Objects.requireNonNull(gate); Objects.requireNonNull(results); }
        @Override public ChannelKind kind() { return ChannelKind.BASELINE_PRODUCER; }
    }
    public record KafkaReceipt(BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results) implements Endpoint {
        public KafkaReceipt { Objects.requireNonNull(gate); Objects.requireNonNull(results); }
        @Override public ChannelKind kind() { return ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT; }
    }

    public TargetWorkerPublishingConfiguration {
        Objects.requireNonNull(admissions); Objects.requireNonNull(outcomes); Objects.requireNonNull(artifacts);
        Objects.requireNonNull(preparation); Objects.requireNonNull(payloads); Objects.requireNonNull(reads);
        Objects.requireNonNull(time); Objects.requireNonNull(ownerClock); Objects.requireNonNull(recoveryHistory);
        endpoints = List.copyOf(endpoints);
        if (endpoints.isEmpty() || endpoints.size() > 3
                || endpoints.stream().map(Endpoint::kind).distinct().count() != endpoints.size()) {
            throw new IllegalArgumentException("publishing requires one explicit endpoint per configured kind");
        }
    }

    boolean supports(ChannelKind kind) { return endpoints.stream().anyMatch(endpoint -> endpoint.kind() == kind); }

    /** Called only by the original Root after validating the actual registered appender/Journal/dispatch owners. */
    TargetPulsarPublishRuntime install(TargetWorkerShardRuntime worker, OwnedSourceSystemWriter writer,
            PulsarAttemptJournal journal, TargetWorkerPublicationDispatch dispatch, Executor io) {
        final var adapter = dispatch.adapter();
        final var managed = endpoints.stream().filter(Managed.class::isInstance).map(Managed.class::cast).findFirst();
        TargetPulsarPublishRuntime driver = null;
        if (managed.isPresent()) {
            final var selected = managed.orElseThrow();
            driver = worker.configurePulsarPublishing(writer, admissions, outcomes, Objects.requireNonNull(journal), adapter,
                    artifacts, preparation, payloads, reads, time, ownerClock, io, io, selected.ordinaryGate(),
                    selected.nativeGate(), selected.results(), selected.negativeAuthority());
            worker.configureJournalNegativeSettlement(journal, selected.negativeAuthority(), selected.protection(),
                    reads, selected.history(), io, ownerClock);
        }
        for (var endpoint : endpoints) {
            switch (endpoint) {
                case Managed ignored -> { }
                case Baseline selected -> {
                    if (driver == null) driver = worker.configureBaselinePublishing(writer, admissions, outcomes, adapter,
                            artifacts, preparation, payloads, reads, time, ownerClock, io, io, selected.gate(), selected.results());
                    else worker.configureBaselineEndpoint(adapter, artifacts, outcomes, selected.gate(), selected.results(), io, ownerClock);
                }
                case KafkaReceipt selected -> {
                    if (driver == null) driver = worker.configureKafkaReceiptPublishing(writer, admissions, outcomes, adapter,
                            artifacts, preparation, payloads, reads, time, ownerClock, io, io, selected.gate(), selected.results());
                    else worker.configureKafkaReceiptEndpoint(adapter, artifacts, outcomes, selected.gate(), selected.results(), io, ownerClock);
                }
            }
        }
        worker.configurePublishRecoveryMaintenance(outcomes, writer, reads, recoveryHistory, ownerClock);
        return Objects.requireNonNull(driver);
    }
}
