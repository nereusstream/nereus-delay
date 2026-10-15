package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.*;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.*;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.GuardedTransactionalProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Actual independent-credential K2 recovery owner. Its public surface has no SEND, anchor or raw producer entry. */
public final class KafkaClientArtifactTargetRecoveryRuntime implements TargetWorkerShardRuntime.PublicationResources {
    private final TargetWorkerSourceStartup startup;
    private final TargetWorkerShardRuntime worker;
    private final OwnerIdentity owner;
    private final TargetChannelIdentity channel;
    private final TargetKafkaReceiptMapping mapping;
    private final SystemMutation admission;
    private final KafkaClientArtifactTargetReceiptFactory.Configuration configuration;
    private final LocalBrokerMtlsResolver resolver;
    private final Path authorizationFile;
    private final TargetRecoveryCredentialAuthorization.Trust recoveryTrust;
    private final ProfileCatalog profiles;
    private final CredentialAttestationTrustSet historicalTrust;
    private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
    private final TargetDestinationSdkBudget budget;
    private final ThreadPoolExecutor io;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final Supplier<BoundedReadBudget> reads;
    private final LongSupplier clock;
    private final TargetWorkerShardRuntime.PublicationReads nativeReads;
    private final String identity;
    private OwnedCheckpointIo<Opened> opening;
    private OwnedCheckpointIo<KafkaClientArtifactTargetReceiptRecovery> readerOpening;
    private OwnedCheckpointIo<KafkaClientArtifactTargetReceiptRecovery.Turn> poll;
    private OwnedCheckpointIo<Boolean> closing;
    private volatile Opened opened;
    private volatile KafkaClientArtifactTargetReceiptRecovery.Observation observation;
    private KafkaClientArtifactTargetReceiptRecovery reader;
    private volatile boolean ready, stopping, closed;

    /** Called only by the accepted Source startup, which reads the original mapping rather than accepting a caller DTO. */
    static KafkaClientArtifactTargetRecoveryRuntime create(TargetWorkerSourceStartup startup, TargetWorkerShardRuntime worker,
            byte[] channelDigest, KafkaClientArtifactTargetReceiptFactory.Configuration config,
            KafkaClientArtifactTargetReceiptFactory.RecoveryAdmission selector, Path authorizationFile,
            LocalBrokerMtlsResolver resolver, TargetRecoveryCredentialAuthorization.Trust recoveryTrust,
            ProfileCatalog profiles, CredentialAttestationTrustSet historicalTrust,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog, TargetDestinationSdkBudget budget,
            Supplier<TrustedUtcIntervalEvidence> time, Supplier<BoundedReadBudget> reads, LongSupplier clock) {
        try (var construction = startup.beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            final var entry = startup.publicationChannel(worker, channelDigest, false);
            final var actual = Objects.requireNonNull(switch (Objects.requireNonNull(selector).phase()) {
                case ADMITTED -> worker.readRecoveryAdmission(reads.get(), selector.image(), clock).kafkaMapping();
                case UNKNOWN -> worker.readKafkaUnknown(reads.get(), selector.image(), clock).mapping();
                default -> throw new IllegalArgumentException("unregistered K2 recovery phase");
            }, "original Source K2 mapping");
            entry.identity().requireExactFrozenIdentity(actual.admission().channel());
            final var binding = actual.admission().kafkaReceipt();
            if (!Arrays.equals(entry.physical().canonicalBytes(), actual.admission().publication().physical().canonicalBytes())
                    || !config.receiptTopicId().equals(binding.receiptResource().receiptTopicUuid())
                    || config.receiptLaneSlot() != binding.receiptResource().receiptLaneSlot()
                    || config.slotGeneration() != binding.receiptResource().slotGeneration()
                    || !Arrays.equals(config.protectionDigest(), binding.retentionProtectionDigest())
                    || !config.settings().targetPhysicalTopic().equals(binding.targetPhysicalTopic()))
                throw new IllegalArgumentException("K2 recovery configuration changes its original Source resource");
            final var result = new KafkaClientArtifactTargetRecoveryRuntime(startup, worker,
                    startup.publicationOwnerIdentity(worker), actual, selector.image(), config, authorizationFile,
                    resolver, recoveryTrust, profiles, historicalTrust, catalog, budget, time, reads, clock);
            construction.requireCurrent(); return result;
        } catch (RuntimeException | Error failure) { startup.failResourceStartup(failure); throw failure; }
    }

    private KafkaClientArtifactTargetRecoveryRuntime(TargetWorkerSourceStartup startup, TargetWorkerShardRuntime worker,
            OwnerIdentity owner, TargetKafkaReceiptMapping mapping, SystemMutation admission,
            KafkaClientArtifactTargetReceiptFactory.Configuration configuration, Path authorizationFile,
            LocalBrokerMtlsResolver resolver, TargetRecoveryCredentialAuthorization.Trust recoveryTrust,
            ProfileCatalog profiles, CredentialAttestationTrustSet historicalTrust,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog, TargetDestinationSdkBudget budget,
            Supplier<TrustedUtcIntervalEvidence> time, Supplier<BoundedReadBudget> reads, LongSupplier clock) {
        this.startup=startup;this.worker=worker; this.owner=owner; this.mapping=mapping; this.admission=admission; channel=mapping.admission().channel();
        this.configuration=configuration; this.authorizationFile=Objects.requireNonNull(authorizationFile);
        this.resolver=Objects.requireNonNull(resolver); this.recoveryTrust=Objects.requireNonNull(recoveryTrust);
        this.profiles=Objects.requireNonNull(profiles); this.historicalTrust=Objects.requireNonNull(historicalTrust);
        this.catalog=Objects.requireNonNull(catalog); this.budget=Objects.requireNonNull(budget);
        this.time=Objects.requireNonNull(time); this.reads=Objects.requireNonNull(reads); this.clock=Objects.requireNonNull(clock);
        final var graph=startup.graph(); io=graph.io();
        worker.requireFleetComposition(graph.classes(),graph.resources()); budget.requireWorker(graph.classes(),graph.resources());
        identity=Bytes.hex(channel.digest());
        worker.registerPublicationResources(identity,this,budget,clock);
        nativeReads=worker.publicationReads(this);
    }

    /** Polls actual opening only; all blocking material, authority and SDK work stays on the original finite pool. */
    public synchronized KafkaClientArtifactTargetReceiptRuntime.Turn advance() {
        if(closed)return new KafkaClientArtifactTargetReceiptRuntime.Turn(KafkaClientArtifactTargetReceiptRuntime.Status.CLOSED,null);
        if(stopping)return new KafkaClientArtifactTargetReceiptRuntime.Turn(KafkaClientArtifactTargetReceiptRuntime.Status.CLOSING,null);
        if(ready)return new KafkaClientArtifactTargetReceiptRuntime.Turn(KafkaClientArtifactTargetReceiptRuntime.Status.READY,null);
        try {
            if(opening==null){
                final var reserved=budget.reserve(identity+"/recovery-producer",configuration.settings().producerCost());
                try { opening=OwnedCheckpointIo.submitResource(guard->open(reserved,guard),()->{},Opened::discard,io); }
                catch(RuntimeException|Error failure){reserved.releaseUnstarted();throw failure;}
                opening.receipt().whenComplete((value,failure)->{if(failure!=null){
                    try{reserved.releaseUnstarted();}catch(IllegalStateException held){/* original material/SDK cleanup owns charge */}}});
            }
            if(!opening.ioExited())return new KafkaClientArtifactTargetReceiptRuntime.Turn(KafkaClientArtifactTargetReceiptRuntime.Status.OPENING,
                    CheckpointInputFuture.ownedFailure(opening.receipt()));
            opened=opening.completedValue().orElseThrow(()->new IllegalStateException("K2 recovery opening failed",CheckpointInputFuture.ownedFailure(opening.receipt())));
            ready=true; return new KafkaClientArtifactTargetReceiptRuntime.Turn(KafkaClientArtifactTargetReceiptRuntime.Status.READY,null);
        } catch(RuntimeException failure){return new KafkaClientArtifactTargetReceiptRuntime.Turn(KafkaClientArtifactTargetReceiptRuntime.Status.FAILED,failure);}
    }

    private TargetPublishOutcomeStore.KafkaRecoveryPending requirePending() {
        if(stopping)throw new IllegalStateException("K2 recovery is stopping");
        nativeReads.requireOwner(owner,clock);
        final var physical=nativeReads.requireRetainedChannel(channel,reads.get(),clock);
        if(!Arrays.equals(physical.canonicalBytes(),mapping.admission().publication().physical().canonicalBytes()))
            throw new IllegalStateException("K2 recovery changed original Target");
        // Historical public credential legality only; the expired SEND secret is neither loaded nor renewed.
        TargetDestinationCredentialChecks.requireBinding(channel,physical,profiles,historicalTrust);
        final var binding=mapping.admission().kafkaReceipt();
        final var protection=catalog.requireActive(binding.receiptResource(),channel.context().evidenceGeneration(),binding.retentionProtectionDigest());
        protection.protection().requireChannel(channel); catalog.requireAnchor(protection,binding);
        final var actual=nativeReads.readRecoveryPending(reads.get(),admission,mapping,clock);
        catalog.adopt(actual.adoption()); nativeReads.requireOwner(owner,clock); return actual;
    }
    private void requireCurrent(LocalBrokerMtlsResolver.TargetRecoveryReceipt material,TargetPublishOutcomeStore.KafkaRecoveryPending original) {
        material.authorization().requireRuntimeScope(owner,original.tenantScope());
        material.requireCurrent(mapping,time.get()); original.requireContinuation(requirePending()); material.requireCurrent(mapping,time.get());
    }
    private void requireCurrent() {
        final var actual=Objects.requireNonNull(opened,"actual K2 recovery SDK");
        requireCurrent(actual.material,actual.pending);
    }
    private void requireOpenResource() {
        final var actual=opened;
        if(!ready||stopping||actual==null||actual.closed)throw new IllegalStateException("original K2 recovery resource is unavailable");
    }
    private void requireRead(TargetKafkaReceiptMapping requested,long offset) {
        if(!Arrays.equals(mapping.canonicalBytes(),requested.canonicalBytes())||offset<mapping.admission().kafkaReceipt().scanStartOffset())
            throw new IllegalArgumentException("K2 recovery changed original mapping/range");
        requireCurrent();
    }

    private Opened open(TargetDestinationSdkBudget.Client reserved,Runnable guard) {
        guard.run(); final var pending=requirePending();
        final byte[] sdk=KafkaClientArtifactTargetReceiptReader.requireSdkIdentity();
        if(!Arrays.equals(sdk,configuration.sdkJar()))throw new IllegalStateException("K2 recovery SDK differs from activated Artifact");
        TargetSdkArtifactIdentity.requireExact(KafkaProducer.class,sdk);
        final var material=resolver.resolveTargetRecovery(authorizationFile,mapping,owner,pending.tenantScope(),recoveryTrust,time,reserved,guard);
        Producer<byte[],byte[]> producer=null; boolean constructorEntered=false;
        try {
            final var settings=configuration.settings();
            final var config=configured(new HashMap<>(configuration.producer()),true,material);
            final var readerConfig=configured(new HashMap<>(configuration.reader()),false,material);
            final Runnable current=()->{guard.run();requireCurrent(material,pending);guard.run();};
            current.run(); constructorEntered=true;
            producer=new KafkaProducer<>(config,new ByteArraySerializer(),new ByteArraySerializer());
            if(!(producer instanceof GuardedTransactionalProducer<byte[],byte[]> guarded))throw new IllegalStateException("guarded transactional K1 required");
            final var binding=mapping.admission().kafkaReceipt();
            final var target=new ProducerResourceGuard(binding.receiptResource().authenticatedClusterId(),binding.targetPhysicalTopic(),
                    uuid(mapping.admission().publication().physical().resource().kafka().nativeTopicUuid()),
                    Math.toIntExact(mapping.admission().publication().physical().physicalPartition()));
            final var receipt=new ProducerResourceGuard(binding.receiptResource().authenticatedClusterId(),binding.receiptPhysicalTopic(),
                    uuid(binding.receiptResource().receiptTopicUuid()),binding.receiptPartition());
            current.run();guarded.requireResourceGuard(target);current.run();guarded.requireResourceGuard(receipt);current.run();
            guarded.initTransactions();current.run();
            guarded.requireResourceGuard(target);current.run();guarded.requireResourceGuard(receipt);current.run();
            final var fence=new KafkaClientArtifactTargetTransactionalFactory.Fence(mapping,channel,owner,sdk,
                    ()->requireCurrent(material,pending),material.publicEvidence(),pending.contextImage());
            return new Opened(producer,material,pending,fence,readerConfig,settings.limits());
        } catch(RuntimeException|Error failure){
            final var actual=producer;
            final Runnable cleanup;
            if(actual!=null)cleanup=()->{KafkaClientArtifactProducerCleanup.close(actual,configuration.settings().limits().operationTimeout());material.close();};
            else if(failure instanceof KafkaProducer.ConstructionCleanupFailure partial)
                throw KafkaClientArtifactProducerCleanup.partial(partial,configuration.settings().limits().operationTimeout(),material::close);
            else if(!constructorEntered||failure instanceof org.apache.kafka.common.config.ConfigException)cleanup=material::close;
            else throw new KafkaClientArtifactTargetCleanupFailure(failure,()->{throw new IllegalStateException("original producer construction has no termination fact");});
            try{cleanup.run();}catch(RuntimeException|Error closeFailure){final var retained=new KafkaClientArtifactTargetCleanupFailure(failure,cleanup);retained.addSuppressed(closeFailure);throw retained;}
            throw failure;
        }
    }
    private Map<String,Object> configured(Map<String,Object> operational,boolean producer,LocalBrokerMtlsResolver.TargetRecoveryReceipt material) {
        final var config=new HashMap<>(KafkaClientArtifactMtlsConfiguration.applyTargetRecovery(operational,material));
        final var s=configuration.settings();final int timeout=Math.toIntExact(s.limits().operationTimeout().toMillis());
        if(!(config.get("bootstrap.servers") instanceof String addresses)||addresses.split(",",-1).length>s.maximumBrokers())
            throw new IllegalArgumentException("K2 recovery requires bounded literal bootstrap servers");
        config.put("send.buffer.bytes",s.socketBufferBytes());config.put("receive.buffer.bytes",s.socketBufferBytes());
        config.put("request.timeout.ms",Math.min(timeout,10000));
        if(producer){
            final String stable=new String(channel.context().producerIdentity(),StandardCharsets.UTF_8);
            if(config.containsKey("transactional.id")&&!stable.equals(config.get("transactional.id")))throw new IllegalArgumentException("recovery cannot change transactional.id");
            config.put("transactional.id",stable);config.put("enable.idempotence",true);config.put("acks","all");
            config.put("max.in.flight.requests.per.connection",1);config.put("linger.ms",0);config.put("max.block.ms",(long)timeout);
            config.put("delivery.timeout.ms",timeout);config.put("transaction.timeout.ms",timeout);
            config.put("buffer.memory",s.producerBufferBytes());config.put("max.request.size",Math.toIntExact(s.physicalBytes()));
        }else config.put("default.api.timeout.ms",timeout);
        return Map.copyOf(config);
    }
    private static Uuid uuid(java.util.UUID id){return new Uuid(id.getMostSignificantBits(),id.getLeastSignificantBits());}

    public synchronized CompletableFuture<KafkaClientArtifactTargetReceiptRecovery.Turn> recoverOnce(int maximumTurns) {
        if(!ready||stopping)throw new IllegalStateException("K2 recovery SDK is not ready");
        if(readerOpening==null){
            final var reserved=budget.reserve(identity+"/recovery-reader",configuration.settings().readerCost());
            try{readerOpening=OwnedCheckpointIo.submitResource(guard->{guard.run();requireCurrent();
                return new KafkaClientArtifactTargetReceiptRecovery(mapping,opened.fence,this::requireOpenResource,opened.readers,
                        this::requireRead,configuration.settings().limits(),maximumTurns,reserved,true);
            },()->{},value->{try{value.close();}catch(RuntimeException|Error failure){throw new KafkaClientArtifactTargetCleanupFailure(failure,value::close);}},io);}
            catch(RuntimeException|Error failure){reserved.releaseUnstarted();throw failure;}
            readerOpening.receipt().whenComplete((value,failure)->{if(failure!=null){try{reserved.releaseUnstarted();}catch(IllegalStateException held){/* actual native reader retains charge */}}});
        }
        if(!readerOpening.ioExited())return readerOpening.receipt().thenApply(ignored->new KafkaClientArtifactTargetReceiptRecovery.Turn(
                KafkaClientArtifactTargetReceiptRecovery.Status.WAITING,mapping.admission().kafkaReceipt().scanStartOffset(),null));
        reader=readerOpening.completedValue().orElseThrow(()->new IllegalStateException("K2 recovery reader opening failed",CheckpointInputFuture.ownedFailure(readerOpening.receipt())));
        if(poll!=null&&!poll.ioExited())return poll.receipt();
        poll=OwnedCheckpointIo.submit(guard->{guard.run();requireCurrent();final var turn=reader.runTurn();requireCurrent();guard.run();
            if(turn.observation()!=null)observation=turn.observation();return turn;},()->{},io);
        return poll.receipt();
    }

    /** Independent recovery requires a durably selected exact proof ref; memory observations cannot use the legacy path. */
    public TargetPublishRecoveryMaintenance.ResolutionAuthority resolutionAuthority() {
        throw new IllegalStateException("independent K2 resolution requires archived proof and proof-bearing Source composition");
    }
    /** Safe public proof image for the original protected evidence provider, not a caller-issued Artifact/ref. */
    public byte[] evidenceImage(PublishEvidence evidence) {
        final var actual=Objects.requireNonNull(observation,"actual guarded recovery observation");
        actual.requireCurrent(evidence); return actual.canonicalBytes();
    }

    /** Only this actual native owner can construct an input accepted by the privileged proof signer. */
    public ArchiveObservation captureArchiveObservation(PublishEvidence evidence){
        requireOpenResource();requireCurrent();
        final var actual=Objects.requireNonNull(observation,"actual complete recovery scan");actual.requireCurrent(evidence);
        final var current=requirePending();
        opened.material.requireArchiveMemory(Math.addExact(configuration.settings().producerBufferBytes(),
                Math.addExact(opened.material.retainedBytes(),8L*TargetRecoveryProofRef.MAX_PROOF_BYTES)));
        final var capability=Objects.requireNonNull(profiles.resolve(mapping.admission().publication().capabilityProfile()),"original capability semantic");
        final var at=Objects.requireNonNull(time.get());
        final var captured=new ArchiveObservation(this,mapping,admission,evidence,owner,at,current.tenantScope(),
                actual.canonicalBytes(),opened.material.archivePublicInputs(),configuration.sdkJar(),capability,current.attempt(),current.adoption().anchor().primaryOwner());
        actual.requireCurrent(evidence);requireCurrent();return captured;
    }
    public static final class ArchiveObservation{
        private final KafkaClientArtifactTargetRecoveryRuntime original;
        private final TargetKafkaReceiptMapping mapping;
        private final SystemMutation admission;
        private final PublishEvidence evidence;
        private final OwnerIdentity owner;
        private final TrustedUtcIntervalEvidence at;
        private final byte[] tenant,observation,material,sdk;
        private final ProfileSemanticEnvelope capability;
        private final TargetQuotaAttemptBudget attempt;
        private final TargetQuotaIncarnation frozenOwner;
        private ArchiveObservation(KafkaClientArtifactTargetRecoveryRuntime original,TargetKafkaReceiptMapping mapping,
                SystemMutation admission,PublishEvidence evidence,OwnerIdentity owner,TrustedUtcIntervalEvidence at,
                byte[] tenant,byte[] observation,byte[] material,byte[] sdk,ProfileSemanticEnvelope capability,
                TargetQuotaAttemptBudget attempt,TargetQuotaIncarnation frozenOwner){
            this.original=original;this.mapping=mapping;this.admission=admission;this.evidence=evidence;this.owner=owner;this.at=at;
            this.tenant=Bytes.copy(tenant);this.observation=Bytes.copy(observation);this.material=Bytes.copy(material);this.sdk=Bytes.copy(sdk);this.capability=capability;
            this.attempt=attempt;this.frozenOwner=frozenOwner;
        }
        void requireCurrent(){original.requireOpenResource();original.requireCurrent();
            Objects.requireNonNull(original.observation).requireCurrent(evidence);}
        TrustedUtcIntervalEvidence currentTime(){requireCurrent();return Objects.requireNonNull(original.time.get());}
        byte[] unsigned(AuthorIdentity issuer,int keyVersion){requireCurrent();return TargetRecoveryProof.unsigned(mapping,admission,evidence,
                owner,at,sdk,KafkaClientArtifactTargetReceiptReader.REQUIRED_K1_SOURCE_COMMIT,observation,material,tenant,issuer,keyVersion,capability,attempt,frozenOwner);}
    }

    // Only the distinct normal-work composition entry can obtain the original context, never a recovery SEND capability.
    KafkaClientArtifactTargetReceiptFactory.ReopenInputs reopenInputs(TargetWorkerSourceStartup original,
            TargetWorkerShardRuntime actual) {
        if(startup!=original||worker!=actual)throw new IllegalArgumentException("K2 resume changed its original Source startup/Worker");
        return new KafkaClientArtifactTargetReceiptFactory.ReopenInputs(configuration,channel,
                mapping.admission().publication().physical(),owner,admission,mapping,resolver,profiles,historicalTrust,
                catalog,budget,time,reads,clock);
    }

    @Override public TargetChannelIdentity channelIdentity(){return channel;}
    @Override public boolean readyForPublishing(){return false;}
    @Override public boolean recoveryOnly(){return true;}
    @Override public boolean readyForRecovery(){return ready&&!stopping;}
    @Override public LocalBrokerMtlsResolver.TargetRecoveryReceipt recoveryCredentialReceipt(){return opened==null?null:opened.material;}
    @Override public boolean closed(){return closed;}
    @Override public synchronized boolean settleClose(){
        stopping=true;ready=false;
        if(opening!=null){opening.requestStop();opening.retryCleanup();if(!opening.ioExited())return false;if(opened==null)opened=opening.completedValue().orElse(null);}
        if(poll!=null){poll.requestStop();poll.retryCleanup();if(!poll.ioExited())return false;}
        if(readerOpening!=null){readerOpening.requestStop();readerOpening.retryCleanup();if(!readerOpening.ioExited())return false;
            if(reader==null)reader=readerOpening.completedValue().orElse(null);}
        if(opened!=null){
            if(closing==null||closing.ioExited()){
                if(opened.closed&&(reader==null||reader.closeCompleted())){closed=true;return true;}
                closing=OwnedCheckpointIo.submit(ignored->{if(reader!=null)reader.close();opened.close();return true;},()->{},io);return false;
            }
            return false;
        }
        closed=true;return true;
    }
    private static final class Opened {
        private final Producer<byte[],byte[]> producer;
        private final LocalBrokerMtlsResolver.TargetRecoveryReceipt material;
        private final TargetPublishOutcomeStore.KafkaRecoveryPending pending;
        private final KafkaClientArtifactTargetTransactionalFactory.Fence fence;
        private final Map<String,Object> readers;
        private final KafkaClientArtifactTargetTransactionalFactory.Limits limits;
        private volatile boolean closed;
        private Opened(Producer<byte[],byte[]> producer,LocalBrokerMtlsResolver.TargetRecoveryReceipt material,
                TargetPublishOutcomeStore.KafkaRecoveryPending pending,KafkaClientArtifactTargetTransactionalFactory.Fence fence,
                Map<String,Object> readers,KafkaClientArtifactTargetTransactionalFactory.Limits limits){
            this.producer=producer;this.material=material;this.pending=pending;this.fence=fence;this.readers=readers;this.limits=limits;
        }
        private synchronized void close(){if(!closed){KafkaClientArtifactProducerCleanup.close(producer,limits.operationTimeout());material.close();closed=true;}}
        private void discard(){try{close();}catch(RuntimeException|Error failure){throw new KafkaClientArtifactTargetCleanupFailure(failure,this::close);}}
    }
}
