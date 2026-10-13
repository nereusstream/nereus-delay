package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.TargetOrdinaryPublishRequest;
import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight;
import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetPulsarPublishRuntime;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Actual one-slot startup/reopen/teardown over the Worker graph; CONTROL Admin and shared executors remain caller-owned. */
public final class KafkaClientArtifactTargetReceiptRuntime implements DestinationPublishAdapter,
        TargetWorkerShardRuntime.PublicationResources {
    @Override public com.nereusstream.delay.ownership.TargetOwnedBackendGroup ownedBackendGroup() { return null; }
    @Override public TargetChannelIdentity channelIdentity() { return channel; }
    public enum Status { SAMPLING, WAITING_ANCHOR, PUBLISHING_ANCHOR, OPENING, READY, CLOSING, CLOSED, FAILED }
    public record Turn(Status status, Throwable failure) {}

    /** Explicit SDK, receipt and physical envelopes. No inferred SDK defaults are used by this entry. */
    public record Settings(String targetPhysicalTopic, KafkaClientArtifactTargetTransactionalFactory.Limits limits, int maximumBrokers,
            long producerBufferBytes, int socketBufferBytes, long physicalBytes,
            CapacityVector producerCost, CapacityVector readerCost) {
        public Settings {
            Objects.requireNonNull(limits,"finite SDK limits");
            if(targetPhysicalTopic==null||!targetPhysicalTopic.matches("[a-zA-Z0-9._-]{1,249}")
                    ||targetPhysicalTopic.equals(".")||targetPhysicalTopic.equals("..")||maximumBrokers<1||maximumBrokers>256||producerBufferBytes<physicalBytes||physicalBytes<1
                    ||producerBufferBytes>=Integer.MAX_VALUE||socketBufferBytes<1
                    ||producerCost.amount(CapacityDimension.ADAPTER_PRODUCERS)!=1
                    ||readerCost.amount(CapacityDimension.ADAPTER_PRODUCERS)!=0
                    ||producerCost.amount(CapacityDimension.ADAPTER_THREADS)<1||readerCost.amount(CapacityDimension.ADAPTER_THREADS)<1
                    ||producerCost.amount(CapacityDimension.ADAPTER_CONNECTIONS)<2L*maximumBrokers
                    ||readerCost.amount(CapacityDimension.ADAPTER_CONNECTIONS)<2L*maximumBrokers
                    ||producerCost.amount(CapacityDimension.BUFFERED_BYTES)<producerBufferBytes
                    ||readerCost.amount(CapacityDimension.BUFFERED_BYTES)<limits.maximumBytes()) {
                throw new IllegalArgumentException("K2 explicit client envelope cannot serve its configured SDK buffers/connections");
            }
        }
        public static Settings load(Map<String,String> properties) {
            final var f=new HashMap<>(properties);
            final var limits=new KafkaClientArtifactTargetTransactionalFactory.Limits(Duration.ofMillis(number(f,"operationMillis")),
                    Duration.ofMillis(number(f,"pollMillis")),Math.toIntExact(number(f,"receiptRecords")),Math.toIntExact(number(f,"receiptBytes")));
            final int brokers=Math.toIntExact(number(f,"maximumBrokers"));final long buffers=number(f,"producerBufferBytes");
            final int sockets=Math.toIntExact(number(f,"socketBufferBytes"));final long physical=number(f,"physicalBytes");
            final var result=new Settings(f.remove("targetPhysicalTopic"),limits,brokers,buffers,sockets,physical,cost(f,"producer"),cost(f,"reader"));
            if(!f.isEmpty())throw new IllegalArgumentException("unknown K2 runtime settings: "+f.keySet());return result;
        }
        private static CapacityVector cost(Map<String,String> values,String prefix){
            final long[] result=new long[CapacityDimension.COUNT];
            for(var d:java.util.List.of(CapacityDimension.OPEN_FILES,CapacityDimension.ADAPTER_CONNECTIONS,
                    CapacityDimension.ADAPTER_PRODUCERS,CapacityDimension.ADAPTER_THREADS,CapacityDimension.BUFFERED_MESSAGES,
                    CapacityDimension.BUFFERED_BYTES,CapacityDimension.DIRECT_BUFFER_BYTES,CapacityDimension.OTHER_NATIVE_BYTES))
                result[d.wireValue()-1]=number(values,prefix+'.'+d.name());
            return new CapacityVector(result);
        }
        private static long number(Map<String,String> values,String name){
            final String value=Objects.requireNonNull(values.remove(name),"missing explicit K2 configuration: "+name);
            if(!value.matches("[0-9]+"))throw new IllegalArgumentException("K2 bound must be an explicit decimal: "+name);
            final long result=Long.parseLong(value);if(result==Long.MAX_VALUE)throw new IllegalArgumentException("unbounded K2 configuration");return result;
        }
    }

    private final TargetWorkerShardRuntime worker;
    private final TargetChannelIdentity channel;
    private final CanonicalTargetPartition physical;
    private final KafkaClientArtifactTargetReceiptProvider provider;
    private final KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher;
    private final TargetDestinationSdkBudget budget;
    private final DestinationPhysicalAdmission physicalAdmission;
    private final Settings settings;
    private final ThreadPoolExecutor io;
    private final Map<String,Object> producerConfiguration, readerConfiguration;
    private final Supplier<BoundedReadBudget> reads;
    private final LongSupplier ownerClock;
    private final TargetKafkaReceiptMapping original;
    private final String identity;
    private long readerSequence;
    private OwnedCheckpointIo<Optional<KafkaClientArtifactTargetReceiptAnchor.Observation>> sampling;
    private CompletableFuture<TargetKafkaReceiptBinding> publication;
    private OwnedCheckpointIo<KafkaClientArtifactTargetTransactionalTransport> opening;
    private OwnedCheckpointIo<Boolean> closing;
    private KafkaClientArtifactTargetTransactionalTransport transport;
    private TargetKafkaReceiptBinding binding;
    private OwnedCheckpointIo<KafkaClientArtifactTargetReceiptRecovery> recoveryOpening;
    private OwnedCheckpointIo<KafkaClientArtifactTargetReceiptRecovery.Turn> recoveryTurn;
    private KafkaClientArtifactTargetReceiptRecovery recovery;
    private boolean stopping, registered, ready, closed;

    /** Construct on the caller's startup I/O path after actual CONTROL provision and Provider creation. */
    public KafkaClientArtifactTargetReceiptRuntime(TargetWorkerShardRuntime worker, TargetChannelIdentity channel,
            CanonicalTargetPartition physical, KafkaClientArtifactTargetReceiptProvider provider,
            KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher, TargetDestinationSdkBudget budget,
            DestinationPhysicalAdmission physicalAdmission, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, Settings settings, ThreadPoolExecutor io,
            Map<String,Object> producerConfiguration, Map<String,Object> readerConfiguration,
            Supplier<BoundedReadBudget> reads, LongSupplier ownerClock, TargetKafkaReceiptMapping original) {
        this.worker=Objects.requireNonNull(worker);this.channel=Objects.requireNonNull(channel);this.physical=Objects.requireNonNull(physical);
        this.provider=Objects.requireNonNull(provider);this.publisher=Objects.requireNonNull(publisher);
        this.budget=Objects.requireNonNull(budget);this.physicalAdmission=Objects.requireNonNull(physicalAdmission);
        this.settings=Objects.requireNonNull(settings);this.io=Objects.requireNonNull(io);this.reads=Objects.requireNonNull(reads);
        this.ownerClock=Objects.requireNonNull(ownerClock);this.original=original;
        worker.requireFleetComposition(classes,resources);budget.requireWorker(classes,resources);
        if(channel.context().kind()!=ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT||!physical.id().equals(channel.context().target()))
            throw new IllegalArgumentException("K2 runtime has another channel/Target");
        final var actual=worker.requirePublicationChannel(channel,original==null,reads.get(),ownerClock);
        if(!java.util.Arrays.equals(actual.canonicalBytes(),physical.canonicalBytes()))throw new IllegalArgumentException("K2 startup changes actual Source Target");
        identity=Bytes.hex(channel.digest());
        this.producerConfiguration=configured(producerConfiguration,true);
        this.readerConfiguration=configured(readerConfiguration,false);
        if(original!=null)binding=original.admission().kafkaReceipt();
        physicalAdmission.registerTargetChannel(new DestinationPhysicalAdmission.TargetChannelSpec(channel,physical,
                1,settings.physicalBytes(),1,settings.physicalBytes()));registered=true;
        try{worker.registerPublicationResources(identity,this,budget,ownerClock);}
        catch(RuntimeException | Error failure){physicalAdmission.unregisterTargetChannel(channel);registered=false;throw failure;}
    }

    private Map<String,Object> configured(Map<String,Object> source,boolean producer){
        final var result=new HashMap<>(source);
        if(result.containsKey("interceptor.classes"))throw new IllegalArgumentException("K2 cannot transform records");
        final Object bootstrap=result.get("bootstrap.servers");
        if(!(bootstrap instanceof String addresses)||addresses.isBlank()||addresses.split(",",-1).length>settings.maximumBrokers())
            throw new IllegalArgumentException("K2 requires a finite explicit bootstrap list");
        result.put("send.buffer.bytes",settings.socketBufferBytes());result.put("receive.buffer.bytes",settings.socketBufferBytes());
        result.put("request.timeout.ms",Math.toIntExact(Math.min(settings.limits().operationTimeout().toMillis(),10000)));
        if(producer){result.put("buffer.memory",settings.producerBufferBytes());result.put("max.request.size",Math.toIntExact(settings.physicalBytes()));}
        else{result.put("default.api.timeout.ms",Math.toIntExact(settings.limits().operationTimeout().toMillis()));}
        return Map.copyOf(result);
    }

    private synchronized TargetDestinationSdkBudget.Client readerBudget(){
        if(stopping)throw new IllegalStateException("K2 runtime is stopping");
        readerSequence=Math.incrementExact(readerSequence);
        return budget.reserve(identity+"/reader/"+readerSequence,settings.readerCost());
    }

    /** One local state transition; actual sampler/catalog/SDK work runs on the existing bounded executor. */
    public synchronized Turn runStartupTurn(){
        if(closed)return new Turn(Status.CLOSED,null);
        if(stopping)return new Turn(Status.CLOSING,null);
        if(ready)return new Turn(Status.READY,null);
        try{
            if(binding==null){
                if(sampling==null){final var p=provider.protection();
                    sampling=KafkaClientArtifactTargetReceiptAnchor.sampleOwned(readerConfiguration,p.resource(),p.physicalTopic(),
                            p.physicalPartition(),provider,settings.limits().pollTimeout(),io,readerBudget());return new Turn(Status.SAMPLING,null);}
                if(!sampling.ioExited())return new Turn(Status.SAMPLING,CheckpointInputFuture.ownedFailure(sampling.receipt()));
                final var sampled=sampling.receipt().join();
                if(sampled.isEmpty()){sampling=null;return new Turn(Status.WAITING_ANCHOR,null);}
                if(publication==null){publication=publisher.publish(sampled.orElseThrow(),settings.targetPhysicalTopic());
                    return new Turn(Status.PUBLISHING_ANCHOR,null);}
                if(!CheckpointInputFuture.ioExited(publication))return new Turn(Status.PUBLISHING_ANCHOR,CheckpointInputFuture.ownedFailure(publication));
                binding=publication.join();
            }
            if(opening==null){
                final var reserved=budget.reserve(identity+"/producer",settings.producerCost());
                try{opening=KafkaClientArtifactTargetTransactionalFactory.openOwned(original,producerConfiguration,readerConfiguration,
                        physical,channel,binding,provider,provider,io,settings.limits(),reserved,this::readerBudget);}
                catch(RuntimeException | Error rejected){reserved.releaseUnstarted();throw rejected;}
                return new Turn(Status.OPENING,null);
            }
            if(!opening.ioExited())return new Turn(Status.OPENING,CheckpointInputFuture.ownedFailure(opening.receipt()));
            transport=opening.completedValue().orElseThrow(()->new IllegalStateException("K2 opening failed",CheckpointInputFuture.ownedFailure(opening.receipt())));
            physicalAdmission.openTargetReady(channel);ready=true;return new Turn(Status.READY,null);
        }catch(RuntimeException failure){return new Turn(Status.FAILED,failure);}
    }

    /** Called by the actual per-Claim preparation on its bounded I/O owner; tuple/key/quota authorities remain frozen. */
    public TargetPulsarPublishRuntime.Inputs inputs(ProfileRef capability,long retryUntil,CapacityVector commitment,
            CapacityVector allocated,com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies,
            com.nereusstream.delay.runtime.TargetQuotaDelta.LocalClaimAuthority revokeQuota,
            com.nereusstream.delay.store.TargetStoreBackend.CommitAuthority revokeWrites) {
        synchronized(this){if(!ready||stopping||original!=null)throw new IllegalStateException("K2 new work requires a current SEND startup");}
        if(!allocated.covers(provider.evidenceCharge(binding,worker.readPublicationAccounting(reads.get(),channel,ownerClock))))
            throw new IllegalArgumentException("K2 preparation has not reserved its actual anchor object");
        return new TargetPulsarPublishRuntime.Inputs(channel,capability,retryUntil,commitment,allocated,policies,
                revokeQuota,revokeWrites,binding,provider.publications(reads,io));
    }

    /** Existing issuer/tuple/time authority is retained; only this actual K2 provider supplies external materialization. */
    public TargetPublishAdmissionVerifier.Authority sourceAuthority(TargetPublishAdmissionVerifier.Authority originalAuthority){
        return (scope,writer,image,source)->{
            final var actual=originalAuthority.resolve(scope,writer,image,source);
            if(actual==null||!scope.shard().equals(worker.shardId()))return actual;
            final var body=TargetPublishAdmissionBody.decode(image.canonicalBody());
            if(body.kafkaReceipt()==null||!java.util.Arrays.equals(body.channel().context().producerIdentity(),channel.context().producerIdentity()))return actual;
            return new TargetPublishAdmissionVerifier.Authorization(actual.writerKey(),actual.activatedTuple(),actual.maximumDecisionWidthMs(),
                    actual.maximumBrokerTimestampDivergenceMs(),actual.maximumMutationEnqueueAgeMs(),actual.decisionTime(),
                    provider.materialization(image,source),actual.nativePolicies());
        };
    }

    public BoundedDestinationPublishAdapter adapter(WorkClassExecutionRegistry classes){return new BoundedDestinationPublishAdapter(this,physicalAdmission,classes,io);}
    @Override public CompletionStage<DestinationPublishResult> publish(DestinationPublishRequest request){return CompletableFuture.completedFuture(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE,null));}
    @Override public CompletionStage<DestinationPublishResult> publishTargetOrdinary(TargetOrdinaryPublishRequest request,TargetOrdinaryPublishPreflight gate){
        synchronized(this){if(!ready||stopping||original!=null)return CompletableFuture.completedFuture(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE,null));}
        return transport.publishTargetOrdinary(request,gate);
    }
    /** Reconstructs the original range once and performs exactly one guarded poll per caller turn. */
    public synchronized CompletableFuture<KafkaClientArtifactTargetReceiptRecovery.Turn> recoverOnce(
            SystemMutation admission,int maximumTurns) {
        if(!ready||stopping||original==null)throw new IllegalStateException("cold K2 recovery is not READY");
        final var unknown=worker.readKafkaUnknown(reads.get(),admission,ownerClock);
        if(!java.util.Arrays.equals(unknown.mapping().canonicalBytes(),original.canonicalBytes()))throw new IllegalArgumentException("recovery selects another actual UNKNOWN mapping");
        if(recoveryOpening==null){final var reserved=readerBudget();
            try{recoveryOpening=OwnedCheckpointIo.submitResource(guard->{guard.run();
                return new KafkaClientArtifactTargetReceiptRecovery(original,transport,readerConfiguration,provider,settings.limits(),maximumTurns,reserved);
            },()->{},value->{try{value.close();}catch(RuntimeException | Error failed){throw new KafkaClientArtifactTargetCleanupFailure(failed,value::close);}},io);}
            catch(RuntimeException | Error rejected){reserved.releaseUnstarted();throw rejected;}
            recoveryOpening.receipt().whenComplete((value,failure)->{if(failure!=null){try{reserved.releaseUnstarted();}catch(IllegalStateException held){/* original cleanup holds its charge */}}});
            return recoveryOpening.receipt().thenApply(value->new KafkaClientArtifactTargetReceiptRecovery.Turn(KafkaClientArtifactTargetReceiptRecovery.Status.WAITING,
                    original.admission().kafkaReceipt().scanStartOffset(),null));
        }
        if(!recoveryOpening.ioExited())return recoveryOpening.receipt().thenApply(value->new KafkaClientArtifactTargetReceiptRecovery.Turn(
                KafkaClientArtifactTargetReceiptRecovery.Status.WAITING,original.admission().kafkaReceipt().scanStartOffset(),null));
        recovery=recoveryOpening.completedValue().orElseThrow(()->new IllegalStateException("cold reader opening failed",CheckpointInputFuture.ownedFailure(recoveryOpening.receipt())));
        if(recoveryTurn!=null&&!recoveryTurn.ioExited())return recoveryTurn.receipt();
        recoveryTurn=OwnedCheckpointIo.submit(guard->{guard.run();final var turn=recovery.runTurn();guard.run();return turn;},()->{},io);
        return recoveryTurn.receipt();
    }

    public synchronized KafkaClientArtifactTargetTransactionalTransport recoveryResource(TargetKafkaReceiptMapping expected){
        if(!ready||stopping||original==null||!java.util.Arrays.equals(original.canonicalBytes(),expected.canonicalBytes()))
            throw new IllegalStateException("cold resource differs from its original retained mapping");
        return transport;
    }
    public KafkaClientArtifactTargetReceiptReader.Observation requireObservation(TargetOrdinaryPublishRequest request,DestinationPublishResult result){
        return Objects.requireNonNull(transport,"actual opened transport").requireObservation(request,result);
    }
    @Override public synchronized boolean closed(){return closed;}
    @Override public synchronized boolean readyForPublishing(){return ready&&!stopping&&original==null;}
    public synchronized boolean readyForNewWork(){return ready&&!stopping&&original==null;}
    @Override public synchronized boolean settleClose(){
        stopping=true;ready=false;if(registered)physicalAdmission.closeTargetReady(channel);
        if(sampling!=null){sampling.requestStop();sampling.retryCleanup();if(!sampling.ioExited())return false;}
        if(publication!=null){CheckpointInputFuture.requestStop(publication);CheckpointInputFuture.retryCleanup(publication);if(!CheckpointInputFuture.ioExited(publication))return false;}
        if(opening!=null){opening.requestStop();opening.retryCleanup();if(!opening.ioExited())return false;
            if(transport==null)transport=opening.completedValue().orElse(null);}
        if(recoveryTurn!=null){recoveryTurn.requestStop();recoveryTurn.retryCleanup();if(!recoveryTurn.ioExited())return false;}
        if(recoveryOpening!=null){recoveryOpening.requestStop();recoveryOpening.retryCleanup();if(!recoveryOpening.ioExited())return false;
            if(recovery==null)recovery=recoveryOpening.completedValue().orElse(null);}
        if(transport!=null){
            if(closing==null||closing.ioExited()){
                if(transport.closeCompleted()&&(recovery==null||recovery.closeCompleted()))closing=null;
                else{closing=OwnedCheckpointIo.submit(ignored->{if(recovery!=null)recovery.close();transport.close();return transport.closeCompleted();},()->{},io);return false;}
            }
            if(closing!=null&&!closing.ioExited())return false;
            if(!transport.closeCompleted()||recovery!=null&&!recovery.closeCompleted())return false;
        }
        if(registered){physicalAdmission.unregisterTargetChannel(channel);registered=false;}
        closed=true;return true;
    }
    @Override public void close(){if(!settleClose())throw new IllegalStateException("K2 native cleanup remains owned and pending");}
}
