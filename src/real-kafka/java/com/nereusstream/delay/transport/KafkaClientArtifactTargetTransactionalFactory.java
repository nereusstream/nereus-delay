package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetKafkaReceiptBinding;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.kafka.clients.producer.GuardedTransactionalProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Creates/reopens the exact Target stable transactional ID, without a Lane Journal or private executor. */
public final class KafkaClientArtifactTargetTransactionalFactory {
    private KafkaClientArtifactTargetTransactionalFactory() {}

    public record Limits(Duration operationTimeout, Duration pollTimeout, int maximumRecords, int maximumBytes) {
        public Limits {
            Objects.requireNonNull(operationTimeout, "operation timeout");
            Objects.requireNonNull(pollTimeout, "poll timeout");
            if (operationTimeout.compareTo(Duration.ofSeconds(1)) < 0
                    || operationTimeout.compareTo(Duration.ofSeconds(60)) > 0
                    || pollTimeout.isZero() || pollTimeout.isNegative() || pollTimeout.compareTo(operationTimeout) > 0
                    || maximumRecords <= 0 || maximumBytes <= 0) {
                throw new IllegalArgumentException("Target K2 requires finite SDK/read bounds");
            }
        }
    }

    /** Independent current Owner/CONTROL channel/resource/credential/retention/Artifact authority. */
    @FunctionalInterface
    public interface Activation {
        OwnerIdentity requireCurrent(TargetChannelIdentity channel, TargetKafkaReceiptBinding resource, byte[] actualSdkJarSha256);
        default void requireAdoption(TargetKafkaReceiptMapping mapping) {
            throw new IllegalStateException("cold K2 activation has no original Source/external publication adoption authority");
        }
        default com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt() { return null; }
        default void requireLoadedMaterial(TargetChannelIdentity channel) {
            if (credentialReceipt() != null) { throw new IllegalStateException("verified K2 activation lacks a local material gate"); }
        }
    }

    public static CompletableFuture<KafkaClientArtifactTargetTransactionalTransport> open(
            Map<String, Object> producerConfiguration, Map<String, Object> readerConfiguration,
            CanonicalTargetPartition physical, TargetChannelIdentity channel, TargetKafkaReceiptBinding receipt, Activation activation,
            KafkaClientArtifactTargetReceiptReader.Protection protection, Executor io, Limits limits) {
        return open(null, producerConfiguration, readerConfiguration, physical, channel, receipt, activation, protection, io, limits);
    }

    /** Actual fencing is newly executed for this retained mapping, never recovered from an old local flag. */
    public static CompletableFuture<KafkaClientArtifactTargetTransactionalTransport> reopen(
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
            Map<String, Object> producerConfiguration, Map<String, Object> readerConfiguration,
            TargetChannelIdentity channel, Activation activation,
            KafkaClientArtifactTargetReceiptReader.Protection protection, Executor io, Limits limits) {
        Objects.requireNonNull(mapping, "retained mapping");
        com.nereusstream.delay.protocol.PublishEvidence.requireTargetFencedChannel(mapping.admission().channel(), channel);
        return open(mapping, producerConfiguration, readerConfiguration, mapping.admission().publication().physical(),
                channel, mapping.admission().kafkaReceipt(),
                activation, protection, io, limits);
    }

    private static CompletableFuture<KafkaClientArtifactTargetTransactionalTransport> open(
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
            Map<String, Object> producerConfiguration, Map<String, Object> readerConfiguration,
            CanonicalTargetPartition physical, TargetChannelIdentity channel, TargetKafkaReceiptBinding receipt, Activation activation,
            KafkaClientArtifactTargetReceiptReader.Protection protection, Executor io, Limits limits) {
        Objects.requireNonNull(channel, "actual durable channel");
        Objects.requireNonNull(receipt, "receipt context");
        Objects.requireNonNull(activation, "actual activation rights");
        Objects.requireNonNull(protection, "actual mapping/receipt protection");
        Objects.requireNonNull(io, "caller bounded I/O executor");
        Objects.requireNonNull(limits, "limits");
        if (channel.context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                || Objects.requireNonNull(physical, "actual canonical Target").resource().kind()
                        != com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.KAFKA
                || !physical.id().equals(channel.context().target())
                || !physical.resource().kafka().authenticatedClusterId().equals(receipt.receiptResource().authenticatedClusterId())) {
            throw new IllegalArgumentException("K2 factory requires a Target transactional channel");
        }
        final var config = new HashMap<>(secured(producerConfiguration, activation));
        final var reads = secured(readerConfiguration, activation);
        if (config.containsKey(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)) {
            throw new IllegalArgumentException("Target K2 cannot run record-transforming interceptors");
        }
        final String stableId = new String(channel.context().producerIdentity(), StandardCharsets.UTF_8);
        if (config.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
                && !stableId.equals(config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))) {
            throw new IllegalArgumentException("Target transaction configuration changes the durable stable ID");
        }
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, stableId);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        final int millis = Math.toIntExact(limits.operationTimeout().toMillis());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) millis);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(millis, 10_000));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, millis);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, millis);
        final var opened = new CompletableFuture<KafkaClientArtifactTargetTransactionalTransport>();
        io.execute(() -> {
            try {
                final var transport = createTransport(mapping, config, reads, physical, channel, receipt, activation,
                        protection, io, limits, null, null, () -> {});
                if (!opened.complete(transport)) { transport.close(); }
            } catch (RuntimeException | Error failure) {
                opened.completeExceptionally(failure);
                if (failure instanceof Error fatal) { throw fatal; }
            }
        });
        return opened;
    }

    /** Production opening owns queue/no-start, original cleanup handle and a pre-reserved SDK generation. */
    public static com.nereusstream.delay.ownership.OwnedCheckpointIo<KafkaClientArtifactTargetTransactionalTransport> openOwned(
            TargetKafkaReceiptMapping original, Map<String,Object> producerConfiguration, Map<String,Object> readerConfiguration,
            CanonicalTargetPartition physical, TargetChannelIdentity channel, TargetKafkaReceiptBinding receipt,
            Activation activation, KafkaClientArtifactTargetReceiptReader.Protection protection,
            java.util.concurrent.ThreadPoolExecutor io, Limits limits,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client producerBudget,
            java.util.function.Supplier<com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client> readers) {
        Objects.requireNonNull(producerBudget,"reserved producer generation"); Objects.requireNonNull(readers,"finite reader generations");
        if(original!=null){com.nereusstream.delay.protocol.PublishEvidence.requireTargetFencedChannel(original.admission().channel(),channel);}
        final var config = new HashMap<>(secured(producerConfiguration, activation));
        final String stable = new String(channel.context().producerIdentity(),StandardCharsets.UTF_8);
        if(config.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)&&!stable.equals(config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)))
            throw new IllegalArgumentException("K2 configuration changes stable producer identity");
        if(config.containsKey(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG))throw new IllegalArgumentException("K2 forbids transforming interceptors");
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,stable);config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        config.put(ProducerConfig.ACKS_CONFIG,"all");config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,1);
        config.put(ProducerConfig.LINGER_MS_CONFIG,0);
        final int millis=Math.toIntExact(limits.operationTimeout().toMillis());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,(long)millis);config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,Math.min(millis,10000));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,millis);config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,millis);
        final var actualConfig=Map.copyOf(config); final var actualReads=secured(readerConfiguration, activation);
        try {
            final var opening=com.nereusstream.delay.ownership.OwnedCheckpointIo.submitResource(guard ->
                    createTransport(original,actualConfig,actualReads,physical,channel,receipt,activation,protection,
                            io,limits,producerBudget,readers,guard), () -> {}, value -> {
                try { value.close(); }
                catch(RuntimeException | Error failure){throw new KafkaClientArtifactTargetCleanupFailure(failure,value::close);}
            },io);
            opening.receipt().whenComplete((value,failure)->{
                if(failure!=null){try{producerBudget.releaseUnstarted();}catch(IllegalStateException started){/* actual native owner retains charge */}}
            });
            return opening;
        } catch(RuntimeException | Error rejected){producerBudget.releaseUnstarted();throw rejected;}
    }

    private static KafkaClientArtifactTargetTransactionalTransport createTransport(TargetKafkaReceiptMapping mapping,
            Map<String,Object> config,Map<String,Object> reads,CanonicalTargetPartition physical,TargetChannelIdentity channel,
            TargetKafkaReceiptBinding receipt,Activation activation,KafkaClientArtifactTargetReceiptReader.Protection protection,
            Executor io,Limits limits,com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client budget,
            java.util.function.Supplier<com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client> readerBudgets,Runnable guard) {
        if(channel.context().kind()!=ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                ||physical.resource().kind()!=com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.KAFKA
                ||!physical.id().equals(channel.context().target())
                ||!physical.resource().kafka().authenticatedClusterId().equals(receipt.receiptResource().authenticatedClusterId()))
            throw new IllegalArgumentException("K2 opening changes exact Source Target/cluster/channel");
        Producer<byte[],byte[]> nativeProducer=null;
        boolean constructing=false;
        try {
            guard.run();final byte[] actualJar=KafkaClientArtifactTargetReceiptReader.requireSdkIdentity();
            final var owner=Objects.requireNonNull(activation.requireCurrent(channel,receipt,actualJar),"actual current Owner");
            if(mapping!=null)activation.requireAdoption(mapping);
            guard.run(); if(budget!=null)budget.beginConstruction();constructing=true;
            nativeProducer=new KafkaProducer<>(config,new ByteArraySerializer(),new ByteArraySerializer());
            if(!(nativeProducer instanceof GuardedTransactionalProducer<byte[],byte[]> guarded))throw new IllegalStateException("locked K1 guarded producer required");
            final var target=new ProducerResourceGuard(receipt.receiptResource().authenticatedClusterId(),receipt.targetPhysicalTopic(),
                    new Uuid(physical.resource().kafka().nativeTopicUuid().getMostSignificantBits(),physical.resource().kafka().nativeTopicUuid().getLeastSignificantBits()),
                    Math.toIntExact(physical.physicalPartition()));
            final var receiptGuard=new ProducerResourceGuard(receipt.receiptResource().authenticatedClusterId(),receipt.receiptPhysicalTopic(),
                    new Uuid(receipt.receiptResource().receiptTopicUuid().getMostSignificantBits(),receipt.receiptResource().receiptTopicUuid().getLeastSignificantBits()),receipt.receiptPartition());
            guard.run();guarded.requireResourceGuard(target);guarded.requireResourceGuard(receiptGuard);
            guard.run();requireOwner(owner,activation.requireCurrent(channel,receipt,actualJar));guarded.initTransactions();
            guard.run();guarded.requireResourceGuard(target);guarded.requireResourceGuard(receiptGuard);
            requireOwner(owner,activation.requireCurrent(channel,receipt,actualJar));guard.run();
            final var transport=new KafkaClientArtifactTargetTransactionalTransport(guarded,channel,receipt,io,reads,protection,
                    limits.operationTimeout(),limits.pollTimeout(),limits.maximumRecords(),limits.maximumBytes(),budget,readerBudgets);
            if (activation.credentialReceipt() != null) {
                transport.installCredentialGate(() -> activation.requireLoadedMaterial(channel));
            }
            if(mapping!=null)transport.installFence(new Fence(mapping,channel,owner,actualJar,()->{
                requireOwner(owner,activation.requireCurrent(channel,receipt,actualJar));
                protection.requireCurrent(mapping,mapping.admission().kafkaReceipt().scanStartOffset());
            }));
            return transport;
        } catch(RuntimeException | Error failure){
            if(nativeProducer!=null){final var actual=nativeProducer;
                try{KafkaClientArtifactProducerCleanup.close(actual, limits.operationTimeout());if(budget!=null)budget.closed();}
                catch(RuntimeException | Error closeFailure){final var held=new KafkaClientArtifactTargetCleanupFailure(failure,()->{
                    KafkaClientArtifactProducerCleanup.close(actual, limits.operationTimeout());if(budget!=null)budget.closed();});held.addSuppressed(closeFailure);throw held;}}
            else if (failure instanceof KafkaProducer.ConstructionCleanupFailure partial) {
                throw KafkaClientArtifactProducerCleanup.partial(partial, limits.operationTimeout(), () -> { if (budget != null) budget.closed(); });
            } else if(budget!=null){if(constructing && failure instanceof org.apache.kafka.common.config.ConfigException)budget.closed();else if (!constructing) budget.releaseUnstarted();
                else throw new KafkaClientArtifactTargetCleanupFailure(failure, () -> { throw new IllegalStateException("producer construction has no native termination fact"); });}
            throw failure;
        }
    }

    private static void requireOwner(OwnerIdentity expected, OwnerIdentity current) {
        if (!expected.equals(Objects.requireNonNull(current, "actual current Owner"))) {
            throw new IllegalStateException("Target producer initialization crossed an Owner/session boundary");
        }
    }

    private static Map<String,Object> secured(Map<String,Object> operational, Activation activation) {
        return activation.credentialReceipt() == null ? Map.copyOf(operational)
                : KafkaClientArtifactMtlsConfiguration.apply(operational, activation.credentialReceipt());
    }

    /** Local private observation of this actual same-ID initialization, never a Broker-signed certificate. */
    public static final class Fence {
        private final com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping;
        private final TargetChannelIdentity channel;
        private final Runnable rights;
        private final byte[] image;
        private Fence(com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
                TargetChannelIdentity channel, OwnerIdentity owner, byte[] actualSdk, Runnable rights) {
            this(mapping,channel,owner,actualSdk,rights,null,null);
        }
        // Only the actual typed recovery opening supplies material/Store context; old v1 images are not reinterpreted.
        Fence(com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
                TargetChannelIdentity channel, OwnerIdentity owner, byte[] actualSdk, Runnable rights,
                byte[] recoveryMaterial, byte[] runtimeContext) {
            this.mapping = mapping; this.channel = channel; this.rights = rights;
            if ((recoveryMaterial == null) != (runtimeContext == null)
                    || recoveryMaterial != null && (recoveryMaterial.length == 0 || recoveryMaterial.length > 65536
                        || runtimeContext.length == 0 || runtimeContext.length > 65536))
                throw new IllegalArgumentException("recovery fence requires bounded actual material and runtime context");
            rights.run();
            final var id = java.util.UUID.randomUUID();
            image = com.nereusstream.delay.protocol.CanonicalProtobuf.message(out -> {
                com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 1, recoveryMaterial == null ? 1 : 2);
                com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 2, mapping.mappingId());
                com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 3, channel.canonicalBytes());
                com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 4, owner.canonicalBytes());
                com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 5, actualSdk);
                com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 6,
                        com.nereusstream.delay.protocol.Bytes.utf8(KafkaClientArtifactTargetReceiptReader.REQUIRED_K1_SOURCE_COMMIT));
                com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 7, java.nio.ByteBuffer.allocate(16)
                        .putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array());
                if (recoveryMaterial != null) {
                    com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 8, recoveryMaterial);
                    com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 9, runtimeContext);
                }
            });
        }
        public TargetChannelIdentity channel() { return channel; }
        public byte[] canonicalBytes() { return com.nereusstream.delay.protocol.Bytes.copy(image); }
        public void requireCurrent(com.nereusstream.delay.protocol.TargetKafkaReceiptMapping actual) {
            if (!java.util.Arrays.equals(mapping.canonicalBytes(), actual.canonicalBytes())) {
                throw new IllegalArgumentException("Fence belongs to another exact retained mapping");
            }
            rights.run();
        }
    }
}
