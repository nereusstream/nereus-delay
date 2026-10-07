package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.PulsarDestinationRequest;
import com.nereusstream.delay.adapter.PulsarNativeSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NativePreparedRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Producer;

/** No-Broker H0 smoke proving both real Pulsar transports stop before Producer ownership. */
public final class PulsarClientArtifactH0Smoke {
    private static final String CLUSTER = "h0-cluster";
    private static final String TOPIC = "persistent://public/default/h0-smoke";
    private static final byte[] RESOURCE_INCARNATION = digest("h0-resource");
    private static final long TOPIC_CREATION_TIMESTAMP = 8_001L;

    private PulsarClientArtifactH0Smoke() {}

    public static void main(final String[] arguments) {
        if (arguments.length != 0) {
            throw new IllegalArgumentException("usage: no arguments; this smoke does not contact a Broker");
        }

        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("h0-lane"));
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final PulsarDestinationRequest destinationRequest = new PulsarDestinationRequest(
                CLUSTER,
                RESOURCE_INCARNATION,
                TOPIC,
                TOPIC_CREATION_TIMESTAMP,
                0,
                lane,
                new byte[16],
                messageId,
                0,
                digest("h0-publish-attempt"),
                900,
                1_000,
                Bytes.utf8("h0-payload"),
                new byte[0]);
        final SourcePosition sourcePosition =
                new KafkaSourcePosition(shard, "h0-source", UUID.randomUUID(), 0, null, 1_000);
        final byte[] preparedPublishHash = digest("h0-prepared-publish");
        final CountingProducer destinationProducer = new CountingProducer(TOPIC);
        final PulsarClientArtifactDestinationTransport destinationTransport =
                new PulsarClientArtifactDestinationTransport(
                        destinationProducer.proxy(),
                        CLUSTER,
                        RESOURCE_INCARNATION,
                        TOPIC,
                        TOPIC_CREATION_TIMESTAMP,
                        0,
                        digest("h0-producer"));
        try {
            final DestinationPublishResult result = destinationTransport
                    .publish(destinationRequest, sourcePosition, preparedPublishHash)
                    .toCompletableFuture()
                    .join();
            require(
                    result.disposition() == DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED,
                    "Managed real transport did not return definitive non-publication");
            require(
                    result.stableCode() == StableCode.CAPABILITY_UNAVAILABLE,
                    "Managed real transport returned the wrong H0 code");
        } finally {
            destinationTransport.close();
        }
        destinationProducer.requireUntouched("Managed destination transport");

        final CountingProducer nativeProducer = new CountingProducer(TOPIC);
        final PulsarClientArtifactSendTransport nativeTransport = new PulsarClientArtifactSendTransport(
                nativeProducer.proxy(), CLUSTER, RESOURCE_INCARNATION, TOPIC, TOPIC_CREATION_TIMESTAMP, 0);
        final PulsarNativeSendRequest nativeRequest = new PulsarNativeSendRequest(
                CLUSTER,
                RESOURCE_INCARNATION,
                TOPIC,
                TOPIC_CREATION_TIMESTAMP,
                0,
                nonZero(NativePreparedRef.NATIVE_DELIVERY_ID_LENGTH),
                digest("h0-submission"),
                Bytes.utf8("h0-native-prepared-record"));
        try {
            final PulsarSendResult result =
                    nativeTransport.send(nativeRequest).toCompletableFuture().join();
            require(
                    result.disposition() == PulsarSendResult.Disposition.DEFINITIVELY_NOT_PERSISTED,
                    "AUTO_FAST real transport did not return definitive non-persistence");
            require(
                    result.stableCode() == StableCode.CAPABILITY_UNAVAILABLE.wireValue(),
                    "AUTO_FAST real transport returned the wrong H0 code");
        } finally {
            nativeTransport.close();
        }
        nativeProducer.requireUntouched("AUTO_FAST real transport");

        verifyPreparedOwnershipGate(shard, messageId);

        System.out.println("Pulsar H0 smoke passed: managed.newMessage=0, managed.sendAsync=0, "
                + "native.newMessage=0, native.sendAsync=0");
    }

    private static void verifyPreparedOwnershipGate(final ShardId shard, final DelayMessageId messageId) {
        final var artifacts = com.nereusstream.delay.protocol.ArtifactGenerationSet.current(
                1, com.nereusstream.delay.protocol.PulsarSourceLock.digest(), digest("gate-schema"));
        final byte[] payload = Bytes.utf8("gate-payload");
        final byte[] attempt = digest("gate-attempt");
        final var reserved = new com.nereusstream.delay.protocol.ReservedPublishMetadata(
                shard.routeIncarnation(), shard.unsignedPartition(), messageId, 0, attempt,
                digest("destination"), digest("capability"), 1000,
                com.nereusstream.delay.protocol.DeliveryMode.MANAGED);
        final var template = new com.nereusstream.delay.protocol.PulsarRecordTemplate(
                com.nereusstream.delay.protocol.BrokerResourceIdentity.pulsar(
                        new com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity(
                                CLUSTER, RESOURCE_INCARNATION, TOPIC, TOPIC_CREATION_TIMESTAMP)),
                0, com.nereusstream.delay.protocol.PulsarKey.none(), null, java.util.List.of(), null,
                reserved, com.nereusstream.delay.protocol.DeliveryContract.NEREUS_MANAGED_NOT_BEFORE, null,
                com.nereusstream.delay.protocol.PayloadForPublish.inline(payload), artifacts.setDigest());
        final byte[] prepared = digest("gate-prepared");
        final var record = new com.nereusstream.delay.protocol.PulsarPreparedRecord(
                template, template.recordTemplateHash(), com.nereusstream.delay.protocol.ResolvedPayload.of(payload),
                com.nereusstream.delay.protocol.PulsarSequenceAuthority.managedJournal(
                        digest("fixture-mapping"), 1, digest("h0-producer")),
                com.nereusstream.delay.protocol.ExternalDeliveryIdentity.publishAttempt(attempt), prepared,
                com.nereusstream.delay.protocol.PulsarReservedProperties.all(reserved, attempt, prepared),
                artifacts.setDigest());
        final var producer = new CountingProducer(TOPIC);
        final var transport = new PulsarClientArtifactDestinationTransport(
                producer.proxy(), CLUSTER, RESOURCE_INCARNATION, TOPIC, TOPIC_CREATION_TIMESTAMP,
                0, digest("h0-producer"));
        final AtomicInteger gateCalls = new AtomicInteger();
        final var ownerActive = new java.util.concurrent.atomic.AtomicBoolean(true);
        producer.afterProperties = () -> ownerActive.set(false);
        final var denied = transport.publishPreparedRecord(record, artifacts, (r, a) -> {
            require(producer.propertiesConfigured, "gate ran before SDK properties construction");
            require(producer.sendAsyncCalls.get() == 0, "gate ran after library SEND ownership");
            require(!ownerActive.get(), "SDK construction did not observe the fixture Owner revocation");
            gateCalls.incrementAndGet();
            return DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null);
        }).toCompletableFuture().join();
        require(denied.disposition() == DestinationPublishResult.Disposition.UNKNOWN, "denial was changed");
        require(gateCalls.get() == 1 && producer.sendAsyncCalls.get() == 0, "denial called SEND");
        transport.publishPreparedRecord(record, artifacts, (r, a) -> {
            throw new IllegalStateException("fixture Owner authority is unavailable");
        }).toCompletableFuture().join();
        require(producer.sendAsyncCalls.get() == 0, "unavailable gate called SEND");
        transport.publishPreparedRecord(record, artifacts, (r, a) -> {
            require(producer.propertiesConfigured, "permitted gate ran before SDK properties");
            return null;
        }).toCompletableFuture().join();
        require(producer.sendAsyncCalls.get() == 1, "permitted gate did not call SEND exactly once");
        producer.throwOnSend = true;
        boolean propagated = false;
        try {
            transport.publishPreparedRecord(record, artifacts, (r, a) -> null);
        } catch (IllegalStateException unobservedSend) {
            propagated = true;
        }
        require(propagated, "unobserved SEND was converted into a completed physical stage");
        transport.close();
        System.out.println("Prepared SDK ownership gate passed: after properties/before SEND; "
                + "denied=0, unavailable=0, allowed=1, synchronous SEND uncertainty propagates");
    }

    private static byte[] digest(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }

    private static byte[] nonZero(final int length) {
        final byte[] value = new byte[length];
        value[0] = 1;
        return value;
    }

    private static void require(final boolean condition, final String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static final class CountingProducer implements InvocationHandler {
        private final String topic;
        private final AtomicInteger newMessageCalls = new AtomicInteger();
        private final AtomicInteger sendAsyncCalls = new AtomicInteger();
        private final Producer<byte[]> proxy;
        private boolean propertiesConfigured;
        private boolean throwOnSend;
        private Runnable afterProperties = () -> {};

        private CountingProducer(final String topic) {
            this.topic = topic;
            this.proxy = producerProxy();
        }

        @SuppressWarnings("unchecked")
        private Producer<byte[]> producerProxy() {
            return (Producer<byte[]>)
                    Proxy.newProxyInstance(Producer.class.getClassLoader(), new Class<?>[] {Producer.class}, this);
        }

        private Producer<byte[]> proxy() {
            return proxy;
        }

        private void requireUntouched(final String label) {
            require(newMessageCalls.get() == 0, label + " called Producer.newMessage()");
            require(sendAsyncCalls.get() == 0, label + " called TypedMessageBuilder.sendAsync()");
        }

        @Override
        public Object invoke(final Object target, final Method method, final Object[] arguments) {
            return switch (method.getName()) {
                case "getTopic" -> topic;
                case "getProducerName" -> "h0-producer";
                case "newMessage" -> {
                    newMessageCalls.incrementAndGet();
                    propertiesConfigured = false;
                    yield messageBuilder(method.getReturnType());
                }
                case "close" -> null;
                case "toString" -> "counting-producer";
                case "hashCode" -> System.identityHashCode(target);
                case "equals" -> target == arguments[0];
                default -> defaultValue(method.getReturnType());
            };
        }

        private Object messageBuilder(final Class<?> builderType) {
            return Proxy.newProxyInstance(
                    builderType.getClassLoader(), new Class<?>[] {builderType}, (target, method, arguments) -> {
                        return switch (method.getName()) {
                            case "value", "key", "keyBytes", "property" -> target;
                            case "properties" -> {
                                propertiesConfigured = true;
                                afterProperties.run();
                                yield target;
                            }
                            case "sendAsync" -> {
                                sendAsyncCalls.incrementAndGet();
                                if (throwOnSend) {
                                    throw new IllegalStateException("fixture unobserved SEND ownership");
                                }
                                yield CompletableFuture.completedFuture(null);
                            }
                            case "toString" -> "counting-message-builder";
                            case "hashCode" -> System.identityHashCode(target);
                            case "equals" -> target == arguments[0];
                            default -> defaultValue(method.getReturnType());
                        };
                    });
        }

        private static Object defaultValue(final Class<?> type) {
            if (!type.isPrimitive()) {
                return null;
            }
            if (type == boolean.class) {
                return false;
            }
            if (type == char.class) {
                return '\0';
            }
            if (type == byte.class) {
                return (byte) 0;
            }
            if (type == short.class) {
                return (short) 0;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            if (type == float.class) {
                return 0.0f;
            }
            if (type == double.class) {
                return 0.0d;
            }
            throw new IllegalArgumentException("unsupported primitive return type: " + type);
        }
    }
}
