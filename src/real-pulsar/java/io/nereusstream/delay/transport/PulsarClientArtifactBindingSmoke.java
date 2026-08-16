package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.PulsarSendRequest;
import io.nereusstream.delay.adapter.PulsarSendResult;
import io.nereusstream.delay.protocol.Bytes;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendErrorEvidence;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TopicResourceGuardException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Focused API/evidence smoke for the source-locked P1 client artifact. */
public final class PulsarClientArtifactBindingSmoke {
    private static final String CLUSTER = "pulsar-binding-cluster";
    private static final String TOPIC = "persistent://tenant/ns/pulsar-binding-smoke";
    private static final byte[] INCARNATION = digest(7);
    private static final long CREATION_TIMESTAMP = 11L;

    private PulsarClientArtifactBindingSmoke() {
    }

    public static void main(final String[] arguments) {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final GuardedSendSuccessEvidence successEvidence = new GuardedSendSuccessEvidence(
                22, 3, 4, 5, new TopicResourceGuardAttestation(guard, TOPIC, 0), 17, 19, 23,
                digest(31), digest(41));
        final GuardedMessageId guardedMessageId = guardedMessageId(guard, successEvidence);
        final Producer<byte[]> producer = producer(TOPIC, guardedMessageId, null);
        final PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                producer, CLUSTER, INCARNATION, TOPIC, CREATION_TIMESTAMP, 0);
        final PulsarSendRequest request = new PulsarSendRequest(CLUSTER, INCARNATION, TOPIC,
                CREATION_TIMESTAMP, 0, io.nereusstream.delay.protocol.CommandId.random(
                        new io.nereusstream.delay.protocol.ShardId(
                                io.nereusstream.delay.protocol.RouteIncarnation.random(), 0)),
                Bytes.utf8("pulsar-binding-frame"));
        final PulsarSendResult persisted = transport.send(request).toCompletableFuture().join();
        if (persisted.disposition() != PulsarSendResult.Disposition.PERSISTED
                || persisted.ledgerId() != 17 || persisted.entryId() != 19
                || persisted.brokerEntryTimestampEpochMs() != 23) {
            throw new IllegalStateException("P1 success evidence was not projected exactly");
        }

        final GuardedSendSuccessEvidence mismatchedEvidence = new GuardedSendSuccessEvidence(
                22, 3, 4, 5, new TopicResourceGuardAttestation(guard, TOPIC, 0), 18, 19, 23,
                digest(31), digest(41));
        final PulsarSendResult mismatched = new PulsarClientArtifactSendTransport(
                producer(TOPIC, guardedMessageId(guard, mismatchedEvidence), null), CLUSTER, INCARNATION,
                TOPIC, CREATION_TIMESTAMP, 0).send(request).toCompletableFuture().join();
        if (mismatched.disposition() != PulsarSendResult.Disposition.UNKNOWN
                || mismatched.stableCode() != io.nereusstream.delay.protocol.StableCode.INTEGRITY_ERROR.wireValue()) {
            throw new IllegalStateException("P1 mismatched success evidence was accepted as persisted");
        }

        final byte[] replacement = digest(99);
        final PulsarSendResult mismatch = transport.send(new PulsarSendRequest(CLUSTER, replacement, TOPIC,
                CREATION_TIMESTAMP, 0, request.commandId(), request.frame()))
                .toCompletableFuture().join();
        if (mismatch.disposition() == PulsarSendResult.Disposition.PERSISTED) {
            throw new IllegalStateException("P1 resource mismatch was accepted as persisted");
        }

        final GuardedSendErrorEvidence errorEvidence = new GuardedSendErrorEvidence(
                22, 6, 7, 8, 26, digest(51), digest(61));
        final TopicResourceGuardException guardFailure = new TopicResourceGuardException(
                "resource incarnation mismatch", guard, Optional.of(errorEvidence), true);
        final Producer<byte[]> rejectingProducer = producer(TOPIC, null, guardFailure);
        final PulsarClientArtifactSendTransport rejectingTransport = new PulsarClientArtifactSendTransport(
                rejectingProducer, CLUSTER, INCARNATION, TOPIC, CREATION_TIMESTAMP, 0);
        final PulsarSendResult rejected = rejectingTransport.send(request).toCompletableFuture().join();
        if (rejected.disposition() != PulsarSendResult.Disposition.DEFINITIVELY_NOT_PERSISTED) {
            throw new IllegalStateException("P1 typed guard rejection was not preserved");
        }
        System.out.println("P1 Delay binding smoke passed: persisted=" + persisted.disposition()
                + ", mismatch=" + mismatch.disposition() + ", rejection=" + rejected.disposition());
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> producer(final String topic, final GuardedMessageId messageId,
                                             final Throwable failure) {
        final TypedMessageBuilder<byte[]> builder = (TypedMessageBuilder<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactBindingSmoke.class.getClassLoader(),
                new Class<?>[]{TypedMessageBuilder.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("value")) {
                        return proxy;
                    }
                    if (method.getName().equals("sendAsync")) {
                        return failure == null ? CompletableFuture.completedFuture(messageId)
                                : CompletableFuture.failedFuture(failure);
                    }
                    return defaultValue(method.getReturnType(), proxy);
                });
        return (Producer<byte[]>) Proxy.newProxyInstance(PulsarClientArtifactBindingSmoke.class.getClassLoader(),
                new Class<?>[]{Producer.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("getTopic")) {
                        return topic;
                    }
                    if (method.getName().equals("newMessage")) {
                        return builder;
                    }
                    return defaultValue(method.getReturnType(), null);
                });
    }

    private static GuardedMessageId guardedMessageId(final TopicResourceGuard guard,
                                                      final GuardedSendSuccessEvidence evidence) {
        return (GuardedMessageId) Proxy.newProxyInstance(PulsarClientArtifactBindingSmoke.class.getClassLoader(),
                new Class<?>[]{GuardedMessageId.class, MessageIdAdv.class}, (proxy, method, arguments) -> {
                    return switch (method.getName()) {
                        case "resourceGuard" -> guard;
                        case "physicalTopic" -> TOPIC;
                        case "partition" -> 0;
                        case "brokerEntryTimestamp" -> 23L;
                        case "responseEvidence" -> evidence;
                        case "getLedgerId" -> 17L;
                        case "getEntryId" -> 19L;
                        case "getPartitionIndex" -> 0;
                        case "getBatchIndex" -> -1;
                        case "getBatchSize" -> 0;
                        default -> defaultValue(method.getReturnType(), null);
                    };
                });
    }

    private static Object defaultValue(final Class<?> returnType, final Object fallback) {
        if (!returnType.isPrimitive()) {
            return fallback;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == byte.class || returnType == short.class || returnType == int.class
                || returnType == long.class || returnType == float.class || returnType == double.class) {
            return 0;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return null;
    }

    private static byte[] digest(final int seed) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
    }
}
