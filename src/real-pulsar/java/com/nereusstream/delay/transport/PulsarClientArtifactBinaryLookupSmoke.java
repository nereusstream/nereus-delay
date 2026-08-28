package com.nereusstream.delay.transport;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;

/** Prints the source-locked P1 binary topic lookup result for listener diagnostics. */
public final class PulsarClientArtifactBinaryLookupSmoke {
    private PulsarClientArtifactBinaryLookupSmoke() {}

    public static void main(final String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: <service-url> <topic>");
        }
        final String serviceUrl = args[0];
        final String topic = args[1];
        final String listenerName = System.getenv("NEREUS_DELAY_PULSAR_LISTENER_NAME");
        final var clientBuilder = PulsarClientArtifactClientBuilder.builder(serviceUrl);
        try (PulsarClient client = clientBuilder.build()) {
            if (!"org.apache.pulsar.client.impl.PulsarClientImpl"
                    .equals(client.getClass().getName())) {
                throw new IllegalStateException("P1 client did not expose PulsarClientImpl lookup");
            }
            final Object lookup = invoke(client, "getLookup");
            final Class<?> topicNameClass = Class.forName("org.apache.pulsar.common.naming.TopicName");
            final Object topicName =
                    topicNameClass.getMethod("get", String.class).invoke(null, topic);
            final Method getBroker = lookup.getClass().getMethod("getBroker", topicNameClass);
            final CompletableFuture<?> lookupFuture = (CompletableFuture<?>) getBroker.invoke(lookup, topicName);
            final Object result = lookupFuture.get(30, TimeUnit.SECONDS);
            final InetSocketAddress logical = (InetSocketAddress) invoke(result, "getLogicalAddress");
            final InetSocketAddress physical = (InetSocketAddress) invoke(result, "getPhysicalAddress");
            System.out.println("Pulsar binary topic lookup passed: listener=" + listenerName
                    + ", logical=" + logical
                    + ", physical=" + physical
                    + ", useProxy=" + invoke(result, "isUseProxy"));
        }
    }

    private static Object invoke(final Object target, final String methodName) throws Exception {
        try {
            return target.getClass().getMethod(methodName).invoke(target);
        } catch (InvocationTargetException exception) {
            final Throwable cause = exception.getCause();
            if (cause instanceof Exception checkedException) {
                throw checkedException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw exception;
        }
    }
}
