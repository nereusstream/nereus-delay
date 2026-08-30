package com.nereusstream.delay.transport;

import java.util.Objects;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;

/** Builds a real P1 client with the explicitly selected listener, when configured. */
public final class PulsarClientArtifactClientBuilder {
    private PulsarClientArtifactClientBuilder() {}

    public static String clusterId() {
        final String configuredClusterId = System.getenv("NEREUS_DELAY_PULSAR_CLUSTER_ID");
        return configuredClusterId == null || configuredClusterId.isBlank() ? "standalone" : configuredClusterId;
    }

    public static ClientBuilder builder(final String serviceUrl) {
        final ClientBuilder clientBuilder =
                PulsarClient.builder().serviceUrl(Objects.requireNonNull(serviceUrl, "serviceUrl"));
        final String listenerName = System.getenv("NEREUS_DELAY_PULSAR_LISTENER_NAME");
        if (listenerName != null && !listenerName.isBlank()) {
            clientBuilder.listenerName(listenerName);
        }
        return clientBuilder;
    }
}
