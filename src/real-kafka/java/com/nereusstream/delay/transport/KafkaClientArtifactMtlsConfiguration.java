package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;

/** Only the resolved snapshot supplies SSL configuration; caller properties cannot replace its material or policy. */
public final class KafkaClientArtifactMtlsConfiguration {
    private KafkaClientArtifactMtlsConfiguration() {}

    public static Map<String, Object> apply(Map<String, Object> operational, LocalBrokerMtlsResolver.Receipt receipt) {
        receipt.requireKafkaBootstrap(operational.get("bootstrap.servers"));
        return configure(operational, receipt.privateKeyPem(), receipt.certificateChainPem(), receipt.serverTrustPem());
    }

    public static Map<String, Object> applySource(
            Map<String, Object> operational, LocalBrokerMtlsResolver.SourceReceipt receipt) {
        receipt.requireKafkaBootstrap(operational.get("bootstrap.servers"));
        return configure(operational, receipt.privateKeyPem(), receipt.certificateChainPem(), receipt.serverTrustPem());
    }

    public static Map<String, Object> applyTargetRecovery(
            Map<String, Object> operational, LocalBrokerMtlsResolver.TargetRecoveryReceipt receipt) {
        receipt.requireKafkaBootstrap(operational.get("bootstrap.servers"));
        return configure(operational, receipt.privateKeyPem(), receipt.certificateChainPem(), receipt.serverTrustPem());
    }

    private static Map<String, Object> configure(
            Map<String, Object> operational, byte[] keyPem, byte[] certPem, byte[] trustPem) {
        for (String key : operational.keySet()) {
            if (key.startsWith("ssl.")
                    || key.startsWith("sasl.")
                    || key.startsWith("security.")
                    || key.startsWith("config.providers")
                    || key.equals("interceptor.classes")
                    || key.equals("metric.reporters")
                    || key.equals("partitioner.class")
                    || key.equals("partition.assignment.strategy")) {
                throw new IllegalArgumentException("new Target mTLS does not accept caller authentication overrides");
            }
            if (key.equals("key.serializer") || key.equals("value.serializer")) {
                requireCodec(operational.get(key), org.apache.kafka.common.serialization.ByteArraySerializer.class);
            } else if (key.equals("key.deserializer") || key.equals("value.deserializer")) {
                requireCodec(operational.get(key), org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
            }
        }
        final var config = new HashMap<>(operational);
        config.put("security.protocol", "SSL");
        config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        config.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, new Password(new String(keyPem, StandardCharsets.US_ASCII)));
        config.put(
                SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
                new Password(new String(certPem, StandardCharsets.US_ASCII)));
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        config.put(
                SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
                new Password(new String(trustPem, StandardCharsets.US_ASCII)));
        config.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        return Map.copyOf(config);
    }

    private static void requireCodec(Object configured, Class<?> expected) {
        if (configured != expected && !expected.getName().equals(configured)) {
            throw new IllegalArgumentException("verified Broker mTLS requires the original byte-array codecs");
        }
    }
}
