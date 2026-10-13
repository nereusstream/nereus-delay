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
        for (String key : operational.keySet()) {
            if (key.startsWith("ssl.") || key.startsWith("sasl.") || key.startsWith("security.")
                    || key.startsWith("config.providers") || key.equals("interceptor.classes")) {
                throw new IllegalArgumentException("new Target mTLS does not accept caller authentication overrides");
            }
        }
        final var config = new HashMap<>(operational);
        config.put("security.protocol", "SSL");
        config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        config.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG,
                new Password(new String(receipt.privateKeyPem(), StandardCharsets.US_ASCII)));
        config.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
                new Password(new String(receipt.certificateChainPem(), StandardCharsets.US_ASCII)));
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        config.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
                new Password(new String(receipt.serverTrustPem(), StandardCharsets.US_ASCII)));
        config.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        return Map.copyOf(config);
    }
}
