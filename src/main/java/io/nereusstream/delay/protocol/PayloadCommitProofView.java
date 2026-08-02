package io.nereusstream.delay.protocol;

/** Common proof projection consumed by the shard commit state machine. */
public interface PayloadCommitProofView {
    long trustSetVersion();

    int proofKeyVersion();

    byte[] routeIncarnationUuid();

    int partition();

    DelayMessageId delayMessageId();

    byte[] reservationId();

    byte[] objectStoreProfileHash();

    byte[] container();

    byte[] objectKey();

    byte[] immutableObjectVersion();

    byte[] etag();

    long length();

    byte[] payloadSha256();

    long notAfterEpochMs();

    byte[] proofId();

    byte[] signature();

    boolean verifySignature(java.security.PublicKey publicKey);
}
