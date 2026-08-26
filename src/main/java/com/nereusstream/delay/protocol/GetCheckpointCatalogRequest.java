package com.nereusstream.delay.protocol;

/** Empty Registry §6.3 branch that requests the checkpoint catalog. */
public final class GetCheckpointCatalogRequest implements ControlOperationRequestBranch {
    private static final GetCheckpointCatalogRequest INSTANCE = new GetCheckpointCatalogRequest();

    public GetCheckpointCatalogRequest() {}

    public static GetCheckpointCatalogRequest instance() {
        return INSTANCE;
    }

    @Override
    public byte[] canonicalBytes() {
        return new byte[0];
    }

    public static GetCheckpointCatalogRequest decode(final byte[] encoded) {
        if (encoded == null || encoded.length != 0) {
            throw new IllegalArgumentException("GetCheckpointCatalogRequest must be empty");
        }
        return INSTANCE;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof GetCheckpointCatalogRequest;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}
