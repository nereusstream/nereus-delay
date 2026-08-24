package com.nereusstream.delay.protocol;

/** Empty Registry §6.3 branch that requests the checkpoint catalog. */
public final class GetCheckpointCatalogRequestV1 implements ControlOperationRequestBranchV1 {
    private static final GetCheckpointCatalogRequestV1 INSTANCE = new GetCheckpointCatalogRequestV1();

    public GetCheckpointCatalogRequestV1() {}

    public static GetCheckpointCatalogRequestV1 instance() {
        return INSTANCE;
    }

    @Override
    public byte[] canonicalBytes() {
        return new byte[0];
    }

    public static GetCheckpointCatalogRequestV1 decode(final byte[] encoded) {
        if (encoded == null || encoded.length != 0) {
            throw new IllegalArgumentException("GetCheckpointCatalogRequestV1 must be empty");
        }
        return INSTANCE;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof GetCheckpointCatalogRequestV1;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}
