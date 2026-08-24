package com.nereusstream.delay.protocol;

/** Canonical payload branch selected by one {@link ControlOperationKindV1}. */
public interface ControlOperationRequestBranchV1 {
    /** Returns the canonical protobuf bytes for this branch, excluding the outer oneof tag. */
    byte[] canonicalBytes();
}
