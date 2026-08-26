package com.nereusstream.delay.protocol;

/** Canonical payload branch selected by one {@link ControlOperationKind}. */
public interface ControlOperationRequestBranch {
    /** Returns the canonical protobuf bytes for this branch, excluding the outer oneof tag. */
    byte[] canonicalBytes();
}
