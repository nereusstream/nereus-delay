package com.nereusstream.delay.protocol;

/** Canonical body common interface for the closed Profile semantic envelope. */
public interface ProfileSemanticBodyV1 {
    ProfileKindV1 profileKind();

    int schemaVersion();

    byte[] canonicalBytes();
}
