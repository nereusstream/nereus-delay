package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Authorization-safe destination binding projection for public views. */
public final class PublicDestinationBindingView {
    private static final int MAX_ALIAS_BYTES = 256;

    private final ProfileRef destinationProfile;
    private final ProfileRef capabilityProfile;
    private final AdapterKind adapterKind;
    private final byte[] destinationAliasUtf8Nfc;
    private final int physicalPartition;
    private final OrderingMode orderingMode;

    public PublicDestinationBindingView(
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final AdapterKind adapterKind,
            final byte[] destinationAliasUtf8Nfc,
            final int physicalPartition,
            final OrderingMode orderingMode) {
        this.destinationProfile = Objects.requireNonNull(destinationProfile, "destinationProfile");
        this.capabilityProfile = Objects.requireNonNull(capabilityProfile, "capabilityProfile");
        if (destinationProfile.profileKind() != ProfileKind.DESTINATION
                || capabilityProfile.profileKind() != ProfileKind.DELIVERY_CAPABILITY) {
            throw new IllegalArgumentException("public binding ProfileRef kinds do not match their slots");
        }
        this.adapterKind = Objects.requireNonNull(adapterKind, "adapterKind");
        this.destinationAliasUtf8Nfc = requireAlias(destinationAliasUtf8Nfc);
        this.physicalPartition = physicalPartition;
        this.orderingMode = Objects.requireNonNull(orderingMode, "orderingMode");
    }

    public ProfileRef destinationProfile() {
        return destinationProfile;
    }

    public ProfileRef capabilityProfile() {
        return capabilityProfile;
    }

    public AdapterKind adapterKind() {
        return adapterKind;
    }

    public byte[] destinationAliasUtf8Nfc() {
        return Bytes.copy(destinationAliasUtf8Nfc);
    }

    public int physicalPartition() {
        return physicalPartition;
    }

    public OrderingMode orderingMode() {
        return orderingMode;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, destinationProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, capabilityProfile.canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, adapterKind.wireValue());
            CanonicalProtobuf.bytes(output, 4, destinationAliasUtf8Nfc);
            CanonicalProtobuf.uint32Bits(output, 5, physicalPartition);
            CanonicalProtobuf.uint32(output, 6, orderingMode.wireValue());
        });
    }

    public static PublicDestinationBindingView decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PublicDestinationBindingView");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6}, "PublicDestinationBindingView");
        final PublicDestinationBindingView result = new PublicDestinationBindingView(
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                AdapterKind.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                QueryCodecSupport.bytes(fields.get(3), 4),
                QueryCodecSupport.uint32Bits(fields.get(4), 5),
                OrderingMode.fromWire(QueryCodecSupport.uint(fields.get(5), 6)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PublicDestinationBindingView");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PublicDestinationBindingView that)) {
            return false;
        }
        return physicalPartition == that.physicalPartition
                && destinationProfile.equals(that.destinationProfile)
                && capabilityProfile.equals(that.capabilityProfile)
                && adapterKind == that.adapterKind
                && orderingMode == that.orderingMode
                && Arrays.equals(destinationAliasUtf8Nfc, that.destinationAliasUtf8Nfc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                destinationProfile,
                capabilityProfile,
                adapterKind,
                Arrays.hashCode(destinationAliasUtf8Nfc),
                physicalPartition,
                orderingMode);
    }

    private static byte[] requireAlias(final byte[] value) {
        Objects.requireNonNull(value, "destinationAliasUtf8Nfc");
        if (value.length == 0 || value.length > MAX_ALIAS_BYTES) {
            throw new IllegalArgumentException("destination alias is outside the bound");
        }
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)
                || !Normalizer.normalize(decoded, Normalizer.Form.NFC).equals(decoded)
                || decoded.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("destination alias must be valid NFC UTF-8");
        }
        return Bytes.copy(value);
    }
}
