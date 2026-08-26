package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.Bytes;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;

/** Tenant-scoped Route selector; it carries no tenant authority or credential. */
public final class RouteSelectionHint {
    private final AdapterKind adapterKind;
    private final byte[] routeAliasUtf8Nfc;

    public RouteSelectionHint(final AdapterKind adapterKind, final byte[] routeAliasUtf8Nfc) {
        this.adapterKind = Objects.requireNonNull(adapterKind, "adapterKind");
        Objects.requireNonNull(routeAliasUtf8Nfc, "routeAliasUtf8Nfc");
        if (routeAliasUtf8Nfc.length == 0 || routeAliasUtf8Nfc.length > 128) {
            throw new IllegalArgumentException("route alias is outside the Gateway/Route bound");
        }
        final String alias = new String(routeAliasUtf8Nfc, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(alias.getBytes(StandardCharsets.UTF_8), routeAliasUtf8Nfc)
                || !alias.equals(Normalizer.normalize(alias, Normalizer.Form.NFC))
                || alias.indexOf('\0') >= 0
                || alias.isBlank()) {
            throw new IllegalArgumentException("route alias must be nonblank NFC UTF-8");
        }
        this.routeAliasUtf8Nfc = Bytes.copy(routeAliasUtf8Nfc);
    }

    public AdapterKind adapterKind() {
        return adapterKind;
    }

    public byte[] routeAliasUtf8Nfc() {
        return Bytes.copy(routeAliasUtf8Nfc);
    }
}
