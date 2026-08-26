package com.nereusstream.delay.store;

/** Closed RocksDB column-family set (plus RocksDB's empty default CF). */
public enum ColumnFamily {
    TIMELINE("timeline_cf"),
    ID("id_cf"),
    INFLIGHT("inflight_cf"),
    DEDUPE("dedupe_cf"),
    TERMINAL("terminal_cf"),
    GC("gc_cf"),
    META("meta_cf");

    private final String name;

    ColumnFamily(final String name) {
        this.name = name;
    }

    public String rocksName() {
        return name;
    }
}
