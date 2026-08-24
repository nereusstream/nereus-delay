package com.nereusstream.delay.protocol;

/** Internal RBAC roles required by the V1 Control Operation matrix. */
public enum ControlRoleV1 {
    COMMAND_PRODUCER(1),
    QUERY_READER(2),
    DEAD_LETTER_OPERATOR(3),
    TENANT_POLICY_ADMINISTRATOR(4),
    PLATFORM_OPERATOR(5);

    private final int wireValue;

    ControlRoleV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }
}
