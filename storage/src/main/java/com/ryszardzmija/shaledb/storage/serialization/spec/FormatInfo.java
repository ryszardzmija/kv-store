package com.ryszardzmija.shaledb.storage.serialization.spec;

import java.nio.ByteOrder;

public class FormatInfo {
    public static final int CHECKSUM_FIELD_SIZE = 4;
    public static final int TYPE_FIELD_SIZE = 1;
    public static final int KEY_LENGTH_FIELD_SIZE = 4;
    public static final int VALUE_LENGTH_FIELD_SIZE = 4;

    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    public static int getHeaderSize() {
        return CHECKSUM_FIELD_SIZE + TYPE_FIELD_SIZE + KEY_LENGTH_FIELD_SIZE + VALUE_LENGTH_FIELD_SIZE;
    }

    public static int getChecksumHeaderFieldsSize() {
        return TYPE_FIELD_SIZE + KEY_LENGTH_FIELD_SIZE + VALUE_LENGTH_FIELD_SIZE;
    }
}
