package com.ryszardzmija.storage.format;

import java.nio.ByteOrder;

class FormatInfo {
    static final int CHECKSUM_FIELD_SIZE = 4;
    static final int KEY_LENGTH_FIELD_SIZE = 4;
    static final int VALUE_LENGTH_FIELD_SIZE = 4;

    static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    static int getHeaderSize() {
        return CHECKSUM_FIELD_SIZE + KEY_LENGTH_FIELD_SIZE + VALUE_LENGTH_FIELD_SIZE;
    }
}
