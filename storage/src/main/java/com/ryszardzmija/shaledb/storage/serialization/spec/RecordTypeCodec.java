package com.ryszardzmija.shaledb.storage.serialization.spec;

import com.ryszardzmija.shaledb.storage.serialization.record.RecordType;

import java.util.Map;
import java.util.stream.Collectors;

public class RecordTypeCodec {
    private static final Map<RecordType, Byte> TYPE_TO_BYTE = Map.ofEntries(
            Map.entry(RecordType.NORMAL, (byte) 0),
            Map.entry(RecordType.TOMBSTONE, (byte) 1)
    );

    private static final Map<Byte, RecordType> BYTE_TO_TYPE =
            TYPE_TO_BYTE.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    public static byte encode(RecordType type) {
        return TYPE_TO_BYTE.get(type);
    }

    public static RecordType decode(byte value) {
        RecordType type = BYTE_TO_TYPE.get(value);
        if (type == null) {
            throw new TypeDecodingException("Unknown record type: " + value);
        }
        return type;
    }
}
