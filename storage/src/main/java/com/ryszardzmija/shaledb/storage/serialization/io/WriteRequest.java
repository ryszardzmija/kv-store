package com.ryszardzmija.shaledb.storage.serialization.io;

import com.ryszardzmija.shaledb.storage.serialization.record.RecordPayload;
import com.ryszardzmija.shaledb.storage.serialization.record.RecordType;

public record WriteRequest(RecordPayload payload, RecordType type) {
}
