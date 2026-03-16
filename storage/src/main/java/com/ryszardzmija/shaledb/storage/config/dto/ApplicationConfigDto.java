package com.ryszardzmija.shaledb.storage.config.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApplicationConfigDto {
    public StorageConfigDto storage;
}
