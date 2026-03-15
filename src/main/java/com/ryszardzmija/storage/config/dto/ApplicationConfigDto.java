package com.ryszardzmija.storage.config.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApplicationConfigDto {
    public StorageConfigDto storage;
}
