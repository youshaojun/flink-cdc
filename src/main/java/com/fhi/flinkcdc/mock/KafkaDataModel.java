package com.fhi.flinkcdc.mock;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class KafkaDataModel {

    private int cycle;

    @NotBlank
    private String bootstrapServers;

    @NotBlank
    private String topic;

    @NotNull
    private Object data;

}
