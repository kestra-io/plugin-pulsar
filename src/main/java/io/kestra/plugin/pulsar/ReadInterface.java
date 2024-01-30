package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;
import jakarta.validation.constraints.NotNull;

public interface ReadInterface {
    @Schema(
        title = "Pulsar topic(s) where to consume messages from.",
        description = "Can be a string or a list of strings to consume from multiple topics."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Object getTopic();

    @Schema(
        title = "Deserializer used for the value."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    SerdeType getDeserializer();

    @Schema(
        title = "Duration waiting for record to be polled.",
        description = "If no records are available, the maximum wait to wait for a new record. "
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Duration getPollDuration();

    @Schema(
        title = "The maximum number of records to fetch before stopping.",
        description = "It's not a hard limit and is evaluated every second."
    )
    @PluginProperty(dynamic = false)
    Integer getMaxRecords();

    @Schema(
        title = "The maximum duration waiting for new record.",
        description = "It's not a hard limit and is evaluated every second."
    )
    @PluginProperty(dynamic = false)
    Duration getMaxDuration();
}
