package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;
import javax.validation.constraints.NotNull;

public interface ReadInterface {
    @Schema(
        title = "Pulsar topic(s) where to consume message",
        description = "Can be a string or a List of string to consume from multiple topic"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Object getTopic();

    @Schema(
        title = "Deserializer used for the value"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    SerdeType getDeserializer();

    @Schema(
        title = "Duration waiting for record to be polled",
        description = "If no records are available, the max wait to wait for a new records. "
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Duration getPollDuration();

    @Schema(
        title = "The max number of rows to fetch before stopping",
        description = "It's not an hard limit and is evaluated every second"
    )
    @PluginProperty(dynamic = false)
    Integer getMaxRecords();

    @Schema(
        title = "The max duration waiting for new rows",
        description = "It's not an hard limit and is evaluated every second"
    )
    @PluginProperty(dynamic = false)
    Duration getMaxDuration();
}
