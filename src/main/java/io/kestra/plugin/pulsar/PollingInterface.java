package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public interface PollingInterface {

    @Schema(
        title = "Duration waiting for record to be polled.",
        description = "If no records are available, the maximum wait to wait for a new record. "
    )
    @NotNull
    Property<Duration> getPollDuration();
}
