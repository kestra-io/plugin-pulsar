package io.kestra.plugin.pulsar;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public interface PollingInterface {

    @Schema(
        title = "Poll wait duration",
        description = "Maximum time to wait for a new record when none are immediately available."
    )
    @NotNull
    Property<Duration> getPollDuration();
}
