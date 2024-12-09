package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
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
    Property<SerdeType> getDeserializer();
}
