package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ReadInterface {
    @Schema(
        title = "Source topic(s)",
        description = "Single topic or list of topics to consume."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Object getTopic();

    @Schema(
        title = "Value deserializer"
    )
    @NotNull
    Property<SerdeType> getDeserializer();
}
