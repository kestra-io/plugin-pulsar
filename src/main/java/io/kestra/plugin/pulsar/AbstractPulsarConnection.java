package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPulsarConnection extends Task implements PulsarConnectionInterface {
    private Property<String> uri;

    private Property<String> authenticationToken;

    private TlsOptions tlsOptions;

    @Schema(
      title = "JSON string of the topic's schema",
      description = "Required for connecting with topics with a defined schema and strict schema checking"
    )
    protected Property<String> schemaString;

    @Schema(
      title = "The schema type of the topic",
      description = "Can be one of NONE, AVRO or JSON. None means there will be no schema enforced."
    )
    @Builder.Default
    protected Property<SchemaType> schemaType = Property.of(SchemaType.NONE);

    @Value
    public static class TlsOptions {
        @Schema(
            title = "The client certificate.",
            description = "Must be a base64-encoded pem file."

        )
        Property<String> cert;

        @Schema(
            title = "The key certificate.",
            description = "Must be a base64-encoded pem file."

        )
        Property<String> key;

        @Schema(
            title = "The ca certificate.",
            description = "Must be a base64-encoded pem file."

        )
        Property<String> ca;
    }
}
