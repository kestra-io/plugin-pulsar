package io.kestra.plugin.pulsar;

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
      title = "Topic schema definition",
      description = "JSON representation of the topic schema when schema enforcement is enabled."
    )
    protected Property<String> schemaString;

    @Schema(
      title = "Topic schema type",
      description = "One of `NONE` (default, no enforcement), `AVRO`, or `JSON`."
    )
    @Builder.Default
    protected Property<SchemaType> schemaType = Property.ofValue(SchemaType.NONE);

    @Value
    public static class TlsOptions {
        @Schema(
            title = "Client certificate",
            description = "Base64-encoded PEM content for the client certificate."

        )
        Property<String> cert;

        @Schema(
            title = "Client key",
            description = "Base64-encoded PEM private key matching the client certificate."

        )
        Property<String> key;

        @Schema(
            title = "CA certificate",
            description = "Base64-encoded PEM of the trusted CA chain."

        )
        Property<String> ca;
    }
}
