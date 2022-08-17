package io.kestra.plugin.pulsar;

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
    private String uri;

    private TlsOptions tlsOptions;

    @Value
    public static class TlsOptions {
        @Schema(
            title = "The client certificate",
            description = "Must be a pem file as base64."

        )
        String cert;

        @Schema(
            title = "The key certificate",
            description = "Must be a pem file as base64."

        )
        String key;

        @Schema(
            title = "The ca certificate",
            description = "Must be a pem file as base64."

        )
        String ca;
    }
}
