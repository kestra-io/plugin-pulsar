package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface PulsarConnectionInterface {
    @Schema(
        title = "Connection URLs.",
        description = "You need to specify a Pulsar protocol URL.\n" +
            "- Example of localhost: `pulsar://localhost:6650`\n" +
            "- If you have multiple brokers: `pulsar://localhost:6650,localhost:6651,localhost:6652`\n" +
            "- If you use TLS authentication: `pulsar+ssl://pulsar.us-west.example.com:6651`"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getUri();

    @Schema(
        title = "Authentication token.",
        description = "Authentication token that can be required by some providers such as Clever Cloud."
    )
    @PluginProperty(dynamic = true)
    String getAuthenticationToken();

    @Schema(
        title = "TLS authentication options.",
        description = "You need to use \"pulsar+ssl://\" in serviceUrl to enable TLS support."
    )
    @PluginProperty(dynamic = false)
    AbstractPulsarConnection.TlsOptions getTlsOptions();
}
