package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface PulsarConnectionInterface {
    @Schema(
        title = "Pulsar service URL",
        description = "One or more Pulsar protocol URLs, e.g. `pulsar://localhost:6650` or `pulsar://host1:6650,host2:6651`. Use `pulsar+ssl://` when enabling TLS."
    )
    @NotNull
    Property<String> getUri();

    @Schema(
        title = "Authentication token",
        description = "Token used when the broker requires token-based auth (e.g., hosted providers)."
    )
    Property<String> getAuthenticationToken();

    @Schema(
        title = "TLS options",
        description = "Certificate/key material for TLS client authentication. Requires a `pulsar+ssl://` URL."
    )
    @PluginProperty(dynamic = false)
    AbstractPulsarConnection.TlsOptions getTlsOptions();
}
