package io.kestra.plugin.pulsar;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Map;

public interface SubscriptionInterface {
    @Schema(
        title = "Subscription name",
        description = "Identifies the subscription so only unconsumed records for that subscription are fetched."
    )
    @NotNull
    Property<String> getSubscriptionName();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Initial subscription position",
        description = "Where to start consuming (default `Earliest`)."
    )
    @NotNull
    Property<SubscriptionInitialPosition> getInitialPosition();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Subscription type",
        description = "Delivery semantics such as `Exclusive` (default), `Shared`, or `Failover`."
    )
    @NotNull
    Property<SubscriptionType> getSubscriptionType();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Consumer properties",
        description = "Key/value properties applied to the Pulsar consumer builder."
    )
    Property<Map<String, String>> getConsumerProperties();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Public encryption key",
        description = "Key used for payload encryption/decryption when the topic is secured."
    )
    Property<String> getEncryptionKey();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Consumer name",
        description = "Optional name reused on reconnects; helps coordinate with broker policies."
    )
    Property<String> getConsumerName();
}
