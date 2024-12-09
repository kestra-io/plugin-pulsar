package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Map;
import jakarta.validation.constraints.NotNull;

public interface SubscriptionInterface {
    @Schema(
        title = "The subscription name.",
        description = "Using subscription name, we will fetch only records that haven't been consumed yet."
    )
    @NotNull
    Property<String> getSubscriptionName();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The position of a subscription to the topic."
    )
    @NotNull
    Property<SubscriptionInitialPosition> getInitialPosition();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The subscription type."
    )
    @NotNull
    Property<SubscriptionType> getSubscriptionType();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Add all the properties in the provided map to the consumer."
    )
    Property<Map<String, String>> getConsumerProperties();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Add a public encryption key to the producer/consumer."
    )
    Property<String> getEncryptionKey();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The consumer name."
    )
    Property<String> getConsumerName();
}
