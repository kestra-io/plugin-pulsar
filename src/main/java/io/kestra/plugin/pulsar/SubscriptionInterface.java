package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Map;
import javax.validation.constraints.NotNull;

public interface SubscriptionInterface {
    @Schema(
        title = "The subscription name.",
        description = "Using subscription name, we will fetch only records that haven't been consumed yet."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getSubscriptionName();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The position of a subscription to the topic."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    SubscriptionInitialPosition getInitialPosition();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The subscription type."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    SubscriptionType getSubscriptionType();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Add all the properties in the provided map to the consumer."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    Map<String, String> getConsumerProperties();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Add a public encryption key to the producer/consumer."
    )
    @PluginProperty(dynamic = true)
    String getEncryptionKey();

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The consumer name."
    )
    @PluginProperty(dynamic = true)
    String getConsumerName();
}
