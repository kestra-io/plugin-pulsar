package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow on periodic message consumption in real-time from Pulsar topics.",
    description = "Note that you don't need an extra task to consume the message from the event trigger. The trigger will automatically consume messages and you can retrieve their content in your flow using the `{{ trigger.uri }}` variable. If you would like to consume each message from a Pulsar topic in real-time and create one execution per message, you can use the [io.kestra.plugin.pulsar.RealtimeTrigger](https://kestra.io/plugins/plugin-pulsar/triggers/io.kestra.plugin.pulsar.realtimetrigger) instead."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: pulsar_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.value }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.pulsar.Trigger
                    interval: PT30S
                    topic: kestra_trigger
                    uri: pulsar://localhost:26650
                    deserializer: JSON
                    subscriptionName: kestra_trigger_sub
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractReader.Output>, PulsarConnectionInterface, SubscriptionInterface, ReadInterface, PollingInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private Property<String> uri;

    private Property<String> authenticationToken;

    private AbstractPulsarConnection.TlsOptions tlsOptions;

    private Object topic;

    @Builder.Default
    private Property<SerdeType> deserializer = Property.of(SerdeType.STRING);

    @Builder.Default
    private Property<Duration> pollDuration = Property.of(Duration.ofSeconds(2));

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The maximum number of records to fetch before stopping.",
        description = "It's not a hard limit and is evaluated every second."
    )
    private Property<Integer> maxRecords;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The maximum duration waiting for new record.",
        description = "It's not a hard limit and is evaluated every second."
    )
    private Property<Duration> maxDuration;

    private Property<String> subscriptionName;

    @Builder.Default
    private Property<SubscriptionInitialPosition> initialPosition = Property.of(SubscriptionInitialPosition.Earliest);

    @Builder.Default
    private Property<SubscriptionType> subscriptionType = Property.of(SubscriptionType.Exclusive);

    private Property<Map<String, String>> consumerProperties;

    private Property<String> encryptionKey;

    private Property<String> consumerName;

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

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume task = Consume.builder()
            .id(this.id)
            .type(Consume.class.getName())
            .uri(this.uri)
            .authenticationToken(this.authenticationToken)
            .tlsOptions(this.tlsOptions)
            .topic(this.topic)
            .deserializer(this.deserializer)
            .pollDuration(this.pollDuration)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .subscriptionName(this.subscriptionName)
            .initialPosition(this.initialPosition)
            .subscriptionType(this.subscriptionType)
            .consumerProperties(this.consumerProperties)
            .encryptionKey(this.encryptionKey)
            .consumerName(this.consumerName)
            .schemaString(this.schemaString)
            .schemaType(this.schemaType)
            .build();
        Consume.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Found '{}' messages from '{}'", run.getMessagesCount(), task.topics(runContext));
        }

        if (run.getMessagesCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }
}

