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
    title = "Trigger a flow by polling Pulsar messages",
    description = "Periodically consumes from topics, stores the batch in Kestra storage, and exposes it via `{{ trigger.uri }}`. Use `RealtimeTrigger` for one execution per message."
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
    private Property<SerdeType> deserializer = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(2));

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Maximum records before stop",
        description = "Soft limit evaluated each second; stops after this many messages if set."
    )
    private Property<Integer> maxRecords;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Maximum read duration",
        description = "Soft timeout evaluated each second; stops when exceeded."
    )
    private Property<Duration> maxDuration;

    private Property<String> subscriptionName;

    @Builder.Default
    private Property<SubscriptionInitialPosition> initialPosition = Property.ofValue(SubscriptionInitialPosition.Earliest);

    @Builder.Default
    private Property<SubscriptionType> subscriptionType = Property.ofValue(SubscriptionType.Exclusive);

    private Property<Map<String, String>> consumerProperties;

    private Property<String> encryptionKey;

    private Property<String> consumerName;

    @Schema(
        title = "Topic schema definition",
        description = "JSON schema used when schema enforcement is enabled."
    )
    protected Property<String> schemaString;

    @Schema(
        title = "Topic schema type",
        description = "One of `NONE` (default), `AVRO`, or `JSON`."
    )
    @Builder.Default
    protected Property<SchemaType> schemaType = Property.ofValue(SchemaType.NONE);

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
