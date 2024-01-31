package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
    title = "Wait for messages from a Pulsar topic."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "interval: PT30S",
                "topic: kestra_trigger",
                "uri: pulsar://localhost:26650",
                "deserializer: JSON",
                "subscriptionName: kestra_trigger_sub",
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<AbstractReader.Output>, PulsarConnectionInterface, SubscriptionInterface, ReadInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private String uri;

    private String authenticationToken;

    private AbstractPulsarConnection.TlsOptions tlsOptions;

    private Object topic;

    @Builder.Default
    private SerdeType deserializer = SerdeType.STRING;

    @Builder.Default
    private Duration pollDuration = Duration.ofSeconds(2);

    private Integer maxRecords;

    private Duration maxDuration;

    private String subscriptionName;

    @Builder.Default
    private SubscriptionInitialPosition initialPosition = SubscriptionInitialPosition.Earliest;

    @Builder.Default
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    private Map<String, String> consumerProperties;

    private String encryptionKey;

    private String consumerName;

    @io.swagger.v3.oas.annotations.media.Schema(
      title = "JSON strong of the topics schema",
      description = "Required for connecting with topics using AVRO or JSON schemas"
    )
    @PluginProperty
    private String schemaString;

    @io.swagger.v3.oas.annotations.media.Schema(
      title = "The topics schema type. If not AVRO or JSON, leave as byte",
      description = "Required for connecting with topics using AVRO or JSON schemas"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    @Builder.Default
    private String schemaType = "byte";

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
        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
            this,
            run
        );

        Execution execution = Execution.builder()
            .id(runContext.getTriggerExecutionId())
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .trigger(executionTrigger)
            .build();

        return Optional.of(execution);
    }
}

