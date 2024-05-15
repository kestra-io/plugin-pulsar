package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "React and consume messages from a Pulsar topic."
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
    },
    beta = true
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<AbstractReader.Output>, PulsarConnectionInterface, SubscriptionInterface, ReadInterface {
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

    @Schema(
        title = "JSON string of the topic's schema",
        description = "Required for connecting with topics with a defined schema and strict schema checking"
    )
    @PluginProperty(dynamic = true)
    protected String schemaString;

    @Schema(
        title = "The schema type of the topic",
        description = "Can be one of NONE, AVRO or JSON. None means there will be no schema enforced."
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    protected SchemaType schemaType = SchemaType.NONE;

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
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

        return Flux.from(task.stream(conditionContext.getRunContext()))
            .map(message -> TriggerService.generateRealtimeExecution(this, context, message));
    }
}

