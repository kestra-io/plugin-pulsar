package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume a message in real-time from Pulsar topics and create one execution per message.",
    description = "If you would like to consume multiple messages processed within a given time frame and process them in batch, you can use the [io.kestra.plugin.pulsar.Trigger](https://kestra.io/plugins/plugin-pulsar/triggers/io.kestra.plugin.pulsar.trigger) instead."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume a message from a Pulsar topic in real-time.",
            full = true,
            code = {"""
                id: pulsar
                namespace: company.team

                tasks:
                - id: log
                  type: io.kestra.plugin.core.log.Log
                  message: "{{ trigger.value }}"

                triggers:
                - id: realtime_trigger
                  type: io.kestra.plugin.pulsar.RealtimeTrigger
                  topic: kestra_trigger
                  uri: pulsar://localhost:26650
                  deserializer: JSON
                  subscriptionName: kestra_trigger_sub"""
            }
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Consume.PulsarMessage>, PulsarConnectionInterface, SubscriptionInterface, ReadInterface {
    private static final int DEFAULT_RECEIVE_TIMEOUT = 500;
    
    private String uri;

    private String authenticationToken;

    private AbstractPulsarConnection.TlsOptions tlsOptions;

    private Object topic;

    @Builder.Default
    private SerdeType deserializer = SerdeType.STRING;

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

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

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
            .subscriptionName(this.subscriptionName)
            .initialPosition(this.initialPosition)
            .subscriptionType(this.subscriptionType)
            .consumerProperties(this.consumerProperties)
            .encryptionKey(this.encryptionKey)
            .consumerName(this.consumerName)
            .schemaString(this.schemaString)
            .schemaType(this.schemaType)
            .build();

        return Flux.from(publisher(task, conditionContext.getRunContext()))
            .map(message -> TriggerService.generateRealtimeExecution(this, conditionContext, context, message));
    }

    public Publisher<Consume.PulsarMessage> publisher(final Consume task, final RunContext runContext) {
        return Flux.create(emitter -> {
                try (PulsarClient client = PulsarService.client(task, runContext)) {
                    ConsumerBuilder<byte[]> consumerBuilder = task.newConsumerBuilder(runContext, client);
                    try (Consumer<byte[]> consumer = consumerBuilder.subscribe()) {
                        while (isActive.get()) {
                            // wait for a new message before checking active flag.
                            final Message<byte[]> received = consumer.receive(DEFAULT_RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS);
                            if (received != null) {
                                try {
                                    emitter.next(task.buildMessage(received));
                                    consumer.acknowledge(received);
                                } catch (Exception e) {
                                    consumer.negativeAcknowledge(received);
                                    throw e; // will be handled by the next catch.
                                }
                            }
                        }
                    }
                } catch (Exception exception) {
                    emitter.error(exception);
                } finally {
                    emitter.complete();
                    waitForTermination.countDown();
                }
            });
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }
        if (wait) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

