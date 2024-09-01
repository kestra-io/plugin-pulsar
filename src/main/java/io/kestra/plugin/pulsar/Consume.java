package io.kestra.plugin.pulsar;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.*;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from Pulsar topic(s)."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: pulsar_consume
                namespace: company.name

                tasks:
                  - id: consume
                    type: io.kestra.plugin.pulsar.Consume
                    uri: pulsar://localhost:26650
                    topic: test_kestra
                    deserializer: JSON
                    subscriptionName: kestra_flow
                """
        )
    }
)
public class Consume extends AbstractReader implements RunnableTask<AbstractReader.Output>, SubscriptionInterface {
    private String subscriptionName;

    @Builder.Default
    private SubscriptionInitialPosition initialPosition = SubscriptionInitialPosition.Earliest;

    @Builder.Default
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    private Map<String, String> consumerProperties;

    private String encryptionKey;

    private String consumerName;

    @Override
    public AbstractReader.Output run(RunContext runContext) throws Exception {
        try (PulsarClient client = PulsarService.client(this, runContext)) {
            ConsumerBuilder<byte[]> consumerBuilder = newConsumerBuilder(runContext, client);

            try (Consumer<byte[]> consumer = consumerBuilder.subscribe()) {
                return this.read(
                    runContext,
                    Rethrow.throwSupplier(() -> {
                        try {
                            Messages<byte[]> messages = consumer.batchReceive();

                            return StreamSupport
                                .stream(messages.spliterator(), false)
                                .map(Rethrow.throwFunction(message -> {
                                    consumer.acknowledge(message);

                                    return message;
                                }))
                                .collect(Collectors.toList());
                        } catch (Throwable e) {
                            throw new Exception(e);
                        }
                    })
                );
            }
        }
    }

    public ConsumerBuilder<byte[]> newConsumerBuilder(final RunContext runContext,
                                                      final PulsarClient client) throws IllegalVariableEvaluationException {
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer()
            .topics(this.topics(runContext))
            .subscriptionName(runContext.render(this.subscriptionName))
            .subscriptionInitialPosition(this.initialPosition)
            .subscriptionType(this.subscriptionType);

        if (this.consumerName != null) {
            consumerBuilder.consumerName(runContext.render(this.consumerName));
        }

        if (this.consumerProperties != null) {
            consumerBuilder.properties(this.consumerProperties
                .entrySet()
                .stream()
                .map(throwFunction(e -> new AbstractMap.SimpleEntry<>(
                    runContext.render(e.getKey()),
                    runContext.render(e.getValue())
                )))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }

        if (this.encryptionKey != null) {
            consumerBuilder.defaultCryptoKeyReader(runContext.render(this.encryptionKey));
        }
        return consumerBuilder;
    }

    public PulsarMessage buildMessage(final Message<byte[]> message) throws Exception {
        boolean applySchema = this.schemaType != SchemaType.NONE;
        if (applySchema && this.schemaString == null) {
            throw new IllegalArgumentException("Must pass a \"schemaString\" when the \"schemaType\" is not null");
        }

        PulsarMessage.PulsarMessageBuilder builder =
            PulsarMessage.builder()
                .key(message.getKey())
                .properties(message.getProperties())
                .topic(message.getTopicName());

        builder.value(applySchema ? this.deserializeWithSchema(message.getValue()) : this.getDeserializer().deserialize(message.getValue()));

        if (message.getEventTime() != 0) {
            builder.eventTime(Instant.ofEpochMilli(message.getEventTime()));
        }

        return builder.messageId(message.getMessageId().toString()).build();
    }

    @Getter
    @Builder
    public static class PulsarMessage implements io.kestra.core.models.tasks.Output {
        private String key;
        private Object value;
        private Map<String, String> properties;
        private String topic;
        private Instant eventTime;
        private String messageId;
    }
}
