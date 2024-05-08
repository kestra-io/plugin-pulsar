package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.micronaut.core.exceptions.ExceptionHandler;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericDatumWriter;
import org.apache.pulsar.shade.org.apache.avro.io.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
            code = {
                "uri: pulsar://localhost:26650",
                "topic: test_kestra",
                "deserializer: JSON",
                "subscriptionName: kestra_flow"
            }
        )
    }
)
public class Consume extends AbstractReader implements RunnableTask<AbstractReader.Output>, SubscriptionInterface {
    private String subscriptionName;

    @Builder.Default
    private SerdeType deserializer = SerdeType.STRING;

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

    public Publisher<PulsarMessage> stream(RunContext runContext) {
        return Flux.<PulsarMessage>create(
                sink -> {
                    try (PulsarClient client = PulsarService.client(this, runContext)) {
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
                                .map(
                                    throwFunction(e -> new AbstractMap.SimpleEntry<>(
                                            runContext.render(e.getKey()),
                                            runContext.render(e.getValue())
                                        )
                                    )
                                )
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                            );
                        }

                        if (this.encryptionKey != null) {
                            consumerBuilder.defaultCryptoKeyReader(runContext.render(this.encryptionKey));
                        }

                        try (Consumer<byte[]> consumer = consumerBuilder.subscribe()) {
                            StreamSupport
                                .stream(consumer.batchReceive().spliterator(), false)
                                .map(
                                    throwFunction(message -> {
                                            consumer.acknowledge(message);

                                            return message;
                                        }
                                    )
                                )
                                .map(buildMessage(sink::error))
                                .forEach(sink::next);
                        }
                    } catch (Throwable throwable) {
                        sink.error(throwable);
                    } finally {
                        sink.complete();
                    }
                },
                FluxSink.OverflowStrategy.BUFFER
            )
            .subscribeOn(Schedulers.boundedElastic());
    }

    private Function<Message<byte[]>, PulsarMessage> buildMessage(ExceptionHandler<Throwable> onException) {
        return message -> {
            boolean applySchema = this.schemaType != SchemaType.NONE;
            if (applySchema && this.schemaString == null) {
                onException.handle(
                    new IllegalArgumentException("Must pass a \"schemaString\" when the \"schemaType\" is not null")
                );
            }

            PulsarMessage.PulsarMessageBuilder messageBuilder =
                PulsarMessage.builder()
                    .key(message.getKey())
                    .properties(message.getProperties())
                    .topic(message.getTopicName());

            try {
                messageBuilder.value(applySchema ? this.deserializeWithSchema(message.getValue()) : this.deserializer.deserialize(message.getValue()));
            } catch (Throwable e) {
                onException.handle(e);
            }

            if (message.getEventTime() != 0) {
                messageBuilder.eventTime(Instant.ofEpochMilli(message.getEventTime()));
            }

            return messageBuilder.messageId(message.getMessageId().toString()).build();
        };
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
