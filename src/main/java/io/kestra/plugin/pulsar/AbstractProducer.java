package io.kestra.plugin.pulsar;

import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.serializers.FileSerde;
import org.apache.pulsar.client.api.*;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Plugin(
    metrics = {
        @Metric(
            name = "produce.records.count",
            type = Counter.TYPE,
            description = "The total number of records produced to Pulsar."
        ),
        @Metric(
            name = "produce.duration",
            type = Timer.TYPE,
            description = "The duration in milliseconds to produce messages to Pulsar."
        )
    }
)

public abstract class AbstractProducer<T> {

    ProducerBuilder<T> producerBuilder;

    Producer<T> producer;

    private final PulsarClient client;
    private final RunContext runContext;


    public AbstractProducer(RunContext runContext, PulsarClient client) {
        this.client = client;
        this.runContext = runContext;
    }

    public void constructProducer(
        String topic,
        String producerName,
        ProducerAccessMode accessMode,
        String encryptionKey,
        CompressionType compressionType,
        Map<String, String> producerProperties ) throws Exception {

        this.producerBuilder = this.getProducerBuilder(this.client);
        this.producerBuilder
            .topic(this.runContext.render(topic))
            .enableBatching(true);

        if (producerName != null) {
            this.producerBuilder.producerName(this.runContext.render(producerName));
        }

        if (accessMode != null) {
            this.producerBuilder.accessMode(accessMode);
        }

        if (encryptionKey != null) {
            this.producerBuilder.addEncryptionKey(this.runContext.render(encryptionKey));
        }

        if (compressionType != null) {
            this.producerBuilder.compressionType(compressionType);
        }

        if (producerProperties != null) {
            this.producerBuilder.properties(producerProperties
                .entrySet()
                .stream()
                .map(throwFunction(e -> new AbstractMap.SimpleEntry<>(
                    this.runContext.render(e.getKey()),
                    this.runContext.render(e.getValue())
                )))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        }
    }

    @SuppressWarnings("unchecked")
    public int produceMessage(Object from) throws Exception {
        Integer count = 1;
        long start = System.currentTimeMillis();
        if (from instanceof String || from instanceof List) {
            Flux<Object> flowable;
            Flux<Integer> resultFlowable;
            if (from instanceof String) {
                URI uri = new URI(runContext.render((String) from));
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.storage().getFile(uri)))) {
                    flowable = FileSerde.readAll(inputStream);
                    resultFlowable = this.buildFlowable(flowable);

                    count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
                }
            } else {
                flowable = Flux.fromArray(((List<Object>) from).toArray());
                resultFlowable = this.buildFlowable(flowable);

                count = resultFlowable.reduce(Integer::sum).blockOptional().orElse(0);
            }
        } else {
            this.produceMessage((Map<String, Object>) from);
        }

        this.runContext.metric(Counter.of("produce.records.count", count));

        this.producer.flush();
        this.runContext.metric(Timer.of("produce.duration", java.time.Duration.ofMillis(System.currentTimeMillis() - start)));
        return count;
    }

    @SuppressWarnings("unchecked")
    private Flux<Integer> buildFlowable(Flux<Object> flowable) throws Exception {
        return flowable
        .map(throwFunction(row -> {
            this.produceMessage((Map<String, Object>) row);
            return 1;
        }));
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<MessageId> produceMessage(Map<String, Object> map) throws Exception {
        // TypedMessageBuilder<?> message = producer.newMessage();
        Map<String, Object> renderedMap = this.runContext.render(map);

        TypedMessageBuilder<?> message = this.createMessageWithValue(renderedMap);

        if (renderedMap.containsKey("key")) {
            message.key((String) renderedMap.get("key"));
        }

        if (renderedMap.containsKey("properties")) {
            message.properties((Map<String, String>) renderedMap.get("properties"));
        }

        if (renderedMap.containsKey("eventTime")) {
            message.eventTime(processTimestamp(renderedMap.get("eventTime")));
        }

        if (renderedMap.containsKey("deliverAfter")) {
            message.deliverAfter(processTimestamp(renderedMap.get("deliverAfter")), TimeUnit.MILLISECONDS);
        }

        if (renderedMap.containsKey("deliverAt")) {
            message.deliverAt(processTimestamp(renderedMap.get("deliverAt")));
        }

        if (renderedMap.containsKey("sequenceId")) {
            message.sequenceId((long) renderedMap.get("sequenceId"));
        }
        return message.sendAsync();
    }

    private Long processTimestamp(Object timestamp) {
        if (timestamp == null) {
            return null;
        }

        if (timestamp instanceof Long) {
            return (Long) timestamp;
        }

        if (timestamp instanceof ZonedDateTime) {
            return ((ZonedDateTime) timestamp).toInstant().toEpochMilli();
        }

        if (timestamp instanceof Instant) {
            return ((Instant) timestamp).toEpochMilli();
        }

        if (timestamp instanceof LocalDateTime) {
            return ((LocalDateTime) timestamp).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }

        if (timestamp instanceof String) {
            try {
                return ZonedDateTime.parse((String) timestamp).toInstant().toEpochMilli();
            } catch (Exception ignored) {
                return Instant.parse((String) timestamp).toEpochMilli();
            }
        }

        throw new IllegalArgumentException("Invalid type of timestamp with type '" + timestamp.getClass() + "'");
    }

    protected abstract ProducerBuilder<T> getProducerBuilder(PulsarClient client);

    protected abstract TypedMessageBuilder<T> createMessageWithValue(Map<String, Object> renderedMap) throws Exception;
}
