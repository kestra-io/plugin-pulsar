package io.kestra.plugin.pulsar;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.Message;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractReader extends AbstractPulsarConnection implements ReadInterface, RunnableTask<AbstractReader.Output> {
    private Object topic;

    @Builder.Default
    private SerdeType deserializer = SerdeType.STRING;

    @Builder.Default
    private Duration pollDuration = Duration.ofSeconds(2);

    private Integer maxRecords;

    private Duration maxDuration;

    public Output read(RunContext runContext, Supplier<List<Message<byte[]>>> supplier) throws Exception {
        File tempFile = runContext.tempFile(".ion").toFile();
        Map<String, Integer> count = new HashMap<>();
        AtomicInteger total = new AtomicInteger();
        ZonedDateTime started = ZonedDateTime.now();
        ZonedDateTime lastPool = ZonedDateTime.now();

        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            do {
                for (Message<byte[]> message : supplier.get()) {
                    // data to write
                    Map<Object, Object> map = new HashMap<>();
                    map.put("key", message.getKey());
                    map.put("value", this.deserializer.deserialize(message.getValue()));
                    map.put("properties", message.getProperties());
                    map.put("topic", message.getTopicName());
                    if (message.getEventTime() != 0) {
                        map.put("eventTime", Instant.ofEpochMilli(message.getEventTime()));
                    }
                    map.put("messageId", message.getMessageId());

                    FileSerde.write(output, map);

                    // update internal values
                    total.getAndIncrement();
                    count.compute(message.getTopicName(), (s, integer) -> integer == null ? 1 : integer + 1);
                    lastPool = ZonedDateTime.now();

                }
            } while (!this.ended(total, started, lastPool));

            output.flush();

            count
                .forEach((s, integer) -> runContext.metric(Counter.of("records", integer, "topic", s)));

            return Output.builder()
                .messagesCount(count.values().stream().mapToInt(Integer::intValue).sum())
                .uri(runContext.putTempFile(tempFile))
                .build();
        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(AtomicInteger count, ZonedDateTime start, ZonedDateTime lastPool) {
        if (this.maxRecords != null && count.get() > this.maxRecords) {
            return true;
        }

        if (this.maxDuration != null && ZonedDateTime.now().toEpochSecond() > start.plus(this.maxDuration).toEpochSecond()) {
            return true;
        }

        if (ZonedDateTime.now().toEpochSecond() > lastPool.plus(this.pollDuration).toEpochSecond()) {
            return true;
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    List<String> topics(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.topic instanceof String) {
            return List.of(runContext.render((String) this.topic));
        } else if (this.topic instanceof List) {
            return runContext.render((List<String>) this.topic);
        } else {
            throw new IllegalArgumentException("Invalid topics with type '" + this.topic.getClass().getName() + "'");
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of message consumed"
        )
        private final Integer messagesCount;

        @Schema(
            title = "URI of a kestra internal storage file"
        )
        private URI uri;
    }
}
