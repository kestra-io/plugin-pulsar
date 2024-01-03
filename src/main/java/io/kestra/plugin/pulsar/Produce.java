package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.*;

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
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Produce message to a Pulsar topic."
)
@Plugin(
    examples = {
        @Example(
            title = "Read a CSV file, transform it to the right format, and publish it to Pulsar topic.",
            full = true,
            code = {
                "id: produce",
                "namespace: io.kestra.tests",
                "inputs:",
                "  - type: FILE",
                "    name: file",
                "",
                "tasks:",
                "  - id: csvReader",
                "    type: io.kestra.plugin.serdes.csv.CsvReader",
                "    from: \"{{ inputs.file }}\"",
                "  - id: fileTransform",
                "    type: io.kestra.plugin.scripts.nashorn.FileTransform",
                "    from: \"{{ outputs.csvReader.uri }}\"",
                "    script: |",
                "      var result = {",
                "        \"key\": row.id,",
                "        \"value\": {",
                "          \"username\": row.username,",
                "          \"tweet\": row.tweet",
                "        },",
                "        \"eventTime\": row.timestamp,",
                "        \"properties\": {",
                "          \"key\": \"value\"",
                "        }",
                "      };",
                "      row = result",
                "  - id: produce",
                "    type: io.kestra.plugin.pulsar.Produce",
                "    from: \"{{ outputs.fileTransform.uri }}\"",
                "    uri: pulsar://localhost:26650",
                "    serializer: JSON",
                "    topic: test_kestra",
            }
        )
    }
)
public class Produce extends AbstractPulsarConnection implements RunnableTask<Produce.Output> {
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Pulsar topic to send a message to."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String topic;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source of the sent message.",
        description = "Can be a Kestra internal storage URI, a map or a list " +
            "in the following format: `key`, `value`, `eventTime`, `properties`, " +
            "`deliverAt`, `deliverAfter` and `sequenceId`."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Serializer used for the value."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    @Builder.Default
    private SerdeType serializer = SerdeType.STRING;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Specify a name for the producer."
    )
    @PluginProperty(dynamic = true)
    private String producerName;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Add all the properties in the provided map to the producer."
    )
    @PluginProperty(dynamic = true, additionalProperties = String.class)
    private Map<String, String> producerProperties;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Configure the type of access mode that the producer requires on the topic.",
        description = "Possible values are:\n" +
            "* `Shared`: By default, multiple producers can publish to a topic.\n" +
            "* `Exclusive`: Require exclusive access for producer. Fail immediately if there's already a producer connected.\n" +
            "* `WaitForExclusive`: Producer creation is pending until it can acquire exclusive access."
    )
    @PluginProperty
    private ProducerAccessMode accessMode;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Add public encryption key, used by producer to encrypt the data key."
    )
    @PluginProperty(dynamic = true)
    private String encryptionKey;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Set the compression type for the producer.",
        description = "By default, message payloads are not compressed. Supported compression types are:\n" +
            "* `NONE`: No compression (Default).\n" +
            "* `LZ4`: Compress with LZ4 algorithm. Faster but lower compression than ZLib.\n" +
            "* `ZLIB`: Standard ZLib compression.\n" +
            "* `ZSTD` Compress with Zstandard codec. Since Pulsar 2.3.\n" +
            "* `SNAPPY` Compress with Snappy codec. Since Pulsar 2.4."
    )
    @PluginProperty
    private CompressionType compressionType;

    @SuppressWarnings("unchecked")
    @Override
    public Output run(RunContext runContext) throws Exception {
        try (PulsarClient client = PulsarService.client(this, runContext)) {
            ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic(runContext.render(this.topic))
                .enableBatching(true);

            if (this.producerName != null) {
                producerBuilder.producerName(runContext.render(this.producerName));
            }

            if (this.accessMode != null) {
                producerBuilder.accessMode(this.accessMode);
            }

            if (this.encryptionKey != null) {
                producerBuilder.addEncryptionKey(runContext.render(this.encryptionKey));
            }

            if (this.compressionType != null) {
                producerBuilder.compressionType(this.compressionType);
            }

            if (this.producerProperties != null) {
                producerBuilder.properties(this.producerProperties
                    .entrySet()
                    .stream()
                    .map(throwFunction(e -> new AbstractMap.SimpleEntry<>(
                        runContext.render(e.getKey()),
                        runContext.render(e.getValue())
                    )))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                );
            }

            try (Producer<byte[]> producer = producerBuilder.create()) {
                Integer count = 1;

                if (this.from instanceof String || this.from instanceof List) {
                    Flowable<Object> flowable;
                    Flowable<Integer> resultFlowable;
                    if (this.from instanceof String) {
                        URI from = new URI(runContext.render((String) this.from));
                        try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                            flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                            resultFlowable = this.buildFlowable(flowable, runContext, producer);

                            count = resultFlowable
                                .reduce(Integer::sum)
                                .blockingGet();
                        }
                    } else {
                        flowable = Flowable.fromArray(((List<Object>) this.from).toArray());
                        resultFlowable = this.buildFlowable(flowable, runContext, producer);

                        count = resultFlowable
                            .reduce(Integer::sum)
                            .blockingGet();
                    }
                } else {
                    this.produceMessage(producer, runContext, (Map<String, Object>) this.from);
                }

                runContext.metric(Counter.of("records", count));

                producer.flush();

                return Output.builder()
                    .messagesCount(count)
                    .build();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, RunContext runContext, Producer<byte[]> producer) {
        return flowable
            .map(row -> {
                this.produceMessage(producer, runContext, (Map<String, Object>) row);
                return 1;
            });
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<MessageId> produceMessage(Producer<byte[]> producer, RunContext runContext, Map<String, Object> map) throws Exception {
        TypedMessageBuilder<byte[]> message = producer.newMessage();
        Map<String, Object> renderedMap = runContext.render(map);

        if (renderedMap.containsKey("key")) {
            message.key((String) renderedMap.get("key"));
        }

        if (renderedMap.containsKey("properties")) {
            message.properties((Map<String, String>) renderedMap.get("properties"));
        }

        if (renderedMap.containsKey("value")) {
            message.value(this.serializer.serialize(renderedMap.get("value")));
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

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of messages produced."
        )
        private final Integer messagesCount;
    }
}
