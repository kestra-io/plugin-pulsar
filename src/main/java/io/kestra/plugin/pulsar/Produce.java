package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.Map;
import io.kestra.plugin.serdes.SerdeType;
import io.kestra.plugin.pulsar.AbstractProducer;
import io.kestra.plugin.pulsar.ByteArrayProducer;
import io.kestra.plugin.pulsar.GenericRecordProducer;
import io.kestra.plugin.pulsar.PulsarService;
import io.kestra.plugin.pulsar.SchemaType;
import io.kestra.plugin.pulsar.Data;
import io.swagger.v3.oas.annotations.media.Schema;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Produce a message in a Pulsar topic."
)
@Plugin(
    examples = {
        @Example(
            title = "Read a CSV file, transform it to the right format, and publish it to Pulsar topic.",
            full = true,
            code = """
                id: produce
                namespace: company.team

                inputs:
                  - type: FILE
                    id: file

                tasks:
                  - id: csv_reader
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ inputs.file }}"

                  - id: file_transform
                    type: io.kestra.plugin.scripts.nashorn.FileTransform
                    from: {{ outputs.csv_reader.uri }}"
                    script: |
                      var result = {
                        "key": row.id,
                        "value": {
                          "username": row.username,
                          "tweet": row.tweet
                        },
                        "eventTime": row.timestamp,
                        "properties": {
                          "key": "value"
                        }
                      };
                      row = result

                  - id: produce
                    type: io.kestra.plugin.pulsar.Produce
                    from: "{{ outputs.file_transform.uri }}"
                    uri: pulsar://localhost:26650
                    serializer: JSON
                    topic: test_kestra
                """
        )
    }
)
public class Produce extends AbstractPulsarConnection implements RunnableTask<Produce.Output> , Data.From  {
    @Schema(
        title = "Pulsar topic to send a message to."
    )
    @NotNull
    private Property<String> topic;

    @Schema(
        title = "Source of the sent message.",
        description = "Can be a Kestra internal storage URI, a map or a list " +
            "in the following format: `key`, `value`, `eventTime`, `properties`, " +
            "`deliverAt`, `deliverAfter` and `sequenceId`."
    )
    @NotNull
    private Object from;

    @Schema(
        title = "Serializer used for the value."
    )
    @NotNull
    @Builder.Default
    private Property<SerdeType> serializer = Property.ofValue(SerdeType.STRING);

    @Schema(
        title = "Specify a name for the producer."
    )
    private Property<String> producerName;

    @Schema(
        title = "Add all the properties in the provided map to the producer."
    )
    private Property<Map<String, String>> producerProperties;

    @Schema(
        title = "Configure the type of access mode that the producer requires on the topic.",
        description = "Possible values are:\n" +
            "* `Shared`: By default, multiple producers can publish to a topic.\n" +
            "* `Exclusive`: Require exclusive access for producer. Fail immediately if there's already a producer connected.\n" +
            "* `WaitForExclusive`: Producer creation is pending until it can acquire exclusive access."
    )
    private Property<ProducerAccessMode> accessMode;

    @Schema(
        title = "Add public encryption key, used by producer to encrypt the data key."
    )
    private Property<String> encryptionKey;

    @Schema(
        title = "Set the compression type for the producer.",
        description = "By default, message payloads are not compressed. Supported compression types are:\n" +
            "* `NONE`: No compression (Default).\n" +
            "* `LZ4`: Compress with LZ4 algorithm. Faster but lower compression than ZLib.\n" +
            "* `ZLIB`: Standard ZLib compression.\n" +
            "* `ZSTD` Compress with Zstandard codec. Since Pulsar 2.3.\n" +
            "* `SNAPPY` Compress with Snappy codec. Since Pulsar 2.4."
    )
    private Property<CompressionType> compressionType;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (PulsarClient client = PulsarService.client(this, runContext)) {
            AbstractProducer<?> producer = switch (runContext.render(this.schemaType).as(SchemaType.class).orElseThrow()) {
                case AVRO, JSON -> new GenericRecordProducer(
                    runContext,
                    client,
                    runContext.render(this.schemaString).as(String.class).orElse(null),
                    runContext.render(this.schemaType).as(SchemaType.class).orElseThrow()
                );
                default -> new ByteArrayProducer(runContext, client, runContext.render(this.serializer).as(SerdeType.class).orElseThrow());
            };

            producer.constructProducer(runContext.render(this.topic).as(String.class).orElseThrow(),
                runContext.render(this.producerName).as(String.class).orElse(null),
                runContext.render(this.accessMode).as(ProducerAccessMode.class).orElse(null),
                runContext.render(this.encryptionKey).as(String.class).orElse(null),
                runContext.render(this.compressionType).as(CompressionType.class).orElse(null),
                runContext.render(this.producerProperties).asMap(String.class, String.class)
            );
            int messageCount = Data.from(from).read(runContext)
                .map(row -> producer.produceMessage(row))
                .reduce(Integer::sum)
                .blockOptional().orElse(0);

            return Output.builder()
                .messagesCount(messageCount)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of messages produced."
        )
        private final Integer messagesCount;
    }
}
