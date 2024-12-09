package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.*;

import java.util.Map;
import jakarta.validation.constraints.NotNull;


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
                default -> new ByteArrayProducer(runContext, client, this.serializer);
            };

            producer.constructProducer(this.topic, this.producerName, this.accessMode,this.encryptionKey, this.compressionType, this.producerProperties);
            int messageCount = producer.produceMessage(this.from);

            return Output.builder()
                .messagesCount(messageCount)
                .build();
        }
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
