package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish messages to a Pulsar topic",
    description = "Reads records from Kestra storage or inline maps/lists and sends them with a Pulsar producer. Uses STRING serialization and no compression by default; Pulsar's shared producer mode applies unless another access mode is set."
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
                    type: io.kestra.plugin.graalvm.js.FileTransform
                    from: "{{ outputs.csv_reader.uri }}"
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
public class Produce extends AbstractPulsarConnection implements RunnableTask<Produce.Output>, Data.From {
    @Schema(
        title = "Target Pulsar topic"
    )
    @NotNull
    private Property<String> topic;

    @Schema(
        title = "Message source",
        description = "Kestra internal storage URI, or map/list objects with optional `key`, `value`, `eventTime`, `properties`, `deliverAt`, `deliverAfter`, and `sequenceId` fields."
    )
    @NotNull
    private Object from;

    @Schema(
        title = "Serializer for message value",
        description = "Defaults to `STRING`. Choose a serializer compatible with consumers and topic schema."
    )
    @NotNull
    @Builder.Default
    private Property<SerdeType> serializer = Property.ofValue(SerdeType.STRING);

    @Schema(
        title = "Custom producer name",
        description = "Optional name reused on reconnects; may affect exclusive access checks."
    )
    private Property<String> producerName;

    @Schema(
        title = "Producer properties",
        description = "Key/value properties passed to the Pulsar producer builder."
    )
    private Property<Map<String, String>> producerProperties;

    @Schema(
        title = "Producer access mode",
        description = "`Shared` (default Pulsar behavior) allows multiple producers; `Exclusive` fails if another producer is connected; `WaitForExclusive` waits for exclusivity."
    )
    private Property<ProducerAccessMode> accessMode;

    @Schema(
        title = "Public encryption key",
        description = "PEM-encoded key used to encrypt the data key for message payload encryption."
    )
    private Property<String> encryptionKey;

    @Schema(
        title = "Producer compression type",
        description = "Default `NONE`. Other options: `LZ4`, `ZLIB`, `ZSTD`, `SNAPPY`. Use to reduce payload size at the cost of CPU."
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
                default ->
                    new ByteArrayProducer(runContext, client, runContext.render(this.serializer).as(SerdeType.class).orElseThrow());
            };

            producer.constructProducer(runContext.render(this.topic).as(String.class).orElseThrow(),
                runContext.render(this.producerName).as(String.class).orElse(null),
                runContext.render(this.accessMode).as(ProducerAccessMode.class).orElse(null),
                runContext.render(this.encryptionKey).as(String.class).orElse(null),
                runContext.render(this.compressionType).as(CompressionType.class).orElse(null),
                runContext.render(this.producerProperties).asMap(String.class, String.class)
            );

            int messageCount;
            try {
                messageCount = Data.from(from).read(runContext)
                    .map(throwFunction(producer::produceMessage))
                    .reduce(Integer::sum)
                    .blockOptional()
                    .orElse(0);
            } catch (RuntimeException e) {
                if (e.getCause() instanceof Exception ex) {
                    throw ex;
                }
                throw e;
            }

            return Output.builder()
                .messagesCount(messageCount)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of messages produced"
        )
        private final Integer messagesCount;
    }
}
