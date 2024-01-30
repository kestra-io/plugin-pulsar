package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read messages from Pulsar topic(s) without subscription."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "uri: pulsar://localhost:26650",
                "topic: test_kestra",
                "deserializer: JSON",
            }
        )
    }
)
public class Reader extends AbstractReader {
    @Schema(
        title = "The initial reader positioning can be set at specific timestamp by providing total rollback duration.",
        description = "So, broker can find a latest message that was published before given duration. eg: " +
            "`since` set to 5 minutes (`PT5M`) indicates that broker should find message published 5 minutes in the past, " +
            "and set the initial position to that messageId."
    )
    @PluginProperty(dynamic = true)
    private Duration since;

    @Schema(
        title = "Position the reader on a particular message.",
        description = "The first message read will be the one immediately *after* the specified message.\n" +
            "If no `since` or `messageId` are provided, we start at the beginning of the topic."
    )
    @PluginProperty(dynamic = true)
    private String messageId;

    @Override
    public AbstractReader.Output run(RunContext runContext) throws Exception {
        try (PulsarClient client = PulsarService.client(this, runContext)) {
          // SchemaDefinition<GenericRecord> schemaDef = SchemaDefinition
          //     .<GenericRecord>builder()
          //     .withJsonDef("{\"type\": \"record\", \"name\": \"CentroidCollectionSchema\", \"fields\": [{\"name\": \"img_id\", \"type\": \"string\"}, {\"name\": \"centroids\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"record\", \"name\": \"CentroidSchema\", \"fields\": [{\"name\": \"centroid_y\", \"type\": \"int\"}, {\"name\": \"centroid_x\", \"type\": \"int\"}, {\"name\": \"centroid_id\", \"type\": \"string\"}, {\"name\": \"confidence\", \"type\": \"float\"}, {\"name\": \"classification\", \"type\": \"int\"}]}}}]}")
          //     .build();

          // org.apache.pulsar.client.api.ReaderBuilder<GenericRecord> readerBuilder = client.newReader(org.apache.pulsar.client.api.Schema.generic(org.apache.pulsar.client.api.Schema.AVRO(schemaDef).getSchemaInfo()))
          org.apache.pulsar.client.api.ReaderBuilder<byte[]> readerBuilder = client.newReader()
                .topics(this.topics(runContext));

            if (this.since != null) {
                readerBuilder.startMessageFromRollbackDuration(this.since.getNano(), TimeUnit.NANOSECONDS);
            } else if (this.messageId != null) {
                readerBuilder.startMessageId(MessageId.fromByteArray(runContext.render(this.messageId).getBytes(StandardCharsets.UTF_8)));
            } else {
                readerBuilder.startMessageId(MessageId.earliest);
            }

            try (org.apache.pulsar.client.api.Reader<byte[]> reader = readerBuilder.create()) {
                return this.read(
                    runContext,
                    Rethrow.throwSupplier(() -> {
                        Message<byte[]> message = reader.readNext(this.getPollDuration().getNano(), TimeUnit.NANOSECONDS);

                        if (message == null) {
                            return List.of();
                        } else {
                            return List.of(message);
                        }
                    })
                );
            }
        }
    }
}
