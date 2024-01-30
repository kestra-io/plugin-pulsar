package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.schema.SchemaInfo;

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
    private SubscriptionInitialPosition initialPosition = SubscriptionInitialPosition.Earliest;

    @Builder.Default
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    private Map<String, String> consumerProperties;

    private String encryptionKey;

    private String consumerName;

    @Override
    public AbstractReader.Output run(RunContext runContext) throws Exception {
        try (PulsarClient client = PulsarService.client(this, runContext)) {
            System.out.println("run_1");
            SchemaDefinition<GenericRecord> schemaDef = SchemaDefinition
              .<GenericRecord>builder()
              .withJsonDef("{\"type\": \"record\", \"name\": \"CentroidCollectionSchema\", \"fields\": [{\"name\": \"img_id\", \"type\": \"string\"}, {\"name\": \"centroids\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"record\", \"name\": \"CentroidSchema\", \"fields\": [{\"name\": \"centroid_y\", \"type\": \"int\"}, {\"name\": \"centroid_x\", \"type\": \"int\"}, {\"name\": \"centroid_id\", \"type\": \"string\"}, {\"name\": \"confidence\", \"type\": \"float\"}, {\"name\": \"classification\", \"type\": \"int\"}]}}}]}")
              .build();
            System.out.println("run_2");
            ConsumerBuilder<GenericRecord> consumerBuilder = client.newConsumer(org.apache.pulsar.client.api.Schema.generic(org.apache.pulsar.client.api.Schema.AVRO(schemaDef).getSchemaInfo()))
            // ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer()
                .topics(this.topics(runContext))
                .subscriptionName(runContext.render(this.subscriptionName))
                .subscriptionInitialPosition(this.initialPosition)
                .subscriptionType(this.subscriptionType);
            System.out.println("run_3");
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

            try (Consumer<GenericRecord> consumer = consumerBuilder.subscribe()) {
                System.out.println("run_4");
                return this.read(
                    runContext,
                    Rethrow.throwSupplier(() -> {
                        try {
                            Messages<GenericRecord> messages = consumer.batchReceive();

                            return StreamSupport
                                .stream(messages.spliterator(), false)
                                .map(Rethrow.throwFunction(message -> {
                                    consumer.acknowledge(message);

                                    return message;
                                }))
                                .collect(Collectors.toList());
                        } catch (Throwable e) {
                            System.out.println(e);
                            throw new Exception(e);
                        }
                    })
                );
            }
        }
    }
}
