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
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericDatumWriter;
// import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.shade.org.apache.avro.io.DatumReader;
import org.apache.pulsar.shade.org.apache.avro.io.DatumWriter;
import org.apache.pulsar.shade.org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.shade.org.apache.avro.io.JsonEncoder;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

    @SuppressWarnings("unchecked")
    public Output read(RunContext runContext, Supplier<List<Message<GenericRecord>>> supplier) throws Exception {
      System.out.println("read_1");  
      File tempFile = runContext.tempFile(".ion").toFile();
        Map<String, Integer> count = new HashMap<>();
        AtomicInteger total = new AtomicInteger();
        ZonedDateTime started = ZonedDateTime.now();
        ZonedDateTime lastPool = ZonedDateTime.now();

        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            System.out.println("read_2");
            do {
                for (Message<GenericRecord> message : supplier.get()) {
                    System.out.println("read_3");
                    // data to write
                    // System.out.println(message.toString());
                    // System.out.println(message.getValue().toString());
                    // System.out.println(message.getValue().getFields());
                    // System.out.println(((List<Map<String, Object>>)(message.getValue().getField("centroids"))).get(0));
                    // System.out.println(((List<Map<String, Object>>)(message.getValue().getField("centroids"))).get(0).getClass());
                    // Map<String, Object> valueMap = new HashMap<>();
                    // for (Field field: message.getValue().getFields()) {
                    //   valueMap.put(field.getName(), message.getValue().getField(field));
                    // }
                    // System.out.println(valueMap);
                    // try {
                    //   ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
                    //   String json = ow.writeValueAsString(message.getValue());
                    //   System.out.println(json);
                    // } catch (Exception e) { System.out.println(e);}
                    String value = avroToJson(message.getValue());
                    System.out.println(value);
                    // try {
                    // } catch (Exception e) { System.out.println(e); System.out.println(e.getStackTrace());}


                    System.out.println("read_3b");

                    try {
                      Map<Object, Object> map = new HashMap<>();
                      System.out.println("read_x1");
                      map.put("key", message.getKey());
                      System.out.println("read_x2");
                      map.put("value", value);
                      System.out.println("read_x3");
                      map.put("properties", message.getProperties());
                      System.out.println("read_x4");
                      map.put("topic", message.getTopicName());
                      System.out.println("read_x5");
                      if (message.getEventTime() != 0) {
                        map.put("eventTime", Instant.ofEpochMilli(message.getEventTime()));
                      }
                      System.out.println("read_x6");
                      map.put("messageId", message.getMessageId());
                      System.out.println("read_x7");
                      System.out.println(map);
                      FileSerde.write(output, map);
                      System.out.println("read_x8");
                    } catch (Exception e) { System.out.println(e);}
  
                      // update internal values
                      total.getAndIncrement();
                      count.compute(message.getTopicName(), (s, integer) -> integer == null ? 1 : integer + 1);
                      lastPool = ZonedDateTime.now();
                    System.out.println("read_4");

                } 
            } while (!this.ended(total, started, lastPool));

            output.flush();

            count
                .forEach((s, integer) -> runContext.metric(Counter.of("records", integer, "topic", s)));

            System.out.println("read_5");
            return Output.builder()
                .messagesCount(count.values().stream().mapToInt(Integer::intValue).sum())
                .uri(runContext.putTempFile(tempFile))
                .build();
        }
    }

    private String avroToJson(GenericRecord avroObj) throws IOException {
      // // byte to datum
      // DatumReader<Object> datumReader = new GenericDatumReader<>(schema);
      // Decoder deco
      SchemaDefinition<GenericRecord> schemaDef = SchemaDefinition
        .<GenericRecord>builder()
        .withJsonDef("{\"type\": \"record\", \"name\": \"CentroidCollectionSchema\", \"fields\": [{\"name\": \"img_id\", \"type\": \"string\"}, {\"name\": \"centroids\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"record\", \"name\": \"CentroidSchema\", \"fields\": [{\"name\": \"centroid_y\", \"type\": \"int\"}, {\"name\": \"centroid_x\", \"type\": \"int\"}, {\"name\": \"centroid_id\", \"type\": \"string\"}, {\"name\": \"confidence\", \"type\": \"float\"}, {\"name\": \"classification\", \"type\": \"int\"}]}}}]}")
        .build();
      org.apache.pulsar.client.api.Schema<GenericRecord> schema = org.apache.pulsar.client.api.Schema.AVRO(schemaDef);
      org.apache.pulsar.shade.org.apache.avro.Schema schema2 = org.apache.pulsar.shade.org.apache.avro.Schema.parse("{\"type\": \"record\", \"name\": \"CentroidCollectionSchema\", \"fields\": [{\"name\": \"img_id\", \"type\": \"string\"}, {\"name\": \"centroids\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"record\", \"name\": \"CentroidSchema\", \"fields\": [{\"name\": \"centroid_y\", \"type\": \"int\"}, {\"name\": \"centroid_x\", \"type\": \"int\"}, {\"name\": \"centroid_id\", \"type\": \"string\"}, {\"name\": \"confidence\", \"type\": \"float\"}, {\"name\": \"classification\", \"type\": \"int\"}]}}}]}");

      String json = null;
      try (ByteArrayOutputStream boas = new ByteArrayOutputStream()) {
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema2);
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema2, boas, false);
        writer.write(avroObj.getNativeObject(), encoder);
        boas.flush();
        return new String(boas.toByteArray(), StandardCharsets.UTF_8);
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
            title = "Number of messages consumed."
        )
        private final Integer messagesCount;

        @Schema(
            title = "URI of a Kestra internal storage file containing the consumed messages."
        )
        private URI uri;
    }
}
