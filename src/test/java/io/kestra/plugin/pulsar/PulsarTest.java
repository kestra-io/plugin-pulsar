package io.kestra.plugin.pulsar;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException.IncompatibleSchemaException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.shade.org.apache.avro.AvroMissingFieldException;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class PulsarTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    URI createInternalStorage() throws IOException {
        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 50; i++) {
            HashMap<Object, Object> data = new HashMap<>();
            data.put("username", "Kestra-" + i);
            data.put("number", i);

            FileSerde.write(output, ImmutableMap.builder()
                .put("key", "key-" + i)
                .put("value", data)
                .put("eventTime", ZonedDateTime.parse("1998-01-23T06:00:00-05:00"))
                .build()
            );
        }

        return storageInterface.put(null, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }

    @SuppressWarnings("unchecked")
    @Test
    void jsonStorage() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        URI uri = createInternalStorage();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .serializer(Property.of(SerdeType.JSON))
            .topic(Property.of(topic))
            .from(uri.toString())
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(50));

        Consume consume = Consume.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .subscriptionName(Property.of(IdUtils.create()))
            .deserializer(Property.of(SerdeType.JSON))
            .topic(task.getTopic().toString())
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(50));

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, null, consumeOutput.getUri())));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(result.size(), is(50));

        Map<String, Object> value = (Map<String, Object>) result.get(1).get("value");
        assertThat(value.get("username"), is("Kestra-1"));
        assertThat(result.get(1).get("eventTime"), is(ZonedDateTime.parse("1998-01-23T06:00:00-05:00").toInstant()));
    }

    @SuppressWarnings("unchecked")
    @Test
    void reader() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        URI uri = createInternalStorage();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .serializer(Property.of(SerdeType.JSON))
            .topic(Property.of(topic))
            .from(uri.toString())
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(50));

        Reader reader = Reader.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .deserializer(Property.of(SerdeType.JSON))
            .topic(task.getTopic().toString())
            .build();

        Reader.Output consumeOutput = reader.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(50));

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, null, consumeOutput.getUri())));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(result.size(), is(50));

        Map<String, Object> value = (Map<String, Object>) result.get(1).get("value");
        assertThat(value.get("username"), is("Kestra-1"));
        assertThat(result.get(1).get("eventTime"), is(ZonedDateTime.parse("1998-01-23T06:00:00-05:00").toInstant()));
    }

    @Test
    void jsonMap() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .serializer(Property.of(SerdeType.JSON))
            .topic(Property.of(topic))
            .from(ImmutableMap.builder()
                .put("key", "string")
                .put("value", Map.of(
                    "username", "Kestra",
                    "tweet", "Kestra is open source",
                    "timestamp", System.currentTimeMillis() / 1000
                ))
                .put("timestamp", Instant.now().toEpochMilli())
                .build()
            )
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .subscriptionName(Property.of(IdUtils.create()))
            .deserializer(task.getSerializer())
            .topic(task.getTopic().toString())
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
    }

    @Test
    void jsonArray() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .serializer(Property.of(SerdeType.STRING))
            .topic(Property.of(topic))
            .from(List.of(
                ImmutableMap.builder()
                    .put("key", "string")
                    .put("value", Map.of(
                        "username", "Kestra",
                        "tweet", "Kestra is open source",
                        "timestamp", System.currentTimeMillis() / 1000
                    ))
                    .put("timestamp", Instant.now().toEpochMilli())
                    .build(),
                ImmutableMap.builder()
                    .put("key", "string")
                    .put("value", Map.of(
                        "username", "Kestra",
                        "tweet", "Kestra is open source",
                        "timestamp", System.currentTimeMillis() / 1000
                    ))
                    .put("timestamp", Instant.now().toEpochMilli())
                    .build()
            ))
            .build();

        Produce.Output runOutput = task.run(runContext);

        assertThat(runOutput.getMessagesCount(), is(2));

        Consume consume = Consume.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .subscriptionName(Property.of(IdUtils.create()))
            .deserializer(task.getSerializer())
            .topic(List.of(topic))
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(2));
    }

    @Test
    void avroSchema() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        String namespace = "public/default";
        String fullTopicName = namespace + "/" + topic;

        // Configure the topic to have strict schema rules and set the schema
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl("http://localhost:28080")
            .build();

        admin.namespaces().setIsAllowAutoUpdateSchema(namespace, false);
        admin.topics().createNonPartitionedTopic(fullTopicName);
        admin.topics().setSchemaValidationEnforced(fullTopicName, true);

        String schemaString = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"string\", \"type\": \"string\"}, {\"name\": \"array\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}, {\"name\": \"int\", \"type\": \"int\"}]}";
        admin.schemas().createSchema(fullTopicName, Schema.AVRO(SchemaDefinition.<GenericRecord>builder().withJsonDef(schemaString).build()).getSchemaInfo());

        ImmutableMap<Object, Object> item = ImmutableMap.builder()
        .put("value", Map.of(
            "string", "hello",
            "array", Arrays.asList(1,2,3),
            "int", 2
        ))
        .build();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .topic(Property.of(topic))
            .from(item)
            .schemaType(Property.of(SchemaType.AVRO))
            .schemaString(Property.of(schemaString))
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .subscriptionName(Property.of(IdUtils.create()))
            .deserializer(task.getSerializer())
            .topic(task.getTopic().toString())
            .schemaType(task.schemaType)
            .schemaString(task.schemaString)
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
    }

    @Test
    void nestedRecordTypeArray() throws Exception {
      RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        String namespace = "public/default";
        String fullTopicName = namespace + "/" + topic;

        // Configure the topic to have strict schema rules and set the schema
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl("http://localhost:28080")
            .build();

        admin.namespaces().setIsAllowAutoUpdateSchema(namespace, false);
        admin.topics().createNonPartitionedTopic(fullTopicName);
        admin.topics().setSchemaValidationEnforced(fullTopicName, true);

        String schemaString = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"string\", \"type\": \"string\"}, {\"name\": \"array\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"record\", \"name\": \"SubType\", \"fields\": [{\"name\":\"testVal\", \"type\":\"int\"}]}}}, {\"name\": \"int\", \"type\": \"int\"}]}";
        admin.schemas().createSchema(fullTopicName, Schema.AVRO(SchemaDefinition.<GenericRecord>builder().withJsonDef(schemaString).build()).getSchemaInfo());

        ImmutableMap<Object, Object> item = ImmutableMap.builder()
        .put("value", Map.of(
            "string", "hello",
            "array", Arrays.asList(new HashMap<String, Integer>(){{put("testVal", 3);}}, new HashMap<String, Integer>(){{put("testVal", 3);}}),
            "int", 2
        ))
        .build();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .topic(Property.of(topic))
            .from(item)
            .schemaType(Property.of(SchemaType.AVRO))
            .schemaString(Property.of(schemaString))
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .subscriptionName(Property.of(IdUtils.create()))
            .deserializer(task.getSerializer())
            .topic(task.getTopic().toString())
            .schemaType(task.schemaType)
            .schemaString(task.schemaString)
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
    }

    @Test
    void nestedRecordTypeMap() throws Exception {
      RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        String namespace = "public/default";
        String fullTopicName = namespace + "/" + topic;

        // Configure the topic to have strict schema rules and set the schema
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl("http://localhost:28080")
            .build();

        admin.namespaces().setIsAllowAutoUpdateSchema(namespace, false);
        admin.topics().createNonPartitionedTopic(fullTopicName);
        admin.topics().setSchemaValidationEnforced(fullTopicName, true);

        String schemaString = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"string\", \"type\": \"string\"}, {\"name\": \"map\", \"type\": {\"type\": \"map\", \"values\": {\"type\": \"record\", \"name\": \"SubType\", \"fields\": [{\"name\":\"testVal\", \"type\":\"int\"}]}}}, {\"name\": \"int\", \"type\": \"int\"}]}";
        admin.schemas().createSchema(fullTopicName, Schema.AVRO(SchemaDefinition.<GenericRecord>builder().withJsonDef(schemaString).build()).getSchemaInfo());

        ImmutableMap<Object, Object> item = ImmutableMap.builder()
        .put("value", Map.of(
            "string", "hello",
            "map", new HashMap<String, Object>(){{ put("a", new HashMap<String, Integer>(){{put("testVal", 3);}}); put("b", new HashMap<String, Integer>(){{put("testVal", 3);}}); }},
            "int", 2
        ))
        .build();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .topic(Property.of(topic))
            .from(item)
            .schemaType(Property.of(SchemaType.AVRO))
            .schemaString(Property.of(schemaString))
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .subscriptionName(Property.of(IdUtils.create()))
            .deserializer(task.getSerializer())
            .topic(task.getTopic().toString())
            .schemaType(task.schemaType)
            .schemaString(task.schemaString)
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
    }

    @Test
    void missingSchemaString() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .topic(Property.of(topic))
            .from(null)
            .schemaType(Property.of(SchemaType.AVRO))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
    }

    @Test
    void missingSchema() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        String namespace = "public/default";
        String fullTopicName = namespace + "/" + topic;

        // Configure the topic to have strict schema rules and set the schema
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl("http://localhost:28080")
            .build();

        admin.namespaces().setIsAllowAutoUpdateSchema(namespace, false);
        admin.topics().createNonPartitionedTopic(fullTopicName);
        admin.topics().setSchemaValidationEnforced(fullTopicName, true);

        String schemaString = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"string\", \"type\": \"string\"}, {\"name\": \"array\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}, {\"name\": \"int\", \"type\": \"int\"}]}";
        admin.schemas().createSchema(fullTopicName, Schema.AVRO(SchemaDefinition.<GenericRecord>builder().withJsonDef(schemaString).build()).getSchemaInfo());

        ImmutableMap<Object, Object> item = ImmutableMap.builder()
        .put("value", Map.of(
            "string", "hello",
            "array", Arrays.asList(1,2,3),
            "int", 2
        ))
        .build();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .topic(Property.of(topic))
            .from(item)
            .build();
        assertThrows(IncompatibleSchemaException.class, () -> task.run(runContext));
    }

    @Test
    void incorrectSchema() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        String namespace = "public/default";
        String fullTopicName = namespace + "/" + topic;

        // Configure the topic to have strict schema rules and set the schema
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl("http://localhost:28080")
            .build();

        admin.namespaces().setIsAllowAutoUpdateSchema(namespace, false);
        admin.topics().createNonPartitionedTopic(fullTopicName);
        admin.topics().setSchemaValidationEnforced(fullTopicName, true);

        String schemaString = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"string\", \"type\": \"string\"}, {\"name\": \"array\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}, {\"name\": \"int\", \"type\": \"int\"}]}";
        admin.schemas().createSchema(fullTopicName, Schema.AVRO(SchemaDefinition.<GenericRecord>builder().withJsonDef(schemaString).build()).getSchemaInfo());

        ImmutableMap<Object, Object> item = ImmutableMap.builder()
        .put("value", Map.of(
            "string", "hello",
            "array", Arrays.asList(1,2,3)
        ))
        .build();

        String incorrectSchemaString = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"string\", \"type\": \"string\"}, {\"name\": \"array\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}]}";
        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .topic(Property.of(topic))
            .from(item)
            .schemaType(Property.of(SchemaType.AVRO))
            .schemaString(Property.of(incorrectSchemaString))
            .build();
        assertThrows(IncompatibleSchemaException.class, () -> task.run(runContext));
    }

    @Test
    void incorrectItem() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        String namespace = "public/default";
        String fullTopicName = namespace + "/" + topic;

        // Configure the topic to have strict schema rules and set the schema
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl("http://localhost:28080")
            .build();

        admin.namespaces().setIsAllowAutoUpdateSchema(namespace, false);
        admin.topics().createNonPartitionedTopic(fullTopicName);
        admin.topics().setSchemaValidationEnforced(fullTopicName, true);

        String schemaString = "{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": [{\"name\": \"string\", \"type\": \"string\"}, {\"name\": \"array\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}, {\"name\": \"int\", \"type\": \"int\"}]}";
        admin.schemas().createSchema(fullTopicName, Schema.AVRO(SchemaDefinition.<GenericRecord>builder().withJsonDef(schemaString).build()).getSchemaInfo());

        ImmutableMap<Object, Object> item = ImmutableMap.builder()
        .put("value", Map.of(
            "string", "hello",
            "array", Arrays.asList(1,2,3)
        ))
        .build();

        Produce task = Produce.builder()
            .uri(Property.of("pulsar://localhost:26650"))
            .topic(Property.of(topic))
            .from(item)
            .schemaType(Property.of(SchemaType.AVRO))
            .schemaString(Property.of(schemaString))
            .build();
        assertThrows(AvroMissingFieldException.class, () -> task.run(runContext));
    }
}
