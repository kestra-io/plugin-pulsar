package io.kestra.plugin.pulsar;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
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

        return storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));
    }

    @SuppressWarnings("unchecked")
    @Test
    void jsonStorage() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        URI uri = createInternalStorage();

        Produce task = Produce.builder()
            .uri("pulsar://localhost:26650")
            .serializer(SerdeType.JSON)
            .topic(topic)
            .from(uri.toString())
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(50));

        Consume consume = Consume.builder()
            .uri("pulsar://localhost:26650")
            .subscriptionName(IdUtils.create())
            .deserializer(SerdeType.JSON)
            .topic(task.getTopic())
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(50));

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(consumeOutput.getUri())));
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
            .uri("pulsar://localhost:26650")
            .serializer(SerdeType.JSON)
            .topic(topic)
            .from(uri.toString())
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(50));

        Reader reader = Reader.builder()
            .uri("pulsar://localhost:26650")
            .deserializer(SerdeType.JSON)
            .topic(task.getTopic())
            .build();

        Reader.Output consumeOutput = reader.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(50));

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(consumeOutput.getUri())));
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
            .uri("pulsar://localhost:26650")
            .serializer(SerdeType.JSON)
            .topic(topic)
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
            .uri("pulsar://localhost:26650")
            .subscriptionName(IdUtils.create())
            .deserializer(task.getSerializer())
            .topic(task.getTopic())
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
    }

    @Test
    void jsonArray() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce task = Produce.builder()
            .uri("pulsar://localhost:26650")
            .serializer(SerdeType.STRING)
            .topic(topic)
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
            .uri("pulsar://localhost:26650")
            .subscriptionName(IdUtils.create())
            .deserializer(task.getSerializer())
            .topic(List.of(topic))
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(2));
    }
}
