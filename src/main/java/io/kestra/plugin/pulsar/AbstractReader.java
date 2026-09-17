package io.kestra.plugin.pulsar;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericDatumWriter;
import org.apache.pulsar.shade.org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import lombok.*;
import lombok.experimental.SuperBuilder;

@Plugin(
    metrics = {
        @Metric(
            name = "reader.records",
            type = Counter.TYPE,
            description = "The total number of records consumed from Pulsar."
        ),
        @Metric(
            name = "records.total",
            type = Counter.TYPE,
            description = "The total number of records consumed across all topics."
        )
    }
)

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractReader extends AbstractPulsarConnection implements ReadInterface, PollingInterface, RunnableTask<AbstractReader.Output> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractReader.class);

    private Object topic;

    @Builder.Default
    private Property<SerdeType> deserializer = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(2));

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Maximum records before stop",
        description = "Soft limit evaluated each second; stops after this many messages if set."
    )
    private Property<Integer> maxRecords;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Maximum read duration",
        description = "Soft timeout evaluated each second; stops when exceeded."
    )
    private Property<Duration> maxDuration;

    /**
     * Cooperative exit flag flipped by {@link #kill()} or {@link #stop()}. The read loop in
     * {@link #read(RunContext, Supplier)} checks it to end the current run promptly instead of waiting
     * for {@code maxDuration}/{@code maxRecords}, since {@code kill()}/{@code stop()} are invoked from a
     * different thread than the one running the task.
     */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    /**
     * The live Pulsar {@code Consumer}/{@code Reader} currently in use, tracked so that {@link #kill()}
     * can close it and unblock an in-flight blocking receive call.
     */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<AutoCloseable> trackedCloseable = new AtomicReference<>();

    protected boolean isActive() {
        return this.isActive.get();
    }

    /**
     * Registers the currently open consumer/reader so that a kill signal can close it. Must be paired
     * with {@link #untrackCloseable()} once the caller is done with it.
     */
    protected void trackCloseable(AutoCloseable closeable) {
        this.trackedCloseable.set(closeable);
    }

    protected void untrackCloseable() {
        this.trackedCloseable.set(null);
    }

    @Override
    public void kill() {
        if (this.isActive.compareAndSet(true, false)) {
            LOG.info("Received a kill signal, closing the Pulsar consumer/reader");
            this.closeTracked();
        }
    }

    @Override
    public void stop() {
        if (this.isActive.compareAndSet(true, false)) {
            LOG.info("Received a stop signal, the current batch will complete and the task will end");
        }
    }

    private void closeTracked() {
        AutoCloseable closeable = this.trackedCloseable.getAndSet(null);
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while closing the Pulsar consumer/reader", e);
        } catch (Exception e) {
            LOG.warn("Failed to close the Pulsar consumer/reader", e);
        }
    }

    public Output read(RunContext runContext, Supplier<List<Message<byte[]>>> supplier) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        Map<String, Integer> count = new HashMap<>();
        AtomicInteger total = new AtomicInteger();
        ZonedDateTime started = ZonedDateTime.now();
        ZonedDateTime lastPool = ZonedDateTime.now();

        try (BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            do {
                if (!this.isActive()) {
                    break;
                }

                for (Message<byte[]> message : supplier.get()) {
                    boolean applySchema = runContext.render(this.schemaType).as(SchemaType.class).orElseThrow() != SchemaType.NONE;
                    if (applySchema && this.schemaString == null) {
                        throw new IllegalArgumentException("Must pass a \"schemaString\" when the \"schemaType\" is not null");
                    }

                    Map<Object, Object> map = new HashMap<>();
                    map.put("key", message.getKey());
                    map.put(
                        "value",
                        applySchema ? deserializeWithSchema(message.getValue(), runContext)
                            : runContext.render(this.deserializer).as(SerdeType.class).orElseThrow().deserialize(message.getValue())
                    );
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

                if (!this.isActive()) {
                    break;
                }
            } while (!this.ended(total, started, lastPool, runContext));

            output.flush();

            count
                .forEach((s, integer) -> runContext.metric(Counter.of("reader.records", integer, "topic", s)));

            runContext.metric(Counter.of("records.total", count.values().stream().mapToInt(Integer::intValue).sum()));

            return Output.builder()
                .messagesCount(count.values().stream().mapToInt(Integer::intValue).sum())
                .uri(runContext.storage().putFile(tempFile))
                .build();
        }
    }

    public String deserializeWithSchema(byte[] avroBinary, RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        Schema schema = Schema.parse(runContext.render(this.schemaString).as(String.class).orElse(null));

        // byte to datum
        DatumReader<Object> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avroBinary, null);
        Object avroObj = datumReader.read(null, decoder);

        try (ByteArrayOutputStream boas = new ByteArrayOutputStream()) {
            DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, boas, false);
            writer.write(avroObj, encoder);
            encoder.flush();
            boas.flush();
            return boas.toString(StandardCharsets.UTF_8);
        }
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(AtomicInteger count, ZonedDateTime start, ZonedDateTime lastPool, RunContext runContext) throws IllegalVariableEvaluationException {
        var max = runContext.render(this.maxRecords).as(Integer.class);
        if (max.isPresent() && count.get() > max.get()) {
            return true;
        }

        var maxDuration = runContext.render(this.maxDuration).as(Duration.class);
        if (maxDuration.isPresent() && ZonedDateTime.now().toEpochSecond() > start.plus(maxDuration.get()).toEpochSecond()) {
            return true;
        }

        if (ZonedDateTime.now().toEpochSecond() > lastPool.plus(runContext.render(this.pollDuration).as(Duration.class).orElseThrow()).toEpochSecond()) {
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
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of messages consumed"
        )
        private final Integer messagesCount;

        @io.swagger.v3.oas.annotations.media.Schema(
            title = "URI of Kestra storage file with consumed messages"
        )
        private URI uri;
    }
}
