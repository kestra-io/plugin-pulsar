package io.kestra.plugin.pulsar;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read messages from Pulsar topics without subscription",
    description = "Uses a non-durable reader to fetch messages without creating a subscription. Defaults: deserializer STRING, poll timeout 2s, starts at earliest unless positioned otherwise."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: pulsar_reader
                namespace: company.team

                tasks:
                  - id: reader
                    type: io.kestra.plugin.pulsar.Reader
                    uri: pulsar://localhost:26650
                    topic: test_kestra
                    deserializer: JSON
                """
        )
    }
)
public class Reader extends AbstractReader {
    @Schema(
        title = "Rollback duration for start position",
        description = "Finds the latest message published before the given duration (e.g., `PT5M` starts 5 minutes in the past)."
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> since;

    @Schema(
        title = "Start from specific message ID",
        description = "Reads from the message immediately after the provided ID. Accepts `ledger:entry[:partition]`, `earliest`, `latest`, or the existing Pulsar byte form. If neither `since` nor `messageId` is set, starts at earliest."
    )
    @PluginProperty(group = "advanced")
    private Property<String> messageId;

    @Override
    public AbstractReader.Output run(RunContext runContext) throws Exception {
        try (PulsarClient client = PulsarService.client(this, runContext)) {
            org.apache.pulsar.client.api.ReaderBuilder<byte[]> readerBuilder = client.newReader()
                .topics(this.topics(runContext));

            var duration = runContext.render(this.since).as(Duration.class);
            if (duration.isPresent()) {
                readerBuilder.startMessageFromRollbackDuration(duration.get().getNano(), TimeUnit.NANOSECONDS);
            } else if (this.messageId != null) {
                readerBuilder.startMessageId(parseMessageId(runContext.render(this.messageId).as(String.class).orElseThrow()));
            } else {
                readerBuilder.startMessageId(MessageId.earliest);
            }

            try (org.apache.pulsar.client.api.Reader<byte[]> reader = readerBuilder.create()) {
                this.trackCloseable(reader);

                try {
                    return this.read(
                        runContext,
                        Rethrow.throwSupplier(() ->
                        {
                            try {
                                Message<byte[]> message = reader.readNext(runContext.render(this.getPollDuration()).as(Duration.class).orElseThrow().getNano(), TimeUnit.NANOSECONDS);

                                if (message == null) {
                                    return List.of();
                                } else {
                                    return List.of(message);
                                }
                            } catch (PulsarClientException.AlreadyClosedException e) {
                                // the reader was closed by kill() while blocked here: end the read loop
                                // instead of failing the task, since a kill is a normal way to stop.
                                if (!this.isActive()) {
                                    return List.of();
                                }

                                throw e;
                            }
                        })
                    );
                } finally {
                    this.untrackCloseable();
                }
            }
        }
    }

    static MessageId parseMessageId(String text) throws IOException {
        String value = text.trim();
        if (value.equalsIgnoreCase("earliest")) {
            return MessageId.earliest;
        }
        if (value.equalsIgnoreCase("latest")) {
            return MessageId.latest;
        }

        String[] parts = value.split(":", -1);
        if (parts.length == 2 || parts.length == 3) {
            try {
                long ledgerId = Long.parseLong(parts[0]);
                long entryId = Long.parseLong(parts[1]);
                int partitionIndex = parts.length == 3 ? Integer.parseInt(parts[2]) : -1;
                return new MessageIdImpl(ledgerId, entryId, partitionIndex);
            } catch (NumberFormatException ignored) {
                // Fall back to the existing byte form below.
            }
        }

        return MessageId.fromByteArray(text.getBytes(StandardCharsets.UTF_8));
    }
}
