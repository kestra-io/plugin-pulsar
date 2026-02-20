package io.kestra.plugin.pulsar;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;

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
    private Property<Duration> since;

    @Schema(
        title = "Start from specific message ID",
        description = "Reads from the message immediately after the provided ID. If neither `since` nor `messageId` is set, starts at earliest."
    )
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
                readerBuilder.startMessageId(MessageId.fromByteArray(runContext.render(this.messageId).as(String.class).orElseThrow().getBytes(StandardCharsets.UTF_8)));
            } else {
                readerBuilder.startMessageId(MessageId.earliest);
            }

            try (org.apache.pulsar.client.api.Reader<byte[]> reader = readerBuilder.create()) {
                return this.read(
                    runContext,
                    Rethrow.throwSupplier(() -> {
                        Message<byte[]> message = reader.readNext(runContext.render(this.getPollDuration()).as(Duration.class).orElseThrow().getNano(), TimeUnit.NANOSECONDS);

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
