package io.kestra.plugin.pulsar;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.data.Data;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema as PulsarSchema;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Produce messages to a Pulsar topic."
)
@Plugin(
    examples = {
        @Example(
            title = "Produce a single message to Pulsar topic `kestra.publish`.",
            full = true,
            code = """
                id: pulsar_produce_single
                namespace: company.team

                tasks:
                  - id: produce
                    type: io.kestra.plugin.pulsar.Produce
                    serviceUrl: pulsar://localhost:6650
                    topic: kestra.publish
                    from:
                      data: "Hello Pulsar!"
                """
        ),
        @Example(
            title = "Produce multiple messages to Pulsar.",
            full = true,
            code = """
                id: pulsar_produce_multiple
                namespace: company.team

                tasks:
                  - id: produce
                    type: io.kestra.plugin.pulsar.Produce
                    serviceUrl: pulsar://localhost:6650
                    topic: kestra.publish
                    from:
                      - data: "Message 1"
                      - data: "Message 2"
                """
        ),
        @Example(
            title = "Produce messages from Kestra internal storage file to Pulsar topic.",
            full = true,
            code = """
                id: pulsar_produce_from_file
                namespace: company.team

                tasks:
                  - id: produce
                    type: io.kestra.plugin.pulsar.Produce
                    serviceUrl: pulsar://localhost:6650
                    topic: kestra.publish
                    from: "{{ outputs.some_task_with_output_file.uri }}"
                """
        )
    }
)
public class Produce extends PulsarConnection implements RunnableTask<Produce.Output> {

    @Schema(
        title = "Topic to produce messages to."
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    private String topic;

    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = {String.class, List.class, Map.class}
    )
    @PluginProperty(dynamic = true, internalStorageURI = true)
    @NotNull
    private Data.From from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (PulsarClient client = createClient(runContext);
             Producer<String> producer = client.newProducer(PulsarSchema.STRING)
                 .topic(runContext.render(this.topic))
                 .create()) {

            int messagesCount = switch (from.resolveType(runContext)) {
                case STRING -> sendFromString(runContext, producer, from.asString(runContext));
                case LIST -> sendFromList(runContext, producer, from.asList(runContext));
                case MAP -> sendSingleFromMap(runContext, producer, from.asMap(runContext));
                case URI -> sendFromUri(runContext, producer, from.asUri(runContext));
            };

            producer.flush();
            return Output.builder().messagesCount(messagesCount).build();
        }
    }

    private int sendFromString(RunContext runContext, Producer<String> producer, String message)
        throws Exception {
        producer.send(message);
        return 1;
    }

    private int sendFromList(RunContext runContext, Producer<String> producer, List<?> list)
        throws IllegalVariableEvaluationException {
        return Flux.fromIterable(list)
            .map(throwFunction(object -> {
                Map<String, Object> msg = runContext.render((Map<String, Object>) object);
                producer.send(msg.get("data").toString());
                return 1;
            }))
            .reduce(Integer::sum)
            .blockOptional()
            .orElse(0);
    }

    private int sendSingleFromMap(RunContext runContext, Producer<String> producer, Map<String, Object> map)
        throws Exception {
        producer.send(runContext.render(map.get("data")).toString());
        return 1;
    }

    private int sendFromUri(RunContext runContext, Producer<String> producer, URI uri)
        throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(uri)))) {
            List<Map<String, Object>> messages = FileSerde.readAll(reader);
            return sendFromList(runContext, producer, messages);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of messages produced.")
        private final Integer messagesCount;
    }
}
