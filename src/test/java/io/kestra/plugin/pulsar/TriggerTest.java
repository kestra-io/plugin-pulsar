package io.kestra.plugin.pulsar;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws Exception {
        var task = Produce.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Produce.class.getName())
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .serializer(Property.ofValue(SerdeType.JSON))
            .topic(Property.ofValue("tu_trigger"))
            .from(
                List.of(
                    ImmutableMap.builder()
                        .put("key", "key1")
                        .put("value", "value1")
                        .build(),
                    ImmutableMap.builder()
                        .put("key", "key2")
                        .put("value", "value2")
                        .build()
                )
            )
            .build();

        task.run(runContextFactory.of(ImmutableMap.of()));
    }

    @Test
    @EvaluateTrigger(flow = "flows/trigger.yaml", triggerId = "watch")
    void run(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));
        var execution = optionalExecution.get();
        var messagesCount = (Integer) execution.getTrigger().getVariables().get("messagesCount");
        assertThat(messagesCount, greaterThanOrEqualTo(2));
    }
}
