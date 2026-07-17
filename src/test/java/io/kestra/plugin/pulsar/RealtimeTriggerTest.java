package io.kestra.plugin.pulsar;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import java.time.Duration;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.runners.TestRunnerUtils;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest(startRunner = true, startScheduler = true)
class RealtimeTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    private TestRunnerUtils runnerUtils;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Test
    void flow() throws Exception {
        repositoryLoader.load(
            Objects.requireNonNull(
                RealtimeTriggerTest.class.getClassLoader().getResource("flows/realtime.yaml")
            )
        );

        var task = Produce.builder()
            .id(RealtimeTriggerTest.class.getSimpleName())
            .type(Produce.class.getName())
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .serializer(Property.ofValue(SerdeType.JSON))
            .topic(Property.ofValue("tu_trigger"))
            .from(
                List.of(
                    Map.of(
                        "key", "key1",
                        "value", "value1"
                    )
                )
            )
            .build();

        task.run(runContextFactory.of(Map.of()));
        Execution last = runnerUtils.awaitFlowExecution(e -> true, MAIN_TENANT, "io.kestra.tests", "realtime", Duration.ofMinutes(1));
        assertThat(last, notNullValue());

        var variables = last.getTrigger().getVariables();
        assertThat(variables.get("key"), is("key1"));
        assertThat(variables.get("value"), is("value1"));
        assertThat(variables.get("topic"), is("persistent://public/default/tu_trigger"));
        assertThat(variables.get("messageId"), notNullValue());
    }
}
