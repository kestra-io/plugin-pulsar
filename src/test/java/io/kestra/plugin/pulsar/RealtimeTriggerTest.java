package io.kestra.plugin.pulsar;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.ApplicationContext;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class RealtimeTriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private SchedulerTriggerStateInterface triggerState;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void flow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            Worker worker = applicationContext.createBean(Worker.class, UUID.randomUUID().toString(), 8, null);
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(RealtimeTriggerTest.class, execution -> {
                last.set(execution.getLeft());

                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("realtime"));
            });

            Produce task = Produce.builder()
                .id(RealtimeTriggerTest.class.getSimpleName())
                .type(Produce.class.getName())
                .uri("pulsar://localhost:26650")
                .serializer(SerdeType.JSON)
                .topic("tu_trigger")
                .from(List.of(
                    ImmutableMap.builder()
                        .put("key", "key1")
                        .put("value", "value1")
                        .build()
                ))
                .build();

            worker.run();
            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(RealtimeTriggerTest.class.getClassLoader().getResource("flows/realtime.yaml")));

            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

            queueCount.await(1, TimeUnit.MINUTES);

            Map<String, Object> variables = last.get().getTrigger().getVariables();

            assertThat(variables.get("key"), is("key1"));
            assertThat(variables.get("value"), is("value1"));
            assertThat(variables.get("topic"), is("persistent://public/default/tu_trigger"));
            assertThat(variables.get("messageId"), notNullValue());
        }
    }
}
