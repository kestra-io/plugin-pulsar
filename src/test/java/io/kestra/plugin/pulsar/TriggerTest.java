package io.kestra.plugin.pulsar;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest {
    @Inject
    private ApplicationContext applicationContext;

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
        try (Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null)) {
            try (
                AbstractScheduler scheduler = new JdbcScheduler(
                    this.applicationContext,
                    this.flowListenersService
                );
            ) {
                // wait for execution
                Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                    queueCount.countDown();
                    assertThat(execution.getLeft().getFlowId(), is("trigger"));
                });

                Produce task = Produce.builder()
                    .id(TriggerTest.class.getSimpleName())
                    .type(Produce.class.getName())
                    .uri(Property.ofValue("pulsar://localhost:26650"))
                    .serializer(Property.ofValue(SerdeType.JSON))
                    .topic(Property.ofValue("tu_trigger"))
                    .from(List.of(
                        ImmutableMap.builder()
                            .put("key", "key1")
                            .put("value", "value1")
                            .build(),
                        ImmutableMap.builder()
                            .put("key", "key2")
                            .put("value", "value2")
                            .build()
                    ))
                    .build();

                worker.run();
                scheduler.run();

                repositoryLoader.load(Objects.requireNonNull(TriggerTest.class.getClassLoader()
                    .getResource("flows/trigger.yaml")));

                task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

                boolean await = queueCount.await(1, TimeUnit.MINUTES);
                assertThat(await, is(true));

                Integer trigger = (Integer) Objects.requireNonNull(receive.blockLast()).getTrigger().getVariables().get("messagesCount");

                assertThat(trigger, greaterThanOrEqualTo(2));
            }
        }
    }
}
