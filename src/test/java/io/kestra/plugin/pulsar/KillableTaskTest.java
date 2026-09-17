package io.kestra.plugin.pulsar;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Verifies that {@link Consume} and {@link Reader} honor {@code kill()}/{@code stop()} instead of
 * running until {@code maxDuration}, since both are long-lived poll loops invoked from a worker
 * thread that a different thread must be able to cancel (server shutdown or execution kill/timeout).
 */
@KestraTest
public class KillableTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void consumeKillEndsThePollLoopPromptly() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Consume consume = Consume.builder()
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .subscriptionName(Property.ofValue(IdUtils.create()))
            .topic(topic)
            .maxDuration(Property.ofValue(Duration.ofHours(1)))
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                consume.run(runContext);
            } catch (Exception ignored) {
                // kill() may surface as an exception depending on where in the loop it lands; either
                // way the important thing is that the run ends promptly instead of after maxDuration.
            } finally {
                latch.countDown();
            }
        });
        thread.start();

        // let the consumer subscribe and enter the read loop before killing it
        Thread.sleep(1000);

        consume.kill();

        assertThat(latch.await(10, TimeUnit.SECONDS), is(true));
    }

    @Test
    void readerKillEndsThePollLoopPromptly() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Reader reader = Reader.builder()
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .topic(topic)
            .maxDuration(Property.ofValue(Duration.ofHours(1)))
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                reader.run(runContext);
            } catch (Exception ignored) {
                // same rationale as consumeKillEndsThePollLoopPromptly
            } finally {
                latch.countDown();
            }
        });
        thread.start();

        Thread.sleep(1000);

        reader.kill();

        assertThat(latch.await(10, TimeUnit.SECONDS), is(true));
    }

    @Test
    void consumeStopKeepsMessagesAlreadyRead() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce produce = Produce.builder()
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .serializer(Property.ofValue(SerdeType.STRING))
            .topic(Property.ofValue(topic))
            .from(List.of(
                Map.of("value", "hello-1"),
                Map.of("value", "hello-2")
            ))
            .build();
        Produce.Output produceOutput = produce.run(runContext);
        assertThat(produceOutput.getMessagesCount(), is(2));

        Consume consume = Consume.builder()
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .subscriptionName(Property.ofValue(IdUtils.create()))
            .topic(topic)
            .maxDuration(Property.ofValue(Duration.ofHours(1)))
            .pollDuration(Property.ofValue(Duration.ofMillis(500)))
            .build();

        AtomicReference<Consume.Output> result = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                result.set(consume.run(runContext));
            } catch (Exception ignored) {
                // a stop signal must let the current batch complete, not throw
            }
        });
        thread.start();

        // let the already-produced messages be picked up by an early batch before signaling stop
        Thread.sleep(1500);

        consume.stop();

        thread.join(10_000);

        assertThat(thread.isAlive(), is(false));

        Consume.Output output = result.get();
        assertThat(output, is(notNullValue()));
        assertThat(output.getMessagesCount(), is(2));
        assertThat(output.getUri(), is(notNullValue()));
    }

    @Test
    void killAndStopAreIdempotentAndNullSafeOutsideOfARun() {
        Consume consume = Consume.builder()
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .subscriptionName(Property.ofValue(IdUtils.create()))
            .topic("tu_" + IdUtils.create())
            .build();

        // before run() starts, there is no tracked consumer: must be a silent no-op
        assertDoesNotThrow(consume::kill);
        assertDoesNotThrow(consume::stop);

        // repeated signals, in either order, must stay idempotent
        assertDoesNotThrow(consume::kill);
        assertDoesNotThrow(consume::kill);
        assertDoesNotThrow(consume::stop);
        assertDoesNotThrow(consume::stop);

        Reader reader = Reader.builder()
            .uri(Property.ofValue("pulsar://localhost:26650"))
            .topic("tu_" + IdUtils.create())
            .build();

        assertDoesNotThrow(reader::kill);
        assertDoesNotThrow(reader::stop);
        assertDoesNotThrow(reader::kill);
        assertDoesNotThrow(reader::stop);
    }
}
