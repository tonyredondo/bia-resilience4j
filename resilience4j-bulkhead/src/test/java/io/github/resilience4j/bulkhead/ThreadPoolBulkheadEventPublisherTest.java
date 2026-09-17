/*
 *
 *  Copyright 2026 Robert Winkler, Lucas Lech, Mahmoud Romeh
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package io.github.resilience4j.bulkhead;

import org.awaitility.Awaitility;
import io.github.resilience4j.test.HelloWorldService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

class ThreadPoolBulkheadEventPublisherTest {

    private HelloWorldService helloWorldService;
    private ThreadPoolBulkheadConfig config;
    private Logger logger;
    private ThreadPoolBulkhead bulkhead;

    @BeforeEach
    void setUp() {
        helloWorldService = mock(HelloWorldService.class);
        config = ThreadPoolBulkheadConfig.custom()
            .maxThreadPoolSize(1)
            .coreThreadPoolSize(1)
            .build();

        bulkhead = ThreadPoolBulkhead.of("test", config);

        logger = mock(Logger.class);
        Awaitility.reset();
    }

    @Test
    void shouldReturnTheSameConsumer() {
        ThreadPoolBulkhead.ThreadPoolBulkheadEventPublisher eventPublisher = bulkhead
            .getEventPublisher();
        ThreadPoolBulkhead.ThreadPoolBulkheadEventPublisher eventPublisher2 = bulkhead
            .getEventPublisher();

        assertThat(eventPublisher).isEqualTo(eventPublisher2);
    }

    @Test
    void shouldConsumeOnCallRejectedEvent() throws Exception {
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkhead
            .of("test", ThreadPoolBulkheadConfig.custom()
                .maxThreadPoolSize(1)
                .coreThreadPoolSize(1)
                .queueCapacity(1)
                .build());
        given(helloWorldService.returnHelloWorld()).willReturn("Hello world");
        bulkhead.getEventPublisher().onCallRejected(
            event -> logger.info(event.getEventType().toString()));
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try {
            CompletionStage<String> first = bulkhead.executeCallable(() -> {
                running.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("First call was not released");
                }
                return "first";
            });
            assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();
            CompletionStage<String> queued = bulkhead.executeCallable(helloWorldService::returnHelloWorld);

            assertThatThrownBy(() -> bulkhead.executeCallable(helloWorldService::returnHelloWorld))
                .isInstanceOf(BulkheadFullException.class);
            then(logger).should(times(1)).info("CALL_REJECTED");

            release.countDown();
            assertThat(first.toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("first");
            assertThat(queued.toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo("Hello world");
        } finally {
            release.countDown();
            bulkhead.close();
        }
    }

    @Test
    void shouldConsumeOnCallPermittedEvent()
        throws Exception {
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkhead.of("test", config);
        given(helloWorldService.returnHelloWorld()).willReturn("Hello world");
        bulkhead.getEventPublisher().onCallPermitted(
            event -> logger.info(event.getEventType().toString()));

        String result = bulkhead.executeSupplier(helloWorldService::returnHelloWorld)
            .toCompletableFuture().get();

        assertThat(result).isEqualTo("Hello world");
        then(logger).should(times(1)).info("CALL_PERMITTED");
    }

    @Test
    void shouldConsumeOnCallFinishedEventWhenExecutionIsFinished() throws Exception {
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkhead.of("test", config);
        given(helloWorldService.returnHelloWorld()).willReturn("Hello world");
        bulkhead.getEventPublisher().onCallFinished(
            event -> logger.info(event.getEventType().toString()));

        bulkhead.executeSupplier(helloWorldService::returnHelloWorld).toCompletableFuture().get();

        then(logger).should(times(1)).info("CALL_FINISHED");
    }
}
