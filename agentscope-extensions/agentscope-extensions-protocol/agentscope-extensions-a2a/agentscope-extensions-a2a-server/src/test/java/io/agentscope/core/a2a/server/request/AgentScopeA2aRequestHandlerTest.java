/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.agentscope.core.a2a.server.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.a2a.server.constants.A2aServerConstants;
import io.agentscope.core.a2a.server.executor.AgentExecuteProperties;
import io.agentscope.core.a2a.server.executor.AgentScopeAgentExecutor;
import io.agentscope.core.a2a.server.executor.runner.AgentRequestOptions;
import io.agentscope.core.a2a.server.executor.runner.AgentRunner;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.a2aproject.sdk.server.ServerCallContext;
import org.a2aproject.sdk.server.agentexecution.AgentExecutor;
import org.a2aproject.sdk.server.auth.UnauthenticatedUser;
import org.a2aproject.sdk.server.events.InMemoryQueueManager;
import org.a2aproject.sdk.server.events.MainEventBus;
import org.a2aproject.sdk.server.events.MainEventBusProcessor;
import org.a2aproject.sdk.server.events.QueueManager;
import org.a2aproject.sdk.server.requesthandlers.DefaultRequestHandler;
import org.a2aproject.sdk.server.tasks.InMemoryTaskStore;
import org.a2aproject.sdk.server.tasks.TaskStateProvider;
import org.a2aproject.sdk.server.tasks.TaskStore;
import org.a2aproject.sdk.spec.CancelTaskParams;
import org.a2aproject.sdk.spec.Message;
import org.a2aproject.sdk.spec.MessageSendParams;
import org.a2aproject.sdk.spec.StreamingEventKind;
import org.a2aproject.sdk.spec.Task;
import org.a2aproject.sdk.spec.TaskNotCancelableError;
import org.a2aproject.sdk.spec.TaskState;
import org.a2aproject.sdk.spec.TaskStatus;
import org.a2aproject.sdk.spec.TextPart;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Unit tests for AgentScopeA2aRequestHandler.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Builder validation</li>
 *   <li>AgentScopeTaskStateProvider behavior</li>
 *   <li>QueueManager initialization with different TaskStore types</li>
 * </ul>
 */
@DisplayName("AgentScopeA2aRequestHandler Tests")
class AgentScopeA2aRequestHandlerTest {

    private final List<AgentScopeA2aRequestHandler> handlers = new ArrayList<>();

    /** Registers a handler so that it is closed after the test, whatever the outcome. */
    private AgentScopeA2aRequestHandler track(AgentScopeA2aRequestHandler handler) {
        handlers.add(handler);
        return handler;
    }

    @AfterEach
    void closeHandlers() {
        handlers.forEach(AgentScopeA2aRequestHandler::close);
    }

    private static Object readField(Class<?> type, Object target, String name) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static DefaultRequestHandler delegateOf(AgentScopeA2aRequestHandler handler)
            throws Exception {
        return (DefaultRequestHandler)
                readField(AgentScopeA2aRequestHandler.class, handler, "delegate");
    }

    /**
     * Helper method to create a AgentScopeA2aRequestHandler and extract its TaskStateProvider
     * through reflection.
     *
     * @param taskStore The task store to use
     * @return The TaskStateProvider instance
     * @throws Exception if reflection fails
     */
    private TaskStateProvider getTaskStateProvider(TaskStore taskStore) throws Exception {
        // Create handler with custom TaskStore
        AgentExecutor mockExecutor = mock(AgentExecutor.class);

        AgentScopeA2aRequestHandler handler =
                track(
                        AgentScopeA2aRequestHandler.builder()
                                .agentExecutor(mockExecutor)
                                .taskStore(taskStore)
                                .build());

        // Get TaskStateProvider through reflection
        Field queueManagerField = DefaultRequestHandler.class.getDeclaredField("queueManager");
        queueManagerField.setAccessible(true);
        QueueManager queueManager = (QueueManager) queueManagerField.get(delegateOf(handler));

        Field taskStateProviderField =
                InMemoryQueueManager.class.getDeclaredField("taskStateProvider");
        taskStateProviderField.setAccessible(true);
        return (TaskStateProvider) taskStateProviderField.get(queueManager);
    }

    @Nested
    @DisplayName("Builder Tests")
    class BuilderTests {

        @Test
        @DisplayName("Should throw exception when agentExecutor is null")
        void testBuildWithNullAgentExecutor() {
            AgentScopeA2aRequestHandler.Builder builder = AgentScopeA2aRequestHandler.builder();

            IllegalArgumentException exception =
                    assertThrows(IllegalArgumentException.class, builder::build);

            assertEquals("AgentExecutor is required.", exception.getMessage());
        }

        @Test
        @DisplayName("Should build successfully with valid agentExecutor")
        void testBuildWithValidAgentExecutor() {
            AgentExecutor mockExecutor = mock(AgentExecutor.class);
            AgentScopeA2aRequestHandler.Builder builder =
                    AgentScopeA2aRequestHandler.builder().agentExecutor(mockExecutor);

            AgentScopeA2aRequestHandler handler = track(builder.build());

            assertNotNull(handler);
        }
    }

    @Nested
    @DisplayName("AgentScopeTaskStateProvider Tests")
    class AgentScopeTaskStateProviderTests {

        @Test
        @DisplayName("Should return false for isTaskActive when task is null")
        void testIsTaskActiveWithNullTask() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            when(mockTaskStore.get("nonexistent")).thenReturn(null);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskActive("nonexistent");

            assertFalse(result);
        }

        @Test
        @DisplayName("Should return true for isTaskActive when task status is null")
        void testIsTaskActiveWithNullTaskStatus() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            Task mockTask = mock(Task.class);
            when(mockTaskStore.get("task123")).thenReturn(mockTask);
            when(mockTask.status()).thenReturn(null);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskActive("task123");

            assertTrue(result);
        }

        @Test
        @DisplayName("Should return true for isTaskActive when task state is null")
        void testIsTaskActiveWithNullTaskState() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            Task mockTask = mock(Task.class);
            TaskStatus mockStatus = mock(TaskStatus.class);
            when(mockTaskStore.get("task123")).thenReturn(mockTask);
            when(mockTask.status()).thenReturn(mockStatus);
            when(mockStatus.state()).thenReturn(null);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskActive("task123");

            assertTrue(result);
        }

        @Test
        @DisplayName("Should return true for isTaskActive when task state is not final")
        void testIsTaskActiveWithNonFinalTaskState() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            Task mockTask = mock(Task.class);
            TaskStatus mockStatus = mock(TaskStatus.class);
            when(mockTaskStore.get("task123")).thenReturn(mockTask);
            when(mockTask.status()).thenReturn(mockStatus);
            when(mockStatus.state()).thenReturn(TaskState.TASK_STATE_WORKING);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskActive("task123");

            assertTrue(result);
        }

        @Test
        @DisplayName("Should return false for isTaskActive when task state is final")
        void testIsTaskActiveWithFinalTaskState() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            Task mockTask = mock(Task.class);
            TaskStatus mockStatus = mock(TaskStatus.class);
            when(mockTaskStore.get("task123")).thenReturn(mockTask);
            when(mockTask.status()).thenReturn(mockStatus);
            when(mockStatus.state()).thenReturn(TaskState.TASK_STATE_COMPLETED);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskActive("task123");

            assertFalse(result);
        }

        @Test
        @DisplayName("Should return false for isTaskFinalized when task is null")
        void testIsTaskFinalizedWithNullTask() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            when(mockTaskStore.get("nonexistent")).thenReturn(null);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskFinalized("nonexistent");

            assertFalse(result);
        }

        @Test
        @DisplayName("Should return false for isTaskFinalized when task status is null")
        void testIsTaskFinalizedWithNullTaskStatus() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            Task mockTask = mock(Task.class);
            when(mockTaskStore.get("task123")).thenReturn(mockTask);
            when(mockTask.status()).thenReturn(null);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskFinalized("task123");

            assertFalse(result);
        }

        @Test
        @DisplayName("Should return false for isTaskFinalized when task state is null")
        void testIsTaskFinalizedWithNullTaskState() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            Task mockTask = mock(Task.class);
            TaskStatus mockStatus = mock(TaskStatus.class);
            when(mockTaskStore.get("task123")).thenReturn(mockTask);
            when(mockTask.status()).thenReturn(mockStatus);
            when(mockStatus.state()).thenReturn(null);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskFinalized("task123");

            assertFalse(result);
        }

        @Test
        @DisplayName("Should return false for isTaskFinalized when task state is not final")
        void testIsTaskFinalizedWithNonFinalTaskState() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            Task mockTask = mock(Task.class);
            TaskStatus mockStatus = mock(TaskStatus.class);
            when(mockTaskStore.get("task123")).thenReturn(mockTask);
            when(mockTask.status()).thenReturn(mockStatus);
            when(mockStatus.state()).thenReturn(TaskState.TASK_STATE_WORKING);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskFinalized("task123");

            assertFalse(result);
        }

        @Test
        @DisplayName("Should return true for isTaskFinalized when task state is final")
        void testIsTaskFinalizedWithFinalTaskState() throws Exception {
            TaskStore mockTaskStore = mock(TaskStore.class);
            Task mockTask = mock(Task.class);
            TaskStatus mockStatus = mock(TaskStatus.class);
            when(mockTaskStore.get("task123")).thenReturn(mockTask);
            when(mockTask.status()).thenReturn(mockStatus);
            when(mockStatus.state()).thenReturn(TaskState.TASK_STATE_COMPLETED);

            TaskStateProvider taskStateProvider = getTaskStateProvider(mockTaskStore);

            // Verify it's AgentScopeTaskStateProvider
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());

            boolean result = taskStateProvider.isTaskFinalized("task123");

            assertTrue(result);
        }
    }

    @Nested
    @DisplayName("QueueManager Initialization Tests")
    class QueueManagerInitializationTests {

        @Test
        @DisplayName(
                "Should use InMemoryTaskStore directly when queueManager is null and taskStore is"
                        + " InMemoryTaskStore")
        void testQueueManagerWithInMemoryTaskStore() throws Exception {
            AgentExecutor mockExecutor = mock(AgentExecutor.class);

            AgentScopeA2aRequestHandler handler =
                    track(
                            AgentScopeA2aRequestHandler.builder()
                                    .agentExecutor(mockExecutor)
                                    .taskStore(new InMemoryTaskStore())
                                    .build());

            // Access the queueManager field from the parent class
            Field queueManagerField = DefaultRequestHandler.class.getDeclaredField("queueManager");
            queueManagerField.setAccessible(true);
            QueueManager queueManager = (QueueManager) queueManagerField.get(delegateOf(handler));

            assertNotNull(queueManager);
            assertInstanceOf(InMemoryQueueManager.class, queueManager);

            // Get the taskStateProvider field from InMemoryQueueManager
            Field taskStateProviderField =
                    InMemoryQueueManager.class.getDeclaredField("taskStateProvider");
            taskStateProviderField.setAccessible(true);
            TaskStateProvider taskStateProvider =
                    (TaskStateProvider) taskStateProviderField.get(queueManager);

            // Should be the InMemoryTaskStore instance directly
            assertInstanceOf(InMemoryTaskStore.class, taskStateProvider);
        }

        @Test
        @DisplayName(
                "Should use AgentScopeTaskStateProvider when queueManager is null and taskStore is"
                        + " custom")
        void testQueueManagerWithCustomTaskStore() throws Exception {
            AgentExecutor mockExecutor = mock(AgentExecutor.class);
            TaskStore mockTaskStore = mock(TaskStore.class);

            AgentScopeA2aRequestHandler handler =
                    track(
                            AgentScopeA2aRequestHandler.builder()
                                    .agentExecutor(mockExecutor)
                                    .taskStore(mockTaskStore)
                                    .build());

            // Access the queueManager field from the parent class
            Field queueManagerField = DefaultRequestHandler.class.getDeclaredField("queueManager");
            queueManagerField.setAccessible(true);
            QueueManager queueManager = (QueueManager) queueManagerField.get(delegateOf(handler));

            assertNotNull(queueManager);
            assertInstanceOf(InMemoryQueueManager.class, queueManager);

            // Get the taskStateProvider field from InMemoryQueueManager
            Field taskStateProviderField =
                    InMemoryQueueManager.class.getDeclaredField("taskStateProvider");
            taskStateProviderField.setAccessible(true);
            TaskStateProvider taskStateProvider =
                    (TaskStateProvider) taskStateProviderField.get(queueManager);

            // Should be the AgentScopeTaskStateProvider instance
            assertEquals(
                    "io.agentscope.core.a2a.server.request.AgentScopeA2aRequestHandler$AgentScopeTaskStateProvider",
                    taskStateProvider.getClass().getName());
        }
    }

    @Nested
    @DisplayName("Timeout And Close Tests")
    class TimeoutAndCloseTests {

        @Test
        @DisplayName("Should wait 60s for the agent and 10s for consumption by default")
        void testDefaultTimeouts() throws Exception {
            AgentScopeA2aRequestHandler handler =
                    track(
                            AgentScopeA2aRequestHandler.builder()
                                    .agentExecutor(mock(AgentExecutor.class))
                                    .build());

            assertEquals(60, timeout(handler, "agentCompletionTimeoutSeconds"));
            assertEquals(10, timeout(handler, "consumptionCompletionTimeoutSeconds"));
        }

        @Test
        @DisplayName("Should apply configured timeouts")
        void testConfiguredTimeouts() throws Exception {
            AgentScopeA2aRequestHandler handler =
                    track(
                            AgentScopeA2aRequestHandler.builder()
                                    .agentExecutor(mock(AgentExecutor.class))
                                    .agentCompletionTimeoutSeconds(120)
                                    .consumptionCompletionTimeoutSeconds(30)
                                    .build());

            assertEquals(120, timeout(handler, "agentCompletionTimeoutSeconds"));
            assertEquals(30, timeout(handler, "consumptionCompletionTimeoutSeconds"));
        }

        @Test
        @DisplayName("Should shut down the thread pools and stop the event processor on close")
        void testCloseShutsDownExecutor() throws Exception {
            AgentScopeA2aRequestHandler handler =
                    track(
                            AgentScopeA2aRequestHandler.builder()
                                    .agentExecutor(mock(AgentExecutor.class))
                                    .build());
            ExecutorService executor = (ExecutorService) field(handler, "executor");
            ExecutorService eventConsumerExecutor =
                    (ExecutorService) field(handler, "eventConsumerExecutor");
            Thread processorThread =
                    (Thread)
                            readField(
                                    MainEventBusProcessor.class,
                                    field(handler, "mainEventBusProcessor"),
                                    "processorThread");
            assertFalse(executor.isShutdown());
            assertFalse(eventConsumerExecutor.isShutdown());
            assertTrue(processorThread.isAlive());

            handler.close();

            assertTrue(executor.isShutdown());
            assertTrue(eventConsumerExecutor.isShutdown());
            assertFalse(processorThread.isAlive());
        }

        private Object field(AgentScopeA2aRequestHandler handler, String name) throws Exception {
            return readField(AgentScopeA2aRequestHandler.class, handler, name);
        }

        private Object timeout(AgentScopeA2aRequestHandler handler, String name) throws Exception {
            return readField(DefaultRequestHandler.class, delegateOf(handler), name);
        }
    }

    @Nested
    @DisplayName("Cancel Tests")
    class CancelTests {

        @Test
        @DisplayName("A cancel after completion should be rejected and keep the task completed")
        void testCancelAfterCompletionIsRejected() throws Exception {
            // Keeps the completed task out of the store, so the cancel passes the SDK's
            // final-state check after the execution completed.
            CountDownLatch storeCompletion = new CountDownLatch(1);
            InMemoryTaskStore taskStore =
                    new InMemoryTaskStore() {
                        @Override
                        public void save(Task task, boolean isReplicated) {
                            if (task.status().state().isFinal()) {
                                awaitQuietly(storeCompletion);
                            }
                            super.save(task, isReplicated);
                        }
                    };
            MainEventBus mainEventBus = new MainEventBus();
            InMemoryQueueManager queueManager = new InMemoryQueueManager(taskStore, mainEventBus);
            CountDownLatch completed = new CountDownLatch(1);
            CountDownLatch returnFromExecute = new CountDownLatch(1);
            AgentRunner agentRunner = mock(AgentRunner.class);
            when(agentRunner.streamEvents(anyList(), any(AgentRequestOptions.class)))
                    .thenReturn(
                            Flux.<AgentEvent>just(
                                            new TextBlockDeltaEvent("reply-id", "block-id", "done"))
                                    // Runs after the completion is published, before execute()
                                    // returns.
                                    .doFinally(
                                            signal -> {
                                                completed.countDown();
                                                awaitQuietly(returnFromExecute);
                                            }));
            AgentScopeA2aRequestHandler handler =
                    track(
                            AgentScopeA2aRequestHandler.builder()
                                    .agentExecutor(
                                            new AgentScopeAgentExecutor(
                                                    agentRunner,
                                                    AgentExecuteProperties.builder().build()))
                                    .taskStore(taskStore)
                                    .mainEventBus(mainEventBus)
                                    .queueManager(queueManager)
                                    .build());
            StreamRecorder stream = new StreamRecorder();
            try {
                // The SDK stores streamed events on the subscribing thread.
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                handler.onMessageSendStream(userMessage(), streamingContext())
                                        .subscribe(stream);
                            } catch (Exception e) {
                                stream.onError(e);
                            }
                        });
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertTrue(stream.firstEvent.await(5, TimeUnit.SECONDS));
                String taskId = ((Task) stream.events.get(0)).id();
                assertEquals(TaskState.TASK_STATE_WORKING, taskStore.get(taskId).status().state());

                // The SDK waits for the events of the cancel, which follow the completion.
                CompletableFuture<Task> cancel =
                        CompletableFuture.supplyAsync(
                                () ->
                                        handler.onCancelTask(
                                                new CancelTaskParams(taskId), streamingContext()));
                storeCompletion.countDown();

                ExecutionException rejected =
                        assertThrows(
                                ExecutionException.class, () -> cancel.get(5, TimeUnit.SECONDS));
                assertInstanceOf(TaskNotCancelableError.class, rejected.getCause());
                assertEquals(
                        TaskState.TASK_STATE_COMPLETED, taskStore.get(taskId).status().state());
                assertTrue(stream.done.await(5, TimeUnit.SECONDS));
                returnFromExecute.countDown();
                // -1: the task has no open queue left.
                assertTrue(
                        waitFor(() -> queueManager.getActiveChildQueueCount(taskId) == -1),
                        "the queues of the request and the cancel must be closed");
            } finally {
                storeCompletion.countDown();
                returnFromExecute.countDown();
            }
        }

        private MessageSendParams userMessage() {
            Message message =
                    Message.builder()
                            .role(Message.Role.ROLE_USER)
                            .parts(new TextPart("hello"))
                            .messageId(UUID.randomUUID().toString())
                            .build();
            return new MessageSendParams(message, null, null);
        }

        private ServerCallContext streamingContext() {
            Map<String, Object> state = new HashMap<>();
            state.put(A2aServerConstants.ContextKeys.IS_STREAM_KEY, true);
            return new ServerCallContext(UnauthenticatedUser.INSTANCE, state, Set.of());
        }
    }

    private static final class StreamRecorder implements Flow.Subscriber<StreamingEventKind> {

        private final List<Object> events = new CopyOnWriteArrayList<>();

        private final CountDownLatch firstEvent = new CountDownLatch(1);

        private final CountDownLatch done = new CountDownLatch(1);

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(StreamingEventKind item) {
            events.add(item);
            firstEvent.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            events.add(throwable);
            done.countDown();
        }

        @Override
        public void onComplete() {
            done.countDown();
        }
    }

    private static boolean waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
