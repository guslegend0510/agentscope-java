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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import org.a2aproject.sdk.jsonrpc.common.wrappers.ListTasksResult;
import org.a2aproject.sdk.server.ServerCallContext;
import org.a2aproject.sdk.server.agentexecution.AgentExecutor;
import org.a2aproject.sdk.server.auth.TaskOperation;
import org.a2aproject.sdk.server.events.InMemoryQueueManager;
import org.a2aproject.sdk.server.events.MainEventBus;
import org.a2aproject.sdk.server.events.MainEventBusProcessor;
import org.a2aproject.sdk.server.events.QueueManager;
import org.a2aproject.sdk.server.requesthandlers.DefaultRequestHandler;
import org.a2aproject.sdk.server.requesthandlers.RequestHandler;
import org.a2aproject.sdk.server.tasks.BasePushNotificationSender;
import org.a2aproject.sdk.server.tasks.DefaultPushNotificationUrlValidator;
import org.a2aproject.sdk.server.tasks.InMemoryPushNotificationConfigStore;
import org.a2aproject.sdk.server.tasks.InMemoryTaskStore;
import org.a2aproject.sdk.server.tasks.PushNotificationConfigStore;
import org.a2aproject.sdk.server.tasks.PushNotificationSender;
import org.a2aproject.sdk.server.tasks.TaskStateProvider;
import org.a2aproject.sdk.server.tasks.TaskStore;
import org.a2aproject.sdk.spec.A2AError;
import org.a2aproject.sdk.spec.CancelTaskParams;
import org.a2aproject.sdk.spec.DeleteTaskPushNotificationConfigParams;
import org.a2aproject.sdk.spec.EventKind;
import org.a2aproject.sdk.spec.GetTaskPushNotificationConfigParams;
import org.a2aproject.sdk.spec.ListTaskPushNotificationConfigsParams;
import org.a2aproject.sdk.spec.ListTaskPushNotificationConfigsResult;
import org.a2aproject.sdk.spec.ListTasksParams;
import org.a2aproject.sdk.spec.MessageSendParams;
import org.a2aproject.sdk.spec.StreamingEventKind;
import org.a2aproject.sdk.spec.Task;
import org.a2aproject.sdk.spec.TaskIdParams;
import org.a2aproject.sdk.spec.TaskPushNotificationConfig;
import org.a2aproject.sdk.spec.TaskQueryParams;

/**
 * The {@link RequestHandler} of an AgentScope A2A server, backed by the SDK's {@link
 * DefaultRequestHandler} and the components it needs.
 */
public class AgentScopeA2aRequestHandler implements RequestHandler, AutoCloseable {

    private final DefaultRequestHandler delegate;

    private final ExecutorService executor;

    private final ExecutorService eventConsumerExecutor;

    private final MainEventBusProcessor mainEventBusProcessor;

    private AgentScopeA2aRequestHandler(
            DefaultRequestHandler delegate,
            ExecutorService executor,
            ExecutorService eventConsumerExecutor,
            MainEventBusProcessor mainEventBusProcessor) {
        this.delegate = delegate;
        this.executor = executor;
        this.eventConsumerExecutor = eventConsumerExecutor;
        this.mainEventBusProcessor = mainEventBusProcessor;
    }

    @Override
    public Task onGetTask(TaskQueryParams params, ServerCallContext context) throws A2AError {
        return delegate.onGetTask(params, context);
    }

    @Override
    public ListTasksResult onListTasks(ListTasksParams params, ServerCallContext context)
            throws A2AError {
        return delegate.onListTasks(params, context);
    }

    @Override
    public Task onCancelTask(CancelTaskParams params, ServerCallContext context) throws A2AError {
        return delegate.onCancelTask(params, context);
    }

    @Override
    public EventKind onMessageSend(MessageSendParams params, ServerCallContext context)
            throws A2AError {
        return delegate.onMessageSend(params, context);
    }

    @Override
    public Flow.Publisher<StreamingEventKind> onMessageSendStream(
            MessageSendParams params, ServerCallContext context) throws A2AError {
        return delegate.onMessageSendStream(params, context);
    }

    @Override
    public TaskPushNotificationConfig onCreateTaskPushNotificationConfig(
            TaskPushNotificationConfig params, ServerCallContext context) throws A2AError {
        return delegate.onCreateTaskPushNotificationConfig(params, context);
    }

    @Override
    public TaskPushNotificationConfig onGetTaskPushNotificationConfig(
            GetTaskPushNotificationConfigParams params, ServerCallContext context) throws A2AError {
        return delegate.onGetTaskPushNotificationConfig(params, context);
    }

    @Override
    public Flow.Publisher<StreamingEventKind> onSubscribeToTask(
            TaskIdParams params, ServerCallContext context) throws A2AError {
        return delegate.onSubscribeToTask(params, context);
    }

    @Override
    public ListTaskPushNotificationConfigsResult onListTaskPushNotificationConfigs(
            ListTaskPushNotificationConfigsParams params, ServerCallContext context)
            throws A2AError {
        return delegate.onListTaskPushNotificationConfigs(params, context);
    }

    @Override
    public void onDeleteTaskPushNotificationConfig(
            DeleteTaskPushNotificationConfigParams params, ServerCallContext context)
            throws A2AError {
        delegate.onDeleteTaskPushNotificationConfig(params, context);
    }

    @Override
    public void authorizeTaskAccess(
            String requestedTaskId, ServerCallContext context, TaskOperation operation)
            throws A2AError {
        delegate.authorizeTaskAccess(requestedTaskId, context, operation);
    }

    /**
     * Shuts down the thread pools that run agent executions and consume their events with {@link
     * ExecutorService#shutdownNow()}, which requests interruption of executions still running and
     * does not wait for them to finish, and stops the thread that persists and dispatches events.
     */
    @Override
    public void close() {
        executor.shutdownNow();
        eventConsumerExecutor.shutdownNow();
        try {
            // Not public: the SDK stops the processor through CDI.
            Method stop = MainEventBusProcessor.class.getDeclaredMethod("stop");
            stop.setAccessible(true);
            stop.invoke(mainEventBusProcessor);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to stop the A2A event processor", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private AgentExecutor agentExecutor;

        private TaskStore taskStore;

        private MainEventBus mainEventBus;

        private QueueManager queueManager;

        private PushNotificationConfigStore pushConfigStore;

        private PushNotificationSender pushSender;

        private int agentCompletionTimeoutSeconds = 60;

        private int consumptionCompletionTimeoutSeconds = 10;

        public Builder agentExecutor(AgentExecutor agentExecutor) {
            this.agentExecutor = agentExecutor;
            return this;
        }

        public Builder taskStore(TaskStore taskStore) {
            this.taskStore = taskStore;
            return this;
        }

        /**
         * Set the bus that carries events from the queues to the processor that persists and
         * dispatches them. A custom {@link #queueManager(QueueManager) queue manager} must be
         * created with this bus. By default, a new bus is created.
         *
         * @param mainEventBus the event bus
         * @return this builder
         */
        public Builder mainEventBus(MainEventBus mainEventBus) {
            this.mainEventBus = mainEventBus;
            return this;
        }

        /**
         * Set the queue manager. It must be created with the {@link #mainEventBus(MainEventBus)
         * event bus} of this handler.
         *
         * @param queueManager the queue manager
         * @return this builder
         */
        public Builder queueManager(QueueManager queueManager) {
            this.queueManager = queueManager;
            return this;
        }

        public Builder pushConfigStore(PushNotificationConfigStore pushConfigStore) {
            this.pushConfigStore = pushConfigStore;
            return this;
        }

        public Builder pushSender(PushNotificationSender pushSender) {
            this.pushSender = pushSender;
            return this;
        }

        /**
         * Set how long a blocking request waits for the agent to finish, in seconds. Default 60.
         *
         * @param agentCompletionTimeoutSeconds timeout in seconds
         * @return this builder
         */
        public Builder agentCompletionTimeoutSeconds(int agentCompletionTimeoutSeconds) {
            this.agentCompletionTimeoutSeconds = agentCompletionTimeoutSeconds;
            return this;
        }

        /**
         * Set how long a blocking request waits for the produced events to be consumed after the
         * agent finished, in seconds. Default 10.
         *
         * @param consumptionCompletionTimeoutSeconds timeout in seconds
         * @return this builder
         */
        public Builder consumptionCompletionTimeoutSeconds(
                int consumptionCompletionTimeoutSeconds) {
            this.consumptionCompletionTimeoutSeconds = consumptionCompletionTimeoutSeconds;
            return this;
        }

        public AgentScopeA2aRequestHandler build() {
            if (null == agentExecutor) {
                throw new IllegalArgumentException("AgentExecutor is required.");
            }
            if (null == taskStore) {
                taskStore = new InMemoryTaskStore();
            }
            if (null == mainEventBus) {
                mainEventBus = new MainEventBus();
            }
            if (null == queueManager) {
                TaskStateProvider stateProvider =
                        taskStore instanceof TaskStateProvider provider
                                ? provider
                                : new AgentScopeTaskStateProvider(taskStore);
                queueManager = new InMemoryQueueManager(stateProvider, mainEventBus);
            }
            if (null == pushConfigStore) {
                pushConfigStore = new InMemoryPushNotificationConfigStore();
            }
            if (null == pushSender) {
                pushSender =
                        new BasePushNotificationSender(
                                pushConfigStore, new DefaultPushNotificationUrlValidator());
            }
            MainEventBusProcessor mainEventBusProcessor =
                    new MainEventBusProcessor(mainEventBus, taskStore, pushSender, queueManager);
            ExecutorService executor = Executors.newCachedThreadPool();
            ExecutorService eventConsumerExecutor = Executors.newCachedThreadPool();
            DefaultRequestHandler delegate =
                    DefaultRequestHandler.builder()
                            .agentExecutor(agentExecutor)
                            .taskStore(taskStore)
                            .queueManager(queueManager)
                            .pushConfigStore(pushConfigStore)
                            .mainEventBusProcessor(mainEventBusProcessor)
                            .executor(executor)
                            .eventConsumerExecutor(eventConsumerExecutor)
                            // AgentScope does not authenticate callers yet; with authorization
                            // required and no provider, every task operation is rejected.
                            .authorizationRequired(false)
                            .build();
            AgentScopeA2aRequestHandler result =
                    new AgentScopeA2aRequestHandler(
                            delegate, executor, eventConsumerExecutor, mainEventBusProcessor);
            try {
                setTimeoutProperties(delegate);
            } catch (RuntimeException e) {
                result.close();
                throw e;
            }
            return result;
        }

        /**
         * The SDK builder sets fixed timeouts of a few seconds, too short for agents calling
         * models, and offers no way to change them. Set them by reflection.
         */
        private void setTimeoutProperties(DefaultRequestHandler requestHandler) {
            try {
                setField(
                        requestHandler,
                        "agentCompletionTimeoutSeconds",
                        agentCompletionTimeoutSeconds);
                setField(
                        requestHandler,
                        "consumptionCompletionTimeoutSeconds",
                        consumptionCompletionTimeoutSeconds);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Failed to set the A2A request timeouts", e);
            }
        }

        private static void setField(DefaultRequestHandler handler, String name, int value)
                throws ReflectiveOperationException {
            Field field = DefaultRequestHandler.class.getDeclaredField(name);
            field.setAccessible(true);
            field.setInt(handler, value);
        }
    }

    private record AgentScopeTaskStateProvider(TaskStore taskStore) implements TaskStateProvider {

        @Override
        public boolean isTaskActive(String taskId) {
            Task task = taskStore.get(taskId);
            if (task == null) {
                return false;
            }
            // Task is active if not in final state
            return task.status() == null
                    || task.status().state() == null
                    || !task.status().state().isFinal();
        }

        @Override
        public boolean isTaskFinalized(String taskId) {
            Task task = taskStore.get(taskId);
            if (task == null) {
                return false;
            }
            // Task is finalized if in final state (ignores grace period)
            return task.status() != null
                    && task.status().state() != null
                    && task.status().state().isFinal();
        }
    }
}
