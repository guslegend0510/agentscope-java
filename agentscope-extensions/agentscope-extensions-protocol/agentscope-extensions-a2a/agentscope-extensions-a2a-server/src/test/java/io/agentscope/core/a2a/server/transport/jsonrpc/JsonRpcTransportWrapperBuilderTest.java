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

package io.agentscope.core.a2a.server.transport.jsonrpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.a2aproject.sdk.server.ServerCallContext;
import org.a2aproject.sdk.server.requesthandlers.RequestHandler;
import org.a2aproject.sdk.spec.AgentCapabilities;
import org.a2aproject.sdk.spec.AgentCard;
import org.a2aproject.sdk.spec.AgentInterface;
import org.a2aproject.sdk.spec.Task;
import org.a2aproject.sdk.spec.TaskQueryParams;
import org.a2aproject.sdk.spec.TaskState;
import org.a2aproject.sdk.spec.TaskStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("JsonRpcTransportWrapperBuilder Tests")
class JsonRpcTransportWrapperBuilderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final AgentCard AGENT_CARD =
            AgentCard.builder()
                    .name("agent")
                    .description("agent")
                    .version("1.0.0")
                    .capabilities(
                            AgentCapabilities.builder()
                                    .streaming(true)
                                    .pushNotifications(false)
                                    .build())
                    .defaultInputModes(List.of("text"))
                    .defaultOutputModes(List.of("text"))
                    .skills(List.of())
                    .supportedInterfaces(
                            List.of(new AgentInterface("JSONRPC", "http://localhost:8080")))
                    .build();

    @Test
    @DisplayName("Should answer A2A 0.3 and A2A 1.0 requests with the same request handler")
    void testAnswersBothVersions() throws Exception {
        RequestHandler requestHandler = mock(RequestHandler.class);
        when(requestHandler.onGetTask(any(TaskQueryParams.class), any(ServerCallContext.class)))
                .thenReturn(
                        Task.builder()
                                .id("task123")
                                .contextId("context456")
                                .status(new TaskStatus(TaskState.TASK_STATE_WORKING))
                                .build());
        JsonRpcTransportWrapper wrapper =
                new JsonRpcTransportWrapperBuilder()
                        .build(AGENT_CARD, requestHandler, Runnable::run, null);

        JsonNode v03 =
                readResult(
                        wrapper.handleRequest(
                                "{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"method\":\"tasks/get\","
                                        + "\"params\":{\"id\":\"task123\"}}",
                                Map.of(),
                                Map.of()));
        JsonNode v10 =
                readResult(
                        wrapper.handleRequest(
                                "{\"jsonrpc\":\"2.0\",\"id\":\"1\",\"method\":\"GetTask\","
                                        + "\"params\":{\"id\":\"task123\"}}",
                                Map.of("A2A-Version", "1.0"),
                                Map.of()));

        assertEquals("task", v03.path("kind").asText(), v03.toString());
        assertEquals("task123", v03.path("id").asText());
        assertEquals("working", v03.path("status").path("state").asText());
        assertEquals("task123", v10.path("id").asText(), v10.toString());
        assertEquals("TASK_STATE_WORKING", v10.path("status").path("state").asText());
    }

    private static JsonNode readResult(Object json) throws Exception {
        return MAPPER.readTree((String) json).path("result");
    }
}
