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

import io.agentscope.core.a2a.server.transport.TransportWrapperBuilder;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import org.a2aproject.sdk.compat03.conversion.Convert_v0_3_To10RequestHandler;
import org.a2aproject.sdk.compat03.spec.AgentCapabilities_v0_3;
import org.a2aproject.sdk.compat03.spec.AgentCard_v0_3;
import org.a2aproject.sdk.compat03.transport.jsonrpc.handler.JSONRPCHandler_v0_3;
import org.a2aproject.sdk.server.requesthandlers.RequestHandler;
import org.a2aproject.sdk.spec.AgentCard;
import org.a2aproject.sdk.spec.TransportProtocol;
import org.a2aproject.sdk.transport.jsonrpc.handler.JSONRPCHandler;

/**
 * Builder implementation for {@link JsonRpcTransportWrapper}.
 */
public class JsonRpcTransportWrapperBuilder
        implements TransportWrapperBuilder<JsonRpcTransportWrapper> {

    @Override
    public String getTransportType() {
        return TransportProtocol.JSONRPC.asString();
    }

    @Override
    public JsonRpcTransportWrapper build(
            AgentCard agentCard,
            RequestHandler requestHandler,
            Executor executor,
            AgentCard extendedAgentCard) {
        // TODO: after support of extended agent card after support authenticated.
        JSONRPCHandler jsonrpcHandler = new JSONRPCHandler(agentCard, requestHandler, executor);
        // A2A 0.3 requests are converted and handled by the same request handler.
        JSONRPCHandler_v0_3 jsonRpcHandler03 =
                new JSONRPCHandler_v0_3(
                        a2a03Card(agentCard),
                        executor,
                        new Convert_v0_3_To10RequestHandler(requestHandler));
        return new JsonRpcTransportWrapper(jsonrpcHandler, jsonRpcHandler03);
    }

    /**
     * The card for the A2A 0.3 handler, which only reads its capabilities. Clients get the A2A 1.0
     * card, which has the fields A2A 0.3 clients read too.
     */
    private static AgentCard_v0_3 a2a03Card(AgentCard agentCard) {
        return new AgentCard_v0_3.Builder()
                .name(agentCard.name())
                .description(agentCard.description())
                .url(Objects.requireNonNullElse(agentCard.url(), ""))
                .version(agentCard.version())
                .capabilities(
                        new AgentCapabilities_v0_3(
                                agentCard.capabilities().streaming(),
                                agentCard.capabilities().pushNotifications(),
                                false,
                                null))
                .defaultInputModes(agentCard.defaultInputModes())
                .defaultOutputModes(agentCard.defaultOutputModes())
                .skills(List.of())
                .build();
    }
}
