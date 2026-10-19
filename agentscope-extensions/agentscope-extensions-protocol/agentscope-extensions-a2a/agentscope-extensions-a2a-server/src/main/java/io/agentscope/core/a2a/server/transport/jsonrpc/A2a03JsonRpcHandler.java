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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import io.agentscope.core.a2a.server.constants.A2aServerConstants;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Flow;
import org.a2aproject.sdk.compat03.conversion.A2AProtocol_v0_3;
import org.a2aproject.sdk.compat03.json.JsonProcessingException_v0_3;
import org.a2aproject.sdk.compat03.json.JsonUtil_v0_3;
import org.a2aproject.sdk.compat03.spec.CancelTaskRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.DeleteTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetAuthenticatedExtendedCardRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.GetTaskRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.InternalError_v0_3;
import org.a2aproject.sdk.compat03.spec.InvalidParamsError_v0_3;
import org.a2aproject.sdk.compat03.spec.InvalidRequestError_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONRPCErrorResponse_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONRPCError_v0_3;
import org.a2aproject.sdk.compat03.spec.JSONRPCMessage_v0_3;
import org.a2aproject.sdk.compat03.spec.ListTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.MethodNotFoundError_v0_3;
import org.a2aproject.sdk.compat03.spec.SendMessageRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.SendStreamingMessageRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.SendStreamingMessageResponse_v0_3;
import org.a2aproject.sdk.compat03.spec.SetTaskPushNotificationConfigRequest_v0_3;
import org.a2aproject.sdk.compat03.spec.TaskResubscriptionRequest_v0_3;
import org.a2aproject.sdk.compat03.transport.jsonrpc.handler.JSONRPCHandler_v0_3;
import org.a2aproject.sdk.compat03.util.Utils_v0_3;
import org.a2aproject.sdk.server.ServerCallContext;
import org.a2aproject.sdk.server.auth.UnauthenticatedUser;
import org.a2aproject.sdk.transport.jsonrpc.context.JSONRPCContextKeys;
import org.reactivestreams.FlowAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

/**
 * Answers A2A 0.3 JSON-RPC requests with the A2A 0.3 compatibility layer of the A2A SDK, which
 * hands them to the A2A 1.0 request handler. Requests are read and errors are reported as the A2A
 * SDK reference server for A2A 0.3 does.
 */
final class A2a03JsonRpcHandler {

    private static final Logger log = LoggerFactory.getLogger(A2a03JsonRpcHandler.class);

    private final JSONRPCHandler_v0_3 jsonRpcHandler;

    A2a03JsonRpcHandler(JSONRPCHandler_v0_3 jsonRpcHandler) {
        this.jsonRpcHandler = jsonRpcHandler;
    }

    /**
     * Whether a request with this {@code A2A-Version} header is an A2A 0.3 one. A2A 0.3 clients
     * send no version; other 0.x versions are taken for 0.3 too.
     */
    static boolean isA2a03(String version) {
        return null == version || version.isBlank() || version.trim().startsWith("0.");
    }

    /**
     * Handles a request whose body is a JSON object.
     *
     * @return the JSON-RPC response as a JSON string; for a streaming request, a {@link Flux} of
     * JSON strings, one per response
     */
    Object handle(String body, Map<String, String> headers) {
        Map<String, Object> state = new HashMap<>();
        state.put(JSONRPCContextKeys.HEADERS_KEY, headers);
        ServerCallContext context =
                new ServerCallContext(
                        UnauthenticatedUser.INSTANCE,
                        state,
                        new HashSet<>(),
                        A2AProtocol_v0_3.PROTOCOL_VERSION);
        Object id = null;
        try {
            JsonObject request = JsonParser.parseString(body).getAsJsonObject();
            id = requestId(request);
            String method = method(request);
            // The A2A 0.3 types reject null values in maps such as metadata, which AgentScope
            // clients before A2A 1.0 send; a missing member reads the same.
            String params = withoutNulls(request).toString();
            boolean streaming =
                    SendStreamingMessageRequest_v0_3.METHOD.equals(method)
                            || TaskResubscriptionRequest_v0_3.METHOD.equals(method);
            context.getState().put(JSONRPCContextKeys.METHOD_NAME_KEY, method);
            context.getState().put(A2aServerConstants.ContextKeys.IS_STREAM_KEY, streaming);
            if (streaming) {
                Object requestId = id;
                return JsonRpcTransportWrapper.stream(
                                Flux.from(
                                        FlowAdapters.toPublisher(stream(params, method, context))),
                                method,
                                requestId)
                        .<String>handle((response, sink) -> toJsonOrEnd(requestId, response, sink));
            }
            return toJson(id, respond(params, method, context));
        } catch (JSONRPCError_v0_3 e) {
            return toJson(id, new JSONRPCErrorResponse_v0_3(id, e));
        } catch (Throwable t) {
            log.error("Handle A2A 0.3 JSON-RPC request error:", t);
            return toJson(
                    id, new JSONRPCErrorResponse_v0_3(id, new InternalError_v0_3(t.getMessage())));
        }
    }

    private Object respond(String body, String method, ServerCallContext context) {
        return switch (method) {
            case GetTaskRequest_v0_3.METHOD ->
                    jsonRpcHandler.onGetTask(read(body, GetTaskRequest_v0_3.class), context);
            case CancelTaskRequest_v0_3.METHOD ->
                    jsonRpcHandler.onCancelTask(read(body, CancelTaskRequest_v0_3.class), context);
            case SendMessageRequest_v0_3.METHOD ->
                    jsonRpcHandler.onMessageSend(
                            read(body, SendMessageRequest_v0_3.class), context);
            case SetTaskPushNotificationConfigRequest_v0_3.METHOD ->
                    jsonRpcHandler.setPushNotificationConfig(
                            read(body, SetTaskPushNotificationConfigRequest_v0_3.class), context);
            case GetTaskPushNotificationConfigRequest_v0_3.METHOD ->
                    jsonRpcHandler.getPushNotificationConfig(
                            read(body, GetTaskPushNotificationConfigRequest_v0_3.class), context);
            case ListTaskPushNotificationConfigRequest_v0_3.METHOD ->
                    jsonRpcHandler.listPushNotificationConfig(
                            read(body, ListTaskPushNotificationConfigRequest_v0_3.class), context);
            case DeleteTaskPushNotificationConfigRequest_v0_3.METHOD ->
                    jsonRpcHandler.deletePushNotificationConfig(
                            read(body, DeleteTaskPushNotificationConfigRequest_v0_3.class),
                            context);
            case GetAuthenticatedExtendedCardRequest_v0_3.METHOD ->
                    jsonRpcHandler.onGetAuthenticatedExtendedCardRequest(
                            read(body, GetAuthenticatedExtendedCardRequest_v0_3.class), context);
            default -> throw new MethodNotFoundError_v0_3();
        };
    }

    private Flow.Publisher<SendStreamingMessageResponse_v0_3> stream(
            String body, String method, ServerCallContext context) {
        if (SendStreamingMessageRequest_v0_3.METHOD.equals(method)) {
            return jsonRpcHandler.onMessageSendStream(
                    read(body, SendStreamingMessageRequest_v0_3.class), context);
        }
        return jsonRpcHandler.onResubscribeToTask(
                read(body, TaskResubscriptionRequest_v0_3.class), context);
    }

    /** The request id; an id other than a string, a number or null is invalid. */
    private static Object requestId(JsonObject request) {
        JsonElement id = request.get("id");
        if (null == id || id.isJsonNull()) {
            return null;
        }
        if (!id.isJsonPrimitive()) {
            throw new InvalidRequestError_v0_3(
                    "Invalid JSON-RPC request: 'id' must be a string, number, or null");
        }
        JsonPrimitive primitive = id.getAsJsonPrimitive();
        return primitive.isNumber() ? primitive.getAsLong() : primitive.getAsString();
    }

    private static String method(JsonObject request) {
        JsonElement jsonrpc = request.get("jsonrpc");
        if (null == jsonrpc
                || !jsonrpc.isJsonPrimitive()
                || !JSONRPCMessage_v0_3.JSONRPC_VERSION.equals(jsonrpc.getAsString())) {
            throw new InvalidRequestError_v0_3(
                    "Invalid JSON-RPC request: missing or invalid 'jsonrpc' field");
        }
        JsonElement method = request.get("method");
        if (null == method || !method.isJsonPrimitive()) {
            throw new InvalidRequestError_v0_3(
                    "Invalid JSON-RPC request: missing or invalid 'method' field");
        }
        return method.getAsString();
    }

    private static JsonElement withoutNulls(JsonElement element) {
        if (element.isJsonObject()) {
            JsonObject result = new JsonObject();
            element.getAsJsonObject().entrySet().stream()
                    .filter(member -> !member.getValue().isJsonNull())
                    .forEach(
                            member -> result.add(member.getKey(), withoutNulls(member.getValue())));
            return result;
        }
        if (element.isJsonArray()) {
            JsonArray result = new JsonArray();
            element.getAsJsonArray().forEach(each -> result.add(withoutNulls(each)));
            return result;
        }
        return element;
    }

    private static <T> T read(String body, Class<T> type) {
        try {
            return JsonUtil_v0_3.fromJson(body, type);
        } catch (JsonProcessingException_v0_3 | RuntimeException e) {
            throw new InvalidParamsError_v0_3(e.getMessage());
        }
    }

    /** Writes {@code response}, or an internal error for the same request if that fails. */
    private static String toJson(Object id, Object response) {
        try {
            return Utils_v0_3.toJsonString(response);
        } catch (RuntimeException e) {
            return serializationError(id, response, e);
        }
    }

    /**
     * Writes a streamed {@code response}. If that fails, ends the stream with an internal error for
     * the same request and cancels the subscription to the remaining responses.
     */
    private static void toJsonOrEnd(Object id, Object response, SynchronousSink<String> sink) {
        try {
            sink.next(Utils_v0_3.toJsonString(response));
        } catch (RuntimeException e) {
            sink.next(serializationError(id, response, e));
            sink.complete();
        }
    }

    private static String serializationError(Object id, Object response, RuntimeException cause) {
        log.error(
                "Failed to serialize A2A 0.3 JSON-RPC response: {}",
                response.getClass().getName(),
                cause);
        return Utils_v0_3.toJsonString(
                new JSONRPCErrorResponse_v0_3(
                        id, new InternalError_v0_3("Failed to serialize JSON-RPC response")));
    }
}
