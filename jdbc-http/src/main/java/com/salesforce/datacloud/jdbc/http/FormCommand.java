/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.auth.errors.AuthorizationException;
import com.salesforce.datacloud.jdbc.util.StringCompatibility;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.val;
import okhttp3.FormBody;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

@Value
@Builder(builderClassName = "Builder")
public class FormCommand {
    private static ObjectMapper mapper = new ObjectMapper();

    @NonNull URI url;

    @NonNull URI suffix;

    @Singular
    Map<String, String> headers;

    @Singular
    Map<String, String> bodyEntries;

    @Singular
    Map<String, String> queryParameters;

    public static <T> T get(@NonNull OkHttpClient client, @NonNull FormCommand command, Class<T> type)
            throws SQLException, AuthorizationException {
        val url = getUrl(command);
        val headers = asHeaders(command);
        val request = new Request.Builder().url(url).headers(headers).get().build();

        return executeRequest(client, request, type);
    }

    public static <T> T post(@NonNull OkHttpClient client, @NonNull FormCommand command, Class<T> type)
            throws SQLException, AuthorizationException {
        val url = getUrl(command);
        val headers = asHeaders(command);
        val payload = asFormBody(command);
        val request =
                new Request.Builder().url(url).headers(headers).post(payload).build();

        return executeRequest(client, request, type);
    }

    private static String getUrl(FormCommand command) {
        HttpUrl.Builder builder = Objects.requireNonNull(
                        HttpUrl.parse(command.getUrl().toString()))
                .newBuilder();
        builder.addPathSegments(command.suffix.toString());
        command.queryParameters.forEach(builder::addEncodedQueryParameter);
        return builder.build().toString();
    }

    private static <T> T executeRequest(@NonNull OkHttpClient client, Request request, Class<T> type)
            throws SQLException, AuthorizationException {
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                if (response.code() >= 400 && response.code() < 500) {
                    throw AuthorizationException.builder()
                            .message(response.message())
                            .errorCode(String.valueOf(response.code()))
                            .errorDescription(response.body().string())
                            .build();
                }
                throw new IOException("Unexpected code " + response);
            }
            val body = response.body();
            if (body == null || StringCompatibility.isNullOrEmpty(body.toString())) {
                throw new IOException("Response Body was null " + response);
            }
            val json = body.string();
            return mapper.readValue(json, type);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    private static FormBody asFormBody(FormCommand command) {
        val body = new FormBody.Builder();
        command.getBodyEntries().forEach(body::add);
        return body.build();
    }

    private static Headers asHeaders(FormCommand command) {
        val headers = new HashMap<>(command.getHeaders());

        headers.putIfAbsent("Accept", "application/json");
        headers.putIfAbsent("Content-Type", "application/x-www-form-urlencoded");

        return Headers.of(headers);
    }
}
