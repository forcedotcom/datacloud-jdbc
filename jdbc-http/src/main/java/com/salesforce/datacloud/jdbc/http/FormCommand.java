/*
 * Copyright (c) 2024, Salesforce, Inc.
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
package com.salesforce.datacloud.jdbc.http;

import static com.salesforce.datacloud.jdbc.http.Constants.CONTENT_TYPE_JSON;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
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
    private static final String ACCEPT_HEADER_NAME = "Accept";
    public static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    private static final String URL_ENCODED_CONTENT = "application/x-www-form-urlencoded";
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
            throws SQLException {
        val url = getUrl(command);
        val headers = asHeaders(command);
        val request = new Request.Builder().url(url).headers(headers).get().build();

        return executeRequest(client, request, type);
    }

    public static <T> T post(@NonNull OkHttpClient client, @NonNull FormCommand command, Class<T> type)
            throws SQLException {
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
            throws SQLException {
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
            val body = response.body();
            if (body == null || StringCompatibility.isNullOrEmpty(body.toString())) {
                throw new IOException("Response Body was null " + response);
            }
            val json = body.string();
            return mapper.readValue(json, type);
        } catch (IOException e) {
            throw new DataCloudJDBCException(e);
        }
    }

    private static FormBody asFormBody(FormCommand command) {
        val body = new FormBody.Builder();
        command.getBodyEntries().forEach(body::add);
        return body.build();
    }

    private static Headers asHeaders(FormCommand command) {
        val headers = new HashMap<>(command.getHeaders());

        headers.putIfAbsent(ACCEPT_HEADER_NAME, CONTENT_TYPE_JSON);
        headers.putIfAbsent(CONTENT_TYPE_HEADER_NAME, URL_ENCODED_CONTENT);

        return Headers.of(headers);
    }
}
