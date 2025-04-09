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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.jackson.Jacksonized;
import lombok.val;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class FormCommandTest {
    static URI VALID;

    static {
        try {
            VALID = new URI("https://localhost");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @InjectSoftAssertions
    private SoftAssertions softly;

    private MockWebServer server;
    private OkHttpClient client;

    @BeforeEach
    @SneakyThrows
    void beforeEach() {
        this.server = new MockWebServer();
        this.server.start();
        this.client = new OkHttpClient.Builder().build();
    }

    @SneakyThrows
    @AfterEach
    void afterEach() {
        this.server.close();
    }

    @Test
    void throwsOnNullClient() {
        val ex = assertThrows(
                IllegalArgumentException.class,
                () -> FormCommand.post(
                        null,
                        new FormCommand(
                                VALID, new URI("/suffix"), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()),
                        Object.class));
        assertThat(ex).hasMessage("client is marked non-null but is null").hasNoCause();
    }

    @Test
    void throwsOnNullCommand() {
        val ex = assertThrows(
                IllegalArgumentException.class,
                () -> FormCommand.post(new OkHttpClient.Builder().build(), null, Object.class));
        assertThat(ex).hasMessage("command is marked non-null but is null").hasNoCause();
    }

    @Test
    @SneakyThrows
    void postProperlyMakesFormFromHttpCommandFormContent() {
        val body = "{ \"numbers\": [1, 2, 3] }";
        val userAgent = UUID.randomUUID().toString();
        val a = UUID.randomUUID().toString();
        val b = UUID.randomUUID().toString();
        val expectedBody = ImmutableMap.of("a", a, "b", b);
        val command = new FormCommand(
                new URI(server.url("").toString()),
                new URI("foo"),
                ImmutableMap.of("User-Agent", userAgent),
                expectedBody,
                ImmutableMap.of());

        server.enqueue(new MockResponse().setBody(body));

        val resp = FormCommand.post(client, command, FakeCommandResp.class);

        assertThat(resp.getNumbers()).hasSameElementsAs(ImmutableList.of(1, 2, 3));
        val r = server.takeRequest();
        softly.assertThat(r.getRequestLine()).startsWith("POST");
        softly.assertThat(r.getPath()).as("path").isEqualTo("/foo");
        softly.assertThat(r.getHeader("Accept")).as("Accept header").isEqualTo("application/json");
        softly.assertThat(r.getHeader("Content-Type"))
                .as("Content-Type header")
                .isEqualTo("application/x-www-form-urlencoded");
        softly.assertThat(r.getHeader("User-Agent")).as("User-Agent header").isEqualTo(userAgent);

        val actualBody = getBody(r);
        softly.assertThat(actualBody).containsExactlyInAnyOrderEntriesOf(expectedBody);
    }

    @Test
    @SneakyThrows
    void getProperlyMakesFormFromHttpCommandFormContent() {
        val body = "{ \"numbers\": [1, 2, 3] }";
        val userAgent = UUID.randomUUID().toString();
        val a = UUID.randomUUID().toString();
        val b = UUID.randomUUID().toString();
        val expectedQueryParams = ImmutableMap.of("a", a, "b", b);
        val command = new FormCommand(
                new URI(server.url("").toString()),
                new URI("foo"),
                ImmutableMap.of("User-Agent", userAgent),
                ImmutableMap.of(),
                expectedQueryParams);

        server.enqueue(new MockResponse().setBody(body));

        val resp = FormCommand.get(client, command, FakeCommandResp.class);

        assertThat(resp.getNumbers()).hasSameElementsAs(ImmutableList.of(1, 2, 3));
        val r = server.takeRequest();
        val actualRequestUrl = r.getRequestUrl();
        softly.assertThat(actualRequestUrl.queryParameter("a")).isEqualTo(a);
        softly.assertThat(actualRequestUrl.queryParameter("b")).isEqualTo(b);
        val expectedUrl = HttpUrl.get(server.url("").toString());
        softly.assertThat(actualRequestUrl.scheme()).isEqualTo(expectedUrl.scheme());
        softly.assertThat(actualRequestUrl.host()).isEqualTo(expectedUrl.host());
        softly.assertThat(actualRequestUrl.port()).isEqualTo(expectedUrl.port());
        softly.assertThat(actualRequestUrl.pathSegments()).isEqualTo(ImmutableList.of("foo"));
        softly.assertThat(r.getRequestLine()).startsWith("GET");
        softly.assertThat(r.getHeader("Accept")).as("Accept header").isEqualTo("application/json");
        softly.assertThat(r.getHeader("Content-Type"))
                .as("Content-Type header")
                .isEqualTo("application/x-www-form-urlencoded");
        softly.assertThat(r.getHeader("User-Agent")).as("User-Agent header").isEqualTo(userAgent);
        val actualBody = getBody(r);
        softly.assertThat(actualBody).isEqualTo(ImmutableMap.of());
    }

    private Map<String, String> getBody(RecordedRequest request) {
        return Arrays.stream(request.getBody().readUtf8().split("&"))
                .map(p -> p.split("="))
                .filter(t -> t.length == 2)
                .collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
    }

    @Data
    @Builder
    @Jacksonized
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class FakeCommandResp {
        private List<Integer> numbers;
    }
}
