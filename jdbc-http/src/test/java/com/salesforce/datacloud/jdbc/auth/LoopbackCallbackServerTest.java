/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LoopbackCallbackServerTest {

    @Test
    void capturesCodeAndState() throws Exception {
        try (LoopbackCallbackServer server = LoopbackCallbackServer.start(0)) {
            URI redirect = server.getRedirectUri();
            assertThat(redirect.getPort()).isPositive();
            assertThat(redirect.getHost()).isEqualTo("127.0.0.1");

            CompletableFuture<LoopbackCallbackServer.Result> async = CompletableFuture.supplyAsync(() -> {
                try {
                    return server.awaitCallback(5_000);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            int status = httpGet(redirect.toString() + "?code=abc%20def&state=xyz");
            assertThat(status).isEqualTo(200);

            LoopbackCallbackServer.Result result = async.get(5, TimeUnit.SECONDS);
            assertThat(result.getCode()).isEqualTo("abc def");
            assertThat(result.getState()).isEqualTo("xyz");
        }
    }

    @Test
    void surfacesOAuthErrorParam() throws Exception {
        try (LoopbackCallbackServer server = LoopbackCallbackServer.start(0)) {
            URI redirect = server.getRedirectUri();

            CompletableFuture<Void> async = CompletableFuture.runAsync(() -> {
                try {
                    httpGet(redirect.toString() + "?error=access_denied&error_description=user%20cancelled");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            assertThatThrownBy(() -> server.awaitCallback(5_000))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("access_denied")
                    .hasMessageContaining("user cancelled");

            async.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void rejectsCallbackWithoutCode() throws Exception {
        try (LoopbackCallbackServer server = LoopbackCallbackServer.start(0)) {
            URI redirect = server.getRedirectUri();

            CompletableFuture<Void> async = CompletableFuture.runAsync(() -> {
                try {
                    httpGet(redirect.toString() + "?state=xyz");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            assertThatThrownBy(() -> server.awaitCallback(5_000))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("missing a code parameter");

            async.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void timesOutWhenNoCallback() throws Exception {
        try (LoopbackCallbackServer server = LoopbackCallbackServer.start(0)) {
            assertThatThrownBy(() -> server.awaitCallback(50))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Timed out");
        }
    }

    /** Read the body to drain the response and return the status code. */
    private static int httpGet(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(2_000);
        conn.setReadTimeout(2_000);
        try {
            int status = conn.getResponseCode();
            // Drain the body so the server-side handler completes its sendResponseHeaders+write cycle.
            try (java.io.InputStream is = status >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
                if (is != null) {
                    byte[] buf = new byte[1024];
                    while (is.read(buf) >= 0) {
                        // discard
                    }
                }
            }
            return status;
        } finally {
            conn.disconnect();
        }
    }

    @SuppressWarnings("unused") // kept for future Charset-aware decoding tests
    private static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
