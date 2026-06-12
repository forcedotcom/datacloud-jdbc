/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Single-request HTTP server bound to a loopback address that accepts the
 * OAuth authorization-code callback from the user's browser.
 *
 * <p>Per RFC 8252 (OAuth 2.0 for Native Apps) installed apps should redirect
 * to {@code http://127.0.0.1:<random-port>} and bind that port immediately
 * before opening the browser. The server stops itself as soon as it has
 * captured one valid response or an OAuth error.
 *
 * <p>Closing the server is idempotent.
 */
@Slf4j
public final class LoopbackCallbackServer implements Closeable {

    public static final String CALLBACK_PATH = "/callback";

    private static final String SUCCESS_PAGE = "<!doctype html>"
            + "<html><head><meta charset=\"utf-8\"><title>Authentication complete</title></head>"
            + "<body style=\"font-family:sans-serif;text-align:center;padding-top:4rem\">"
            + "<h1>You can close this tab</h1>"
            + "<p>The Data Cloud JDBC driver received the authorization code.</p>"
            + "</body></html>";

    private static final String ERROR_PAGE_PREFIX = "<!doctype html>"
            + "<html><head><meta charset=\"utf-8\"><title>Authentication failed</title></head>"
            + "<body style=\"font-family:sans-serif;text-align:center;padding-top:4rem\">"
            + "<h1>Authentication failed</h1><p>";

    private static final String ERROR_PAGE_SUFFIX = "</p></body></html>";

    private final HttpServer server;
    private final CompletableFuture<Result> result = new CompletableFuture<>();

    private LoopbackCallbackServer(HttpServer server) {
        this.server = server;
    }

    /**
     * Bind a loopback server on the requested port. Use {@code 0} to let the OS pick
     * an ephemeral port (recommended). The bound port is available via {@link #getPort()}.
     */
    public static LoopbackCallbackServer start(int port) throws IOException {
        // Bind to 127.0.0.1 explicitly — never expose the callback to the network.
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        HttpServer http = HttpServer.create(address, 0);
        LoopbackCallbackServer instance = new LoopbackCallbackServer(http);
        http.createContext(CALLBACK_PATH, instance.new CallbackHandler());
        // Default executor is fine; we only ever serve one useful request.
        http.start();
        log.debug("Loopback OAuth callback listening on http://127.0.0.1:{}{}", instance.getPort(), CALLBACK_PATH);
        return instance;
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public URI getRedirectUri() {
        return URI.create("http://127.0.0.1:" + getPort() + CALLBACK_PATH);
    }

    /**
     * Block up to {@code timeoutMillis} for the browser callback. Throws on
     * OAuth-level errors (the {@code error}/{@code error_description} query
     * params) and on timeout.
     */
    public Result awaitCallback(long timeoutMillis) throws IOException {
        try {
            return result.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            throw new IOException("Timed out after " + timeoutMillis + " ms waiting for OAuth browser callback", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for OAuth browser callback", ex);
        } catch (Exception ex) {
            // CompletionException / ExecutionException — unwrap so the caller sees the real cause.
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException(cause.getMessage(), cause);
        }
    }

    @Override
    public void close() {
        // Stop quickly: in-flight handler has already completed when we get here.
        server.stop(0);
    }

    @Value
    public static class Result {
        String code;
        String state;
    }

    private final class CallbackHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
                String error = params.get("error");
                if (error != null) {
                    String description = params.getOrDefault("error_description", "");
                    String body = ERROR_PAGE_PREFIX + escapeHtml(error + ": " + description) + ERROR_PAGE_SUFFIX;
                    respond(exchange, 400, body);
                    result.completeExceptionally(
                            new IOException("OAuth authorization failed: " + error + " - " + description));
                    return;
                }

                String code = params.get("code");
                String state = params.get("state");
                if (code == null || code.isEmpty()) {
                    String body = ERROR_PAGE_PREFIX + "Authorization response did not include a code parameter."
                            + ERROR_PAGE_SUFFIX;
                    respond(exchange, 400, body);
                    result.completeExceptionally(new IOException("OAuth callback was missing a code parameter"));
                    return;
                }

                respond(exchange, 200, SUCCESS_PAGE);
                result.complete(new Result(code, state));
            } catch (Exception ex) {
                result.completeExceptionally(ex);
                throw ex;
            }
        }
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> result = new HashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return result;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String key = eq >= 0 ? pair.substring(0, eq) : pair;
            String value = eq >= 0 ? pair.substring(eq + 1) : "";
            try {
                result.put(
                        URLDecoder.decode(key, StandardCharsets.UTF_8.name()),
                        URLDecoder.decode(value, StandardCharsets.UTF_8.name()));
            } catch (Exception ex) {
                // Malformed param: skip but don't fail the whole callback.
                log.warn("Skipping malformed OAuth callback parameter `{}`", pair);
            }
        }
        return result;
    }

    private static String escapeHtml(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
