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
package com.salesforce.datacloud.jdbc.hyper;

import static com.salesforce.datacloud.jdbc.core.DataCloudConnectionString.CONNECTION_PROTOCOL;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudJdbcManagedChannel;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.util.DirectDataCloudConnection;
import io.grpc.ManagedChannelBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;

@Slf4j
public class HyperServerProcess implements AutoCloseable {
    private static final Pattern PORT_PATTERN = Pattern.compile(".*gRPC listening on 127.0.0.1:([0-9]+).*");

    private final Process hyperProcess;
    private final ExecutorService hyperMonitors;
    private final ConcurrentLinkedQueue<AutoCloseable> managedResources = new ConcurrentLinkedQueue<>();
    private final Thread shutdownHook;
    private Integer port;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public HyperServerProcess() {
        this(HyperServerConfig.builder());
    }

    @SneakyThrows
    public HyperServerProcess(HyperServerConfig.HyperServerConfigBuilder config) {
        log.info("starting hyperd, this might take a few seconds");

        val isWindows = System.getProperty("os.name").toLowerCase().contains("win");
        val executable = new File("../.hyperd/hyperd" + (isWindows ? ".exe" : ""));
        val yaml = Paths.get(requireNonNull(HyperServerProcess.class.getResource("/hyper.yaml"))
                        .toURI())
                .toFile();

        if (!executable.exists()) {
            throw new IllegalStateException(
                "hyperd executable couldn't be found, have you run `gradle extractHyper`? expected="
                        + executable.getAbsolutePath() + ", os=" + System.getProperty("os.name"));
        }

        val builder = new ProcessBuilder()
                .command(
                        executable.getAbsolutePath(),
                        config.build().toString(),
                        "--config",
                        yaml.getAbsolutePath(),
                        "--no-password",
                        "run");

        log.warn("hyper command: {}", builder.command());
        hyperProcess = builder.start();

        val latch = new CountDownLatch(1);
        hyperMonitors = Executors.newFixedThreadPool(2);
        hyperMonitors.execute(() -> logStream(hyperProcess.getErrorStream(), log::error));
        hyperMonitors.execute(() -> logStream(hyperProcess.getInputStream(), line -> {
            log.warn(line);
            val matcher = PORT_PATTERN.matcher(line);
            if (matcher.matches()) {
                port = Integer.valueOf(matcher.group(1));
                latch.countDown();
            }
        }));

        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException(
                "failed to start instance of hyper within 30 seconds");
        }

        shutdownHook = new Thread(this::forceCleanup, "HyperServerProcess-ShutdownHook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public int getPort() {
        return port;
    }

    public boolean isHealthy() {
        return hyperProcess != null && hyperProcess.isAlive() && !closed.get();
    }

    private static void logStream(InputStream inputStream, Consumer<String> consumer) {
        try (val reader = new BufferedReader(new BufferedReader(new InputStreamReader(inputStream)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                consumer.accept("hyperd - " + line);
            }
        } catch (IOException e) {
            log.warn("Caught exception while consuming log stream, it probably closed", e);
        } catch (Exception e) {
            log.error("Caught unexpected exception", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
            // JVM is already shutting down, hook cannot be removed
        }

        performCleanup();
    }

    private void forceCleanup() {
        try {
            performCleanup();
        } catch (Exception e) {
            log.error("Error during shutdown hook cleanup", e);
        }
    }

    private void performCleanup() throws Exception {
        log.warn("Cleaning up HyperServerProcess resources");

        AutoCloseable resource;
        while ((resource = managedResources.poll()) != null) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("Failed to close managed resource", e);
            }
        }

        if (hyperProcess != null && hyperProcess.isAlive()) {
            log.warn("destroy hyper process");
            hyperProcess.destroy();
            if (!hyperProcess.waitFor(5, TimeUnit.SECONDS)) {
                log.warn("hyper process did not terminate gracefully, force killing");
                hyperProcess.destroyForcibly();
                hyperProcess.waitFor(2, TimeUnit.SECONDS);
            }
        }

        if (hyperMonitors != null) {
            log.warn("shutdown hyper monitors");
            hyperMonitors.shutdown();
            if (!hyperMonitors.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("hyper monitors did not terminate gracefully, force shutdown");
                hyperMonitors.shutdownNow();
                if (!hyperMonitors.awaitTermination(2, TimeUnit.SECONDS)) {
                    log.error("hyper monitors failed to terminate even after force shutdown");
                }
            }
        }
    }

    public DataCloudConnection getConnection() {
        return getConnection(ImmutableMap.of());
    }

    @SneakyThrows
    public HyperGrpcClientExecutor getRawClient() {
        if (closed.get()) {
            throw new IllegalStateException("HyperServerProcess is already closed");
        }

        val channel = DataCloudJdbcManagedChannel.of(
                ManagedChannelBuilder.forAddress("127.0.0.1", getPort()).usePlaintext());
        managedResources.add(channel);
        
        val stub = HyperServiceGrpc.newBlockingStub(channel.getChannel());
        return HyperGrpcClientExecutor.of(stub, new HashMap<String, String>());
    }

    @SneakyThrows
    public DataCloudConnection getConnection(Map<String, String> connectionSettings) {
        if (closed.get()) {
            throw new IllegalStateException("HyperServerProcess is already closed");
        }

        val properties = new Properties();
        properties.put(DirectDataCloudConnection.DIRECT, "true");
        properties.putAll(connectionSettings);
        val url = CONNECTION_PROTOCOL + "//127.0.0.1:" + getPort();
        val connection = DirectDataCloudConnection.of(url, properties);
        managedResources.add(connection);
        return connection;
    }
}
