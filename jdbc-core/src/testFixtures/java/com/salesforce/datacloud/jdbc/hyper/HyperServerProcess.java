/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.hyper;

import static com.salesforce.datacloud.jdbc.core.DataCloudConnectionString.CONNECTION_PROTOCOL;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.util.DirectDataCloudConnection;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class HyperServerProcess implements AutoCloseable {
    private static final Pattern PORT_PATTERN = Pattern.compile(".*gRPC listening on 127.0.0.1:([0-9]+).*");

    private final Process hyperProcess;
    private final ExecutorService hyperMonitors;
    private Integer port;

    public HyperServerProcess() {
        this(HyperServerConfig.builder());
    }

    public HyperServerProcess(String yamlName) {
        this(HyperServerConfig.builder(), yamlName);
    }

    public HyperServerProcess(HyperServerConfig.HyperServerConfigBuilder config) {
        this(config, "hyper.yaml");
    }

    @SneakyThrows
    public HyperServerProcess(HyperServerConfig.HyperServerConfigBuilder config, String yamlName) {
        log.info("starting hyperd, this might take a few seconds");

        val isWindows = System.getProperty("os.name").toLowerCase().contains("win");
        val executable = new File("../.hyperd/hyperd" + (isWindows ? ".exe" : ""));
        val yaml = Paths.get(requireNonNull(HyperServerProcess.class.getResource("/" + yamlName))
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

        // Wait until process is listening and extract port on which it is listening
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
    }

    public int getPort() {
        return port;
    }

    public boolean isHealthy() {
        return hyperProcess != null && hyperProcess.isAlive();
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
        if (hyperProcess != null && hyperProcess.isAlive()) {
            log.warn("destroy hyper process");
            hyperProcess.destroy();
            hyperProcess.waitFor();
        }

        log.warn("shutdown hyper monitors");
        hyperMonitors.shutdown();
    }

    public DataCloudConnection getConnection() {
        return getConnection(ImmutableMap.of());
    }

    @SneakyThrows
    public DataCloudConnection getConnection(Map<String, String> connectionSettings) {
        val properties = new Properties();
        properties.put(DirectDataCloudConnection.DIRECT, "true");
        properties.putAll(connectionSettings);
        val url = CONNECTION_PROTOCOL + "//127.0.0.1:" + getPort();
        return DirectDataCloudConnection.of(url, properties);
    }
}
