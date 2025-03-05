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

import static java.util.Objects.requireNonNull;

import java.io.*;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;

@Slf4j
public class HyperServerProcess {
    private static final Pattern PORT_PATTERN = Pattern.compile(".*gRPC listening on 127.0.0.1:([0-9]+).*");

    private final Process hyperProcess;
    private final ExecutorService hyperMonitors;
    private Integer port;

    @SneakyThrows
    public HyperServerProcess() {
        log.info("starting hyperd, this might take a few seconds");

        val executable = new File("../target/hyper/hyperd");
        val properties = Paths.get(requireNonNull(HyperTestBase.class.getResource("/hyper.yaml"))
                        .toURI())
                .toFile();

        if (!executable.exists()) {
            Assertions.fail("hyperd executable couldn't be found, have you run mvn process-test-resources? expected="
                    + executable.getAbsolutePath());
        }

        hyperProcess = new ProcessBuilder()
                .command(executable.getAbsolutePath(), "--config", properties.getAbsolutePath(), "--no-password", "run")
                .start();

        // Wait until process is listening and extract port on which it is listening
        val latch = new CountDownLatch(1);
        hyperMonitors = Executors.newFixedThreadPool(2);
        hyperMonitors.execute(() -> logStream(hyperProcess.getErrorStream(), log::error));
        hyperMonitors.execute(() -> logStream(hyperProcess.getInputStream(), line -> {
            log.info(line);
            val matcher = PORT_PATTERN.matcher(line);
            if (matcher.matches()) {
                port = Integer.valueOf(matcher.group(1));
                latch.countDown();
            }
        }));

        if (!latch.await(30, TimeUnit.SECONDS)) {
            Assertions.fail("failed to start instance of hyper within 30 seconds");
        }
    }

    @SneakyThrows
    void shutdown() throws InterruptedException {
        if (hyperProcess != null && hyperProcess.isAlive()) {
            log.info("destroy hyper process");
            hyperProcess.destroy();
            hyperProcess.waitFor();
        }

        log.info("shutdown hyper monitors");
        hyperMonitors.shutdown();
    }

    int getPort() {
        return port;
    }

    boolean isHealthy() {
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
}
