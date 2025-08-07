package com.salesforce.datacloud.jdbc.hyper;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public final class HyperServerManager {
    @AllArgsConstructor
    public enum ConfigFile {
        DEFAULT("default.yaml"),
        SMALL_CHUNKS("hyper.yaml"),
        TIMEOUT("timeout.yaml");

        final String filename;
    }

    private static final Map<HyperProcessHandle, HyperServerProcess> instances = new ConcurrentHashMap<>();

    private static final AtomicBoolean installed = new AtomicBoolean(false);

    private HyperServerManager() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    @Value
    static class HyperProcessHandle {
        String yaml;
        HyperServerConfig config;
    }

    static {
        if (installed.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.warn("Cleaning up running Hyper servers...");

                try {
                    for (val entry : instances.entrySet()) {
                        val key = entry.getKey();
                        val process = entry.getValue();
                        log.warn("cleaning up Hyper server port={}, yaml={}, config={}", process.getPort(), key.yaml, key.config);
                        process.close();
                    }
                } catch (Exception ex) {
                    log.error("Error while cleaning up Hyper servers", ex);
                }
            }));
        }
    }

    static HyperServerProcess get(HyperServerConfig.HyperServerConfigBuilder builder, String yaml) {
        val key = new HyperProcessHandle(yaml, builder.build());
        return instances.computeIfAbsent(key, k -> {
            log.warn("Cache miss for Hyper server yaml={}, config={}", yaml, builder.build());
            return new HyperServerProcess(k.config.toBuilder(), k.yaml);
        });
    }

    public static HyperServerProcess get(HyperServerConfig.HyperServerConfigBuilder builder, ConfigFile yaml) {
        return get(builder, yaml.filename);
    }

    @Deprecated
    public static HyperServerProcess closeToDefault() {
        val config = HyperServerConfig.builder();
        val name = "default.yaml";
        return get(config, name);
    }

    @Deprecated
    public static HyperServerProcess withSmallChunks() {
        val config = HyperServerConfig.builder();
        val name = "hyper.yaml";
        return get(config, name);
    }
}
