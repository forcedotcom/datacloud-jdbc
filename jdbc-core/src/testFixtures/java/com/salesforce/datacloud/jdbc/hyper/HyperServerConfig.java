/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.hyper;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;
import lombok.val;

@Builder(toBuilder = true)
@Value
public class HyperServerConfig {
    @Builder.Default
    @JsonProperty("grpc-request-timeout")
    String grpcRequestTimeoutSeconds = "120s";

    @Override
    public String toString() {
        val mapper = new ObjectMapper();
        val map = mapper.convertValue(this, new TypeReference<Map<String, Object>>() {});
        return map.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .map(entry -> String.format("--%s=%s", entry.getKey().replace("_", "-"), entry.getValue()))
                .collect(Collectors.joining(" "));
    }

    public HyperServerProcess start() {
        return new HyperServerProcess(this.toBuilder());
    }
}
