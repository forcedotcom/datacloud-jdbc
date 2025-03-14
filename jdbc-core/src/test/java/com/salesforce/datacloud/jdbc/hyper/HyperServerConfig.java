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
