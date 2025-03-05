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
package com.salesforce.datacloud.jdbc.interceptor;

import static com.salesforce.datacloud.jdbc.interceptor.MetadataUtilities.keyOf;
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.optional;

import io.grpc.Metadata;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class HyperExternalClientContextHeaderInterceptor implements SingleHeaderMutatingClientInterceptor {
    public static HyperExternalClientContextHeaderInterceptor of(Properties properties) {
        return optional(properties, PROPERTY)
                .map(HyperExternalClientContextHeaderInterceptor::new)
                .orElse(null);
    }

    static final String PROPERTY = "external-client-context";

    @ToString.Exclude
    private final Metadata.Key<String> key = keyOf("x-hyperdb-external-client-context");

    private final String value;
}
