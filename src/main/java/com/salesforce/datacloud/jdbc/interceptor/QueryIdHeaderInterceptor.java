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

import com.salesforce.datacloud.jdbc.util.StringCompatibility;
import io.grpc.Metadata;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class QueryIdHeaderInterceptor implements SingleHeaderMutatingClientInterceptor {
    @ToString.Exclude
    public final Metadata.Key<String> key = keyOf("x-hyperdb-query-id");

    @NonNull private final String value;

    @Override
    public void mutate(final Metadata headers) {
        if (StringCompatibility.isBlank(value)) {
            return;
        }

        headers.put(key, value);
    }
}
