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

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
@AllArgsConstructor
public class DuplicateKeyDetectionInterceptor implements HeaderMutatingClientInterceptor {
    private final String name;

    @Override
    public void mutate(Metadata headers) throws DataCloudJDBCException {
        validateNoDuplicateKeys(headers);
    }

    private boolean isDuplicate(final String key, final Metadata headers) {
        val name = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
        val iterable = headers.getAll(name);

        if (iterable == null) {
            return false;
        }

        val count = StreamUtilities.toStream(iterable.iterator()).count();

        return count > 1;
    }

    private void validateNoDuplicateKeys(final Metadata headers) throws DataCloudJDBCException {
        val duplicates = new HashSet<String>();

        headers.keys().forEach(key -> {
            if (isDuplicate(key, headers)) {
                duplicates.add(key);
            }
        });

        log.warn("{} headers={}, duplicates={}", name, headers.keys(), duplicates);


        if (!duplicates.isEmpty()) {
            throw new DataCloudJDBCException(
                    "Duplicate metadata keys detected, each metadata key should only appear once in the request headers. name=" + name + ", duplicates="
                            + String.join(", ", duplicates));
        }
    }
}
