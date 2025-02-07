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
package com.salesforce.datacloud.jdbc.util;

import com.google.protobuf.Any;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import lombok.experimental.UtilityClass;
import salesforce.cdp.hyperdb.v1.ErrorInfo;

@UtilityClass
public class GrpcUtils {
    private static final com.google.rpc.Status rpcStatus = com.google.rpc.Status.newBuilder()
            .setCode(io.grpc.Status.INVALID_ARGUMENT.getCode().value())
            .setMessage("Resource Not Found")
            .addDetails(Any.pack(ErrorInfo.newBuilder()
                    .setSqlstate("42P01")
                    .setPrimaryMessage("Table not found")
                    .build()))
            .build();

    private static final Metadata.Key<String> metaDataKey =
            Metadata.Key.of("test-metadata", Metadata.ASCII_STRING_MARSHALLER);
    private static final String metaDataValue = "test metadata value";

    public static Metadata getFakeMetaData() {
        Metadata metadata = new Metadata();
        metadata.put(metaDataKey, metaDataValue);
        return metadata;
    }

    public static StatusRuntimeException getFakeStatusRuntimeExceptionAsInvalidArgument() {
        return StatusProto.toStatusRuntimeException(rpcStatus, getFakeMetaData());
    }
}
