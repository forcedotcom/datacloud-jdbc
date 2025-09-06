/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
