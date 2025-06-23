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
package com.salesforce.datacloud.jdbc.exception;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import salesforce.cdp.hyperdb.v1.ErrorInfo;

@Slf4j
public final class QueryExceptionHandler {
    // We introduce a limit to avoid truncating important details from the log due to large queries.
    // When testing with 60 MB queries the exception formatting also took multi second hangs.
    private static final int MAX_QUERY_LENGTH_IN_EXCEPTION = 16 * 1024;

    private QueryExceptionHandler() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static DataCloudJDBCException createQueryException(String query, Exception e) {
        String exceptionQuery = query.length() > MAX_QUERY_LENGTH_IN_EXCEPTION
                ? query.substring(0, MAX_QUERY_LENGTH_IN_EXCEPTION) + "<truncated>"
                : query;
        return QueryExceptionHandler.createException("Failed to execute query: " + exceptionQuery, e);
    }

    public static DataCloudJDBCException createException(String message, Exception e) {
        if (e instanceof StatusRuntimeException) {
            StatusRuntimeException ex = (StatusRuntimeException) e;
            com.google.rpc.Status status = StatusProto.fromThrowable(ex);

            if (status != null) {
                List<Any> detailsList = status.getDetailsList();
                Any firstError = detailsList.stream()
                        .filter(any -> any.is(ErrorInfo.class))
                        .findFirst()
                        .orElse(null);
                if (firstError != null) {
                    ErrorInfo errorInfo;
                    try {
                        errorInfo = firstError.unpack(ErrorInfo.class);
                    } catch (InvalidProtocolBufferException exc) {
                        return new DataCloudJDBCException("Invalid error info", e);
                    }

                    String sqlState = errorInfo.getSqlstate();
                    String customerHint = errorInfo.getCustomerHint();
                    String customerDetail = errorInfo.getCustomerDetail();
                    String primaryMessage = String.format(
                            "%s: %s%nDETAIL:%n%s%nHINT:%n%s",
                            sqlState, errorInfo.getPrimaryMessage(), customerDetail, customerHint);
                    return new DataCloudJDBCException(primaryMessage, sqlState, customerHint, customerDetail, ex);
                }
            }
        }
        return new DataCloudJDBCException(message, e);
    }

    public static SQLException createException(String message, String sqlState, Exception e) {
        return new SQLException(message, sqlState, e.getCause());
    }

    public static SQLException createException(String message, String sqlState) {
        return new SQLException(message, sqlState);
    }

    public static SQLException createException(String message) {
        return new DataCloudJDBCException(message);
    }
}
