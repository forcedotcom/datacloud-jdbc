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
import java.util.List;
import javax.annotation.Nullable;
import lombok.NonNull;
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

    /**
     * Creates an exception for a specific query id.
     *
     * @param query The query that failed (can be null for scenarios like fetching by query id)
     * @param queryId The query id that failed (can be null in case execution failed before request reaches the server)
     * @param includeCustomerDetailInReason Whether to include customer details in the exception message (i.e. fragments of the query that can contain strings provided by the customer)
     * @param e The exception that occurred.
     * @return A DataCloudJDBCException with the query id and the exception message.
     */
    public static DataCloudJDBCException createException(
            @Nullable String query,
            @Nullable String queryId,
            boolean includeCustomerDetailInReason,
            @NonNull Exception e) {
        // Ensure query is truncated if it's too long.
        if (query != null) {
            query = query.length() > MAX_QUERY_LENGTH_IN_EXCEPTION
                    ? query.substring(0, MAX_QUERY_LENGTH_IN_EXCEPTION) + "<truncated>"
                    : query;
        }

        // This is a generic error SQL state, we'll use this if we don't have a specific SQL state from the error info
        String sqlState = "HY000";
        String grpcMessage = null;
        String primaryMessage = null;
        String customerHint = null;
        String customerDetail = null;

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
                        return new DataCloudJDBCException("Invalid error info for query " + queryId, e);
                    }

                    grpcMessage = status.getMessage();
                    sqlState = errorInfo.getSqlstate();
                    primaryMessage = errorInfo.getPrimaryMessage();
                    customerHint = errorInfo.getCustomerHint();
                    customerDetail = errorInfo.getCustomerDetail();
                }
            }
        }

        StringBuilder sharedMessageBuilder = new StringBuilder("Failed to execute query");
        // We use status.getMessage() as in contrast to the primary message this already has the trace id
        // encoded.
        if (grpcMessage != null) {
            sharedMessageBuilder.append(": " + grpcMessage);
        } else {
            // If we don't have any information from the server we resort to using the exception message
            sharedMessageBuilder.append(": " + e.getMessage());
        }
        sharedMessageBuilder.append(String.format("%nSQLSTATE: %s", sqlState));
        if (queryId != null) {
            sharedMessageBuilder.append(String.format("%nQUERY-ID: %s", queryId));
        }

        StringBuilder reasonMessageBuilder = new StringBuilder(sharedMessageBuilder.toString());
        StringBuilder fullCustomerMessageBuilder = new StringBuilder(sharedMessageBuilder.toString());

        // Append detail and hint information
        if (customerDetail != null && !customerDetail.isEmpty()) {
            fullCustomerMessageBuilder.append(String.format("%nDETAIL: %s", customerDetail));
            if (includeCustomerDetailInReason) {
                reasonMessageBuilder.append(String.format("%nDETAIL: %s", customerDetail));
            }
        }
        if (customerHint != null && !customerHint.isEmpty()) {
            fullCustomerMessageBuilder.append(String.format("%nHINT: %s", customerHint));
            if (includeCustomerDetailInReason) {
                reasonMessageBuilder.append(String.format("%nHINT: %s", customerHint));
            }
        }

        // Append query information last as it might be large and could make finding the cause more difficult
        if (query != null) {
            String queryMessage = String.format("%nQUERY: %s", query);
            if (includeCustomerDetailInReason) {
                reasonMessageBuilder.append(queryMessage);
            }
            fullCustomerMessageBuilder.append(queryMessage);
        }

        return new DataCloudJDBCException(
                reasonMessageBuilder.toString(),
                fullCustomerMessageBuilder.toString(),
                sqlState,
                primaryMessage,
                customerHint,
                customerDetail,
                e);
    }
}
