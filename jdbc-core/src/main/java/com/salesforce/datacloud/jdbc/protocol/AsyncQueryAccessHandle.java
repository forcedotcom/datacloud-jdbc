/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol;

import com.salesforce.datacloud.jdbc.logging.ElapsedLogger;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.HyperServiceGrpc;
import salesforce.cdp.hyperdb.v1.QueryParam;
import salesforce.cdp.hyperdb.v1.QueryStatus;

@Slf4j
@AllArgsConstructor
public class AsyncQueryAccessHandle implements QueryAccessHandle {
    @Getter
    private final QueryStatus queryStatus;

    public static AsyncQueryAccessHandle of(HyperServiceGrpc.HyperServiceBlockingStub stub, QueryParam param) {
        val message = "executeQuery. mode=" + param.getTransferMode();
        return ElapsedLogger.logTimedValueNonThrowing(
                () -> {
                    val messages = stub.executeQuery(param);
                    // The protocol guarantees that the first message is a Query Status message with a Query Id.
                    val queryStatus = messages.next().getQueryInfo().getQueryStatus();
                    // Consume all the remaining messages to ensure that the initial compilation succeeded and that the
                    // stream is
                    // properly closed so that the query doesn't accidentally get cancelled.
                    messages.forEachRemaining(x -> {});
                    return new AsyncQueryAccessHandle(queryStatus);
                },
                message,
                log);
    }
}
