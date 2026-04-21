/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.interceptor;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;
import salesforce.cdp.hyperdb.v1.AttachedDatabase;
import salesforce.cdp.hyperdb.v1.QueryParam;

/**
 * A gRPC {@link ClientInterceptor} that attaches a database to every {@code ExecuteQuery} call.
 *
 * <p>This interceptor rewrites outgoing {@link QueryParam} messages to
 * include the configured {@link AttachedDatabase} entry, which makes a Hyper
 * database available in SQL under the given alias.</p>
 *
 * <p>Usage example:
 * <pre>{@code
 *   var interceptor = new DatabaseAttachInterceptor("/tmp/test.hyper", "default");
 *   var channel = ManagedChannelBuilder.forAddress("127.0.0.1", port)
 *       .usePlaintext()
 *       .intercept(interceptor)
 *       .build();
 * }</pre>
 */
public class DatabaseAttachInterceptor implements ClientInterceptor {

    private static final String EXECUTE_QUERY_METHOD = "salesforce.hyperdb.grpc.v1.HyperService/ExecuteQuery";

    private final AttachedDatabase attachedDatabase;

    /**
     * Creates an interceptor that attaches the given database under the given alias.
     *
     * @param databasePath the path to the Hyper database (e.g. a file path or {@code hyper.external:} URI)
     * @param alias        the SQL alias under which the database is accessible
     */
    public DatabaseAttachInterceptor(String databasePath, String alias) {
        this.attachedDatabase = AttachedDatabase.newBuilder()
                .setPath(databasePath)
                .setAlias(alias)
                .build();
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);

        if (!EXECUTE_QUERY_METHOD.equals(method.getFullMethodName())) {
            return call;
        }

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(call) {
            @Override
            public void sendMessage(ReqT message) {
                if (message instanceof QueryParam) {
                    QueryParam original = (QueryParam) message;
                    @SuppressWarnings("unchecked")
                    ReqT rewritten = (ReqT) original.toBuilder()
                            .addDatabases(attachedDatabase)
                            .build();
                    super.sendMessage(rewritten);
                } else {
                    super.sendMessage(message);
                }
            }
        };
    }
}
