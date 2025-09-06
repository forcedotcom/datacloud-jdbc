/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.soql;

import static com.salesforce.datacloud.jdbc.logging.ElapsedLogger.logTimedValue;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.auth.DataCloudTokenProvider;
import com.salesforce.datacloud.jdbc.auth.OAuthToken;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.FormCommand;
import com.salesforce.datacloud.jdbc.http.HttpClientProperties;
import com.salesforce.datacloud.jdbc.tracing.Tracer;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.OkHttpClient;

@Slf4j
public class DataspaceClient implements ThrowingJdbcSupplier<List<String>> {
    private final DataCloudTokenProvider tokenProvider;
    private final OkHttpClient client;

    public DataspaceClient(HttpClientProperties clientProperties, final DataCloudTokenProvider tokenProvider)
            throws DataCloudJDBCException {
        this.tokenProvider = tokenProvider;
        this.client = clientProperties.buildOkHttpClient();
    }

    @Override
    public List<String> get() throws SQLException {
        return logTimedValue(this::getWithoutLogging, "getDataspaces", log);
    }

    private List<String> getWithoutLogging() throws SQLException {
        try {

            val dataspaceResponse = getDataSpaceResponse();
            return dataspaceResponse.getRecords().stream()
                    .map(DataspaceResponse.DataSpaceAttributes::getName)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new DataCloudJDBCException(e);
        }
    }

    private DataspaceResponse getDataSpaceResponse() throws SQLException {
        try {
            val token = tokenProvider.getOAuthToken();
            val command = buildGetDataspaceFormCommand(token);
            return FormCommand.get(client, command, DataspaceResponse.class);
        } catch (Exception e) {
            throw new DataCloudJDBCException(e);
        }
    }

    private static FormCommand buildGetDataspaceFormCommand(OAuthToken oAuthToken) throws URISyntaxException {
        val traceId = Tracer.get().nextId();
        val spanId = Tracer.get().nextSpanId();

        val builder = FormCommand.builder();
        builder.url(oAuthToken.getInstanceUrl());
        builder.suffix(new URI("services/data/v61.0/query/"));
        builder.queryParameters(ImmutableMap.of("q", "SELECT+name+from+Dataspace"));
        builder.header("Authorization", oAuthToken.getBearerToken());
        builder.header("Content-Type", "application/json");
        builder.header("enable-stream-flow", "false");
        builder.header("x-b3-traceid", traceId);
        builder.header("x-b3-spanid", spanId);
        return builder.build();
    }
}
