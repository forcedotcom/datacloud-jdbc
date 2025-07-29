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
package com.salesforce.datacloud.jdbc.soql;

import static com.salesforce.datacloud.jdbc.logging.ElapsedLogger.logTimedValue;

import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.auth.OAuthToken;
import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.ClientBuilder;
import com.salesforce.datacloud.jdbc.http.Constants;
import com.salesforce.datacloud.jdbc.http.FormCommand;
import com.salesforce.datacloud.jdbc.tracing.Tracer;
import com.salesforce.datacloud.jdbc.util.ThrowingJdbcSupplier;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.OkHttpClient;

@Slf4j
public class DataspaceClient implements ThrowingJdbcSupplier<List<String>> {
    private static final String SOQL_ENDPOINT_SUFFIX = "services/data/v61.0/query/";
    private static final String SOQL_QUERY_PARAM_KEY = "q";

    public DataspaceClient(final Properties properties, final TokenProcessor tokenProcessor) {
        this.tokenProcessor = tokenProcessor;
        this.client = ClientBuilder.buildOkHttpClient(properties);
    }

    private final TokenProcessor tokenProcessor;
    private final OkHttpClient client;

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
            val token = tokenProcessor.getOAuthToken();
            val command = buildGetDataspaceFormCommand(token);
            return FormCommand.get(client, command, DataspaceResponse.class);
        } catch (Exception e) {
            throw new DataCloudJDBCException(e);
        }
    }

    private static final String TRACE_ID = "x-b3-traceid";
    private static final String SPAN_ID = "x-b3-spanid";

    private static FormCommand buildGetDataspaceFormCommand(OAuthToken oAuthToken) throws URISyntaxException {
        val traceId = Tracer.get().nextId();
        val spanId = Tracer.get().nextSpanId();

        val builder = FormCommand.builder();
        builder.url(oAuthToken.getInstanceUrl());
        builder.suffix(new URI(SOQL_ENDPOINT_SUFFIX));
        builder.queryParameters(ImmutableMap.of(SOQL_QUERY_PARAM_KEY, "SELECT+name+from+Dataspace"));
        builder.header(Constants.AUTHORIZATION, oAuthToken.getBearerToken());
        builder.header(FormCommand.CONTENT_TYPE_HEADER_NAME, Constants.CONTENT_TYPE_JSON);
        builder.header("User-Agent", "cdp/jdbc");
        builder.header("enable-stream-flow", "false");
        builder.header(TRACE_ID, traceId);
        builder.header(SPAN_ID, spanId);
        return builder.build();
    }
}
