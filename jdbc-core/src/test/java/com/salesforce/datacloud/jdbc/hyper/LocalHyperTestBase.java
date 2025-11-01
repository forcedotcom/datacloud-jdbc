/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.hyper;

import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.HyperDatasource;
import com.salesforce.datacloud.jdbc.core.ConnectionProperties;
import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.core.GrpcChannelProperties;
import com.salesforce.datacloud.jdbc.core.JdbcDriverStubProvider;
import com.salesforce.datacloud.jdbc.core.SslProperties;
import com.salesforce.datacloud.jdbc.hyper.HyperServerManager.ConfigFile;
import com.salesforce.datacloud.jdbc.util.PropertyParsingUtils;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Base class for tests with a local `hyperd` server running
 */
@Slf4j
public class LocalHyperTestBase implements BeforeAllCallback {
    private static boolean isRegistered = false;

    @SneakyThrows
    public static void assertEachRowIsTheSame(ResultSet rs, AtomicInteger prev) {
        val expected = prev.incrementAndGet();
        val a = rs.getBigDecimal(1).intValue();
        assertThat(expected).isEqualTo(a);
    }

    @SneakyThrows
    public static void assertWithConnection(ThrowingConsumer<DataCloudConnection> assertion) {
        try (val connection = getHyperQueryConnection()) {
            assertion.accept(connection);
        }
    }

    @SneakyThrows
    public static void assertWithStatement(ThrowingConsumer<DataCloudStatement> assertion) {
        try (val connection = getHyperQueryConnection();
                val result = connection.createStatement()) {
            assertion.accept(result.unwrap(DataCloudStatement.class));
        }
    }

    @SneakyThrows
    public static void assertWithStubProvider(ThrowingConsumer<JdbcDriverStubProvider> assertion) {
        val server = HyperServerManager.get(ConfigFile.SMALL_CHUNKS);
        ManagedChannelBuilder<?> channel =
                ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext();
        try (val stubProvider = JdbcDriverStubProvider.of(channel)) {
            assertion.accept(stubProvider);
        }
    }

    @SneakyThrows
    public static DataCloudConnection getHyperQueryConnection(
            HyperServerProcess server, ClientInterceptor interceptor) {
        ManagedChannelBuilder<?> channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .intercept(interceptor);
        val stubProvider = JdbcDriverStubProvider.of(channel);

        return DataCloudConnection.of(stubProvider, ConnectionProperties.defaultProperties(), "", null);
    }

    @SneakyThrows
    public static DataCloudConnection getHyperQueryConnection(HyperServerProcess server, Properties properties) {
        properties.setProperty("ssl.disabled", "true");
        SslProperties sslProps = SslProperties.ofDestructive(properties);
        ConnectionProperties connectionProps = ConnectionProperties.ofDestructive(properties);
        GrpcChannelProperties grpcProps = GrpcChannelProperties.ofDestructive(properties);

        // Validate remaining properties after parsing
        PropertyParsingUtils.validateRemainingProperties(properties);
        return (DataCloudConnection) HyperDatasource.builder()
                .host("127.0.0.1")
                .port(server.getPort())
                .sslProperties(sslProps)
                .connectionProperties(connectionProps)
                .grpcChannelProperties(grpcProps)
                .dataspace("")
                .build()
                .getConnection();
    }

    @SneakyThrows
    public static DataCloudConnection getHyperQueryConnection(HyperServerProcess server) {
        SslProperties sslProps =
                SslProperties.builder().sslMode(SslProperties.SslMode.DISABLED).build();
        return (DataCloudConnection) HyperDatasource.builder()
                .host("127.0.0.1")
                .port(server.getPort())
                .sslProperties(sslProps)
                .connectionProperties(ConnectionProperties.defaultProperties())
                .grpcChannelProperties(GrpcChannelProperties.defaultProperties())
                .dataspace("")
                .build()
                .getConnection();
    }

    public static DataCloudConnection getHyperQueryConnection(Properties properties) {
        return getHyperQueryConnection(HyperServerManager.get(ConfigFile.SMALL_CHUNKS), properties);
    }

    public static DataCloudConnection getHyperQueryConnection() throws SQLException {
        SslProperties sslProps =
                SslProperties.builder().sslMode(SslProperties.SslMode.DISABLED).build();
        return (DataCloudConnection) HyperDatasource.builder()
                .host("127.0.0.1")
                .port(HyperServerManager.get(ConfigFile.SMALL_CHUNKS).getPort())
                .sslProperties(sslProps)
                .connectionProperties(ConnectionProperties.defaultProperties())
                .grpcChannelProperties(GrpcChannelProperties.defaultProperties())
                .dataspace("")
                .build()
                .getConnection();
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        synchronized (LocalHyperTestBase.class) {
            if (!isRegistered) {
                context.getRoot()
                        .getStore(ExtensionContext.Namespace.GLOBAL)
                        .put(LocalHyperTestBase.class.getName(), this);
                isRegistered = true;
                System.out.println("Registered database shutdown hook");
            }
        }
    }
}
