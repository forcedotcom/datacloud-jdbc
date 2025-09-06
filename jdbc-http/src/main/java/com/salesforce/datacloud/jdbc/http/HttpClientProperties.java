/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.http;

import static com.salesforce.datacloud.jdbc.config.DriverVersion.formatDriverInfo;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptionalBoolean;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptionalEnum;
import static com.salesforce.datacloud.jdbc.util.PropertyParsingUtils.takeOptionalInteger;

import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.http.internal.SocketFactoryWrapper;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.logging.HttpLoggingInterceptor;

/**
 * Properties that control the HTTP client behavior.
 *
 * Configures timeouts, logging, and proxy settings for the OkHttp client.
 *
 * - http.logging.level: HTTP logging level, default is BASIC
 * - http.readTimeOutSeconds: read timeout in seconds, default is 600
 * - http.connectTimeOutSeconds: connect timeout in seconds, default is 600
 * - http.callTimeOutSeconds: call timeout in seconds, default is 600
 * - http.disableSocksProxy: disable SOCKS proxy, default is false
 * - http.cacheTtlMs: metadata cache TTL in milliseconds, default is 10000
 */
@Getter
@Builder
public class HttpClientProperties {
    public static final String HTTP_LOG_LEVEL = "http.logging.level";
    public static final String HTTP_READ_TIMEOUT_SECONDS = "http.readTimeOutSeconds";
    public static final String HTTP_CONNECT_TIMEOUT_SECONDS = "http.connectTimeOutSeconds";
    public static final String HTTP_CALL_TIMEOUT_SECONDS = "http.callTimeOutSeconds";
    public static final String HTTP_DISABLE_SOCKS_PROXY = "http.disableSocksProxy";
    public static final String HTTP_CACHE_TTL_MS = "http.cacheTtlMs";
    public static final String HTTP_MAX_RETRIES = "http.maxRetries";

    @Builder.Default
    private final HttpLoggingInterceptor.Level logLevel = HttpLoggingInterceptor.Level.BASIC;

    @Builder.Default
    private final int readTimeoutSeconds = 600;

    @Builder.Default
    private final int connectTimeoutSeconds = 600;

    @Builder.Default
    private final int callTimeoutSeconds = 600;

    @Builder.Default
    private final boolean disableSocksProxy = false;

    @Builder.Default
    private final int metadataCacheTtlMs = 10000;

    @Builder.Default
    private final int maxRetries = 3;

    public static HttpClientProperties defaultProperties() {
        return builder().build();
    }

    /**
     * Parses HTTP channel properties from a Properties object.
     * Removes the interpreted properties from the Properties object.
     *
     * @param props The properties to parse
     * @return An HttpChannelProperties instance
     */
    public static HttpClientProperties ofDestructive(Properties props) throws DataCloudJDBCException {
        val builder = HttpClientProperties.builder();

        takeOptionalEnum(props, HTTP_LOG_LEVEL, HttpLoggingInterceptor.Level.class)
                .ifPresent(builder::logLevel);
        takeOptionalInteger(props, HTTP_READ_TIMEOUT_SECONDS).ifPresent(builder::readTimeoutSeconds);
        takeOptionalInteger(props, HTTP_CONNECT_TIMEOUT_SECONDS).ifPresent(builder::connectTimeoutSeconds);
        takeOptionalInteger(props, HTTP_CALL_TIMEOUT_SECONDS).ifPresent(builder::callTimeoutSeconds);
        takeOptionalBoolean(props, HTTP_DISABLE_SOCKS_PROXY).ifPresent(builder::disableSocksProxy);
        takeOptionalInteger(props, HTTP_CACHE_TTL_MS).ifPresent(builder::metadataCacheTtlMs);
        takeOptionalInteger(props, HTTP_MAX_RETRIES).ifPresent(builder::maxRetries);

        return builder.build();
    }

    /**
     * Serializes this instance into a Properties object.
     *
     * @return A Properties object containing the HTTP channel properties
     */
    public Properties toProperties() {
        Properties props = new Properties();

        if (logLevel != HttpLoggingInterceptor.Level.BASIC) {
            props.setProperty(HTTP_LOG_LEVEL, logLevel.name());
        }
        if (readTimeoutSeconds != 600) {
            props.setProperty(HTTP_READ_TIMEOUT_SECONDS, String.valueOf(readTimeoutSeconds));
        }
        if (connectTimeoutSeconds != 600) {
            props.setProperty(HTTP_CONNECT_TIMEOUT_SECONDS, String.valueOf(connectTimeoutSeconds));
        }
        if (callTimeoutSeconds != 600) {
            props.setProperty(HTTP_CALL_TIMEOUT_SECONDS, String.valueOf(callTimeoutSeconds));
        }
        if (disableSocksProxy) {
            props.setProperty(HTTP_DISABLE_SOCKS_PROXY, "true");
        }
        if (metadataCacheTtlMs != 10000) {
            props.setProperty(HTTP_CACHE_TTL_MS, String.valueOf(metadataCacheTtlMs));
        }
        if (maxRetries != 3) {
            props.setProperty(HTTP_MAX_RETRIES, String.valueOf(maxRetries));
        }
        return props;
    }

    /**
     * Builds an OkHttpClient with the properties of this instance.
     */
    public OkHttpClient buildOkHttpClient() throws DataCloudJDBCException {
        HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor(new HttpClientLogger());
        loggingInterceptor.setLevel(getLogLevel());

        return new OkHttpClient.Builder()
                .socketFactory(new SocketFactoryWrapper(isDisableSocksProxy()))
                .callTimeout(getCallTimeoutSeconds(), TimeUnit.SECONDS)
                .connectTimeout(getConnectTimeoutSeconds(), TimeUnit.SECONDS)
                .readTimeout(getReadTimeoutSeconds(), TimeUnit.SECONDS)
                .addInterceptor(new MetadataCacheInterceptor(metadataCacheTtlMs))
                .addInterceptor(loggingInterceptor)
                .addInterceptor(new Interceptor() {
                    @NonNull @Override
                    public Response intercept(@NonNull Chain chain) throws IOException {
                        Request originalRequest = chain.request();
                        Request requestWithUserAgent = originalRequest
                                .newBuilder()
                                .header("User-Agent", formatDriverInfo())
                                .build();
                        return chain.proceed(requestWithUserAgent);
                    }
                })
                .build();
    }
}
