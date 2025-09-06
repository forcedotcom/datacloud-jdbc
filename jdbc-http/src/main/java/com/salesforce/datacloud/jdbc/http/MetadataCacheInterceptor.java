/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.http;

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.getIntegerOrDefault;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;

@Slf4j
public class MetadataCacheInterceptor implements Interceptor {
    private static final MediaType mediaType = MediaType.parse(Constants.CONTENT_TYPE_JSON);

    private final Cache<String, String> metaDataCache;

    public MetadataCacheInterceptor(Properties properties) {
        val metaDataCacheDurationInMs = getIntegerOrDefault(properties, "metadataCacheTtlMs", 10000);

        this.metaDataCache = CacheBuilder.newBuilder()
                .expireAfterWrite(metaDataCacheDurationInMs, TimeUnit.MILLISECONDS)
                .maximumSize(10)
                .build();
    }

    @NonNull @Override
    public Response intercept(@NonNull Chain chain) throws IOException {
        val request = chain.request();
        val cacheKey = request.url().toString();
        val cachedResponse = metaDataCache.getIfPresent(cacheKey);
        val builder = new Response.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .message("OK");

        if (cachedResponse != null) {
            log.trace("Getting the metadata response from local cache");
            builder.body(ResponseBody.create(cachedResponse, mediaType));
        } else {
            log.trace("Cache miss for metadata response. Getting from server");
            val response = chain.proceed(request);

            if (response.isSuccessful()) {
                Optional.of(response)
                        .map(Response::body)
                        .map(t -> {
                            try {
                                return t.string();
                            } catch (IOException ex) {
                                log.error("Caught exception when extracting body from response. {}", cacheKey, ex);
                                return null;
                            }
                        })
                        .ifPresent(responseString -> {
                            builder.body(ResponseBody.create(responseString, mediaType));
                            metaDataCache.put(cacheKey, responseString);
                        });
            } else {
                return response;
            }
        }

        return builder.build();
    }
}
