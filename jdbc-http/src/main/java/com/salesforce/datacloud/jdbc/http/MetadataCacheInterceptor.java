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
