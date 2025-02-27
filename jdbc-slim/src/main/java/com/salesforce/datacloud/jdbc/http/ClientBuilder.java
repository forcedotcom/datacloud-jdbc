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
import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.optional;

import com.salesforce.datacloud.jdbc.util.internal.SFDefaultSocketFactoryWrapper;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import okhttp3.OkHttpClient;

@Slf4j
@UtilityClass
public class ClientBuilder {

    static final String READ_TIME_OUT_SECONDS_KEY = "readTimeOutSeconds";
    static final int DEFAULT_READ_TIME_OUT_SECONDS = 600;

    static final String CONNECT_TIME_OUT_SECONDS_KEY = "connectTimeOutSeconds";
    static final int DEFAULT_CONNECT_TIME_OUT_SECONDS = 600;

    static final String CALL_TIME_OUT_SECONDS_KEY = "callTimeOutSeconds";
    static final int DEFAULT_CALL_TIME_OUT_SECONDS = 600;

    static final String DISABLE_SOCKS_PROXY_KEY = "disableSocksProxy";
    static final Boolean DISABLE_SOCKS_PROXY_DEFAULT = false;

    public static OkHttpClient buildOkHttpClient(Properties properties) {
        val disableSocksProxy = optional(properties, DISABLE_SOCKS_PROXY_KEY)
                .map(Boolean::valueOf)
                .orElse(DISABLE_SOCKS_PROXY_DEFAULT);

        val readTimeout = getIntegerOrDefault(properties, READ_TIME_OUT_SECONDS_KEY, DEFAULT_READ_TIME_OUT_SECONDS);
        val connectTimeout =
                getIntegerOrDefault(properties, CONNECT_TIME_OUT_SECONDS_KEY, DEFAULT_CONNECT_TIME_OUT_SECONDS);
        val callTimeout = getIntegerOrDefault(properties, CALL_TIME_OUT_SECONDS_KEY, DEFAULT_CALL_TIME_OUT_SECONDS);

        return new OkHttpClient.Builder()
                .socketFactory(new SFDefaultSocketFactoryWrapper(disableSocksProxy))
                .callTimeout(callTimeout, TimeUnit.SECONDS)
                .connectTimeout(connectTimeout, TimeUnit.SECONDS)
                .readTimeout(readTimeout, TimeUnit.SECONDS)
                .addInterceptor(new MetadataCacheInterceptor())
                .build();
    }
}
