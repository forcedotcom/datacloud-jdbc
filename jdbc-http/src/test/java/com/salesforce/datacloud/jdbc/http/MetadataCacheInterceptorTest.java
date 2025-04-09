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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.salesforce.datacloud.jdbc.auth.ResponseEnum;
import java.util.Properties;
import lombok.SneakyThrows;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetadataCacheInterceptorTest {
    private static final String POST = "POST";
    private static final String CDP_URL = "/api/v1";
    private static final String METADATA_URL = "/metadata";
    private static final String URL =
            "https://mjrgg9bzgy2dsyzvmjrgkmzzg1.c360a.salesforce.com" + CDP_URL + METADATA_URL;

    private Interceptor.Chain chain;

    private MetadataCacheInterceptor metadataCacheInterceptor;

    @BeforeEach
    public void init() {
        chain = mock(Interceptor.Chain.class);
        metadataCacheInterceptor = new MetadataCacheInterceptor(new Properties());
        doReturn(buildRequest()).when(chain).request();
    }

    @Test
    @SneakyThrows
    public void testMetadataRequestWithNoCachePresent() {
        doReturn(buildResponse(200, ResponseEnum.EMPTY_RESPONSE))
                .doReturn(buildResponse(200, ResponseEnum.QUERY_RESPONSE))
                .when(chain)
                .proceed(any(Request.class));
        metadataCacheInterceptor.intercept(chain);
        verify(chain, times(1)).proceed(any(Request.class));

        metadataCacheInterceptor.intercept(chain);
        metadataCacheInterceptor.intercept(chain);
        metadataCacheInterceptor.intercept(chain);
        verify(chain, times(1)).proceed(any(Request.class));
    }

    private Request buildRequest() {
        return new Request.Builder()
                .url(URL)
                .method(POST, RequestBody.create("{test: test}", MediaType.parse("application/json")))
                .build();
    }

    private Response buildResponse(int statusCode, ResponseEnum responseEnum) {
        String jsonString = responseEnum.getResponse();
        return new Response.Builder()
                .code(statusCode)
                .request(buildRequest())
                .protocol(Protocol.HTTP_1_1)
                .message("Redirected")
                .body(ResponseBody.create(jsonString, MediaType.parse("application/json")))
                .build();
    }
}
