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

import static com.salesforce.datacloud.jdbc.ResponseEnum.EMPTY_RESPONSE;
import static com.salesforce.datacloud.jdbc.ResponseEnum.QUERY_RESPONSE;
import static com.salesforce.datacloud.jdbc.ResponseEnum.TABLE_METADATA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.salesforce.datacloud.jdbc.ResponseEnum;
import com.salesforce.datacloud.jdbc.util.Constants;
import com.salesforce.datacloud.jdbc.util.MetadataCacheUtil;
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

    private Interceptor.Chain chain;

    private MetadataCacheInterceptor metadataCacheInterceptor;

    @BeforeEach
    public void init() {
        chain = mock(Interceptor.Chain.class);
        metadataCacheInterceptor = new MetadataCacheInterceptor();
        doReturn(buildRequest()).when(chain).request();
    }

    @Test
    @SneakyThrows
    public void testMetadataRequestWithNoCachePresent() {
        doReturn(buildResponse(200, EMPTY_RESPONSE))
                .doReturn(buildResponse(200, QUERY_RESPONSE))
                .when(chain)
                .proceed(any(Request.class));
        metadataCacheInterceptor.intercept(chain);
        verify(chain, times(1)).proceed(any(Request.class));
    }

    @Test
    @SneakyThrows
    public void testMetadataFromCache() {
        MetadataCacheUtil.cacheMetadata(
                "https://mjrgg9bzgy2dsyzvmjrgkmzzg1.c360a.salesforce.com" + Constants.CDP_URL + Constants.METADATA_URL,
                TABLE_METADATA.getResponse());
        metadataCacheInterceptor.intercept(chain);
        verify(chain, times(0)).proceed(any(Request.class));
    }

    private Request buildRequest() {
        return new Request.Builder()
                .url("https://mjrgg9bzgy2dsyzvmjrgkmzzg1.c360a.salesforce.com"
                        + Constants.CDP_URL
                        + Constants.METADATA_URL)
                .method(Constants.POST, RequestBody.create("{test: test}", MediaType.parse("application/json")))
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
