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
package com.salesforce.datacloud.jdbc.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

public class TokenCacheImplTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void canSetGetAndClearADataCloudToken() {
        val accessToken = UUID.randomUUID().toString();
        val token = makeDataCloudToken(accessToken);

        val sut = new TokenCacheImpl();

        assertThat(sut.getDataCloudToken()).isNull();
        sut.setDataCloudToken(token);
        assertThat(sut.getDataCloudToken()).isEqualTo(token);
        sut.clearDataCloudToken();
        assertThat(sut.getDataCloudToken()).isNull();
    }

    @SneakyThrows
    private DataCloudToken makeDataCloudToken(String accessToken) {
        val json = String.format(
                "{\"access_token\": \"%s\", \"instance_url\": \"something.salesforce.com\", \"token_type\": \"something\", \"expires_in\": 100 }",
                accessToken);
        val model = mapper.readValue(json, DataCloudTokenResponse.class);
        return DataCloudToken.of(model);
    }
}
