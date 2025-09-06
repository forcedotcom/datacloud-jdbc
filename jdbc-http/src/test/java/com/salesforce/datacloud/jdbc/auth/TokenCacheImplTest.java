/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
