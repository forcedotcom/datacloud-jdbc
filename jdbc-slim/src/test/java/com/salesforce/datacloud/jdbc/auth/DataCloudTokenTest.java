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

import static com.salesforce.datacloud.jdbc.auth.PrivateKeyHelpersTest.fakeTenantId;
import static com.salesforce.datacloud.jdbc.auth.PrivateKeyHelpersTest.fakeToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.util.Messages;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class DataCloudTokenTest {
    private final String validToken = "token-" + UUID.randomUUID();
    private final String validUrl = "https://login.something.salesforce.com";

    @InjectSoftAssertions
    SoftAssertions softly;

    @SneakyThrows
    @Test
    void whenTokenHasExpiredIsAliveIsFalse() {
        val expired = new DataCloudTokenResponse();
        expired.setTokenType("type");
        expired.setToken(validToken);
        expired.setInstanceUrl(validUrl);
        expired.setExpiresIn(-100);
        assertThat(DataCloudToken.of(expired).isAlive()).isFalse();
    }

    @SneakyThrows
    @Test
    void whenTokenHasNotExpiredIsAliveIsTrue() {
        val notExpired = new DataCloudTokenResponse();
        notExpired.setTokenType("type");
        notExpired.setToken(validToken);
        notExpired.setInstanceUrl(validUrl);
        notExpired.setExpiresIn(100);

        assertThat(DataCloudToken.of(notExpired).isAlive()).isTrue();
    }

    @Test
    void throwsWhenIllegalArgumentsAreProvided() {
        val noTokenResponse = new DataCloudTokenResponse();
        noTokenResponse.setTokenType("type");
        noTokenResponse.setInstanceUrl(validUrl);
        noTokenResponse.setExpiresIn(10000);
        noTokenResponse.setToken("");
        assertThat(assertThrows(IllegalArgumentException.class, () -> DataCloudToken.of(noTokenResponse)))
                .hasMessageContaining("token");
        val noUriResponse = new DataCloudTokenResponse();
        noUriResponse.setTokenType("type");
        noUriResponse.setInstanceUrl("");
        noUriResponse.setExpiresIn(10000);
        noUriResponse.setToken(validToken);
        assertThat(assertThrows(IllegalArgumentException.class, () -> DataCloudToken.of(noUriResponse)))
                .hasMessageContaining("instance_url");
    }

    @Test
    void throwsWhenTenantUrlIsIllegal() {
        val nonNullOrBlankIllegalUrl = "%XY";
        val bad = new DataCloudTokenResponse();
        bad.setInstanceUrl(nonNullOrBlankIllegalUrl);
        bad.setToken("token");
        bad.setTokenType("type");
        bad.setExpiresIn(123);
        val exception = assertThrows(DataCloudJDBCException.class, () -> DataCloudToken.of(bad));
        assertThat(exception.getMessage()).contains(Messages.FAILED_LOGIN);
        assertThat(exception.getCause().getMessage())
                .contains("Malformed escape pair at index 0: " + nonNullOrBlankIllegalUrl);
    }

    @SneakyThrows
    @Test
    void properlyReturnsCorrectValues() {
        val validResponse = new DataCloudTokenResponse();
        val token = fakeToken;

        validResponse.setInstanceUrl(validUrl);
        validResponse.setToken(token);
        validResponse.setTokenType("Bearer");
        validResponse.setExpiresIn(123);

        val actual = DataCloudToken.of(validResponse);
        softly.assertThat(actual.getAccessToken()).isEqualTo("Bearer " + token);
        softly.assertThat(actual.getTenantUrl()).isEqualTo(validUrl);
        softly.assertThat(actual.getTenantId()).isEqualTo(fakeTenantId);
    }
}
