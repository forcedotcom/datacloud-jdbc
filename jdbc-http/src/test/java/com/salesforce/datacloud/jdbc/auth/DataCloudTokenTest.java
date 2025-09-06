/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.auth.DataCloudToken.FAILED_LOGIN;
import static com.salesforce.datacloud.jdbc.auth.PrivateKeyHelpersTest.fakeTenantId;
import static com.salesforce.datacloud.jdbc.auth.PrivateKeyHelpersTest.fakeToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
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
        assertThat(exception.getMessage()).contains(FAILED_LOGIN);
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
