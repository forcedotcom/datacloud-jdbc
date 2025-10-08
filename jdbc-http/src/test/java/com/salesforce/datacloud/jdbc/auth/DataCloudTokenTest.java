/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import static com.salesforce.datacloud.jdbc.auth.DataCloudToken.FAILED_LOGIN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.datacloud.jdbc.auth.model.DataCloudTokenResponse;
import java.sql.SQLException;
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
    static final String FAKE_PRIVATE_KEY =
            "-----BEGIN PRIVATE KEY-----MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDaaLkxUbuT6lBD9ZPXBjjJkA4+JzewQ/kXPAryD1cvh7hQN07KNy/bn0eBviRbpqp7sbCRo2hk8F2TRGb13yHM/h0uPTzcDGMmwqScZ7BvP0lvWKs8AtvwLJbyPpXOXa8cGy1al5mA9sc0xLLprKGrWW0GK2pzcIzvOiDyG1W9a0oOJPPvA6nS5N9AusvRrnaLhukL4bXo6iYPRMj6vkJwEAKp0S5Lj9/5lqN9QynipYrpwMcBnmFZst+IFGfpu5w9EKxTLR4O/3Cf8CXdcEidGZMQE2jYtQPyTJlscG6fdy421q0qUKf/fFN6n+cRw5aqXRXZsm+UC1aqhIMr3LD5AgMBAAECggEAAWhs5TCdZZvkDRXBGDPEkL0rCpyqR4c94s30/QClG0A+iDaI2iB54HMxqQjhJRYIqYxlSd4WH+6oP+dh5B1pZzaGan4Uh+wxAJsvrG3zt/xrXgaShlpjZlMatTytmKTOIZoPuvBxRf+ruyx4iIFUo72zF9sj02BMysLAocnwzbURT7hFTc3TjzQRC0NWMNASPXqxhTcM+DI6YWMaxJxbXcW47t6a6NNI0/LvRNeP6PyaVEVM2N3Br52zgnM1Arv0l3jBKlCMhLQAavWbKVtRZUVLVMuOMeRdSpfaXlo9AEB2uxtPvuWJm+4p56Irlh/ggu/9idBmhWNjKF7yEJ7OVwKBgQD6AmZGyvl9nQQ1cnRjutaTlCwxPbaka+Z4V0IZ6knUSLMfydbgUJjI17odNkBmaAVBHY0mFjv48t38/ooCai50dzW3RIEqGVS39e9I2/0ONtZlyZrypvNZxrZ3Pa8F4cNTd05/ldSdiwl2Ric6zqXk5QrBspJnpT4aQ56qf21UewKBgQDfpHtzr3VdhBioyoYCR6R7sQIp2wyLEJ1cEBqpTW97RViiiNTLops8P0+kHZK3Rx8etv9/DTlufyuA+qGNLs+qnJ4MOD/GMTeTu/LPnQSx/ilR2coDFQKh3n/Z0p18FfG8evLNfeRjlTGSYvMR/YvIc0OkuqRWCuDBeXwCHxfYGwKBgCXQ4RmKMCzA6FcRReuj4jsWaYzVMeAy9fxz7mqvFpXGnVmMlTT+2+1dPCiZASq8RzcvOh9ts4qXad6Pvd5Zo0c4lOZwtTzh8f+VcqlJpUBWKR3iXc6gVCTbOtRUfznbiUkBvdzsk+l0k2zRdbOeeFdkEbl0wlJtGzSrz78oYSgrAoGAGkeEripe+zcrgqIRrzDl9hbtrydrSOgR5aCK0Xwk7nJOoQK9JpSb8y9pV1qWQ+0ajgxo53ARYJeW8BgDZcirZFv1AnCVpd9grX53YMgNpjC8gD68SzJr1cOEeH8UPGGDv2cfIuB5Nu5wHch80Y9enpZUy4WXC/lJQdLZrJIkxiMCgYEAhTU67BVB/sSKYY+wKqcu96fz6U5FrhEHqgV24326OOpyCjf+aPyK6O0lBw5bYfaXCR1XmO4mwk+ZCcfRg9kqmFUb+OoJGMJipeuQmDXDv2naGqECZ14dOid32KeJva11CJfzAT0SFQFD+tb7HZK5VrY9C/t5FFsSxKF0G2QGb9k=-----END PRIVATE KEY-----";
    static final String FAKE_JWT =
            "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJjbGllbnRfaWQiLCJzdWIiOiJ1c2VyX25hbWUiLCJhdWQiOiJsb2dpbi50ZXN0MS5wYy1ybmQuc2FsZXNmb3JjZS5jb20iLCJpYXQiOjE3MTgyMjkxNjAsImV4cCI6MTcxODIyOTI4MH0.QzwM8DP0CInYPvj8u7wP1bJ3bXnOXOh5mTFFXkiInIdZ_NveVyiv5xnupAAm7ri8c2C4it4dkIXX3SlrMkX8dSJoff5JG6J_t8qJCGYqnW4MZHuAwEYg1Pbn9eqDvni0PRsA4kUloYr1OLIkIEFySPnzEBPcD-Yxrm5jdAlMINHC38brHMhBBPjuKnWQIZz4iVcQaLt9HcC6yx351PwMUX64yiUPbht1qP54Ohbu6zAn5wf1h1-47_X-7ewYVytqw7teqbN-2vlDiHiHsMsh1rmnp0wFDicE-q8aWpaY7m5DQJLxpUmwhnGPjQ8-EsEeG6jr0Ul6EQv4FuO9WiinmQ";
    static final String FAKE_TOKEN =
            "eyJraWQiOiJDT1JFLjAwRE9LMDAwMDAwOVp6ci4xNzE4MDUyMTU0NDIyIiwidHlwIjoiSldUIiwiYWxnIjoiRVMyNTYifQ.eyJzdWIiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS9pZC8wMERPSzAwMDAwMDlaenIyQUUvMDA1T0swMDAwMDBVeTkxWUFDIiwic2NwIjoiY2RwX3Byb2ZpbGVfYXBpIGNkcF9pbmdlc3RfYXBpIGNkcF9pZGVudGl0eXJlc29sdXRpb25fYXBpIGNkcF9zZWdtZW50X2FwaSBjZHBfcXVlcnlfYXBpIGNkcF9hcGkiLCJpc3MiOiJodHRwczovL2xvZ2luLnRlc3QxLnBjLXJuZC5zYWxlc2ZvcmNlLmNvbS8iLCJvcmdJZCI6IjAwRE9LMDAwMDAwOVp6ciIsImlzc3VlclRlbmFudElkIjoiY29yZS9mYWxjb250ZXN0MS1jb3JlNG9yYTE1LzAwRE9LMDAwMDAwOVp6cjJBRSIsInNmYXBwaWQiOiIzTVZHOVhOVDlUbEI3VmtZY0tIVm5sUUZzWEd6cUJuMGszUC5zNHJBU0I5V09oRU1OdkgyNzNpM1NFRzF2bWl3WF9YY2NXOUFZbHA3VnJnQ3BGb0ZXIiwiYXVkaWVuY2VUZW5hbnRJZCI6ImEzNjAvZmFsY29uZGV2L2E2ZDcyNmE3M2Y1MzQzMjdhNmE4ZTJlMGYzY2MzODQwIiwiY3VzdG9tX2F0dHJpYnV0ZXMiOnsiZGF0YXNwYWNlIjoiZGVmYXVsdCJ9LCJhdWQiOiJhcGkuYTM2MC5zYWxlc2ZvcmNlLmNvbSIsIm5iZiI6MTcyMDczMTAyMSwic2ZvaWQiOiIwMERPSzAwMDAwMDlaenIiLCJzZnVpZCI6IjAwNU9LMDAwMDAwVXk5MSIsImV4cCI6MTcyMDczODI4MCwiaWF0IjoxNzIwNzMxMDgxLCJqdGkiOiIwYjYwMzc4OS1jMGI2LTQwZTMtYmIzNi03NDQ3MzA2MzAxMzEifQ.lXgeAhJIiGoxgNpBi0W5oBWyn2_auB2bFxxajGuK6DMHlkqDhHJAlFN_uf6QPSjGSJCh5j42Ow5SrEptUDJwmQ";
    static final String FAKE_TENANT_ID = "a360/falcondev/a6d726a73f534327a6a8e2e0f3cc3840";

    private final String VALID_TOKEN = "token-" + UUID.randomUUID();
    private final String VALID_LOGIN_URL = "https://login.something.salesforce.com";

    @InjectSoftAssertions
    SoftAssertions softly;

    @SneakyThrows
    @Test
    void whenTokenHasExpiredIsAliveIsFalse() {
        val expired = new DataCloudTokenResponse();
        expired.setTokenType("type");
        expired.setToken(VALID_TOKEN);
        expired.setInstanceUrl(VALID_LOGIN_URL);
        expired.setExpiresIn(-100);
        assertThat(DataCloudToken.of(expired).isAlive()).isFalse();
    }

    @SneakyThrows
    @Test
    void whenTokenHasNotExpiredIsAliveIsTrue() {
        val notExpired = new DataCloudTokenResponse();
        notExpired.setTokenType("type");
        notExpired.setToken(VALID_TOKEN);
        notExpired.setInstanceUrl(VALID_LOGIN_URL);
        notExpired.setExpiresIn(100);

        assertThat(DataCloudToken.of(notExpired).isAlive()).isTrue();
    }

    @Test
    void throwsWhenIllegalArgumentsAreProvided() {
        val noTokenResponse = new DataCloudTokenResponse();
        noTokenResponse.setTokenType("type");
        noTokenResponse.setInstanceUrl(VALID_LOGIN_URL);
        noTokenResponse.setExpiresIn(10000);
        noTokenResponse.setToken("");
        assertThat(assertThrows(IllegalArgumentException.class, () -> DataCloudToken.of(noTokenResponse)))
                .hasMessageContaining("token");
        val noUriResponse = new DataCloudTokenResponse();
        noUriResponse.setTokenType("type");
        noUriResponse.setInstanceUrl("");
        noUriResponse.setExpiresIn(10000);
        noUriResponse.setToken(VALID_TOKEN);
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
        val exception = assertThrows(SQLException.class, () -> DataCloudToken.of(bad));
        assertThat(exception.getMessage()).contains(FAILED_LOGIN);
        assertThat(exception.getCause().getMessage())
                .contains("Malformed escape pair at index 0: " + nonNullOrBlankIllegalUrl);
    }

    @SneakyThrows
    @Test
    void properlyReturnsCorrectValues() {
        val validResponse = new DataCloudTokenResponse();
        val token = FAKE_TOKEN;

        validResponse.setInstanceUrl(VALID_LOGIN_URL);
        validResponse.setToken(token);
        validResponse.setTokenType("Bearer");
        validResponse.setExpiresIn(123);

        val actual = DataCloudToken.of(validResponse);
        softly.assertThat(actual.getAccessToken()).isEqualTo("Bearer " + token);
        softly.assertThat(actual.getTenantUrl()).isEqualTo(VALID_LOGIN_URL);
        softly.assertThat(actual.getTenantId()).isEqualTo(FAKE_TENANT_ID);
    }
}
