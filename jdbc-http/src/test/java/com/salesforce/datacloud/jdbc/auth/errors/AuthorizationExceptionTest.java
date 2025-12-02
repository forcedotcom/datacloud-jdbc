/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth.errors;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AuthorizationExceptionTest {

    @Test
    void getMessage_IncludesHttpCodeAndDescription() {
        AuthorizationException ex = AuthorizationException.builder()
                .message("Bad Request")
                .errorCode("400")
                .errorDescription("Invalid credentials")
                .build();

        assertThat(ex.getMessage()).isEqualTo("Bad Request (HTTP 400): Invalid credentials");
    }

    @Test
    void getMessage_WithOnlyErrorCode() {
        AuthorizationException ex =
                AuthorizationException.builder().errorCode("500").build();

        assertThat(ex.getMessage()).isEqualTo("Authorization failed (HTTP 500)");
    }

    @Test
    void getMessage_WithOnlyDescription() {
        AuthorizationException ex = AuthorizationException.builder()
                .errorDescription("Server error")
                .build();

        assertThat(ex.getMessage()).isEqualTo("Authorization failed: Server error");
    }

    @Test
    void getMessage_WithEmptyFields() {
        AuthorizationException ex = AuthorizationException.builder().build();

        assertThat(ex.getMessage()).isEqualTo("Authorization failed");
    }

    @Test
    void isRetriable_ReturnsTrueFor5xxErrors() {
        assertThat(AuthorizationException.builder().errorCode("500").build().isRetriable())
                .isTrue();
        assertThat(AuthorizationException.builder().errorCode("502").build().isRetriable())
                .isTrue();
        assertThat(AuthorizationException.builder().errorCode("503").build().isRetriable())
                .isTrue();
        assertThat(AuthorizationException.builder().errorCode("504").build().isRetriable())
                .isTrue();
        assertThat(AuthorizationException.builder().errorCode("599").build().isRetriable())
                .isTrue();
    }

    @Test
    void isRetriable_ReturnsFalseForNon5xxErrors() {
        assertThat(AuthorizationException.builder().errorCode("200").build().isRetriable())
                .isFalse();
        assertThat(AuthorizationException.builder().errorCode("401").build().isRetriable())
                .isFalse();
        assertThat(AuthorizationException.builder().errorCode("403").build().isRetriable())
                .isFalse();
        assertThat(AuthorizationException.builder().errorCode("404").build().isRetriable())
                .isFalse();
    }

    @Test
    void isRetriable_ReturnsTrueFor400WithRetryHints() {
        assertThat(AuthorizationException.builder()
                        .errorCode("400")
                        .errorDescription("{\"error\":\"unknown_error\",\"error_description\":\"retry your request\"}")
                        .build()
                        .isRetriable())
                .isTrue();

        assertThat(AuthorizationException.builder()
                        .errorCode("400")
                        .errorDescription("Please retry your request")
                        .build()
                        .isRetriable())
                .isTrue();

        assertThat(AuthorizationException.builder()
                        .errorCode("400")
                        .errorDescription("Temporary error occurred")
                        .build()
                        .isRetriable())
                .isTrue();
    }

    @Test
    void isRetriable_ReturnsFalseFor400WithoutRetryHints() {
        assertThat(AuthorizationException.builder()
                        .errorCode("400")
                        .errorDescription("Invalid credentials")
                        .build()
                        .isRetriable())
                .isFalse();

        assertThat(AuthorizationException.builder()
                        .errorCode("400")
                        .errorDescription("Bad request")
                        .build()
                        .isRetriable())
                .isFalse();
    }

    @Test
    void isRetriable_ReturnsTrueFor429RateLimit() {
        assertThat(AuthorizationException.builder()
                        .errorCode("429")
                        .errorDescription("Rate limit exceeded")
                        .build()
                        .isRetriable())
                .isTrue();

        assertThat(AuthorizationException.builder()
                        .errorCode("429")
                        .errorDescription("Too many requests")
                        .build()
                        .isRetriable())
                .isTrue();

        // 429 should be retriable even without description
        assertThat(AuthorizationException.builder().errorCode("429").build().isRetriable())
                .isTrue();
    }

    @Test
    void isRetriable_ReturnsFalseForNullOrEmptyErrorCode() {
        assertThat(AuthorizationException.builder().build().isRetriable()).isFalse();
        assertThat(AuthorizationException.builder().errorCode("").build().isRetriable())
                .isFalse();
        assertThat(AuthorizationException.builder().errorCode(null).build().isRetriable())
                .isFalse();
    }

    @Test
    void isRetriable_ReturnsFalseForInvalidErrorCode() {
        assertThat(AuthorizationException.builder().errorCode("invalid").build().isRetriable())
                .isFalse();
        assertThat(AuthorizationException.builder().errorCode("abc").build().isRetriable())
                .isFalse();
    }
}
