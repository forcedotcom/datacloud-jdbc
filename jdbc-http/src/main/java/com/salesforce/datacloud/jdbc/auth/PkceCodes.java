/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import lombok.Value;

/**
 * Proof Key for Code Exchange (PKCE) verifier and challenge per RFC 7636.
 *
 * <p>The verifier is a 32-byte cryptographically random string, base64url-encoded
 * without padding (43 characters). The S256 challenge is the base64url-encoded
 * SHA-256 of the verifier's ASCII bytes. Salesforce External Client Apps support
 * S256 only.
 */
@Value
public final class PkceCodes {

    public static final String CHALLENGE_METHOD_S256 = "S256";

    String verifier;
    String challenge;

    public static PkceCodes generate() {
        return generate(new SecureRandom());
    }

    static PkceCodes generate(SecureRandom random) {
        byte[] verifierBytes = new byte[32];
        random.nextBytes(verifierBytes);
        String verifier = Base64.getUrlEncoder().withoutPadding().encodeToString(verifierBytes);
        String challenge = s256Challenge(verifier);
        return new PkceCodes(verifier, challenge);
    }

    static String s256Challenge(String verifier) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] digest = sha256.digest(verifier.getBytes(StandardCharsets.US_ASCII));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException ex) {
            // SHA-256 is required by every JRE — this branch is unreachable in practice.
            throw new IllegalStateException("SHA-256 algorithm not available", ex);
        }
    }
}
