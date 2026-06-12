/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.auth;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;

/**
 * Opens the system browser at the OAuth authorization URL. Extracted as an
 * interface so tests can drive the loopback callback without a real browser.
 */
public interface BrowserLauncher {

    /**
     * Open the URL in the user's default browser. If launching the browser is
     * not possible (e.g. headless servers, sandboxed CI), implementations should
     * surface the URL through some other channel — the default implementation
     * logs it to stderr so a human can paste it into a browser manually.
     */
    void open(URI authorizationUrl) throws IOException;

    static BrowserLauncher defaultLauncher() {
        return new DesktopBrowserLauncher();
    }

    @Slf4j
    final class DesktopBrowserLauncher implements BrowserLauncher {
        @Override
        public void open(URI authorizationUrl) throws IOException {
            if (Desktop.isDesktopSupported()) {
                Desktop desktop = Desktop.getDesktop();
                if (desktop.isSupported(Desktop.Action.BROWSE)) {
                    try {
                        desktop.browse(authorizationUrl);
                        return;
                    } catch (Exception ex) {
                        // Fall through and print the URL so the user can complete the flow manually.
                        log.warn("Desktop.browse() failed, falling back to printing the URL", ex);
                    }
                }
            }
            // Headless or restricted environment: surface the URL so a human can paste it.
            String message = "Open the following URL in a browser to complete authentication:\n" + authorizationUrl;
            log.info("{}", message);
            System.err.println(message);
        }
    }
}
