import nl.littlerobots.vcu.plugin.versionSelector

plugins {
    id("nl.littlerobots.version-catalog-update")
}

versionCatalogUpdate {
    versionSelector {
        // For Guava we want to also allow picking the -jre version
        val candidateVersion = if (it.candidate.version.contains("-jre")) {
            it.candidate.version.split("-jre")[0]
        } else {
            it.candidate.version
        }
        val stableKeyword = listOf("RELEASE", "FINAL", "GA").any { candidateVersion.uppercase().contains(it) }
        val regex = "^[0-9,.v-]+(-r)?$".toRegex()

        // Special cases for libraries where we have to do soft pinning. We don't want to fully pin to still get the latest version for the allowed range.
        if ("org.junit.platform" in it.candidate.group && !candidateVersion.startsWith("1.")) {
            // JUnit Platform uses 1.x versioning; pinned to 1.* (corresponding to JUnit Jupiter 5.x)
            false
        } else if ("org.junit" in it.candidate.group && !candidateVersion.startsWith("5.")) {
            // JUnit BOM and Jupiter are soft pinned to 5.* (as 6 is not Java 8 compatible)
            false
        } else if ("org.apache.spark" in it.candidate.group && !candidateVersion.startsWith("3.")) {
            // This is soft pinned to 3.* (as the driver is targetting in Spark 3)
            false
        } else if ("com.squareup.okhttp3" in it.candidate.group && !candidateVersion.startsWith("4.")) {
            // This is soft pinned to 4.* (as we didn't invest in the upgrade yet)
            false
        } else if ("org.mockito" in it.candidate.group && !candidateVersion.startsWith("4.")) {
            //This is soft pinned to 4.* (as 5 is not Java 8 compatible)
            false
        } else if ("io.netty" in it.candidate.group && !candidateVersion.startsWith("4.1.")) {
            // Netty is soft pinned to 4.1.* as gRPC requires the 4.1.x line
            false
        } else {
            stableKeyword || regex.matches(candidateVersion)
        }
    }
}