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
        if (("org.junit" in it.candidate.group) && candidateVersion.startsWith("6.")) {
            false
        } else if ("org.apache.spark" in it.candidate.group && candidateVersion.startsWith("4.")) {
            false
        } else if ("com.squareup.okhttp3" in it.candidate.group && candidateVersion.startsWith("5.")) {
            false
        } else if ("org.mockito" in it.candidate.group && candidateVersion.startsWith("5.")) {
            false
        } else {
            stableKeyword || regex.matches(candidateVersion)
        }
    }
}