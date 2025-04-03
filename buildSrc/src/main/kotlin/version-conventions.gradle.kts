
val revision: String by project

private val ci = object {
    private val snapshotVersion = when (System.getenv("GITHUB_RUN_NUMBER")) {
        null -> "$revision-LOCAL"
        else -> "$revision-SNAPSHOT"
    }

    private val releaseVersion = System.getenv("RELEASE_VERSION")?.ifBlank {
        logger.lifecycle("env.RELEASE_VERSION not present, assuming snapshot")
        null
    }

    val resolvedVersion = releaseVersion ?: snapshotVersion
}

group = "com.salesforce.datacloud"
version = ci.resolvedVersion