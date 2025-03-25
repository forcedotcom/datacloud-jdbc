

object CI {

    private const val SNAPSHOT_BASE = "0.25.7"


    private val isCI = System.getenv("CI").toBoolean()

    private val snapshotVersion = when (val githubBuildNumber = System.getenv("GITHUB_RUN_NUMBER")) {
        null -> "$SNAPSHOT_BASE-LOCAL"
        else -> "$SNAPSHOT_BASE.${githubBuildNumber}-SNAPSHOT"
    }

    private val releaseVersion = System.getenv("RELEASE_VERSION")?.ifBlank { null }

    val isRelease = releaseVersion != null

    val version = releaseVersion ?: snapshotVersion
}