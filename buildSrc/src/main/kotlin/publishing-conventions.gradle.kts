plugins {
    id("base-conventions")
    signing
    `maven-publish`
    id("dev.adamko.dev-publish")
}

val revision: String by project

val mavenCentralRepoName = "MavenCentral"

private val ci = object {
    private val snapshotVersion = when (System.getenv("GITHUB_RUN_NUMBER")) {
        null -> "$revision-LOCAL"
        else -> "$revision-SNAPSHOT"
    }

    private val releaseVersion = System.getenv("RELEASE_VERSION")?.ifBlank {
        logger.lifecycle("env.RELEASE_VERSION not present, assuming snapshot")
        null
    }

    val isRelease = releaseVersion != null

    val resolvedVersion = releaseVersion ?: snapshotVersion
}

group = "com.salesforce.datacloud"
version = ci.resolvedVersion

signing {
    val signingPassword: String? by project
    val signingKey: String? by project

    if (!signingKey.isNullOrBlank() && !signingPassword.isNullOrBlank()) {
        useInMemoryPgpKeys(signingKey, signingPassword)
    }

    sign(publishing.publications)
    setRequired { ci.isRelease }
}

gradle.taskGraph.whenReady {
    val isPublishingToMavenCentral = allTasks
        .filterIsInstance<PublishToMavenRepository>()
        .any { it.repository?.name == mavenCentralRepoName }

    signing.setRequired({ isPublishingToMavenCentral || ci.isRelease })

    tasks.withType<Sign> {
        val isPublishingToMavenCentralCustom = isPublishingToMavenCentral
        inputs.property("isPublishingToMavenCentral", isPublishingToMavenCentralCustom)
        onlyIf("publishing to Maven Central") { isPublishingToMavenCentralCustom }
    }
}

/**
 * https://central.sonatype.org/publish/publish-gradle/
 */
publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
//            artifact(tasks())
        }
    }
    repositories {
        maven {
            name = mavenCentralRepoName
            val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")
            url = if (ci.isRelease) releasesRepoUrl else snapshotsRepoUrl
            credentials {
                username = System.getenv("OSSRH_USERNAME")
                password = System.getenv("OSSRH_PASSWORD")
            }
        }
        maven(rootDir.resolve("build/maven-repo")) {
            name = "RootBuildDir"
        }
    }

    publications.withType<MavenPublication>().configureEach {
        updateName()
        updateDescription()
        pom {
            url.set("https://github.com/forcedotcom/datacloud-jdbc")

            scm {
                connection.set("scm:git:https://github.com/forcedotcom/datacloud-jdbc.git")
                developerConnection.set("scm:git:git@github.com:forcedotcom/datacloud-jdbc.git")
                url.set("https://github.com/forcedotcom/datacloud-jdbc")
            }

            licenses {
                license {
                    name.set("Apache-2.0")
                    url.set("https://opensource.org/licenses/Apache-2.0")
                    distribution.set("repo")
                }
            }

            developers {
                developer {
                    name.set("Data Cloud Query Developer Team")
                    email.set("datacloud-query-connector-owners@salesforce.com")
                    organization.set("Salesforce Data Cloud")
                    organizationUrl.set("https://www.salesforce.com/data/")
                }
            }
        }
    }
}

abstract class MavenPublishLimiter : BuildService<BuildServiceParameters.None>

val mavenPublishLimiter = gradle.sharedServices.registerIfAbsent("mavenPublishLimiter", MavenPublishLimiter::class) {
    maxParallelUsages = 1
}

tasks.withType<PublishToMavenRepository>()
    .matching { it.name.endsWith("PublicationTo${mavenCentralRepoName}Repository") }
    .configureEach {
        usesService(mavenPublishLimiter)
    }

fun MavenPublication.updateName() {
    val name = when (this.artifactId) {
        "jdbc" -> "Salesforce Data Cloud JDBC Driver"
        "jdbc-core" -> "Salesforce Data Cloud JDBC Core"
        "jdbc-grpc" -> "Salesforce Data Cloud JDBC gRPC"
        else -> {
            logger.lifecycle("Unknown project, can't update name. artifactId=${this.artifactId}")
            null
        }
    }

    if (name != null) {
        pom { this.name.set(name) }
    }
}


fun MavenPublication.updateDescription() {
    val description = when (this.artifactId) {
        "jdbc" -> "Salesforce Data Cloud JDBC driver"
        "jdbc-core" -> "Salesforce Data Cloud JDBC core implementation"
        "jdbc-grpc" -> "Salesforce Data Cloud Query v3 API gRPC stubs"
        else -> {
            logger.lifecycle("Unknown project, can't update description. artifactId=${this.artifactId}")
            null
        }
    }

    if (description != null) {
        pom { this.description.set(description) }
    }
}
