plugins {
    id("base-conventions")
    id("version-conventions")
    signing
    `maven-publish`
    id("dev.adamko.dev-publish")
}

val mavenCentralRepoName = "MavenCentral"
val isRelease = !System.getenv("RELEASE_VERSION").isNullOrBlank()

signing {
    val signingPassword: String? by project
    val signingKey: String? by project

    if (!signingKey.isNullOrBlank() && !signingPassword.isNullOrBlank()) {
        useInMemoryPgpKeys(signingKey, signingPassword)
    }

    sign(publishing.publications)
    setRequired { isRelease }
}

gradle.taskGraph.whenReady {
    val isPublishingToMavenCentral = allTasks
        .filterIsInstance<PublishToMavenRepository>()
        .any { it.repository?.name == mavenCentralRepoName }

    signing.setRequired({ isPublishingToMavenCentral || isRelease })

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
        }
    }
    repositories {
        maven {
            name = mavenCentralRepoName
            val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")
            url = if (isRelease) releasesRepoUrl else snapshotsRepoUrl
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
        "jdbc-proto" -> "Salesforce Data Cloud JDBC Proto Files"
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
        "jdbc-proto" -> "Salesforce Data Cloud Query API proto files"
        else -> {
            logger.lifecycle("Unknown project, can't update description. artifactId=${this.artifactId}")
            null
        }
    }

    if (description != null) {
        pom { this.description.set(description) }
    }
}
