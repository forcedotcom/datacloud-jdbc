plugins {
    signing
    `maven-publish`
    id("dev.adamko.dev-publish")
}

group = "com.salesforce.datacloud"

 val ossrhUsername: String by project
 val ossrhPassword: String by project
 val signingKey: String? by project
 val signingPassword: String? by project

tasks.withType<Javadoc> {
    options.encoding = "UTF-8"
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}


/**
 * https://central.sonatype.org/publish/publish-gradle/
 */

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            // Additional artifact configurations
        }
    }
    repositories {
        maven {
            name = "OSSRH"
            val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")
            url = if (CI.isRelease) releasesRepoUrl else snapshotsRepoUrl
            credentials {
                username = findProperty("ossrhUsername") as String? ?: System.getenv("OSSRH_USERNAME")
                password = findProperty("ossrhPassword") as String? ?: System.getenv("OSSRH_PASSWORD")
            }
        }
       maven(rootDir.resolve("build/maven-repo")) {
          // Run ./gradlew publishAllPublicationsToRootBuildDirRepository, and check `$rootDir/build/maven-repo/`
          name = "RootBuildDir"
       }
    }

    publications.withType<MavenPublication>().configureEach {
       pom {
          // name.set("c")
          description.set("Salesforce Datacloud JDBC Driver")
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


// version = Ci.publishVersion



// val mavenCentralRepoName = "Deploy"

// signing {
//    if (!signingKey.isNullOrBlank() && !signingPassword.isNullOrBlank()) {
//       useGpgCmd()
//       useInMemoryPgpKeys(signingKey, signingPassword)
//    }
//    sign(publishing.publications)
//    setRequired { Ci.isRelease } // only require signing when releasing
// }

// //region Only enabling signing when publishing to Maven Central.
// // (Otherwise signing is required for dev-publish, which prevents testing if the credentials aren't present.)
// gradle.taskGraph.whenReady {
//    val isPublishingToMavenCentral = allTasks
//       .filterIsInstance<PublishToMavenRepository>()
//       .any { it.repository?.name == mavenCentralRepoName }

//    signing.setRequired({ isPublishingToMavenCentral })

//    tasks.withType<Sign>().configureEach {
//       val isPublishingToMavenCentral_ = isPublishingToMavenCentral
//       inputs.property("isPublishingToMavenCentral", isPublishingToMavenCentral_)
//       onlyIf("publishing to Maven Central") { isPublishingToMavenCentral_ }
//    }
// }
// //endregion

// publishing {
//    repositories {
//       maven {
//          val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
//          val snapshotsRepoUrl = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
//          name = mavenCentralRepoName
//          url = if (Ci.isRelease) releasesRepoUrl else snapshotsRepoUrl
//          credentials {
//             username = System.getenv("OSSRH_USERNAME") ?: ossrhUsername
//             password = System.getenv("OSSRH_PASSWORD") ?: ossrhPassword
//          }
//       }
//       maven(rootDir.resolve("build/maven-repo")) {
//          // Publish to a project-local directory, for easier verification of published artifacts
//          // Run ./gradlew publishAllPublicationsToRootBuildDirRepository, and check `$rootDir/build/maven-repo/`
//          name = "RootBuildDir"
//       }
//    }

//    publications.withType<MavenPublication>().configureEach {
//       pom {
//          // name.set("c")
//          description.set("Salesforce Datacloud JDBC Driver")
//          url.set("https://github.com/forcedotcom/datacloud-jdbc")

//          scm {
//             connection.set("scm:git:https://github.com/forcedotcom/datacloud-jdbc.git")
//             developerConnection.set("scm:git:git@github.com:forcedotcom/datacloud-jdbc.git")
//             url.set("https://github.com/forcedotcom/datacloud-jdbc")
//          }

//          licenses {
//             license {
//                name.set("Apache-2.0")
//                url.set("https://opensource.org/licenses/Apache-2.0")
//                distribution.set("repo")
//             }
//          }

//          developers {
//             developer {
//                name.set("Data Cloud Query Developer Team")
//                email.set("datacloud-query-connector-owners@salesforce.com")
//                organization.set("Salesforce Data Cloud")
//                organizationUrl.set("https://www.salesforce.com/data/")
//             }
//          }
//       }
//    }
// }

// pluginManager.withPlugin("org.jetbrains.kotlin.multiplatform") {
//    val javadocJar by tasks.registering(Jar::class) {
//       group = JavaBasePlugin.DOCUMENTATION_GROUP
//       description = "Create Javadoc JAR"
//       archiveClassifier.set("javadoc")
//    }

//    publishing.publications.withType<MavenPublication>().configureEach {
//       artifact(javadocJar)
//    }

//    publishPlatformArtifactsInRootModule(project)
// }

// pluginManager.withPlugin("java-gradle-plugin") {
//    extensions.configure<JavaPluginExtension> {
//       withSourcesJar()
//    }
// }

// //region Maven Central can't handle parallel uploads, so limit parallel uploads with a BuildService
// abstract class MavenPublishLimiter : BuildService<BuildServiceParameters.None>

// val mavenPublishLimiter =
//    gradle.sharedServices.registerIfAbsent("mavenPublishLimiter", MavenPublishLimiter::class) {
//       maxParallelUsages = 1
//    }

// tasks.withType<PublishToMavenRepository>()
//    .matching { it.name.endsWith("PublicationTo${mavenCentralRepoName}Repository") }
//    .configureEach {
//       usesService(mavenPublishLimiter)
//    }

// // Fix Gradle error Reason: Task <publish> uses this output of task <sign> without declaring an explicit or implicit dependency.
// // https://github.com/gradle/gradle/issues/26091
// tasks.withType<AbstractPublishToMaven>().configureEach {
//    val signingTasks = tasks.withType<Sign>()
//    mustRunAfter(signingTasks)
// }