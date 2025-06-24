import org.jreleaser.model.Active

plugins {
    id("base-conventions")
    id("version-conventions")
    signing
    `maven-publish`
    id("org.jreleaser")
    id("dev.adamko.dev-publish")
}

val mavenName: String by project.extra
val mavenDescription: String by project.extra

// workaround for https://github.com/gradle/gradle/issues/16543
inline fun <reified T : Task> TaskContainer.provider(taskName: String): Provider<T> =
    providers.provider { taskName }.flatMap { named<T>(it) }

fun MavenPublication.configurePom(nameProvider: Provider<String>, descProvider: Provider<String>) {
    pom {
        url.set("https://github.com/forcedotcom/datacloud-jdbc")
        name.set(nameProvider)
        description.set(descProvider)

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


publishing {
    publications {
        if (components.findByName("java") != null) {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                configurePom(provider { mavenName }, provider { mavenDescription })
            }
        }
         else {
            afterEvaluate {
                findByName("mavenProto")?.let { publication ->
                    (publication as MavenPublication).configurePom(provider { mavenName }, provider { mavenDescription })
                }
            }
        }
    }
    
    repositories {
        maven {
            url = layout.buildDirectory.dir("staging-deploy").get().asFile.toURI()
        }
    }
}


jreleaser {
    gitRootSearch = true
    deploy {
        maven {
            mavenCentral {
                active.set(Active.ALWAYS)
                register("sonatype") {
                    active = Active.ALWAYS
                    url = "https://central.sonatype.com/api/v1/publisher"
                    stagingRepository(layout.buildDirectory.dir("staging-deploy").get().asFile.path)
                }
            }
        }
    }

    signing {
        active.set(Active.ALWAYS)
        armored.set(true)
    }

    release {
        github {
            enabled = false
            skipRelease = true
            skipTag = true
        }
    }
}

