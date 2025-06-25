plugins {
    id("base-conventions")
    id("version-conventions")
    signing
    `maven-publish`
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
        val nameProvider = provider { mavenName }
        val descProvider = provider { mavenDescription }
        if (components.findByName("java") != null) {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                configurePom(nameProvider, descProvider)
            }
        } else {
            afterEvaluate {
                findByName("mavenProto")?.let { publication ->
                    (publication as MavenPublication).configurePom(nameProvider, descProvider)
                }
            }
        }
    }
}
