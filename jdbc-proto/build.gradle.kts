plugins {
    id("base-conventions")
    id("version-conventions")
    id("publishing-conventions")
}

description = "Salesforce Data Cloud Query v3 API Protocol Buffer Definitions"
val mavenName: String by extra("Salesforce Data Cloud JDBC Proto")
val mavenDescription: String by extra("${project.description}")

val protoJar by tasks.registering(Jar::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    archiveBaseName.set("jdbc-proto")
    from(project.projectDir.resolve("src/main/proto"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    archiveBaseName.set("jdbc-proto")
    from(project.projectDir.resolve("src/main/proto"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val emptyJavadocJar by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
    archiveBaseName.set("jdbc-proto")
}

artifacts {
    archives(protoJar)
    archives(sourcesJar)
    archives(emptyJavadocJar)
}

publishing {
    publications {
        create<MavenPublication>("mavenProto") {
            artifactId = "jdbc-proto"
            artifact(protoJar)
            artifact(sourcesJar)
            artifact(emptyJavadocJar)
        }
    }
}
