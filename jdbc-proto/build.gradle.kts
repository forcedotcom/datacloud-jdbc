plugins {
    id("base-conventions")
    id("version-conventions")
    id("publishing-conventions")
}

description = "Salesforce Data Cloud Query v3 API Protocol Buffer Definitions"
extra.set("mavenName", "Salesforce Data Cloud JDBC Proto")
extra.set("mavenDescription", project.description.toString())

val protoJar = tasks.register<Jar>("protoJar") {
    group = LifecycleBasePlugin.BUILD_GROUP
    archiveBaseName.set("jdbc-proto")
    from(project.projectDir.resolve("src/main/proto"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    archiveClassifier.set("sources")
    archiveBaseName.set("jdbc-proto")
    from(project.projectDir.resolve("src/main/proto"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val emptyJavadocJar = tasks.register<Jar>("emptyJavadocJar") {
    archiveClassifier.set("javadoc")
    archiveBaseName.set("jdbc-proto")
}

tasks.named("assemble") {
    dependsOn(protoJar, sourcesJar, emptyJavadocJar)
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
