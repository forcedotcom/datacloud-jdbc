plugins {
    id("publishing-conventions")
    id("java-conventions")
}

tasks.named<Jar>("jar") {
    from(project.projectDir.resolve("src/main/proto"))
}

tasks.named<Jar>("sourcesJar") {
    archiveClassifier.set("sources")
    from(project.projectDir.resolve("src/main/proto"))
}

tasks.named<Jar>("javadocJar") {
    archiveClassifier.set("javadoc")
}