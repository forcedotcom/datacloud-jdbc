import com.google.protobuf.gradle.*

plugins {
    id("java-conventions")
    id("publishing-conventions")
    id("com.google.osdetector")
    alias(libs.plugins.protobuf)
}

dependencies {
    api(platform(libs.protobuf.bom))
    implementation(libs.bundles.grpc.impl)
}

// Based on: https://github.com/google/protobuf-gradle-plugin/blob/master/examples/exampleKotlinDslProject
protobuf {
    protoc {
        artifact = libs.protoc.get().toString()
    }
    plugins {
        create("grpc") {
            artifact = libs.grpc.protoc.get().toString()
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc") { }
            }
            it.generateDescriptorSet = true
        }
    }
}

tasks.withType<JavaCompile> {
    dependsOn("generateProto")
}

val protoJar by tasks.registering(Jar::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    archiveBaseName.set("jdbc-proto")
    from(project.projectDir.resolve("src/main/proto"))
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.jar {
    archiveBaseName.set("jdbc-grpc")
    val tasks = sourceSets.map { sourceSet ->
        from(sourceSet.output)
        sourceSet.getCompileTaskName("java")
    }.toTypedArray()

    dependsOn(protoJar, *tasks)

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val emptySourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    archiveBaseName.set("jdbc-proto")
}

val emptyJavadocJar by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
    archiveBaseName.set("jdbc-proto")
}

publishing {
    publications {
        named<MavenPublication>("mavenJava") {
            artifactId = "jdbc-grpc"
        }

        create<MavenPublication>("mavenProto") {
            artifactId = "jdbc-proto"
            artifact(protoJar)
            artifact(emptySourcesJar)
            artifact(emptyJavadocJar)
        }
    }
}
