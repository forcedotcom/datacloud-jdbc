import com.google.protobuf.gradle.*

plugins {
    id("java-conventions")
    id("publishing-conventions")
    id("com.google.osdetector")
    alias(libs.plugins.protobuf)
}

dependencies {
    api(platform(libs.grpc.bom))
    implementation(libs.bundles.grpc)
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
    archiveClassifier.set("proto")
    from(project.projectDir.resolve("src/main/proto"))
}

tasks.jar {
    val tasks = sourceSets.map { sourceSet ->
        from(sourceSet.output)
        sourceSet.getCompileTaskName("java")
    }.toTypedArray()

    dependsOn(protoJar, *tasks)

    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

publishing {
    publications {
        named<MavenPublication>("mavenJava") {
            artifact(protoJar)
        }
    }
}
