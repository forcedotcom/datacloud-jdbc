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

description = "Salesforce Data Cloud Query v3 gRPC stubs"

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
        }
    }
}

tasks.withType<JavaCompile> {
    dependsOn("generateProto")
}

tasks.withType<Javadoc> {
    dependsOn("protoJar")
}

tasks.register<Jar>("protoJar") {
    archiveClassifier.set("proto")
    from(project.projectDir.resolve("src/main/proto"))
}
