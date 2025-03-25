import com.google.protobuf.gradle.*

plugins {
    id("java-conventions")
    id("publishing-conventions")
    id("com.google.osdetector")
    alias(libs.plugins.protobuf)
}

dependencies {
    api(platform(libs.grpc.bom))
    api(libs.grpc.netty)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.javax.annotation.javax.annotation.api)
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
//        all().forEach { task ->
//            task.plugins {
//                create("grpc")
//            }
//        }
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
