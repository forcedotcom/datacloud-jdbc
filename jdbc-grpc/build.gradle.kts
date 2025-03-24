plugins {
    id("java-conventions")
    id("publishing-conventions")
    alias(libs.plugins.protobuf)
    alias(libs.plugins.osdetector)
}

dependencies {
    protobuf(project(":jdbc-proto"))

    api(libs.grpc.netty)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.javax.annotation.javax.annotation.api)
}

description = "jdbc-grpc"

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
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

tasks.withType<JavaCompile> {
    dependsOn("generateProto")
}

tasks.register<Jar>("protoJar") {
    archiveClassifier.set("proto")
    from(project.projectDir.resolve("src/main/proto"))
}
