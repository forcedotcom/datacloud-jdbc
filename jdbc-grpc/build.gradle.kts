import com.google.protobuf.gradle.*

plugins {
    id("java-conventions")
    id("publishing-conventions")
    id("com.google.osdetector")
    alias(libs.plugins.protobuf)
}

description = "Salesforce Data Cloud Query v3 API gRPC stubs"
val mavenName: String by extra("Salesforce Data Cloud JDBC gRPC")
val mavenDescription: String by extra("${project.description}")

dependencies {
    api(platform(libs.protobuf.bom))
    implementation(libs.bundles.grpc.impl)
}

// Based on: https://github.com/google/protobuf-gradle-plugin/blob/master/examples/exampleKotlinDslProject
sourceSets {
    main {
        proto {
            srcDir(project(":jdbc-proto").projectDir.resolve("src/main/proto"))
        }
    }
}

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

tasks.jar {
    archiveBaseName.set("jdbc-grpc")
    val tasks = sourceSets.map { sourceSet ->
        from(sourceSet.output)
        sourceSet.getCompileTaskName("java")
    }.toTypedArray()

    dependsOn(*tasks)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
