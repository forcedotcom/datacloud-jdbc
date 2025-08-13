plugins {
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
}

description = "Salesforce Data Cloud Spark DataSource core implementation"
val mavenName: String by extra("Salesforce Data Cloud Spark DataSource Core")
val mavenDescription: String by extra("${project.description}")

dependencies {
    // gRPC dependencies: compileOnly because jdbc-core also uses compileOnly for gRPC
    // This follows the same pattern as jdbc-core to avoid forcing users to provide gRPC
    compileOnly(project(":jdbc-grpc"))
    compileOnly(libs.grpc.stub)
    compileOnly(libs.grpc.protobuf)
    
    // Core JDBC dependencies - these provide the actual DataCloud connection logic
    implementation(project(":jdbc-core"))  // Provides DataCloudConnection interface
    implementation(project(":jdbc-util"))
    implementation(libs.slf4j.api)
    
    // Spark dependencies (provided by user at runtime)
    compileOnly(libs.bundles.spark)

    // Test dependencies: Need actual gRPC implementations since compileOnly doesn't provide them at test time
    testImplementation(project(":jdbc-grpc"))  // Needed because main uses compileOnly
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(libs.bundles.grpc.impl)
    testImplementation(libs.bundles.grpc.testing)
    testImplementation(libs.bundles.scala.testing)
}

tasks.named("compileScala") {
    dependsOn(":jdbc-grpc:compileJava")
    dependsOn(":jdbc-core:compileJava")
} 