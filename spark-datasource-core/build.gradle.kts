plugins {
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
}

description = "Salesforce Data Cloud Spark DataSource core implementation"
extra.set("mavenName", "Salesforce Data Cloud Spark DataSource Core")
extra.set("mavenDescription", project.description.toString())

dependencies {
    // gRPC dependencies: compileOnly to avoid forcing users to provide gRPC.
    // This follows the same pattern as jdbc-core.
    compileOnly(project(":jdbc-grpc"))
    compileOnly(libs.grpc.stub)
    compileOnly(libs.grpc.protobuf)
    
    // Core JDBC dependencies - these provide the actual DataCloud connection logic
    implementation(project(":jdbc-core"))  // Provides DataCloudConnection interface
    implementation(libs.slf4j.api)
    
    // Spark dependencies (provided by user at runtime)
    compileOnly(libs.bundles.spark)

    // Test dependencies: Need actual implementations since main uses compileOnly
    testImplementation(project(":jdbc-grpc"))  // Needed because main uses compileOnly
    testImplementation(project(":jdbc-util"))   // Needed for DataCloudJDBCException in tests
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(libs.bundles.grpc.impl)
    testImplementation(libs.bundles.grpc.testing)
    testImplementation(libs.bundles.scala.testing)
    
    // Spark test dependencies: Need Spark for test compilation since main uses compileOnly
    testImplementation(libs.bundles.spark)  // compileOnly dependencies aren't available at test time
    
    // Override transitive Jackson Scala module from Spark with newer version (same as spark-datasource)
    testImplementation(libs.jackson.module.scala)
    
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.scalatest)
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.scalatestplus.junit5)
}

tasks.named("compileScala") {
    dependsOn(":jdbc-grpc:compileJava")
    dependsOn(":jdbc-core:compileJava")
}
