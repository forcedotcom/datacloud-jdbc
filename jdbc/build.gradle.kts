plugins {
    id("java-conventions")
    id("lombok-conventions")
}

dependencies {
    api(project(":jdbc-core"))
    api(project(":jdbc-grpc"))
    api(libs.slf4j.api)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.junit.jupiter.engine)
    testImplementation(libs.junit.jupiter.params)
    testImplementation(libs.junit.platform.launcher)
    testImplementation(libs.assertj)
}

description = "Salesforce Data Cloud JDBC Driver"

tasks.register<Copy>("createServiceFile") {
    val serviceFile = file("${layout.buildDirectory}/generated/META-INF/services/java.sql.Driver")
    serviceFile.parentFile.mkdirs()
    serviceFile.writeText("com.salesforce.datacloud.jdbc.DataCloudJDBCDriver")

    from("${layout.buildDirectory}/generated")
    into("src/main/resources")
}

tasks.processResources {
    dependsOn("createServiceFile")
}
