plugins {
    id("java-conventions")
    id("publishing-conventions")
    alias(libs.plugins.shadow)
    alias(libs.plugins.lombok)
}

dependencies {
    implementation(project(":jdbc-core"))
    implementation(project(":jdbc-grpc"))
    implementation(libs.slf4j.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
}

description = "Salesforce Data Cloud JDBC Driver"

// https://gradleup.com/shadow/
tasks.shadowJar {
    val shadeBase = "com.salesforce.datacloud.jdbc.internal.shaded"

    archiveBaseName = "jdbc"
    archiveClassifier = "shaded"

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    mergeServiceFiles()

    relocate("org.apache", "$shadeBase.apache")
    relocate("org.apache.calcite.avatica", "$shadeBase.org.apache.calcite.avatica") {
        exclude("org.apache.calcite.avatica.remote.Driver")
    }


    relocate("okhttp3", "$shadeBase.okhttp3")
    relocate("io.netty", "$shadeBase.io.netty")
    relocate("io.grpc", "$shadeBase.io.grpc")
    relocate("com.fasterxml.jackson", "$shadeBase.com.fasterxml.jackson")
    relocate("io.jsonwebtoken", "$shadeBase.io.jsonwebtoken")
    relocate("com.squareup", "$shadeBase.com.squareup")
    relocate("com.google", "$shadeBase.com.google")
    relocate("net.jodah", "$shadeBase.net.jodah")
    relocate("org.projectlombok", "$shadeBase.org.projectlombok")
    relocate("javax.annotation", "$shadeBase.javax.annotation")
    relocate("com.google.protobuf", "$shadeBase.com.google.protobuf")
    relocate("org.slf4j", "$shadeBase.org.slf4j")
    relocate("lombok", "$shadeBase.lombok")
    relocate("kotlin", "$shadeBase.kotlin")

    exclude("META-INF/LICENSE*")
    exclude("META-INF/NOTICE*")
    exclude("META-INF/DEPENDENCIES")
    exclude("META-INF/maven/**")
    exclude("META-INF/services/com.fasterxml.*")
    exclude("META-INF/*.xml")
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    exclude(".netbeans_automatic_build")
    exclude("git.properties")
    exclude("google-http-client.properties")
    exclude("storage.v1.json")
    exclude("pipes-fork-server-default-log4j2.xml")
    exclude("dependencies.properties")

//    minimize()

//    minimize {
//        exclude(dependency("org.apache.arrow:arrow-vector")) {
//            exclude("codegen/**")
//        }
//
//        // Exclude Driver class from avatica
//        exclude(dependency('org.apache.calcite.avatica:avatica')) {
//            exclude 'org/apache/calcite/avatica/**/Driver.class'
//        }
//    }
}

tasks.named("compileJava") {
    dependsOn(":jdbc-core:build")
}