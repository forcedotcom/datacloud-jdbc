plugins {
    alias(libs.plugins.shadow)
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
}

description = "Spark Datasource for Salesforce Data Cloud JDBC"
val mavenName: String by extra("Spark Datasource for Salesforce Data Cloud JDBC")
val mavenDescription: String by extra("${project.description}")

dependencies {
    // Core spark datasource implementation
    implementation(project(":spark-datasource-core"))
    
    // JDBC core dependencies needed for compilation
    implementation(project(":jdbc-core"))
    implementation(project(":jdbc-util"))
    
    // Spark dependencies (provided by user at runtime, but needed for compilation)
    implementation(libs.bundles.spark)
    
    // Use compileOnly for compilation + runtimeOnly for shading (not exposed to consumers)
    compileOnly(project(":jdbc-grpc"))
    compileOnly(libs.bundles.grpc.impl)
    runtimeOnly(project(":jdbc-grpc"))
    runtimeOnly(libs.bundles.grpc.impl)
    
    // Override transitive Jackson Scala module from Spark with newer version
    implementation(libs.jackson.module.scala)

    testImplementation(platform(libs.junit.bom))
    testImplementation(testFixtures(project(":jdbc-core")))
    testImplementation(libs.scalatest)
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.scalatestplus.junit5)
}

// Common shading configuration to be reused
fun com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar.configureShading() {
    val shadeBase = "com.salesforce.datacloud.shaded"
    
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    isZip64 = true  // Enable ZIP64 for large archives

    relocate("com.google", "$shadeBase.com.google")
    relocate("io.grpc", "$shadeBase.io.grpc")
    relocate("com.fasterxml.jackson", "$shadeBase.com.fasterxml.jackson")
    relocate("dev.failsafe", "$shadeBase.dev.failsafe")
    relocate("io.jsonwebtoken", "$shadeBase.io.jsonwebtoken")
    relocate("io.netty", "$shadeBase.io.netty")
    relocate("kotlin", "$shadeBase.kotlin")
    relocate("okhttp3", "$shadeBase.okhttp3")
    relocate("okio", "$shadeBase.okio")
    relocate("org.apache.arrow", "$shadeBase.org.apache.arrow")
    relocate("org.apache.calcite", "$shadeBase.org.apache.calcite") {
        exclude("org.apache.calcite.avatica.remote.Driver")
    }
    relocate("org.apache.commons", "$shadeBase.org.apache.commons")
    relocate("org.apache.hc", "$shadeBase.org.apache.hc")

    mergeServiceFiles {
        exclude("META-INF/services/java.sql.Driver")
    }
    
    exclude("org.slf4j")

    exclude("org.apache.calcite.avatica.remote.Driver")
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
    exclude("**/*.proto")
    exclude("arrow-git.properties")
    
    // Exclude Spark and Scala from shading (user provides these)
    exclude("org/apache/spark/**")
    exclude("scala/**")
    exclude("org/scala-lang/**")
}

// Default JAR with no classifier but is shaded 
tasks.shadowJar {
    archiveBaseName = "spark-datasource"
    archiveClassifier = ""
    configureShading()
    shouldRunAfter(tasks.jar)
}

// Create an additional shadowJar with "shaded" classifier
val shadedJar = tasks.register<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadedJar") {
    from(sourceSets.main.get().output)
    configurations = listOf(project.configurations.runtimeClasspath.get())
    archiveBaseName = "spark-datasource"
    archiveClassifier = "shaded"
    configureShading()
    shouldRunAfter(tasks.jar)
}

// This is the base JAR with an "original" classifier, it's not shaded 
tasks.jar {
    archiveClassifier = "original"
}

tasks.named("compileScala") {
    dependsOn(":spark-datasource-core:compileScala")
    dependsOn(":jdbc-core:compileJava")
}

tasks.assemble {
    dependsOn(tasks.shadowJar)
    dependsOn(shadedJar)
}

// Configure publishing to include the shaded JAR after evaluation
afterEvaluate {
   publishing {
       publications {
           named<MavenPublication>("mavenJava") {
               artifact(shadedJar.get()) {
                   classifier = "shaded"
               }
               
               // For shaded JAR, remove all dependencies from POM since everything is included
               pom.withXml {
                   val dependenciesNode = asNode().get("dependencies")
                   if (dependenciesNode is groovy.util.NodeList && dependenciesNode.isNotEmpty()) {
                       asNode().remove(dependenciesNode[0] as groovy.util.Node)
                   }
               }
           }
       }
   }
}