import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.Project
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.kotlin.dsl.*
import javax.inject.Inject

plugins {
    java
    id("com.gradleup.shadow")
}

/**
 * Gradle extension for JAR shading with JDBC/Spark presets.
 */
abstract class ShadingExtension @Inject constructor(
    private val project: Project
) {
    companion object {
        private val JDBC_ADDITIONAL_EXCLUSIONS = emptyList<String>()
        private val SPARK_ADDITIONAL_EXCLUSIONS = listOf(
            "org/apache/spark/**",
            "scala/**",
            "org/scala-lang/**"
        )
    }
    
    fun jdbc() = configureShading(JDBC_ADDITIONAL_EXCLUSIONS)
    fun spark() = configureShading(SPARK_ADDITIONAL_EXCLUSIONS)
    
    private fun configureShading(moduleExclusions: List<String>) {
        val exclusions = getCommonExclusions() + moduleExclusions
        
        configureShadowJarTasks(exclusions)
    }
    
    private fun getCommonExclusions() = listOf(
        "META-INF/maven/**", "META-INF/DEPENDENCIES",
        "META-INF/LICENSE*", "META-INF/NOTICE*",
        "META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.xml",
        "**/*.proto", "org.slf4j",
        "META-INF/services/com.fasterxml.*", "org.apache.calcite.avatica.remote.Driver",
        ".netbeans_automatic_build", "git.properties", "google-http-client.properties",
        "storage.v1.json", "pipes-fork-server-default-log4j2.xml",
        "dependencies.properties", "arrow-git.properties"
    )
    
    private fun configureShadowJarTasks(
        exclusions: List<String>
    ) {
        project.tasks.withType<ShadowJar>().configureEach {
            val shadeBase = "com.salesforce.datacloud.shaded"
            
            isReproducibleFileOrder = true
            isPreserveFileTimestamps = false
            duplicatesStrategy = DuplicatesStrategy.INCLUDE
            
            // Exclude duplicates for non-service files to avoid bloat
            filesNotMatching("META-INF/services/**") {
                duplicatesStrategy = DuplicatesStrategy.EXCLUDE
            }
            
            // JAR naming configuration
            archiveBaseName.set(project.name)
            archiveClassifier.set("shaded")
            
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

            // Use built-in service file merging with package relocation support
            // This automatically handles relocated class names in service files!
            mergeServiceFiles {
                exclude("META-INF/services/java.sql.Driver")
            }
            
            exclusions.forEach { exclude(it) }
        }
        
        // Configure assemble task dependency
        project.tasks.named("assemble").configure {
            dependsOn(project.tasks.named("shadowJar"))
        }
    }
}

extensions.create<ShadingExtension>("shading", project)
