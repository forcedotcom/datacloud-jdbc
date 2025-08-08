import org.gradle.api.tasks.scala.ScalaCompile

plugins {
    id("java-base-conventions")
    id("scala")
    `java-library`
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withJavadocJar()
    withSourcesJar()
}

tasks.withType<ScalaCompile> {
    javaLauncher = javaToolchains.launcherFor {
        languageVersion = JavaLanguageVersion.of(8)
    }
}

spotless {    
    scala {
        scalafmt()
    }
}