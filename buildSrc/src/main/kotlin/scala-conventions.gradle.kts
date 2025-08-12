import org.gradle.api.tasks.scala.ScalaCompile

plugins {
    id("java-base-conventions")
    id("scala")
    `java-library`
}

tasks.withType<ScalaCompile> {
    javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(8) }
}

spotless { scala { scalafmt() } }
