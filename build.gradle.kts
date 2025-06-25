plugins {
    id("hyper-conventions")
    id("base-conventions")
    id("version-conventions")
    id("com.diffplug.spotless")
    id("dev.iurysouza.modulegraph") version "0.12.0"
}

tasks.register("printVersion") {
    val projectVersion = version.toString()
    doLast {
        println(projectVersion)
    }
}

tasks.register("printHyperApiVersion") {
    val hyperVersion = project.findProperty("hyperApiVersion")?.toString() ?: "unknown"
    doLast {
        println(hyperVersion)
    }
}

subprojects {
    plugins.withId("java-conventions") {
        tasks.withType<Test>().configureEach {
            dependsOn(rootProject.tasks.named("extractHyper"))
        }
    }
}

moduleGraphConfig {
    readmePath.set("${rootDir}/DEVELOPMENT.md")
    heading.set("## Module Graph")
    rootModulesRegex.set(":jdbc")
}
