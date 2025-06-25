plugins {
    id("hyper-conventions")
    id("base-conventions")
    id("com.diffplug.spotless")
    id("dev.iurysouza.modulegraph") version "0.12.0"
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

val flattenJarsTask = tasks.register("flattenJars") {
    description = "Collects all JARs required for the JDBC driver into a single directory"
    group = "build"
    
    doLast {
        val outputDir = layout.buildDirectory.dir("libs/all-jars").get().asFile
        outputDir.mkdirs()
        
        // Find all subprojects that have publishing-conventions plugin
        val publishingProjects = subprojects.filter { subproject ->
            subproject.plugins.hasPlugin("publishing-conventions")
        }
        
        logger.lifecycle("Found ${publishingProjects.size} projects with publishing-conventions:")
        publishingProjects.forEach { proj ->
            logger.lifecycle("  - ${proj.name}")
        }
        
        publishingProjects.forEach { proj ->
            proj.tasks.findByName("jar")?.outputs?.files?.forEach { file ->
                if (file.isFile && file.extension == "jar") {
                    file.copyTo(outputDir.resolve(file.name), overwrite = true)
                    logger.lifecycle("Copied project JAR: ${file.name}")
                }
            }
        }
        
        val processedJars = mutableSetOf<String>()
        publishingProjects.forEach { proj ->
            proj.configurations.findByName("runtimeClasspath")?.resolvedConfiguration?.resolvedArtifacts?.forEach { artifact ->
                val file = artifact.file
                if (file.extension == "jar" && processedJars.add(file.name)) {
                    file.copyTo(outputDir.resolve(file.name), overwrite = true)
                    logger.lifecycle("Copied dependency: ${file.name}")
                }
            }
        }
        
        logger.lifecycle("All JARs have been collected in ${outputDir.absolutePath}")
    }
}

// Configure task dependencies after all projects are evaluated
afterEvaluate {
    val publishingProjects = subprojects.filter { subproject ->
        subproject.plugins.hasPlugin("publishing-conventions")
    }
    
    publishingProjects.forEach { proj ->
        flattenJarsTask.configure {
            dependsOn("${proj.path}:jar")
        }
    }
}
