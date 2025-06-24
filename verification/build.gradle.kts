plugins {
  id("base-conventions")
  id("version-conventions")
  id("dev.adamko.dev-publish")
}

dependencies {
  devPublication(project(":jdbc"))
  devPublication(project(":jdbc-core"))
  devPublication(project(":jdbc-proto"))
  devPublication(project(":jdbc-grpc"))
  devPublication(project(":jdbc-http"))
  devPublication(project(":jdbc-util"))
}

tasks.named("updateDevRepo") {
  dependsOn(":jdbc:publishAllToDevRepo")
  dependsOn(":jdbc-core:publishAllToDevRepo") 
  dependsOn(":jdbc-proto:publishAllToDevRepo")
  dependsOn(":jdbc-grpc:publishAllToDevRepo")
  dependsOn(":jdbc-http:publishAllToDevRepo")
  dependsOn(":jdbc-util:publishAllToDevRepo")
}

tasks.named("check") {
  dependsOn(tasks.updateDevRepo)

  group = "verification"
  description = "Verifies that all subprojects produce required JAR artifacts"

  val expectedVersion = provider { "$version" }

  val repo = devPublish.devMavenRepo.file("com/salesforce/datacloud/").get().asFile

  doLast {
    val expectedPublications = setOf("jdbc", "jdbc-grpc", "jdbc-proto", "jdbc-core", "jdbc-util", "jdbc-http")
    val shaded = setOf("jdbc")

    val resolvedVersion = expectedVersion.get()

    fun hasJar(artifactId: String, version: String, classifier: String? = null): Boolean {
      val versionDir = repo.resolve("${artifactId}/${version}")
      
      if (!versionDir.exists()) {
        logger.lifecycle("Directory does not exist: ${versionDir.absolutePath}")
        return false
      }
      
      val classifierPart = classifier?.let { "-$it" } ?: ""
      
      // Check for exact version match (non-snapshot)
      val exactFile = versionDir.resolve("${artifactId}-${version}${classifierPart}.jar")
      if (exactFile.exists()) {
        logger.lifecycle("Found exact version match: ${exactFile.name}")
        return true
      }
      
      // Check for snapshot version with timestamp (if it's a snapshot)
      if (version.endsWith("-SNAPSHOT")) {
        // Look for timestamp pattern like: artifactId-version-timestamp-number-classifier.jar
        val snapshotFiles = versionDir.listFiles { file -> 
          file.isFile && 
          file.name.matches("${artifactId}-.*${classifierPart}\\.jar".toRegex()) && 
          !file.name.endsWith("-SNAPSHOT${classifierPart}.jar")
        }
        
        if (snapshotFiles?.isNotEmpty() == true) {
          logger.lifecycle("Found snapshot version: ${snapshotFiles.first().name}")
          return true
        }
      }
      
      logger.lifecycle("No matching JAR found for ${artifactId}-${version}${classifierPart}.jar")
      return false
    }

    val failures = mutableListOf<String>()
    val verified = mutableListOf<String>()

    expectedPublications.forEach { artifactId ->
      logger.lifecycle("Verifying artifacts for publication $artifactId:$resolvedVersion...")

      if (!hasJar(artifactId, resolvedVersion)) {
        failures.add("Publication '$artifactId' is missing main JAR")
      }

      if (!hasJar(artifactId, resolvedVersion, "sources")) {
        failures.add("Publication '$artifactId' is missing sources JAR")
      }

      if (!hasJar(artifactId, resolvedVersion, "javadoc")) {
        failures.add("Publication '$artifactId' is missing javadoc JAR")
      }

      if (shaded.contains(artifactId) && !hasJar(artifactId, resolvedVersion, "original")) {
        failures.add("Publication '$artifactId' is missing original JAR which is required since it's shaded")
      }

      if (shaded.contains(artifactId) && !hasJar(artifactId, resolvedVersion, "shaded")) {
        failures.add("Publication '$artifactId' is missing shaded JAR which is required since it's shaded")
      }

      verified.add(artifactId)
      logger.lifecycle("  Checked publication: $artifactId version $resolvedVersion")
    }

    if (failures.isNotEmpty()) {
      failures.forEach { logger.error(it) }
      throw GradleException("Artifact verification failed. Some projects are missing required JAR files:\n" + failures.joinToString("\n"))
    } else if (!verified.containsAll(expectedPublications)) {
      val missing = expectedPublications - verified.toSet()
      throw GradleException("Missing expected publication(s): $missing")
    } else {
      logger.lifecycle("All subprojects successfully generated required JAR artifacts.")
    }
  }
}
