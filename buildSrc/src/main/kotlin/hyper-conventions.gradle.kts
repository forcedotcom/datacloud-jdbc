plugins {
    id("de.undercouch.download")
    id("com.google.osdetector")
}

val hyperApiVersion: String by project
val hyperZipPath = ".hyper/hyper-$hyperApiVersion.zip"
val hyperDir = ".hyperd"

tasks.register<de.undercouch.gradle.tasks.download.Download>("downloadHyper") {
    group = "hyper"
    val urlBase = when (osdetector.os) {
        "windows" -> "https://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-windows-x86_64-release-main"
        "osx" -> "https://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-macos-arm64-release-main"
        "linux" -> "https://downloads.tableau.com/tssoftware/tableauhyperapi-cxx-linux-x86_64-release-main"
        else -> throw GradleException("Unsupported os settings. os=${osdetector.os}, arch=${osdetector.arch}, release=${osdetector.release}, classifier=${osdetector.classifier}}")
    }

    val url = "$urlBase.$hyperApiVersion.zip"

    src(url)
    dest(project.layout.projectDirectory.file(hyperZipPath))
    overwrite(false)

    inputs.property("hyperApiVersion", hyperApiVersion)
    inputs.property("osdetector.os", osdetector.os)
    outputs.file(project.layout.projectDirectory.file(hyperZipPath))
}

tasks.register<Copy>("extractHyper") {
    dependsOn("downloadHyper")

    val os = "os=${osdetector.os}, arch=${osdetector.arch}, release=${osdetector.release}, classifier=${osdetector.classifier}"

    group = "hyper"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    includeEmptyDirs = false

    from(zipTree(project.layout.projectDirectory.file(hyperZipPath))) {
        when(osdetector.os) {
            "windows" -> include("**/bin/**/*.dll", "**/bin/hyper/hyperd.exe")
            "osx" -> include("**/lib/**/*.dylib", "**/lib/hyper/hyperd")
            "linux" -> include("**/lib/**/*.so", "**/lib/hyper/hyperd")
            else -> throw GradleException("Unsupported os settings. $os")
        }
    }

    eachFile {
        relativePath = RelativePath(true, name)
    }

    into(project.layout.projectDirectory.dir(hyperDir))

    filePermissions {
        unix("rwx------")
    }

    inputs.file(project.layout.projectDirectory.file(hyperZipPath))

    val hyperdDir = project.layout.projectDirectory.dir(hyperDir).asFileTree

    val exe = hyperdDir.firstOrNull { it.name.contains("hyperd") }
        ?: throw GradleException("zip missing hyperd executable. $os, files=[${hyperdDir.map { it.absolutePath }}]")

    val lib = hyperdDir.firstOrNull { it.name.contains("tableauhyperapi") }
        ?: throw GradleException("zip missing hyperd library, $os, files=[${hyperdDir.map { it.absolutePath }}]")

    outputs.files(exe, lib)

    doLast {
        if (!exe.exists() || !lib.exists()) {
            throw GradleException("extractHyper failed validation. hyperd=${exe.exists()}, lib=${lib.exists()}, $os")
        }
    }
}

tasks.register<Exec>("hyperd") {
    dependsOn("extractHyper")
    group = "hyper"

    val name = when (osdetector.os) {
        "windows" -> "hyperd.exe"
        else -> "hyperd"
    }

    val executable = project.layout.projectDirectory.dir(hyperDir).file(name).asFile.absolutePath
    val config = project.project(":jdbc-core").file("src/test/resources/hyper.yaml")

    commandLine(executable)
    args("--config", config.absolutePath, "run")

    inputs.files(executable, config)
    inputs.property("os", osdetector.os)
}
