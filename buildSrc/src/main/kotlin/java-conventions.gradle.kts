plugins {
    id("base-conventions")
    `java-library`
    id("com.diffplug.spotless")
}

repositories {
    mavenLocal()
    maven {
        url = uri("https://repo.maven.apache.org/maven2/")
    }
}

group = "com.salesforce.datacloud"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withJavadocJar()
    withSourcesJar()
}

tasks.withType<JavaCompile> {
    javaCompiler = javaToolchains.compilerFor {
        languageVersion = JavaLanguageVersion.of(8)
    }
    options.encoding = "UTF-8"
    options.setIncremental(true)
    // This only works if we're on a newer toolchain, but java 8 is faster to build while we use lombok for "val"
    // options.release.set(8)
}

tasks.withType<Javadoc> {
    options.encoding = "UTF-8"
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:none", "-quiet")
        addBooleanOption("html5", true)
    }
}

tasks.withType<Test>().configureEach {

    javaLauncher = javaToolchains.launcherFor {
        languageVersion = JavaLanguageVersion.of(8)
    }

    useJUnitPlatform()

    testLogging {
        events("passed", "skipped", "failed")
        showExceptions = true
        showStandardStreams = true
        showStackTraces = true
    }

    jvmArgs("-Xmx2g", "-Xms512m")
}

spotless {
    ratchetFrom("origin/main")

    format("misc") {
        target(".gitattributes", ".gitignore")
        trimTrailingWhitespace()
        endWithNewline()
    }
    
    java {
        target("src/main/java/**/*.java", "src/test/java/**/*.java")
        palantirJavaFormat("2.62.0")
        formatAnnotations()
        importOrder()
        removeUnusedImports()
        licenseHeaderFile(rootProject.file("license-header.txt"))
    }
}
