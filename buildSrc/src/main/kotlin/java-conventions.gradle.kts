plugins {
    id("base-conventions")
    `java-library`
//    id("com.diffplug.spotless")
}

repositories {
    mavenLocal()
    maven {
        url = uri("https://repo.maven.apache.org/maven2/")
    }
}

group = "com.salesforce.datacloud"
version = "0.25.7-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withJavadocJar()
    withSourcesJar()
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(8)
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
        events("skipped", "failed")
        showExceptions = true
        showStandardStreams = true
        showStackTraces = true
    }

    jvmArgs("-Xmx2g", "-Xms512m")
}

//spotless {
//    ratchetFrom("origin/main")
//
//    java {
//        palantirJavaFormat()
//        licenseHeaderFile(layout.projectDirectory.file("../license-header.txt"))
//    }
//}
