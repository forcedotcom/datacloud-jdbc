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

spotless {    
    scala {
        scalafmt()
    }
}