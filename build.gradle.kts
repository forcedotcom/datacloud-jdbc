plugins {
    id("hyper-conventions")
    id("base-conventions")
    id("com.diffplug.spotless")
}

subprojects {
    plugins.withId("java-conventions") {
        tasks.withType<Test>().configureEach {
            dependsOn(rootProject.tasks.named("extractHyper"))
        }
    }
}
