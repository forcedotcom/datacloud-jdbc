plugins {
    id("scala")
    id("scala-conventions")
    id("publishing-conventions")
    id("shading")
}

description = "Spark Datasource for Salesforce Data Cloud JDBC"
extra.set("mavenName", "Spark Datasource for Salesforce Data Cloud JDBC")
extra.set("mavenDescription", project.description.toString())

dependencies {
    implementation(project(":spark-datasource-core"))
    implementation(project(":jdbc"))
}

// Configure shading using DSL
shading {
    spark()
}
