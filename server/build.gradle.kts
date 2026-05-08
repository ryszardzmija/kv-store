plugins {
    application
}

dependencies {
    implementation(project(":storage"))
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.21.1")
}

application {
    mainClass.set("com.ryszardzmija.shaledb.server.Application")
}

tasks.named<JavaExec>("run") {
    workingDir = rootProject.projectDir
}
