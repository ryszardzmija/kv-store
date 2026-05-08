plugins {
    application
}

val grpcVersion = "1.81.0"

dependencies {
    implementation(project(":storage"))
    implementation(project(":api"))
    implementation(platform("io.grpc:grpc-bom:$grpcVersion"))
    implementation("io.grpc:grpc-netty-shaded")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.21.1")
}

application {
    mainClass.set("com.ryszardzmija.shaledb.server.Application")
}

tasks.named<JavaExec>("run") {
    workingDir = rootProject.projectDir
}
