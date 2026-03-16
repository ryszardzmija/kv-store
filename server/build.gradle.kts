plugins {
    application
}

dependencies {
    implementation(project(":storage"))
}

application {
    mainClass.set("com.ryszardzmija.shaledb.server.Application")
}