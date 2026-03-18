plugins {
    application
    id("com.google.protobuf") version "0.9.6"
}

dependencies {
    implementation(project(":storage"))

    implementation("io.grpc:grpc-netty-shaded:1.79.0")
    implementation("io.grpc:grpc-protobuf:1.79.0")
    implementation("io.grpc:grpc-stub:1.79.0")
    implementation("com.google.protobuf:protobuf-java:4.34.0")
}

application {
    mainClass.set("com.ryszardzmija.shaledb.server.Application")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.34.0"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.79.0"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}