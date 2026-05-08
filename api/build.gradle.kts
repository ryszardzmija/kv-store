import com.google.protobuf.gradle.id

plugins {
    `java-library`
    id("com.google.protobuf") version "0.10.0"
}

val grpcVersion = "1.81.0"
val protobufVersion = "4.34.1"

dependencies {
    api(platform("io.grpc:grpc-bom:$grpcVersion"))

    api("com.google.protobuf:protobuf-java:${protobufVersion}")
    api("io.grpc:grpc-protobuf")
    api("io.grpc:grpc-stub")

    compileOnly("org.apache.tomcat:annotations-api:6.0.53")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }

    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }

    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}
