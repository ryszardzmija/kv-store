import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.jvm.toolchain.JavaLanguageVersion

plugins {
    java
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

subprojects {
    apply(plugin = "java")

    group = "com.ryszardzmija.shaledb"
    version = "1.0-SNAPSHOT"

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    repositories {
        mavenCentral()
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    dependencies {
        testImplementation(platform("org.junit:junit-bom:5.10.0"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")

        testImplementation("org.assertj:assertj-core:3.27.7")
        testImplementation("org.mockito:mockito-core:5.23.0")
        testImplementation("org.mockito:mockito-junit-jupiter:5.23.0")

        implementation("ch.qos.logback:logback-classic:1.5.32")
    }
}
