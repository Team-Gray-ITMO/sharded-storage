plugins {
    id("java")
    application
}

group = "vk.itmo.teamgray.sharded.storage"
version = "1.0-SNAPSHOT"

allprojects {
    apply(plugin = "java")
    apply(plugin = "application")

    repositories {
        mavenCentral()
    }
}

subprojects {
    java {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }
}
