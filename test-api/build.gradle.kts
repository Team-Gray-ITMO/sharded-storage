plugins {
    id("java")
    `java-library`
    application
}

private val mainClassName = "vk.itmo.teamgray.sharded.storage.test.api.TestApiApplication"

application {
    mainClass.set(mainClassName)
}

dependencies {
    api(projects.client)

    implementation(libs.assertj.core)
    implementation(libs.mockito)
    implementation(libs.junit.jupiter.api)
    runtimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}

repositories {
    mavenCentral()
}
