plugins {
    id("java")
    application
}

dependencies {
    testImplementation(projects.testApi)

    testImplementation(libs.assertj.core)
    testImplementation(libs.mockito)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}

repositories {
    mavenCentral()
}
