plugins {
    id("java")
    application
}

dependencies {
    testImplementation(project(":test-api"))
}

repositories {
    mavenCentral()
}
