plugins {
    id("java")
    application
}

dependencies {
    testImplementation(project(":test-api"))

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.12.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.12.1")
}

repositories {
    mavenCentral()
}
