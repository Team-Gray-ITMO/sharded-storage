plugins {
    id("java")
    application
}

dependencies {
    testImplementation(projects.testApi)
}

tasks.test {
    useJUnitPlatform()
}

repositories {
    mavenCentral()
}
