rootProject.name = "sharded-storage"

include("node")
include("master")
include("client")
include("common")
include("discovery")
include("test-api")
include("integration-tests")
include("load-tests")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
