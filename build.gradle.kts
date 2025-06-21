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

    allprojects {
        ext {
            set(
                "coverageIncludes", listOf(
                    "**/*service*/**",
                    "**/service/**",
                    "**/*Service*/**",
                    "**/*Service*"
                )
            )
            set(
                "coverageExcludes", listOf(
                    "**/*Test*",
                    "**/config/**",
                    "**/exception/**",
                    "**/dto/**",
                    "**/proto/**",
                    "**/model/**",
                    "**/*Application*"
                )
            )
            set("coverageMinimum", "0.80".toBigDecimal())
        }
    }
}

subprojects {
    java {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }
}
