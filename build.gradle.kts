plugins {
    id("org.springframework.boot") version "4.1.0-RC1" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

allprojects {
    group = "com.trading.orderreliability"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
        maven("https://repo.spring.io/milestone")
    }
}

subprojects {
    if (!buildFile.exists()) {
        return@subprojects
    }

    apply(plugin = "java")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    plugins.withId("org.springframework.boot") {
        val mockitoAgent = configurations.create("mockitoAgent") {
            isCanBeConsumed = false
            isCanBeResolved = true
        }
        dependencies.add("mockitoAgent", "org.mockito:mockito-core") {
            isTransitive = false
        }

        tasks.withType<Test>().configureEach {
            doFirst {
                jvmArgs(
                    "-javaagent:${mockitoAgent.singleFile.absolutePath}",
                    "-Xshare:off",
                )
            }
        }
    }
}
