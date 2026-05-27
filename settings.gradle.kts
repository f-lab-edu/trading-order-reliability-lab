pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven("https://repo.spring.io/milestone")
    }
}

rootProject.name = "trading-order-reliability-lab"

include(
    "apps:order-service",
    "apps:broker-gateway-service",
    "apps:recovery-service",
    "apps:broker-simulator",
    "libs:common-id",
    "libs:common-messaging",
    "libs:common-observability",
    "libs:broker-protocol",
    "libs:test-support",
)
