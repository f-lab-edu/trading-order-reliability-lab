import org.gradle.api.tasks.SourceSetContainer

plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

val orderServiceMainClasses = project(":apps:order-service")
    .extensions
    .getByType<SourceSetContainer>()["main"]
    .output
    .classesDirs

dependencies {
    implementation(project(":libs:common-id"))
    implementation(project(":libs:common-messaging"))
    implementation(project(":libs:common-observability"))
    implementation(project(":libs:broker-protocol"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.flywaydb:flyway-mysql")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("io.netty:netty-all")
    runtimeOnly("com.mysql:mysql-connector-j")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation(orderServiceMainClasses)
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.21.4"))
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:mysql")
}
