plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":libs:common-id"))
    implementation(project(":libs:common-messaging"))
    implementation(project(":libs:common-observability"))
    implementation(project(":libs:broker-protocol"))

    implementation("org.springframework.boot:spring-boot-starter-web")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
