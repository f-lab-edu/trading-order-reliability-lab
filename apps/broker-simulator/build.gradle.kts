plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":libs:common-id"))
    implementation(project(":libs:broker-protocol"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("io.netty:netty-all")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
