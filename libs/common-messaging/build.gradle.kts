plugins {
    `java-library`
    id("io.spring.dependency-management")
}

dependencies {
    api(project(":libs:common-id"))

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}
