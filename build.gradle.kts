plugins {
    kotlin("jvm") version "2.2.0"
}

group = "aab"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.postgresql:postgresql:42.7.10")
    implementation("org.jooq:jooq:3.20.11")
    implementation("net.datafaker:datafaker:2.5.4")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}