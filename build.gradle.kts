plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.gatling)
}

group = "org.radarbase"
version = properties["projectVersion"] as String

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(libs.kotlin.test)
    gatlingImplementation(libs.avro)
    gatlingImplementation(libs.radarSchemas)
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}
