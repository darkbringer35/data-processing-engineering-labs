plugins {
    kotlin("jvm") version "2.1.0"
}

group = "lab.kafka"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.9.0")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.2")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.18.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.2")

    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<JavaExec>("runProducer") {
    group = "application"
    description = "Run Kafka producer"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("lab.kafka.UserEventProducerKt")
}

tasks.register<JavaExec>("runConsumer") {
    group = "application"
    description = "Run Kafka consumer"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("lab.kafka.UserEventConsumerKt")
}