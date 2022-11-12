import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    id("org.jetbrains.dokka") version "1.6.10"
    id("java-library")
}

repositories {
    mavenCentral()
}

val kotlinVersion = "1.7.20"

dependencies {
    implementation("org.slf4j:slf4j-api:2.0.3")
    implementation("ch.qos.logback:logback-classic:1.4.4")
    implementation("com.typesafe:config:1.4.2")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    implementation("com.codahale.metrics:metrics-core:3.0.2")
    testImplementation(kotlin("test"))
    testImplementation("com.h2database:h2:2.1.214")
}

tasks.test {
    useTestNG()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "16"
}
