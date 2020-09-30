val coroutines_version = "1.3.9"
val kutil_version = "0.3.0"
val kotlin_version = "1.4.10"


plugins {
    kotlin("multiplatform") version "1.4.10"
    id("com.jfrog.artifactory") version "4.17.2"
    `maven-publish`
}
group = "org.daiv.jpersistence"
version = "0.9.0"

repositories {
    mavenCentral()
    maven("https://daiv.org/artifactory/gradle-dev-local")
}

kotlin {
    jvm {
        compilations.all {
            kotlinOptions.jvmTarget = "1.8"
        }
    }
    js {
        browser {
            testTask {
                useKarma {
                    useChromeHeadless()
                    webpackConfig.cssSupport.enabled = true
                }
            }
        }
    }
//    val hostOs = System.getProperty("os.name")
//    val isMingwX64 = hostOs.startsWith("Windows")
//    val nativeTarget = when {
//        hostOs == "Mac OS X" -> macosX64("native")
//        hostOs == "Linux" -> linuxX64("native")
//        isMingwX64 -> mingwX64("native")
//        else -> throw GradleException("Host OS is not supported in Kotlin/Native.")
//    }


    sourceSets {
        val commonMain by getting {
            dependencies {
                api("org.daiv.util:kutil:$kutil_version")
                api("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutines_version")
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(kotlin("test-common"))
                implementation(kotlin("test-annotations-common"))
            }
        }
        val jvmMain by getting {
            dependencies {
                api("org.jetbrains.kotlin:kotlin-reflect:$kotlin_version")
                api("org.xerial:sqlite-jdbc:3.27.2.1")

            }
        }
        val jvmTest by getting {
            dependencies {
                implementation(kotlin("test-junit"))
                api("io.mockk:mockk:1.9.2")
            }
        }
        val jsMain by getting
        val jsTest by getting {
            dependencies {
                implementation(kotlin("test-js"))
            }
        }
//        val nativeMain by getting
//        val nativeTest by getting
    }
}
