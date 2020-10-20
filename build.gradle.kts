buildscript {
    repositories {
        gradlePluginPortal()
        maven("https://daiv.org/artifactory/gradle-dev-local")
    }
    dependencies {
        classpath("org.daiv.dependency:DependencyHandling:0.0.12")
    }
}

plugins {
    kotlin("multiplatform") version "1.4.10"
    id("com.jfrog.artifactory") version "4.17.2"
    `maven-publish`
}

val versions = org.daiv.dependency.Versions.versions1_4_0

group = "org.daiv.jpersistence"
version = "0.9.1"

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
            versions.deps(this){
                kutil()
                coroutines()
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(kotlin("test-common"))
                implementation(kotlin("test-annotations-common"))
            }
        }
        val jvmMain by getting {
            versions.deps(this){
                kotlinModule("reflect")
                postgres_jdbc()
                sqlite_jdbc()
            }
        }
        val jvmTest by getting {
            versions.deps(this){
                kotlinModule("test-junit")
                mockk()
            }
        }
        val jsMain by getting
        val jsTest by getting {
            dependencies {
                implementation(kotlin("test-js"))
            }
        }
    }
}

artifactory {
    setContextUrl("${project.findProperty("daiv_contextUrl")}")
    publish(delegateClosureOf<org.jfrog.gradle.plugin.artifactory.dsl.PublisherConfig> {
        repository(delegateClosureOf<groovy.lang.GroovyObject> {
            setProperty("repoKey", "gradle-dev-local")
            setProperty("username", project.findProperty("daiv_user"))
            setProperty("password", project.findProperty("daiv_password"))
            setProperty("maven", true)
        })
        defaults(delegateClosureOf<groovy.lang.GroovyObject> {
            invokeMethod("publications", arrayOf("jvm", "js", "kotlinMultiplatform", "metadata", "linuxX64"))
            setProperty("publishPom", true)
            setProperty("publishArtifacts", true)
        })
    })
}
