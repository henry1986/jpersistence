buildscript {
    repositories {
        mavenCentral()
        maven { url = uri("https://repo.gradle.org/gradle/libs-releases") }
    }
    dependencies {
        classpath("org.daiv.dependency:DependencyHandling:0.1.41")
    }
}

plugins {
    kotlin("multiplatform") version "1.6.10"
    id("org.daiv.dependency.VersionsPlugin") version "0.1.4"
    id("signing")
    `maven-publish`
}

val versions = org.daiv.dependency.DefaultDependencyBuilder(org.daiv.dependency.Versions.current())

group = "org.daiv.jpersistence"
version = versions.setVersion { jpersistence }

repositories {
    mavenCentral()
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
                implementation(versions.kutil())
                implementation(versions.coroutines())
            }
            //            versions.deps(this){
//                kutil()
//                coroutines()
//            }
        }
        val commonTest by getting {
            dependencies {
                implementation(kotlin("test-common"))
                implementation(kotlin("test-annotations-common"))
            }
        }
        val jvmMain by getting {
            dependencies {
                implementation(kotlin("reflect"))
//                implementation(versions.postgres_jdbc())
                implementation(versions.sqlite_jdbc())
            }
        }
        val jvmTest by getting {
            dependencies {
                implementation(kotlin("test-junit"))
                implementation(versions.mockk())
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

val javadocJar by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
}

signing {
    sign(publishing.publications)
}

publishing {
    publications.withType<MavenPublication> {
        artifact(javadocJar.get())
        pom {
            packaging = "jar"
            name.set("jpersistence")
            description.set("a object orientated mapping framework for kotlin data classes")
            url.set("https://github.com/henry1986/jpersistence")
            licenses {
                license {
                    name.set("The Apache Software License, Version 2.0")
                    url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
            issueManagement {
                system.set("Github")
                url.set("https://github.com/henry1986/jpersistence/issues")
            }
            scm {
                connection.set("scm:git:https://github.com/henry1986/jpersistence.git")
                developerConnection.set("scm:git:https://github.com/henry1986/jpersistence.git")
                url.set("https://github.com/henry1986/kutil")
            }
            developers {
                developer {
                    id.set("henry86")
                    name.set("Martin Heinrich")
                    email.set("martin.heinrich.dresden@gmx.de")
                }
            }
        }
    }
    repositories {
        maven {
            name = "sonatypeRepository"
            val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
            credentials(PasswordCredentials::class)
        }
    }
}

versionPlugin {
    versionPluginBuilder = org.daiv.dependency.Versions.versionPluginBuilder {
        versionMember = { jpersistence }
        resetVersion = { copy(jpersistence = it) }
        publishTaskName = "publish"
    }
    setDepending(tasks, "publish")
}
