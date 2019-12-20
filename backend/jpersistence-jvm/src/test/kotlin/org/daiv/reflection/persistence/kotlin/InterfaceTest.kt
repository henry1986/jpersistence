package org.daiv.reflection.persistence.kotlin

import mu.KotlinLogging
import org.daiv.reflection.annotations.Including
import org.daiv.reflection.annotations.IFaceForObject
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class InterfaceTest :
        Spek({
                 data class SimpleClass(override val x: Int, val y: String) : TheInterface
                 data class AnotherClass(override val x: Int, val z: Int, val y: String) : TheInterface
                 data class ComplexObj(val x: Int, @IFaceForObject([SimpleClass::class, AnotherClass::class]) val interf: TheInterface)
                 @Including
                 data class AnotherClass2(override val x: Int, val z: Int, val y: String) : TheInterface

                 data class ComplexObj2(val x: Int, @IFaceForObject([SimpleClass::class, AnotherClass2::class]) val interf: TheInterface)
                 describe("InterfaceTest") {
                     val logger = KotlinLogging.logger {}
                     val persister = Persister("InterfaceTest.db")
                     on("test theInterface") {
                         it("test persist onekey objects") {
                             val table = persister.Table(ComplexObj::class)
                             table.persist()
                             val c = listOf(ComplexObj(5, SimpleClass(3, "Hello")), ComplexObj(6, AnotherClass(2, 6, "Hello")))
                             table.insert(c)
                             val read = table.readAll()
                             assertEquals(c, read)
                         }

                         it("test persist twokey objects") {
                             val table = persister.Table(ComplexObj2::class)
                             table.persist()
                             val c = listOf(ComplexObj2(5, SimpleClass(3, "Hello")),
                                            ComplexObj2(6, AnotherClass2(2, 6, "Wow")),
                                            ComplexObj2(7, AnotherClass2(2, 6, "Wow")))
                             table.insert(c)
                             val read = table.readAll()
                             assertEquals(c, read)
                         }
                     }
                     afterGroup { persister.delete() }
                 }
             })