package org.daiv.reflection.persistence.kotlin

import mu.KotlinLogging
import org.daiv.reflection.annotations.IFaceForObject
import org.daiv.reflection.annotations.Including
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.persister.Persister
import org.daiv.util.json.log
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
                 data class ComplexObj3(@IFaceForObject([SimpleClass::class, AnotherClass2::class]) val interf: TheInterface, val x: Int)
                 data class EmbeddedComplexObj(val x: Int, val complexObj2: ComplexObj2)
                 data class EmbeddedComplexObj2(val complexObj2: ComplexObj2, val x: Int)

                 //                 data class ListClass(val x: Int, @IFaceForList([IFaceForObject([SimpleClass::class, AnotherClass2::class],
//                                                                                1)]) val interf: List<TheInterface>)
                 data class ListClass(val x: Int, @IFaceForObject([SimpleClass::class, AnotherClass2::class]) val interf: List<TheInterface>)

                 @MoreKeys(1, true)
                 data class ListAutoClass(@IFaceForObject([SimpleClass::class, AnotherClass2::class]) val interf: List<TheInterface>)

                 @MoreKeys(auto = true)
                 data class ObjectHashCodeTest(@IFaceForObject([RunXImpl::class, TestSingleton::class]) val key: RunX, val z: String)

                 data class ObjectKeyTest(@IFaceForObject([TestSingleton::class, RunXImpl::class]) val key: RunX, val z: String)

                 data class ObjectHolder(val objectKeyTest: ObjectKeyTest, val value: ObjectKeyTest?, val string: String)

                 @MoreKeys(2)
                 data class ObjectHolder2(val x: Long, val objectKeyTest: ObjectKeyTest, val value: ObjectKeyTest?, val string: String)

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
                         it("test persist list object") {
                             val table = persister.Table(ListClass::class)
                             table.persist()
                             val c = listOf(ListClass(5, listOf(SimpleClass(3, "Hello"), AnotherClass2(2, 6, "Wow"))),
                                            ListClass(6, listOf(AnotherClass2(3, 6, "Wow"))),
                                            ListClass(7, listOf(AnotherClass2(2, 6, "Wow"))))
                             table.insert(c)
                             val read = table.readAll()
                             assertEquals(c, read)
                         }
                         it("test persist auto list object") {
                             val table = persister.Table(ListAutoClass::class)
                             table.persist()
                             val c = listOf(ListAutoClass(listOf(SimpleClass(3, "Hello"))),
                                            ListAutoClass(listOf(AnotherClass2(3, 9, "Wow"))),
                                            ListAutoClass(listOf(AnotherClass2(2, 10, "Wow"))))
                             table.insert(c)
                             val read = table.readAll()
                             read.log(logger)
                             assertEquals(c.toSet(), read.toSet())
                         }
                     }
                     on("test single read") {
                         it("test persist twokey objects") {
                             val table = persister.Table(ComplexObj2::class)
                             table.persist()
                             table.clear()
                             val co1 = ComplexObj2(5, SimpleClass(3, "Hello"))
                             val c = listOf(co1,
                                            ComplexObj2(6, AnotherClass2(2, 6, "Wow")),
                                            ComplexObj2(7, AnotherClass2(2, 6, "Wow")))
                             table.insert(c)
                             assertEquals(co1, table.read(5))
                         }
                         it("test persist embedded twokey objects") {
                             val table = persister.Table(EmbeddedComplexObj::class)
                             table.persist()
                             table.clear()
                             val co1 = EmbeddedComplexObj(1, ComplexObj2(5, SimpleClass(3, "Hello")))
                             val co2 = EmbeddedComplexObj(2, ComplexObj2(6, AnotherClass2(2, 6, "Wow")))
                             val co3 = EmbeddedComplexObj(3, ComplexObj2(7, AnotherClass2(2, 6, "Wow")))
                             val c = listOf(co1, co2, co3)
                             table.insert(c)
                             assertEquals(co1, table.read(1))
                             assertEquals(co2, table.read(2))
                             assertEquals(co3, table.read(3))
                         }
                         it("test persist embedded twokey objects 2") {
                             val table = persister.Table(EmbeddedComplexObj2::class)
                             table.persist()
                             table.clear()
                             val cx1 = ComplexObj2(5, SimpleClass(3, "Hello"))
                             val cx2 = ComplexObj2(6, AnotherClass2(2, 6, "Wow"))
                             val cx3 = ComplexObj2(7, AnotherClass2(2, 6, "Wow"))
                             val co1 = EmbeddedComplexObj2(cx1, 1)
                             val co2 = EmbeddedComplexObj2(cx2, 2)
                             val co3 = EmbeddedComplexObj2(cx3, 3)
                             val c = listOf(co1, co2, co3)
                             table.insert(c)
                             table.clearCache()
                             assertEquals(co1, table.read(cx1))
                             assertEquals(co2, table.read(cx2))
                             assertEquals(co3, table.read(cx3))
                         }
                         it("test interface as key") {
                             val table = persister.Table(ComplexObj3::class)
                             table.persist()
                             table.clear()
                             val s1 = SimpleClass(3, "Hello")
                             val s2 = AnotherClass2(2, 6, "Wow")
                             val s3 = AnotherClass2(9, 1, "Wow")
                             val cx1 = ComplexObj3(s1, 1)
                             val cx2 = ComplexObj3(s2, 2)
                             val cx3 = ComplexObj3(s3, 3)
                             val c = listOf(cx1, cx2, cx3)
                             table.insert(c)
                             table.clearCache()
                             assertEquals(cx1, table.read(s1))
                             assertEquals(cx2, table.read(s2))
                             assertEquals(cx3, table.read(s3))
                         }
                     }
                     on("test HashCode") {
                         val table = persister.Table(ObjectHashCodeTest::class)
                         table.persist()
                         val testImpl = RunXImpl(5)
                         val testImpl2 = RunXImpl(6)
                         val x1 = ObjectHashCodeTest(testImpl, "Hello")
                         val x2 = ObjectHashCodeTest(testImpl2, "Hello1")
                         val x3 = ObjectHashCodeTest(TestSingleton, "Hello2")
                         val list = listOf(x1, x2, x3)
                         it("test insert and read") {
                             table.insert(list)
                             persister.clearCache()
                             val read = table.readAll()
                             assertEquals(list.toSet(), read.toSet())
                         }
                     }
                     on("test as key") {
                         val table = persister.Table(ObjectKeyTest::class)
                         table.persist()
                         val testImpl = RunXImpl(5)
                         val testImpl2 = RunXImpl(6)
                         val x1 = ObjectKeyTest(testImpl, "Hello")
                         val x2 = ObjectKeyTest(testImpl2, "Hello1")
                         val x3 = ObjectKeyTest(TestSingleton, "Hello2")
                         val list = listOf(x1, x2, x3)
                         it("test insert and read") {
                             table.insert(list)
                             persister.clearCache()
                             val read = table.readAll()
                             assertEquals(list.toSet(), read.toSet())
                         }

                     }
                     on("test as included null") {
                         val table = persister.Table(ObjectHolder::class)
                         table.persist()
                         val testImpl = RunXImpl(5)
                         val testImpl2 = RunXImpl(6)
                         val x1 = ObjectKeyTest(testImpl, "Hello")
                         val x2 = ObjectKeyTest(testImpl2, "Hello1")
                         val x3 = ObjectKeyTest(TestSingleton, "Hello2")
                         val o1 = ObjectHolder(x1, x2, "t1")
                         val o2 = ObjectHolder(x3, null, "t2")
                         val o3 = ObjectHolder(x2, null, "t3")
                         val list = listOf(o1, o2, o3)
                         it("test insert and read") {
                             table.insert(list)
                             persister.clearCache()
                             val read = table.readAll()
                             assertEquals(list.toSet(), read.toSet())
                         }

                     }
                     on("test as multiple keys") {
                         val table = persister.Table(ObjectHolder2::class)
                         table.persist()
                         val testImpl = RunXImpl(5)
                         val testImpl2 = RunXImpl(6)
                         val x1 = ObjectKeyTest(testImpl, "Hello")
                         val x2 = ObjectKeyTest(testImpl2, "Hello1")
                         val x3 = ObjectKeyTest(TestSingleton, "Hello2")
                         val o1 = ObjectHolder2(1, x1, x2, "t1")
                         val o2 = ObjectHolder2(1, x3, null, "t2")
                         val o3 = ObjectHolder2(2, x2, null, "t3")
                         val list = listOf(o1, o2, o3)
                         table.insert(list)
                         it("test readAll") {
                             persister.clearCache()
                             val read = table.readAll()
                             assertEquals(list.toSet(), read.toSet())
                         }
                         it("test read") {
                             persister.clearCache()
                             val read = table.read(listOf(1L, x1))
                             assertEquals(o1, read)
                             val read2 = table.read(listOf(1L, x3))
                             assertEquals(o2, read2)
                         }

                     }
                     on("test as sealedHolder") {
                         val table = persister.Table(SealedHolder::class)
                         table.persist()
                         val testImpl = SealedHolder(5L, SealedObject)
                         val testImpl2 = SealedHolder(6L, SealedData(5L))
                         val list = listOf(testImpl, testImpl2)
                         it("test insert and read") {
                             table.insert(list)
                             persister.clearCache()
                             val read = table.readAll()
                             assertEquals(list.toSet(), read.toSet())
                         }

                     }
                     on("test as sealedKeyHolder") {
                         val table = persister.Table(SealedKeyHolder::class)
                         table.persist()
                         val testImpl = SealedKeyHolder(SealedObject, SealedObject, "")
                         val testImpl2 = SealedKeyHolder(SealedObject, SealedData(5L), "")
                         val r3 = SealedKeyHolder(SealedObject, SealedData(6L), "")
                         val list = listOf(testImpl, testImpl2, r3)
                         it("test insert and read") {
                             table.insert(list)
                             persister.clearCache()
                             val read = table.readAll()
                             assertEquals(list.toSet(), read.toSet())
                         }

                     }
                     on("test nullability of interfaceField") {
                         val table = persister.Table(SealedKeyHolder2::class)
                         table.persist()
                         val testImpl = SealedKeyHolder2(SealedObject, null, "wow")
                         val testImpl2 = SealedKeyHolder2(SealedData(5L), null, "hello")
                         val r3 = SealedKeyHolder2(SealedData(6L), SealedObject, "no")
                         val list = listOf(testImpl, testImpl2, r3)
                         it("test insert and read") {
                             table.insert(list)
                             persister.clearCache()
                             val read = table.readAll()
                             assertEquals(list.toSet(), read.toSet())
                         }

                     }
                     afterGroup { persister.delete() }
                 }
             })