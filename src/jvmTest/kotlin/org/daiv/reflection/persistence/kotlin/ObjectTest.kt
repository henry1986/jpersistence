package org.daiv.reflection.persistence.kotlin

import mu.KotlinLogging
import org.daiv.reflection.annotations.IFaceForObject
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.persister.Persister
import kotlin.test.assertEquals

//class ObjectTest :
//        Spek({
//                 data class MyObjectTest(val x: Int, @IFaceForObject([RunXImpl::class, TestSingleton::class]) val runX: RunX)
//
//                 @MoreKeys(2)
//                 data class MyObjectKey(val x: Int, @IFaceForObject([RunXImpl::class, TestSingleton::class]) val runX: RunX)
//
//                 @MoreKeys(auto = true)
//                 data class MyObject2Test(val myObjectTest: MyObjectKey, val s: String)
//
//                 describe("Object test") {
//                     val logger = KotlinLogging.logger {}
//                     val persister = Persister("ObjectTest.db")
//                     on("do test") {
//                         val table = persister.Table(MyObjectTest::class)
//                         table.persist()
//                         val m1 = MyObjectTest(5, TestSingleton)
//                         val m2 = MyObjectTest(6, RunXImpl(5))
//                         val list = listOf(m1, m2)
//                         table.insert(list)
//                         persister.clearCache()
//                         it("test readAll") {
//                             val read = table.readAll()
//                             persister.clearCache()
//                             assertEquals(list, read)
//                         }
//                         it("test single read") {
//                             val read = table.read(5)
//                             persister.clearCache()
//                             assertEquals(m1, read)
//                         }
//                     }
//                     on("do test with hashCode") {
//                         val table = persister.Table(MyObject2Test::class)
//                         table.persist()
//                         val m1 = MyObject2Test(MyObjectKey(5, TestSingleton), "Hello")
//                         val m2 = MyObject2Test(MyObjectKey(6, RunXImpl(5)), "Hello")
//                         val list = listOf(m1, m2)
//                         table.insert(list)
//                         persister.clearCache()
//                         it("test readAll") {
//                             persister.clearCache()
//                             val read = table.readAll()
//                             assertEquals(list.toSet(), read.toSet())
//                         }
//                         it("test single read") {
//                             persister.clearCache()
//                             val read = table.read(MyObjectKey(5, TestSingleton))
//                             assertEquals(m1, read)
//                         }
//                     }
//                     afterGroup { persister.delete() }
//                 }
//             })