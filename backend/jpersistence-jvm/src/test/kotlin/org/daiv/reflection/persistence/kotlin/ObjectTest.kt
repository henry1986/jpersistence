package org.daiv.reflection.persistence.kotlin

import mu.KotlinLogging
import org.daiv.reflection.annotations.IFaceForObject
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class ObjectTest :
        Spek({
                 data class MyObjectTest(val x: Int, @IFaceForObject([RunXImpl::class, TestSingleton::class]) val runX: RunX)

                 describe("Object test") {
                     val logger = KotlinLogging.logger {}
                     val persister = Persister("ObjectTest.db")
                     on("do test") {
                         val table = persister.Table(MyObjectTest::class)
                         table.persist()
                         val m1 = MyObjectTest(5, TestSingleton)
                         val m2 = MyObjectTest(6, RunXImpl(5))
                         val list = listOf(m1, m2)
                         table.insert(list)
                         persister.clearCache()
                         it("test readAll") {
                             val read = table.readAll()
                             persister.clearCache()
                             assertEquals(list, read)
                         }
                         it("test single read") {
                             val read = table.read(5)
                             persister.clearCache()
                             assertEquals(m1, read)
                         }
                     }
                     afterGroup { persister.delete() }
                 }
             })