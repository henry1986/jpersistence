package org.daiv.reflection.persistence.kotlin

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import kotlinx.coroutines.runBlocking
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.common.TableHandlerCreator
import org.daiv.reflection.persister.InsertCachePreference
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.ReadKey
import org.daiv.reflection.plain.HashCodeKey
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.ObjectKeyToWrite
import org.daiv.reflection.plain.PersistenceKey
import org.daiv.reflection.read.KeyType
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.reflect.KClass
import kotlin.test.assertFailsWith
import kotlin.test.fail

internal class InsertMapTest :
        Spek({
                 data class FirstTestObject(val x: Int, val y: String)

                 @MoreKeys(1, true)
                 data class AutoIdTestObject(val x: Int, val y: String)

                 data class ComplexWithAutoId(val autoIdTestObject: AutoIdTestObject, val s: String)

                 class TestCalled {
                     fun testCalled() {
                     }
                 }

                 describe("insertMap") {
                     val persister = Persister("InsertMapTest.db")
                     val table = persister.Table(FirstTestObject::class)
                     val cTable = persister.Table(ComplexWithAutoId::class)
                     table.persist()
                     cTable.persist()
                     val readCache = persister.readCache()
                     val insertMap = InsertMap(persister,
                                               InsertCachePreference(true, true),
                                               readCache.tableHandlers(persister.persisterProviderForTest(null)!!),
                                               readCache)
                     val m = mockk<TestCalled>()
                     every { m.testCalled() } answers {}
                     on("test normal no auto id") {
                         val first = FirstTestObject(5, "Hello")
                         it("test called") {
                             runBlocking {
                                 insertMap.nextTask(table, ObjectKeyToWriteForTest(false, listOf(5), first)) {
                                     m.testCalled()
                                 }
                             }
                             verifyAll { m.testCalled() }
                         }
                         it("alreadyInserted") {
                             val second = FirstTestObject(6, "six")
                             runBlocking {
                                 readCache.readTableCache(table, false)[ReadKey(table, PersistenceKey(listOf(6)))] = second
                                 insertMap.nextTask(table, ObjectKeyToWriteForTest(false, listOf(6), second)) {
                                     fail("must not be called a second time, because it was already inserted")
                                 }
                             }
                         }
                         it("test exception for double object") {
                             readCache.readTableCache(table, false)[ReadKey(table, PersistenceKey(listOf(5)))] = first
                             val objectWithWrongKey = FirstTestObject(5, "Wow")
                             assertFailsWith<RuntimeException>("object that is already in Cache $first \n is not same as to be insert: $objectWithWrongKey") {
                                 runBlocking {
                                     insertMap.nextTask(table, ObjectKeyToWriteForTest(false, listOf(5), objectWithWrongKey)) {
                                     }
                                 }
                             }
                         }
                         runBlocking { }
                     }
                     on("test normal auto id") {
                         val firstKey = AutoIdTestObject(5, "Hello")
                         val first = ComplexWithAutoId(firstKey, "Now")
                         val secondKey = AutoIdTestObject(6, "six")
                         val second = ComplexWithAutoId(secondKey, "Now")
                         val objectWithWrongKey = ComplexWithAutoId(firstKey, "Wow")

                         it("test called") {
                             runBlocking {
                                 insertMap.nextTask(cTable, ObjectKeyToWriteForTest(true, listOf(firstKey), first, 5)) {
                                     m.testCalled()
                                 }
                             }
                             verifyAll { m.testCalled() }
                         }
                         it("alreadyInserted") {
                             runBlocking {
                                 readCache.readTableCache(cTable, true)[ReadKey(cTable, PersistenceKey(listOf(secondKey)))] = second
                                 insertMap.nextTask(cTable, ObjectKeyToWriteForTest(true, listOf(secondKey), second)) {
                                     fail("must not be called a second time, because it was already inserted")
                                 }
                             }
                         }
                         it("test exception for double object") {
                             readCache.readTableCache(cTable, true)[ReadKey(cTable, PersistenceKey(listOf(firstKey)))] = first
                             assertFailsWith<RuntimeException>("object that is already in Cache $first \n is not same as to be insert: $objectWithWrongKey") {
                                 runBlocking {
                                     insertMap.nextTask(cTable, ObjectKeyToWriteForTest(true, listOf(firstKey), objectWithWrongKey, 5)) {
                                     }
                                 }
                             }
                         }
                         runBlocking { }
                     }
                 }
             })