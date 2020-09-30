package org.daiv.reflection.persistence.kotlin

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import kotlinx.coroutines.runBlocking
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.common.TableHandlerCreator
import org.daiv.reflection.persister.*
import org.daiv.reflection.plain.HashCodeKey
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.ObjectKeyToWrite
import org.daiv.reflection.plain.PersistenceKey
import org.daiv.reflection.read.KeyType
import kotlin.reflect.KClass
import kotlin.test.*

data class FirstTestObject(val x: Int, val y: String)

@MoreKeys(1, true)
data class AutoIdTestObject(val x: Int, val y: String)

data class ComplexWithAutoId(val autoIdTestObject: AutoIdTestObject, val s: String)

class TestCalled {
    fun testCalled() {
    }
}

open class InsertMapTest {
    val persister = Persister("InsertMapTest.db")
    val table = persister.Table(FirstTestObject::class)
    val cTable = persister.Table(ComplexWithAutoId::class)

    init {
        table.persist()
        cTable.persist()
    }

    internal val readCache = persister.readCache()
    internal val insertMap = InsertMap(
        persister,
        InsertCachePreference(true, true),
        readCache.tableHandlers(persister.persisterProviderForTest(null)!!),
        readCache
    )
    val m = mockk<TestCalled>()

    init {
        every { m.testCalled() } answers {}
    }
    @AfterTest
    fun afterTest(){
        persister.delete()
    }
}

class TestNormalNoAutoId : InsertMapTest() {
    val first = FirstTestObject(5, "Hello")
    @Test
    fun testCalled() {
        runBlocking {
            insertMap.nextTask(table, ObjectKeyToWriteForTest(false, listOf(5), first)) {
                m.testCalled()
            }
        }
        verifyAll { m.testCalled() }
    }

    @Test
    fun alreadyInserted() {
        val second = FirstTestObject(6, "six")
        runBlocking {
            readCache.readTableCache(table, false)[ReadKey(table, PersistenceKey(listOf(6)))] = second
            insertMap.nextTask(table, ObjectKeyToWriteForTest(false, listOf(6), second)) {
                fail("must not be called a second time, because it was already inserted")
            }
        }
    }

    @Test
    fun testExceptionForDoubleObject() {
        readCache.readTableCache(table, false)[ReadKey(table, PersistenceKey(listOf(5)))] = first
        val objectWithWrongKey = FirstTestObject(5, "Wow")
        runBlocking {
            try {
                insertMap.nextTask(table, ObjectKeyToWriteForTest(false, listOf(5), objectWithWrongKey)) {
                }
                fail("exception should have been thrown")
            } catch (t: DifferentObjectSameKeyException) {
                assertEquals(first, t.objectFromCache)
                assertEquals(objectWithWrongKey, t.objectToInsert)
            }
        }
    }
}

class TestNormalAutoId : InsertMapTest() {
    val firstKey = AutoIdTestObject(5, "Hello")
    val first = ComplexWithAutoId(firstKey, "Now")
    val secondKey = AutoIdTestObject(6, "six")
    val second = ComplexWithAutoId(secondKey, "Now")
    val objectWithWrongKey = ComplexWithAutoId(firstKey, "Wow")

    @Test
    fun testCalled() {
        runBlocking {
            insertMap.nextTask(cTable, ObjectKeyToWriteForTest(true, listOf(firstKey), first, 5)) {
                m.testCalled()
            }
        }
        verifyAll { m.testCalled() }
    }

    @Test
    fun alreadyInserted() {
        runBlocking {
            readCache.readTableCache(cTable, true)[ReadKey(cTable, PersistenceKey(listOf(secondKey)))] = second
            insertMap.nextTask(cTable, ObjectKeyToWriteForTest(true, listOf(secondKey), second)) {
                fail("must not be called a second time, because it was already inserted")
            }
        }
    }

    @Test
    fun testExceptionForDoubleObject() {
        readCache.readTableCache(cTable, true)[ReadKey(cTable, PersistenceKey(listOf(firstKey)))] = first
        runBlocking {
            try {
                insertMap.nextTask(cTable, ObjectKeyToWriteForTest(true, listOf(firstKey), objectWithWrongKey, 5)) {
                }
                fail("exception should have been thrown")
            } catch (t: DifferentObjectSameKeyException) {
                assertEquals(first, t.objectFromCache)
                assertEquals(objectWithWrongKey, t.objectToInsert)
            }
        }
    }
}
