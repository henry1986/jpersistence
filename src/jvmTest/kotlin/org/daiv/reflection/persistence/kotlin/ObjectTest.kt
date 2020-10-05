package org.daiv.reflection.persistence.kotlin

import kotlin.test.Test
import mu.KotlinLogging
import org.daiv.reflection.annotations.IFaceForObject
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.persister.Persister
import kotlin.test.AfterTest
import kotlin.test.assertEquals

open class ObjectTest {
    data class MyObjectTest(val x: Int, @IFaceForObject([RunXImpl::class, TestSingleton::class]) val runX: RunX)

    @MoreKeys(2)
    data class MyObjectKey(val x: Int, @IFaceForObject([RunXImpl::class, TestSingleton::class]) val runX: RunX)

    @MoreKeys(auto = true)
    data class MyObject2Test(val myObjectTest: MyObjectKey, val s: String)

    val logger = KotlinLogging.logger {}
    val persister = Persister("ObjectTest.db")

    @AfterTest
    fun afterTest() {
        persister.delete()
    }
}

class DoTest : ObjectTest() {
    val table = persister.Table(MyObjectTest::class)
    init {
        table.persist()
    }
    val m1 = MyObjectTest(5, TestSingleton)
    val m2 = MyObjectTest(6, RunXImpl(5))
    val list = listOf(m1, m2)
    init {
        table.insert(list)
        persister.clearCache()
    }
    @Test
    fun testReadAll() {
        val read = table.readAll()
        persister.clearCache()
        assertEquals(list, read)
    }

    @Test
    fun testSingleRead() {
        val read = table.read(5)
        persister.clearCache()
        assertEquals(m1, read)
    }
}

class DoTestWithHashCode : ObjectTest() {
    val table = persister.Table(MyObject2Test::class)
    init {
        table.persist()
    }
    val m1 = MyObject2Test(MyObjectKey(5, TestSingleton), "Hello")
    val m2 = MyObject2Test(MyObjectKey(6, RunXImpl(5)), "Hello")
    val list = listOf(m1, m2)
    init {
        table.insert(list)
        persister.clearCache()
    }
    @Test
    fun testReadAll() {
        persister.clearCache()
        val read = table.readAll()
        assertEquals(list.toSet(), read.toSet())
    }

    @Test
    fun testSingleRead() {
        persister.clearCache()
        val read = table.read(MyObjectKey(5, TestSingleton))
        assertEquals(m1, read)
    }
}
