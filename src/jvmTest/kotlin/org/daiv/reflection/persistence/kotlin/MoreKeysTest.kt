package org.daiv.reflection.persistence.kotlin

import kotlin.test.Test
import mu.KotlinLogging
import org.daiv.reflection.annotations.Including
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.database.persister
import org.daiv.reflection.persister.Persister
import kotlin.test.AfterTest
import kotlin.test.assertEquals


@MoreKeys(2)
data class MKeyClass(val x: Int, val y: String, val z: String)

@MoreKeys(2)
data class IncludingMKeyClass(val x: Int, val mKeyClass: MKeyClass, val z: String)

@Including
data class IncludingClass(val x: Int, val y: Int, val z: String)

@MoreKeys(2)
data class ComplexInclude(val includingClass: IncludingClass, val x: Int, val s: String)

data class IncludingComplexKey(val mKeyClass: MKeyClass, val s: String)

@MoreKeys(auto = true)
data class X2Holder(val map: Map<X2, MKeyClass>, val z: String)

@MoreKeys(auto = true)
data class HoldX2Map(val x2Holder: X2Holder, val b: String)


@MoreKeys(auto = true)
data class X3Holder(val x2: X2, val z: String)

data class X3HolderNoAuto(val x2: X2, val z: String)

data class HolderHolder(val x3Holder: X3Holder, val z: String)
data class HolderHolderNoAuto(val x3Holder: X3HolderNoAuto, val z: String)


open class MoreKeysTest {
    val logger = KotlinLogging.logger { }
    val persister = persister("MoreKeysTest.db")

    @AfterTest
    fun afterTest() {
        persister.delete()
    }
}

class TestMoreKeysWithIncluding : MoreKeysTest() {
    val table = persister.Table(ComplexInclude::class)

    init {
        table.persist()
    }

    val i1 = IncludingClass(5, 6, "Hello")
    val i2 = IncludingClass(5, 7, "Hello")
    val c1 = ComplexInclude(i1, 5, "Hello")
    val c2 = ComplexInclude(i1, 6, "Hello")
    val c3 = ComplexInclude(i2, 6, "Hello")
    val c4 = ComplexInclude(i2, 5, "Hello")
    val list = listOf(c1, c2, c3, c4)

    init {
        table.insert(list)
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
        val read = table.read(listOf(i1, 5))
        assertEquals(c1, read)
    }
}

class TestMoreKeysWithMoreKeys : MoreKeysTest() {
    val table = persister.Table(IncludingMKeyClass::class)

    init {
        table.persist()
    }

    val i1 = MKeyClass(5, "World", "Hello")
    val i2 = MKeyClass(5, "My", "Hello")
    val c1 = IncludingMKeyClass(5, i1, "Hello")
    val c2 = IncludingMKeyClass(6, i1, "Hello")
    val c3 = IncludingMKeyClass(6, i2, "Hello")
    val c4 = IncludingMKeyClass(5, i2, "Hello")
    val list = listOf(c1, c2, c3, c4)

    init {
        table.insert(list)
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
        val read = table.read(listOf(5, i1))
        assertEquals(c1, read)
    }
}

class EnumInMapTest : MoreKeysTest() {
    val keyX22 = mapOf(X2.B2 to MKeyClass(2, "2", "3"))
    val x21 = X2Holder(mapOf(X2.B1 to MKeyClass(1, "2", "3"), X2.B2 to MKeyClass(3, "2", "3")), "4")
    val x22 = X2Holder(keyX22, "4")
    val list = listOf(x21, x22)

    @Test
    fun test() {
        persister.testTable(X2Holder::class, list, listOf(keyX22), x22)
    }
}

class AutokeysDoubleInMapTest : MoreKeysTest() {
    val keyX22 = mapOf(X2.B2 to MKeyClass(2, "2", "3"))
    val x21 = X2Holder(mapOf(X2.B1 to MKeyClass(1, "2", "3"), X2.B2 to MKeyClass(3, "2", "3")), "4")
    val x22 = X2Holder(keyX22, "4")
    val h1 = HoldX2Map(x21, "hello")
    val h2 = HoldX2Map(x22, "world")
    val list = listOf(h1, h2)

    @Test
    fun test() {
        persister.testTable(HoldX2Map::class, list, listOf(x22), h2)
    }
}

class EnumWithParameter : MoreKeysTest() {
    val table = persister.Table(HolderHolder::class)

    init {
        table.persist()
    }

    val x1 = HolderHolder(X3Holder(X2.B1, "Hello"), "World")
    val x2 = HolderHolder(X3Holder(X2.B2, "Hello1"), "World1")
    val list = listOf(x1, x2)

    init {
        table.insert(list)
    }

    @Test
    fun testReadAll() {
        persister.clearCache()
        val read = table.readAll()
        assertEquals(list.toSet(), read.toSet())
    }
}

class EnumWithParameterNoAuto : MoreKeysTest() {
    val table = persister.Table(HolderHolderNoAuto::class)

    init {
        table.persist()
    }

    val x1 = HolderHolderNoAuto(X3HolderNoAuto(X2.B1, "Hello"), "World")
    val x2 = HolderHolderNoAuto(X3HolderNoAuto(X2.B2, "Hello1"), "World1")
    val list = listOf(x1, x2)

    init {
        table.insert(list)
    }

    @Test
    fun testReadAll() {
        persister.clearCache()
        val read = table.readAll()
        assertEquals(list.toSet(), read.toSet())
    }
}

class MKeyClassIncludedTest: MoreKeysTest(){
    val table = persister.Table(IncludingComplexKey::class)
    init {
        table.persist()
    }

    val x1 = IncludingComplexKey(MKeyClass(5, "e", "z"), "s")
    val x2 = IncludingComplexKey(MKeyClass(6, "b", "a"), "d")
    init {
        table.insert(listOf(x1))
    }

    @Test
    fun testRead(){
        persister.clearCache()
        val read = table.read(x1.mKeyClass)
        assertEquals(x1, read)
    }

    @Test
    fun testDelete(){
        table.delete(x1.mKeyClass)
        assertEquals(emptyList(), table.readAll())
    }
}