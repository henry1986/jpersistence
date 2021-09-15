package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.database.persister
import org.junit.After
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ReadTest {
    @MoreKeys(2)
    data class SimpleKey(val x:Int, val y:Int)
    @MoreKeys(2)
    data class MoreComplexKey(val s:SimpleKey, val s2:SimpleKey)

    val persister = persister("ReadTest.db")
    @Test
    fun test(){
        val t = persister.Table(MoreComplexKey::class)
        t.persist()
        val b = MoreComplexKey(SimpleKey(5, 9), SimpleKey(10, 15))
        t.insert(b)
        val k = t.read("x", 5)
        assertEquals(listOf(b), k)
        val k2 = t.read("x", 10)
        assertEquals(emptyList(), k2)
    }

    @After
    fun after(){
        persister.delete()
    }
}