package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.persister.NoPersistantTable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

data class TestXData(val x: Int, val y: Int)

class NoPersistantTableTest {

    @Test
    fun test() {
        val x = NoPersistantTable<TestXData>({ if (it == "x") x else y }, { it == x })
        val v1 = TestXData(5, 6)
        val v2 = TestXData(5, 7)
        val list = listOf(v1, v2)
        x.insert(list)
        assertTrue(x.exists("x", 5))
        assertEquals(list, x.readAll())
        assertEquals(listOf(v1), x.read("y", 6))
        x.clear()
        assertEquals(emptyList(), x.readAll())
        assertFalse(x.exists("x", 5))
    }
}