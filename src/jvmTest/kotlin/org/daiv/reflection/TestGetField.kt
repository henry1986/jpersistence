package org.daiv.reflection

import org.daiv.reflection.persister.*
import org.junit.Test
import kotlin.test.assertEquals

class TestGetField {
    data class LowerClass(val x: String, val y: Int)
    data class HigherClass(val lower: LowerClass)

    val a = AccessObject(HigherClass::lower)
    @Test
    fun testGetPair() {
        val p = a.getPair(PropertyValue(5, LowerClass::x))
        val p2 = a.getPair(PropertyValue(6, LowerClass::y))
        assertEquals("lower_x=\"5\"", p)
        assertEquals("lower_y=6", p2)
    }

    @Test
    fun testToRequest(){
        val l = LowerClass("5", 6)
        val got = l.getAllPropertyValues().toRequest(a)
        assertEquals("lower_x=\"5\" AND lower_y=6", got)
    }

    @Test
    fun testToRequest2(){
        val l = LowerClass("5", 6)
        val got = l.getPropertyValues(LowerClass::x).toRequest(a)
        assertEquals("lower_x=\"5\"", got)
    }
}
