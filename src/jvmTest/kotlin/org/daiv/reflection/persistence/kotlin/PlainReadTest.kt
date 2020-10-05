package org.daiv.reflection.persistence.kotlin

import kotlin.test.Test
import mu.KotlinLogging
import org.daiv.reflection.annotations.IFaceForObject
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.plain.*
import kotlin.test.AfterTest
import kotlin.test.assertEquals


open class PlainReadTest {
    @MoreKeys(2)
    data class MyObject(val long: Long, val x: Int, val string: X1, val isTrue: Boolean)

    data class MyObject2(val y: Int, override val x: Int) : TheInterface

    data class MyCObject(val long: Long, val myObject: MyObject)
    data class MyCObject2(val long: Long, @IFaceForObject([MyObject2::class]) val myObject: TheInterface)

    data class MyResult0(val long: Long, val myObject: Long, val myObject_x_Int: Int)
    data class MyResult1(val long: Long)
    data class MyResult2(val long: Long, val x: Int)
    data class MyResult3(val long: Long, val x: Int, val enum: X1)
    data class MyResult4(val long: Long, val x: Int, val enum: X1, val isTrue: Boolean)
    data class MyResult5(val long: Int)

    val logger = KotlinLogging.logger {}
    val persister = Persister("PlainReadTest.db")

    @AfterTest
    fun afterTest() {
        persister.delete()
    }

}

class TestSimplePlainRead : PlainReadTest() {
    val table = persister.Table(MyObject::class)

    init {
        table.persist()
        table.insert(MyObject(5L, 4, X1.B1, true))
        table.insert(MyObject(6L, 4, X1.B2, false))
    }

    @Test
    fun testWith1Request() {
        val res = table.readPlain(listOf(PlainLongObject("long"))) {
            MyResult1(it.first() as Long)
        }
        assertEquals(listOf(MyResult1(5L), MyResult1(6L)), res)
    }

    @Test
    fun testWith2Requests() {
        val res = table.readPlain(listOf(PlainLongObject("long"), PlainObject("x"))) {
            MyResult2(it.first() as Long, it[1] as Int)
        }
        assertEquals(listOf(MyResult2(5L, 4), MyResult2(6L, 4)), res)
    }

    @Test
    fun testWith3Requests() {
        val res = table.readPlain(
            listOf(
                PlainLongObject("long"),
                PlainObject("x"),
                PlainEnumObject("string", X1::class)
            )
        ) {
            MyResult3(it.first() as Long, it[1] as Int, it[2] as X1)
        }
        assertEquals(listOf(MyResult3(5L, 4, X1.B1), MyResult3(6L, 4, X1.B2)), res)
    }

    @Test
    fun testWith4Requests() {
        val res = table.requestBuilder("long, x, string, isTrue") {
            MyResult4(it.first() as Long, it[1] as Int, it[2] as X1, it[3] as Boolean)
        }
            .read()

        assertEquals(listOf(MyResult4(5L, 4, X1.B1, true), MyResult4(6L, 4, X1.B2, false)), res)
    }
}

class TestComplex : PlainReadTest() {
    val table = persister.Table(MyCObject::class)

    init {
        table.persist()
        table.insert(MyCObject(5L, MyObject(9L, 4, X1.B3, true)))
        table.insert(MyCObject(6L, MyObject(7L, 5, X1.B2, false)))
        logger.error { "name: ${table.readPersisterData.fields[1].toTableHead()}" }
    }

    @Test
    fun testReadComplex() {
        val res = table.requestBuilder("long, myObject_long, myObject_x") {
            MyResult0(it[0] as Long, it[1] as Long, it[2] as Int)
        }
//                                     .l("long")
//                                     .l("myObject_long")
//                                     .p("myObject_x")
            .read()
        assertEquals(listOf(MyResult0(5L, 9L, 4), MyResult0(6L, 7L, 5)), res)
    }
}

class TestInterface : PlainReadTest() {
    val table = persister.Table(MyCObject2::class)

    init {
        table.persist()
        table.insert(MyCObject2(5L, MyObject2(5, 4)))
        table.insert(MyCObject2(6L, MyObject2(4, 6)))
    }

    @Test
    fun testPlain() {
        val res = table.requestBuilder("myObject_MyObject2_y") {
            MyResult5(it[0] as Int)
        }
            .read()
        assertEquals(listOf(MyResult5(5), MyResult5(4)), res)
    }
}
