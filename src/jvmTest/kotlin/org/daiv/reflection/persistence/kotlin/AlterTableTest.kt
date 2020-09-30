package org.daiv.reflection.persistence.kotlin

import kotlin.test.Test
import mu.KotlinLogging
import org.daiv.reflection.database.DatabaseHandler
import org.daiv.reflection.annotations.ManyToOne
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.persister.Persister
import org.daiv.util.json.log
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.assertEquals

data class Before(val x: Int, val y: String)
data class After(val x: Int, val y: String, val z: Double)

@MoreKeys(2)
data class After2(val x: Int, val z: Double, val y: String)

data class BeforeComplex(val x: Int, @ManyToOne("After") val y: Before)

@MoreKeys(2)
data class AfterComplex(val x: Int, val y: After, val z: Double)

data class BeforeList(val x: Int, val y: List<Before>)

@MoreKeys(2)
data class AfterList(val x: Int, val z: Int, val y: List<After2>)

@MoreKeys(2)
data class NextBeforeList(val x: Int, val b: Before, val b2: Before, val z: Int, val y: List<Before>)

@MoreKeys(3)
data class NextAfterList(val x: Int, val b: Before, val b2: Before, val z: Int, val y: List<Before>)

open class AlterTableTest {
    val logger = KotlinLogging.logger { }
    val database = DatabaseHandler("AlterTableTest.db")

    @BeforeTest
    fun beforeEachTest() {
        database.open()
    }

    @AfterTest
    fun afterEachTest() {
        database.delete()
    }

    val persister = Persister(database)
}

class RefactorSimpleObject : AlterTableTest() {
    @Test
    fun renameTable() {
        val table = persister.Table(Before::class)
        table.persist()
        val next = table.rename("BeforeX")
        assertEquals("BeforeX", next._tableName)
    }

    val table = persister.Table(Before::class, "After")

    init {
        table.persist()
        table.insert(Before(1, "hello"))
    }

    val newTable = table.change(After::class, mapOf("z" to "0.1"))
    @Test
    fun toAfter() {
        val read = newTable.readAll()
        assertEquals(1, read.size)
        assertEquals(After(1, "hello", 0.1), read.first())
    }

    @Test
    fun toAfter2() {
        val after2 = newTable.change(After2::class)
        val read = after2.readAll()
        assertEquals(1, read.size)
        assertEquals(After2(1, 0.1, "hello"), read.first())
        newTable.dropTable()
    }
}

class RefactorComplexObject : AlterTableTest() {
    val table = persister.Table(BeforeComplex::class, "AfterComplex")

    init {
        table.persist()
        table.insert(BeforeComplex(1, Before(0, "hello")))
    }

    val simple = persister.Table(Before::class)
        .rename("After")

    init {
        simple.change(After::class, mapOf("z" to "0.1"))
    }

    val newTable = table.change(AfterComplex::class, mapOf("z" to "0.1"))
    @Test
    fun toAfter() {
        val read = newTable.readAll()
        assertEquals(1, read.size)
        assertEquals(AfterComplex(1, After(0, "hello", 0.1), 0.1), read.first())
    }
}

class RefactoreObjectWithList : AlterTableTest() {
    @Test
    fun toAfter() {
        val table = persister.Table(BeforeList::class, "AfterList")
        table.persist()
        val assert = BeforeList(1, listOf(Before(1, "hello"), Before(2, "wow")))
        table.insert(assert)
        persister.clearCache()
        persister.Table(Before::class)
            .change(After2::class, mapOf("z" to "0.0"))
            .rename("After2")
        val changed = table.change(AfterList::class, mapOf("z" to "0"))
            .copyHelperTable(
                mapOf(
                    "y" to mapOf(
                        "&oldTableName" to "AfterList_BeforeList_y",
                        "z" to "0",
                        "value_After2_x" to "value_Before_x",
                        "value_After2_z" to "0.0"
                    )
                )
            )
        val read = changed.readMultiple(listOf(1, 0))
        assertEquals(AfterList(1, 0, listOf(After2(1, 0.0, "hello"), After2(2, 0.0, "wow"))), read)
    }
}

class RefactorObjectWithListAndAddingKey : AlterTableTest() {
    @Test
    fun toAfter() {
        persister.clearCache()
        val table = persister.Table(NextBeforeList::class, "NextAfterList")
        table.persist()
        val b = Before(1, "hello")
        val b3 = Before(15, "hello")
        val b2 = Before(9, "9hello")
        val assert1 = NextBeforeList(1, b, b2, 2, listOf(b, Before(4, "wow")))
        val assert2 = NextBeforeList(2, b3, b2, 2, listOf(Before(2, "2hello"), Before(8, "8wow")))
        val assert3 = NextBeforeList(3, b, b2, 3, listOf(Before(3, "3hello"), Before(5, "5wow")))
        val assert = listOf(assert1, assert2, assert3)
        table.insert(assert)
        val changed = table.change(NextAfterList::class)
            .copyHelperTable(
                mapOf(
                    "y" to mapOf(
                        "&oldTableName" to "NextAfterList_NextBeforeList_y",
                        "&newTableName" to "NextAfterList_NextAfterList_y",
                        "b2_x" to "#addKeyColumn"
                    )
                ), table
            )
        val read = changed.readAll()
        val rassert1 = NextAfterList(1, b, b2, 2, listOf(Before(1, "hello"), Before(4, "wow")))
        val rassert2 = NextAfterList(2, b3, b2, 2, listOf(Before(2, "2hello"), Before(8, "8wow")))
        val rassert3 = NextAfterList(3, b, b2, 3, listOf(Before(3, "3hello"), Before(5, "5wow")))
        val rassert = listOf(rassert1, rassert2, rassert3)
        assertEquals(rassert.toSet(), read.toSet())
    }
}
