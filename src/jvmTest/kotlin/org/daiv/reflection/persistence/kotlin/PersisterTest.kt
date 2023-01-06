/*
 * Copyright (c) 2018. Martin Heinrich - All Rights Reserved
import kotlin.test.Test
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package org.daiv.reflection.persistence.kotlin

import io.mockk.mockk
import io.mockk.verify
import org.daiv.reflection.database.DatabaseHandler
import org.daiv.reflection.annotations.ManyList
import org.daiv.reflection.annotations.ManyMap
import org.daiv.reflection.annotations.ManyToOne
import org.daiv.reflection.database.persister
import org.daiv.reflection.persister.DBChangeListener
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.ReadPersisterData
import kotlin.reflect.KClass
import kotlin.test.*

open class PersisterTest {
    data class SimpleObject(val x: Int, val y: String)
    data class ReadValue(val id: Int, val string: String)

    data class Transaction(val id: String, val bool: Boolean)
    data class ComplexM2O(val id: Int, val name: String, val value: Double)
    data class ComplexHost(val id: Int, @ManyToOne val x1: ComplexM2O, @ManyToOne val x2: ComplexM2O)
    data class ComplexHost2(val id: Int, @ManyList val x1: List<ComplexM2O>, @ManyList val x2: List<ComplexM2O>)
    data class ComplexHostList(val id: Int, @ManyList val x1: List<ComplexM2O>, @ManyList val x2: List<ComplexM2O>)
    data class ComplexHostSet(val id: Int, @ManyList val x1: Set<ComplexM2O>, @ManyList val x2: Set<ComplexM2O>)
    data class ComplexHostMap(
        val id: Int,
        @ManyMap val x1: Map<String, ComplexM2O>,
        @ManyMap val x2: Map<String, ComplexM2O>
    )

    data class L1(val r: Int, val s: String)
    data class L2(val id: Int, val l1: L1)
    data class L2b(val l1Id: L1, val l1Value: L1)
    data class L3(val id: Int, val l2Value: L2)
    data class L4(val idL2b: L2b, val l3Value: L3)
    data class L5(val x: Int, val s: String)
    data class L6(val id: Int, val l5: L5)

    val simpleObject = SimpleObject(5, "Hallo")


    //                 val database =  DatabaseHandler("ReadValue.db")
//                 database.open()
//                 val persister = Persister(database)
    val persister = persister("ReadValue.db")
    val readValues = listOf(ReadValue(5, "HalloX"), ReadValue(6, "HollaY"), ReadValue(7, "neu"))
    val changed6 = ReadValue(6, "neu")

    @AfterTest
    fun afterTest() {
        persister.delete()
    }
}


class TestDelete{
    val p = Persister("myDeleteTest.db")
    @Test
    fun testDelete(){
        val l1 = p.Table(PersisterTest.L1::class)
        l1.persist()
        l1.insert(PersisterTest.L1(5, "Hello"))
        p.deleteAndRestart()
        val l1AfterDelete = p.Table(PersisterTest.L1::class)
        l1AfterDelete.persist()
        assertEquals(emptyList(),  l1AfterDelete.readAll())
    }

    @AfterTest
    fun afterTest(){
        p.delete()
    }
}

class SpecialReads : PersisterTest() {
    val persisterListener = mockk<DBChangeListener>(relaxed = true)
    val table = persister.Table(ReadValue::class)

    init {
        table.persist()
    }

    init {
        readValues.forEach(table::insert)
        persister.clearCache()
    }

    @Test
    fun readFromColumn() {
        val read = table.read("string", "HalloX")
        assertEquals(readValues[0], read[0])
    }

    @Test
    fun checkKeyExistence() {
        assertTrue(table.exists(5))
    }

    @Test
    fun checkKeyNoExistence() {
        assertFalse(table.exists(35))
    }

    @Test
    fun checkRecordExistence() {
        assertTrue(table.exists("string", "HalloX"))
    }

    @Test
    fun checkRecordNoExistence() {
        assertFalse(table.exists("string", "blub"))
    }

    @Test
    fun getAllValues() {
        assertEquals(readValues, table.readAll())
    }

    @Test
    fun checkUpdate() {
        persister.register(persisterListener)
        table.update(6, "string", "neu")
        verify { persisterListener.onChange() }
        assertEquals(changed6, table.read(6))
    }

    @Test
    fun deleteAndCheckDeletion() {
        table.delete("string", "HalloX")
        assertFalse(table.exists("string", "HalloX"))
    }

    @Test
    fun deleteKeyAndCheckDeletion() {
        table.delete(6)
        assertFalse(table.exists("string", "HollaY"))
    }

    @Test
    fun deleteWholeTable() {
        table.clear()
        assertTrue(table.readAll().isEmpty())
    }

    init {
        persister.Table(ComplexM2O::class).persist()
    }

    val e1 = ComplexM2O(2, "myName", 6.0)
    val e2 = ComplexM2O(3, "nextName", 7.0)
    val e3 = ComplexM2O(4, "e3", 4.0)

    @Test
    fun onList() {
        val list = persister.Table(ComplexHostList::class)
        list.persist()
        val c = ComplexHostList(5, listOf(e1, e2, e1, e2, e2, e2, e1), listOf(e2, e3))
        list.insert(c)
        val read = list.read(5)
        assertEquals(c, read)
    }

    @Test
    fun onSet() {
        val list = persister.Table(ComplexHostSet::class)
        list.persist()
        val c = ComplexHostSet(5, setOf(e1, e2), setOf(e2, e3))
        list.insert(c)
        persister.clearCache()
        assertEquals(c, list.read(5))
    }

    @Test
    fun onMap() {
        val list = persister.Table(ComplexHostMap::class)
        list.persist()
        val c = ComplexHostMap(5, mapOf("e1" to e1, "e2" to e2), mapOf("e2" to e2, "e3" to e3))
        list.insert(c)
        assertEquals(c, list.read(5))
    }

    @Test
    fun onManyToOneSimpleObject() {
        val complexHostTable = persister.Table(ComplexHost::class)
        complexHostTable.persist()
        val c = ComplexHost(5, e1, e2)
        complexHostTable.insert(c)
        assertEquals(c, complexHostTable.read(5))
    }

    val host2 = persister.Table(ComplexHost2::class)

    init {
        host2.persist()
    }

    val c = ComplexHost2(5, listOf(e1, e2), listOf(e2, e3))

    @Test
    fun onManyToOneList() {
        host2.insert(c)
        assertEquals(c, host2.read(5))
    }

    @Test
    fun listClear() {
        host2.clear()
        host2.insert(c)
    }

    @Test
    fun readKeys() {
        val table = persister.Table(L1::class)
        table.persist()
        table.insert(L1(5, "hello"))
        table.insert(L1(6, "hello"))
        table.insert(L1(7, "hello"))
        persister.clearCache()
        val keys = table.readAllKeys<Int>()
        assertEquals(listOf(5, 6, 7), keys)
    }

    @Test
    fun readKeysComplexType() {
        val table = persister.Table(L2b::class)
        table.persist()
        table.insert(L2b(L1(5, "hello"), L1(5, "hello")))
        val keys = table.readAllKeys<Int>()
        assertEquals(listOf(5), keys)
    }

    @Test
    fun createTableComplexType() {
        val table = persister.Table(L6::class)
        table.persist()
        val l6 = L6(5, L5(5, "wow"))
        table.insert(l6)
        assertEquals(listOf(l6), table.readAll())
        val keys = table.readAllKeys<Int>()
        assertEquals(listOf(5), keys)
    }
}

class CheckDistinct : PersisterTest() {
    val table = persister.Table(ReadValue::class)

    init {
        table.persist()
        table.insert(listOf(ReadValue(5, "HalloX"), ReadValue(6, "HalloX"), ReadValue(7, "neu"), ReadValue(8, "neu")))
    }

    @Test
    fun test() {
        assertEquals(listOf("HalloX", "neu"), table.distinctValues("string"))
    }
}

class InsertList:PersisterTest(){
    val table = persister.Table(ReadValue::class)
    val tableListener = mockk<DBChangeListener>(relaxed = true)

    init {
        table.persist()
//        table.insert(listOf(ReadValue(5, "HalloX"), ReadValue(6, "HalloX"), ReadValue(7, "neu"), ReadValue(8, "neu")))
    }
    @Test
    fun insertList() {
        table.register(tableListener)
        table.insert(readValues)
        verify { tableListener.onChange() }
        assertEquals(readValues, table.readAll())
    }
}

class MultipleReadFromColumn:PersisterTest(){
    val table = persister.Table(ReadValue::class)
    init {
        table.persist()
//        val read = table.readAll()
//        println("read in mrfc")
//        read.forEach {
//            println("it: $it")
//        }
        table.insert(readValues)
        table.update(6, "string", "neu")
    }
    @Test
    fun multipleReadFromColumn() {
        val read = table.read("string", "neu")
        assertEquals(2, read.size)
        assertEquals(listOf(changed6, readValues.last()), read)
    }
}