/*
 * Copyright (c) 2018. Martin Heinrich - All Rights Reserved
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
import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.annotations.Many
import org.daiv.reflection.annotations.ManyToOne
import org.daiv.reflection.persister.DBChangeListener
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class PersisterTest :
    Spek({
             data class SimpleObject(val x: Int, val y: String)
             data class ComplexKObject(val x: Int, val y: SimpleObject)
             data class ComplexKey(val x: SimpleObject, val string: String)
             data class ReadValue(val id: Int, val string: String)
             data class Transaction(val id: String, val bool: Boolean)
             data class ComplexM2O(val id: Int, val name: String, val value: Double)
             data class ComplexHost(val id: Int, @ManyToOne val x1: ComplexM2O, @ManyToOne val x2: ComplexM2O)
             data class ComplexHost2(val id: Int, @ManyToOne @Many(2) val x1: List<ComplexM2O>, @ManyToOne @Many(2) val x2: List<ComplexM2O>)


             val simpleObject = SimpleObject(5, "Hallo")

             val s = ObjectToTest(simpleObject,
                                  "(x Int NOT NULL, y Text NOT NULL, PRIMARY KEY(x))",
                                  "(x, y ) VALUES (5, \"Hallo\")", "KotlinSimpleObject", 5)
//             val persisterObject = PersisterObject(5, 3.0, 1, "Hallo")
//             val p = ObjectToTest(persisterObject,
//                                  "(i1 Int NOT NULL, d2 Double NOT NULL, i2 Int NOT NULL, s1 Text NOT NULL, PRIMARY KEY(i1))",
//                                  "(i1, d2, i2, s1 ) VALUES (5, 3.0, 1, \"Hallo\")",
//                                  "JavaSimpleObject", 5)
//             val c = ObjectToTest(ComplexObject(3, 6, persisterObject),
//                                  "(i1 Int NOT NULL, i3 Int NOT NULL, p1_i1 Int NOT NULL, p1_d2 Double NOT NULL, "
//                                          + "p1_i2 Int NOT NULL, p1_s1 Text NOT NULL, PRIMARY KEY(i1))",
//                                  "(i1, i3, p1_i1, p1_d2, p1_i2, p1_s1 ) VALUES (3, 6, 5, 3.0, 1, \"Hallo\")",
//                                  "JavaComplexObject",
//                                  3)
             val k = ObjectToTest(ComplexKObject(1, simpleObject),
                                  "(x Int NOT NULL, y_x Int NOT NULL, y_y Text NOT NULL, PRIMARY KEY(x))",
                                  "(x, y_x, y_y ) VALUES (1, 5, \"Hallo\")",
                                  "KotlinComplexObject",
                                  1)

             val key = ComplexKey(simpleObject, "hello")
             val t = ObjectToTest(key,
                                  "(x_x Int NOT NULL, x_y Text NOT NULL, string Text NOT NULL, PRIMARY KEY(x_x, x_y))",
                                  "(x_x, x_y, string ) VALUES (5, \"Hallo\", \"hello\")",
                                  "KotlinComplexKeyObject",
                                  simpleObject)
             val u = ObjectToTest(Transaction("x5", false),
                                  "(id Text NOT NULL, bool Boolean NOT NULL, PRIMARY KEY(id))",
                                  "(id, bool ) VALUES (\"x5\", 0)",
                                  "Transaction",
                                  "x5")

             val listOf = listOf(s, k, t, u)
             val database = DatabaseWrapper.create("ReadValue.db")


             describe("Persister Table creation, read and insert") {
                 listOf.forEach { o ->
                     o.open()
                     on("persist $o") {
                         it("check createTable") {
                             o.checkCreateTable()
                         }
                         it("check insert") {
                             o.checkInsert()
                         }
                         o.beforeReadFromDatabase()
                         it("check readData") {
                             o.checkReadData()
                         }
                     }
                     on("special reads") {
                         database.open()

                         val persisterListener = mockk<DBChangeListener>(relaxed = true)
                         val tableListener = mockk<DBChangeListener>(relaxed = true)
                         val persister = Persister(database)
                         persister.persist(ReadValue::class)
                         val table = persister.Table(ReadValue::class)
                         val readValues = listOf(ReadValue(5, "HalloX"), ReadValue(6, "HollaY"), ReadValue(7, "neu"))
                         val changed6 = ReadValue(6, "neu")
                         readValues.forEach(table::insert)
                         it("read from column") {
                             val read = table.read("string", "HalloX")
                             assertEquals(readValues[0], read[0])
                         }
                         it("check key existence") {
                             assertTrue(table.exists(5))
                         }
                         it("check key no Existence") {
                             assertFalse(table.exists(35))
                         }
                         it("check record existence") {
                             assertTrue(table.exists("string", "HalloX"))
                         }
                         it("check Record no existence") {
                             assertFalse(table.exists("string", "blub"))
                         }
                         it("get all values") {
                             assertEquals(readValues, table.readAll())
                         }
                         it("check update") {
                             persister.register(persisterListener)
                             table.update(6, "string", "neu")
                             verify { persisterListener.onChange() }
                             assertEquals(changed6, table.read(6))
                         }
                         it("multiple read from column") {
                             val read = table.read("string", "neu")
                             assertEquals(2, read.size)
                             assertEquals(listOf(changed6, readValues.last()), read)
                         }
                         it("check distinct") {
                             assertEquals(listOf("HalloX", "neu"), table.distinctValues("string", String::class))
                         }
                         it("delete and check deletion") {
                             table.delete("string", "HalloX")
                             assertFalse(table.exists("string", "HalloX"))
                         }
                         it("delete key and check deletion") {
                             table.delete(6)
                             assertFalse(table.exists("string", "HollaY"))
                         }
                         it("delete whole table") {
                             table.clear()
                             assertTrue(table.readAll().isEmpty())
                         }
                         it("insert list") {
                             table.register(tableListener)
                             table.insert(readValues)
                             verify { tableListener.onChange() }
                             assertEquals(readValues, table.readAll())
                         }
                         it("list creation") {
                             persister.persist(ComplexHost2::class)
                             persister.persist(ComplexHost::class)
                             val complexHostTable = persister.Table(ComplexHost::class)
                             complexHostTable.insert(ComplexHost(5,
                                                                 ComplexM2O(2, "myName", 6.0),
                                                                 ComplexM2O(3, "nextName", 7.0)))
                         }
                     }
//                     pe.Table(ComplexHost2::class)
//                     on("manyToOne") {
//                         val tableM2O = Persister(database).Table(ComplexM2O::class)
//                         val table = Persister(database).Table(ComplexHost::class)
//                         val zero = ComplexM2O(0, "0", 5.0)
//                         val first = ComplexM2O(1, "1", 6.0)
//                         val second = ComplexM2O(2, "2", 7.0)
//                         tableM2O.insert(zero)
//                         tableM2O.insert(first)
//                         tableM2O.insert(second)
//                         table.insert(ComplexHost(9, first, second))
//                     }
                     afterGroup { o.delete(); database.delete() }
                 }
             }
         })

