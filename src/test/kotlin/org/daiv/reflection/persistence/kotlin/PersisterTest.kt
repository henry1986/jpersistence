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

import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.persistence.ComplexObject
import org.daiv.reflection.persistence.PersisterObject
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

             val simpleObject = SimpleObject(5, "Hallo")

             val s = ObjectToTest(simpleObject,
                                  "(x Int NOT NULL, y Text NOT NULL, PRIMARY KEY(x))",
                                  "(x, y ) VALUES (5, \"Hallo\")", "KotlinSimpleObject", 5)
             val persisterObject = PersisterObject(5, 3.0, 1, "Hallo")
             val p = ObjectToTest(persisterObject,
                                  "(i1 Int NOT NULL, d2 Double NOT NULL, i2 Int NOT NULL, s1 Text NOT NULL, PRIMARY KEY(i1))",
                                  "(i1, d2, i2, s1 ) VALUES (5, 3.0, 1, \"Hallo\")",
                                  "JavaSimpleObject", 5)
             val c = ObjectToTest(ComplexObject(3, 6, persisterObject),
                                  "(i1 Int NOT NULL, i3 Int NOT NULL, p1_i1 Int NOT NULL, p1_d2 Double NOT NULL, "
                                          + "p1_i2 Int NOT NULL, p1_s1 Text NOT NULL, PRIMARY KEY(i1))",
                                  "(i1, i3, p1_i1, p1_d2, p1_i2, p1_s1 ) VALUES (3, 6, 5, 3.0, 1, \"Hallo\")",
                                  "JavaComplexObject",
                                  3)
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

             val listOf = listOf(s, p, c, k, t)
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

                         val readValue1 = ReadValue(5, "HalloX")
                         val readValue2 = ReadValue(6, "HollaY")
                         val persister = Persister(database)
                         persister.persist(ReadValue::class)
                         persister.insert(readValue1)
                         persister.insert(readValue2)
                         val table = persister.Table(ReadValue::class)
                         it("read from column") {
                             val read = table.read("string", "HalloX")
                             assertEquals(readValue1, read)
                         }
                         it("check key existence") {
                             assertTrue(table.exists(5))
                         }
                         it("check key no Existence") {
                             assertFalse(table.exists(7))
                         }
                         it("check record existence") {
                             assertTrue(table.exists("string", "HalloX"))
                         }
                         it("check Record no existence") {
                             assertFalse(table.exists("string", "blub"))
                         }
                         it("delete and check deletion") {
                             table.delete("string", "HalloX")
                             assertFalse(table.exists("string", "HalloX"))
                         }
                         it("delete key and check deletion") {
                             table.delete(6)
                             assertFalse(table.exists("string", "HollaY"))
                         }
                     }
                     afterGroup { o.delete(); database.delete() }
                 }
             }
         })

