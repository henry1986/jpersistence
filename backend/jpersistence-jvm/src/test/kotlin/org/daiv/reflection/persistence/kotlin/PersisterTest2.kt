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
import org.daiv.reflection.annotations.SameTable
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class PersisterTest2
    : Spek({
               data class L1(val r: Int, val s: String)
               data class L2(val id: Int, @SameTable val l1: L1)

               data class B1(val r: Int, val s: String)
               data class B2(val id: Int, val s: String)
               data class B3(val id: Int, val b2: B2, @SameTable val b1: B1)
               data class B4(val id: Int, val b2: List<B2>, @SameTable val b1: B1)

               val database = DatabaseWrapper.create("PersisterTest2.db")
               describe("Persister") {
                   database.open()
                   val persister = Persister(database)
                   on("table") {
                       it("includedKey") {
                           val table = persister.Table(L2::class)
                           table.persist()
                           val l2 = L2(5, L1(5, "Halo"))
                           table.insert(l2)
                           assertEquals(l2, table.read(5))
                       }
                       it("includedKey and foreign") {
                           val table = persister.Table(B3::class)
                           table.persist()
                           val l2 = B3(4, B2(5, "wow"), B1(6, "hi"))
                           table.insert(l2)
                           table.readAll()
                           assertEquals(l2, table.read(4))
                       }
                       it("printTableDatas") {
                           val table = persister.Table(B4::class)
                           table.persist()
                           val l2 = listOf(B4(4, listOf(B2(5, "wow"), B2(9, "cool")), B1(6, "hi")),
                                           B4(5, listOf(), B1(9, "now")))
                           table.insert(l2)
                           val allTables = table.allTables()
                           val new = allTables.copy(tableData = allTables.tableData.appendValues(listOf("6",
                                                                                                        "6",
                                                                                                        "hi")),
                                                    helper = listOf(allTables.helper.first().appendValues(listOf("6",
                                                                                                                 "9"))))
                           val values = table.readAllTableData(new)
                           println(values)
                           table.resetTable(values)
                           val x = table.read(6)
                           assertEquals(B4(6, listOf(B2(9, "cool")), B1(6, "hi")), x)
                       }
                   }
                   afterGroup { database.delete() }
               }
           })