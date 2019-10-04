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
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class PersisterTest2
    : Spek({
               data class L1(val r: Int, val s: String)

               data class SimpleObject(val x: Int, val y: String)
               data class ComplexKey(val x: SimpleObject, val string: String)


               data class B1(val r: Int, val s: String)
               data class B2(val id: Int, val s: String)

               data class C1(val id: X1, val x: Int)
               data class C2(val id: Int, val x: X1)
               data class C3(val id: X2, val x: Int)
               data class C4(val id: Int, val x: List<X1>)

               data class D1(val x: Int, val y: List<Int>)
               data class E1(val x: Int, val y: String)

               data class InnerVal(val x:Int){
                   val y = x+1
               }


               val database = DatabaseWrapper.create("PersisterTest2.db")
               describe("Persister") {
                   database.open()
                   val persister = Persister(database)
                   on("table") {
//                       it("printTableDatas") {
//                           val table = persister.Table(B4::class)
//                           table.persist()
//                           val l2 = listOf(B4(4, listOf(B2(5, "wow"), B2(9, "cool")), B1(6, "hi")),
//                                           B4(5, listOf(), B1(9, "now")))
//                           table.insert(l2)
//                           val allTables = table.allTables()
//                           val new = allTables.copy(tableData = allTables.tableData.appendValues(listOf("6", "6", "hi")),
//                                                    helper = listOf(allTables.helper.first().appendValues(listOf("6", "9"))))
//                           val values = table.readAllTableData(new)
//                           println(values)
//                           table.resetTable(values)
//                           val x = table.read(6)
//                           assertEquals(B4(6, listOf(B2(9, "cool")), B1(6, "hi")), x)
//                       }
                       it("enum key test") {
                           val table = persister.Table(C1::class)
                           table.persist()
                           val v = C1(X1.B1, 5)
                           table.insert(v)
                           table.insert(C1(X1.B2, 9))
                           val x = table.read(X1.B1)
                           assertEquals(v, x)
                       }
                       it("enum test") {
                           val table = persister.Table(C2::class)
                           table.persist()
                           val v = C2(5, X1.B1)
                           table.insert(v)
                           table.insert(C2(4, X1.B2))
                           val x = table.read(5)
                           assertEquals(v, x)
                       }
                       it("enum complex test") {
                           val table = persister.Table(C3::class)
                           table.persist()
                           val v = C3(X2.B1, 5)
                           table.insert(v)
                           table.insert(C3(X2.B2, 9))
                           val x = table.read(X2.B1)
                           assertEquals(v, x)
                       }
                       it("enum listTest") {
                           val table = persister.Table(C4::class)
                           table.persist()
                           val v = C4(5, listOf(X1.B1))
                           table.insert(v)
                           table.insert(C4(4, listOf(X1.B2)))
                           val x = table.read(5)
                           assertEquals(v, x)
                       }
                       it("complexKey test") {
                           val table = persister.Table(ComplexKey::class)
                           table.persist()
                           val s = SimpleObject(1, "hello")
                           val v = ComplexKey(SimpleObject(1, "hello"), "world")
                           table.insert(v)
                           table.insert(ComplexKey(SimpleObject(2, "wow"), "kow"))
                           val x = table.read(s)
                           assertEquals(v, x)
                       }
                       it("List simpleType test") {
                           val table = persister.Table(D1::class)
                           table.persist()
                           val v = D1(5, listOf(1, 3, 4))
                           table.insert(v)
                           table.insert(D1(4, listOf(5, 92, 4)))
                           val x = table.read(5)
                           assertEquals(v, x)
                       }
                       it("inner val test"){
                           val table = persister.Table(InnerVal::class)
                           table.persist()
                           val v = InnerVal(5)
                           table.insert(v)
                           assertEquals(v, table.read(5))

                       }
                   }
                   afterGroup { database.delete() }
               }
           })
