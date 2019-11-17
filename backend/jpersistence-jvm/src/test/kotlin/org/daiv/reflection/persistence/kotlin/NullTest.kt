package org.daiv.reflection.persistence.kotlin

import mu.KotlinLogging
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class NullTest : Spek({
                          data class SimpleClass(val x: Int, val s: String)
                          data class ComplexClass(val x: Int, val y: SimpleClass?, val z: SimpleClass?)
                          @MoreKeys(2)
                          data class SimpleClass2(val x: Int, val y: Int, val s: String)

                          data class ComplexClass2(val x: Int, val y: SimpleClass2?, val z: SimpleClass2?)

                          val logger = KotlinLogging.logger {}
                          describe("null test") {
                              val persister = Persister("NullTest.db")
                              on("test simple") {
                                  it("check as value") {
                                      val table = persister.Table(ComplexClass::class)
                                      table.persist()
                                      val list = listOf(ComplexClass(1, SimpleClass(0, "wow"), SimpleClass(1, "hello")),
                                                        ComplexClass(2, SimpleClass(2, "wow"), SimpleClass(3, "now")),
                                                        ComplexClass(3, null, SimpleClass(4, "oh")),
                                                        ComplexClass(4, SimpleClass(4, "oh"), null),
                                                        ComplexClass(5, null, null))
                                      table.insert(list)
                                      val all = table.readAll()
                                      assertEquals(list.size, all.size)
                                      assertEquals(list, all)
                                  }
                                  it("check double key") {
                                      val table = persister.Table(ComplexClass2::class)
                                      table.persist()
                                      val list = listOf(ComplexClass2(1, SimpleClass2(0, 1, "wow"), SimpleClass2(1, 0, "hello")),
                                                        ComplexClass2(2, SimpleClass2(2, 2, "wow"), SimpleClass2(3, 0, "now")),
                                                        ComplexClass2(3, null, SimpleClass2(4, 5, "oh")),
                                                        ComplexClass2(4, SimpleClass2(4, 8, "oh"), null),
                                                        ComplexClass2(5, null, null))
                                      table.insert(list)
                                      val all = table.readAll()
                                      assertEquals(list.size, all.size)
                                      assertEquals(list, all)
                                  }
//                                  it("check as Key") {
                                  //                                      val table = persister.Table(ComplexClass2::class)
//                                      table.persist()
//                                      val list = listOf(ComplexClass2(SimpleClass(4, "what"), SimpleClass(0, "wow")),
//                                                        ComplexClass2(null, SimpleClass(2, "wow")),
//                                                        ComplexClass2(3, null))
//                                      table.insert(list)
//                                      val all = table.readAll()
//                                      assertEquals(list, all)
//                                  }
                              }
                              afterGroup { persister.delete() }
                          }
                      })