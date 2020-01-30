package org.daiv.reflection.persistence.kotlin

import mu.KotlinLogging
import org.daiv.reflection.annotations.Including
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

                          data class HNComplexClass(val x: SimpleClass, val y: SimpleClass)
                          data class HNComplexClass2(val x: Int, val y: HNComplexClass?)

                          @Including
                          data class IncludingSimple(val x: Int, val y: String)

                          data class IncludingComplex(val id: Int, val includingSimple: IncludingSimple)
                          data class IncludingListComplex(val id: Int, val includingSimple: List<IncludingSimple>)

                          @Including
                          data class IncludingAComplex(val x: Int, val includingSimple: IncludingSimple)

                          data class IncludingTheComplex(val id: Int, val includingAComplex: IncludingAComplex)
                          data class IncludingTheComplexList(val id: Int, val includingAComplex: List<IncludingAComplex>)

                          @Including
                          data class IncludingAComplexList(val x: Int, val includingSimple: List<IncludingSimple>)

                          data class IncludingTheComplexWithList(val id: Int, val includingAComplex: IncludingAComplex)

                          data class EnumHolder(val x: X1, val x2: X1?)

                          val logger = KotlinLogging.logger {}
                          describe("null test") {
                              val persister = Persister("NullTest.db")
                              on("test simple") {
                                  val s0 = SimpleClass(0, "wow")
                                  val s1 = SimpleClass(1, "hello")
                                  val s2 = SimpleClass(2, "wow")
                                  val s3 = SimpleClass(3, "now")
                                  it("check as value") {
                                      val table = persister.Table(ComplexClass::class)
                                      table.persist()
                                      val list = listOf(ComplexClass(1, s0, s1),
                                                        ComplexClass(2, s2, s3),
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
                                  it("check hiddenNull") {
                                      val table = persister.Table(HNComplexClass2::class)
                                      table.persist()
                                      val list = listOf(HNComplexClass2(1, null), HNComplexClass2(2, null))
                                      table.insert(list)
                                      val all = table.readAll()
                                      assertEquals(list.size, all.size)
                                      assertEquals(list, all)
                                  }
                              }

                              on("test including") {
                                  it("test Including simple") {
                                      val table = persister.Table(IncludingComplex::class)
                                      table.persist()
                                      val list = listOf(IncludingComplex(2, IncludingSimple(4, "Hello")),
                                                        IncludingComplex(3, IncludingSimple(4, "Now")))
                                      table.insert(list)
                                      val all = table.readAll()
                                      assertEquals(list, all)
                                  }
                                  it("test Including list") {
                                      val table = persister.Table(IncludingListComplex::class)
                                      table.persist()
                                      val list = listOf(IncludingListComplex(2,
                                                                             listOf(IncludingSimple(4, "Hello"),
                                                                                    IncludingSimple(4, "World"))),
                                                        IncludingListComplex(3,
                                                                             listOf(IncludingSimple(4, "Now"),
                                                                                    IncludingSimple(4, "Hello World"),
                                                                                    IncludingSimple(4, "World"))))
                                      table.insert(list)
                                      persister.clearCache()
                                      val all = table.readAll()
                                      assertEquals(list, all)
                                  }
                                  it("test Including aComplex") {
                                      val table = persister.Table(IncludingTheComplex::class)
                                      table.persist()
                                      val list = listOf(IncludingTheComplex(2, IncludingAComplex(2, IncludingSimple(5, "what"))),
                                                        IncludingTheComplex(3, IncludingAComplex(2, IncludingSimple(5, "whatIs"))))
                                      table.insert(list)
                                      persister.clearCache()
                                      val all = table.readAll()
                                      assertEquals(list, all)
                                  }
                                  it("test Including aComplex list") {
                                      val table = persister.Table(IncludingTheComplexList::class)
                                      table.persist()
                                      val list = listOf(IncludingTheComplexList(2,
                                                                                listOf(IncludingAComplex(2, IncludingSimple(5, "what")),
                                                                                       IncludingAComplex(2, IncludingSimple(5, "what")))),
                                                        IncludingTheComplexList(3,
                                                                                listOf(IncludingAComplex(2, IncludingSimple(5, "whatIs")),
                                                                                       IncludingAComplex(2, IncludingSimple(5, "what")))))
                                      table.insert(list)
                                      persister.clearCache()
                                      val all = table.readAll()
                                      assertEquals(list, all)
                                  }
                                  it("test null enum"){
                                      val table = persister.Table(EnumHolder::class)
                                      table.persist()
                                      val enumHolder = EnumHolder(X1.B1, null)
                                      val list = listOf(enumHolder)
                                      table.insert(list)
                                      persister.clearCache()
                                      val read = table.readAll()
                                      assertEquals(list, read)
                                  }
                              }
                              afterGroup { persister.delete() }
                          }
                      })