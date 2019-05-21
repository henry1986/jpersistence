package org.daiv.reflection.persistence.kotlin

import mu.KotlinLogging
import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class BarTest
    : Spek({
               data class Tick(val time: Long, val value: Double)
               data class Candle(val time: Long, val startTick: Tick, val tickList: List<Tick>)
               data class Bar(val x: Int, val s: String)

               val logger = KotlinLogging.logger { }
               val database = DatabaseWrapper.create("BarTest.db")
               describe("BarTest") {
                   database.open()
                   val persister = Persister(database)
                   on("from to") {
                       val list = (0 until 100).map { Bar(it, " - $it - ") }
                               .toList()
                       val table = persister.Table(Bar::class)
                       table.persist()
                       table.insert(list)
                       it("from to") {
                           val read = table.read(5, 10)
                           assertEquals(list.subList(5, 11), read)
                       }
                       it("first") {
                           assertEquals(Bar(0, " - 0 - "), table.first())
                       }
                       it("last") {
                           assertEquals(Bar(99, " - 99 - "), table.last())
                       }
                       it("size") {
                           assertEquals(100, table.size())
                       }
                       it("readLastBefore"){
                           assertEquals(list.take(10), table.readLastBefore(10, 10))
                       }
                       it("readLastBefore 2"){
                           assertEquals(list.subList(5, 10), table.readLastBefore(5, 10))
                       }
                       it("readNextAfter"){
                           assertEquals(list.subList(6, 11), table.readNextAfter(5, 5))
                       }
                   }
                   on("timetest") {
                       val table = persister.Table(Candle::class)
                       table.persist()
                       val candles = (0 until 50).map {
                           Candle(it * 60L,
                                  Tick(it * 60L + 1, 500.0),
                                  (0..6).map { t -> Tick(it * 60L + 2 + t, 498.0) })
                       }
                       logger.info { "starting inserting" }
                       table.insert(candles)
                       logger.info { "finished inserting" }
                   }
                   afterGroup { database.delete() }
               }
           })