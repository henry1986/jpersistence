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

               data class Tick(val time: Long, val value: Double, val offerSide: OfferSide)
               data class Candle(val time: Long, val startTick: Tick, val tickList: List<Tick>)
               data class Bar(val x: Int, val s: String)
               data class WavePoint(val tick: Tick, override val isLong: Boolean, override val period: Period): Point, Timeable{
                   override val time = tick.time
                   override val value = tick.value
               }
               data class RawWave(val id: Int, val wavePoints: List<WavePoint>) {
                   constructor(wavePoints: List<WavePoint>) : this(wavePoints.hashCode(), wavePoints)
               }

               data class DrawnWave(val id: Int, val rawWave: RawWave, val color: String) {
                   constructor(rawWave: RawWave, color: String) : this(rawWave.hashCode() + 31 * color.hashCode(), rawWave, color)
               }

               data class WaveSet(val id: Int, val waves: List<DrawnWave>) {
                   constructor(waves: List<DrawnWave>) : this(waves.hashCode(), waves)
               }

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
                       it("readLastBefore") {
                           assertEquals(list.take(10), table.readLastBefore(10, 10))
                       }
                       it("readLastBefore 2") {
                           assertEquals(list.subList(5, 10), table.readLastBefore(5, 10))
                       }
                       it("readNextAfter") {
                           assertEquals(list.subList(6, 11), table.readNextAfter(5, 5))
                       }
                       it("test hashcode") {
                           val table = persister.Table(WaveSet::class)
                           table.persist()
                           val wave = WaveSet(listOf(DrawnWave(RawWave(listOf(WavePoint(Tick(1000L, 500.0, OfferSide.BID),
                                                                                        true,
                                                                                        Period(6000, "m1")),
                                                                              WavePoint(Tick(1002L, 503.0, OfferSide.BID),
                                                                                        false,
                                                                                        Period(6000, "m1")))), "white")))
                           table.insert(wave)
                           val read = table.readAll()
                           assertEquals(wave, read.first())
                       }
                   }
                   on("timetest") {
                       val table = persister.Table(Candle::class)
                       table.persist()
                       val candles = (0 until 50).map {
                           Candle(it * 60L,
                                  Tick(it * 60L + 1, 500.0, OfferSide.BID),
                                  (0..6).map { t -> Tick(it * 60L + 2 + t, 498.0, OfferSide.BID) })
                       }
                       logger.info { "starting inserting" }
                       table.insert(candles)
                       logger.info { "finished inserting" }
                   }
                   afterGroup { database.delete() }
               }
           })