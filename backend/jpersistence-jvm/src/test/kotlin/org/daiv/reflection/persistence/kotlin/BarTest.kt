package org.daiv.reflection.persistence.kotlin

import mu.KotlinLogging
import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.annotations.MoreKeys
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
               data class WavePoint(val tick: Tick, override val isLong: Boolean, override val period: Period) : Point, Timeable {
                   override val time = tick.time
                   override val value = tick.value
               }

               @MoreKeys(1, true)
               data class Wave(val wavePoints:List<WavePoint>)

               val m1 = Period(60000, "m1")

               @MoreKeys(2)
               data class Descriptor(val start: WavePoint, val end: WavePoint, val isValid: Boolean)

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
                                                                                        m1),
                                                                              WavePoint(Tick(1002L, 503.0, OfferSide.BID),
                                                                                        false,
                                                                                        m1))), "white")))
                           table.insert(wave)
                           val read = table.readAll()
                           assertEquals(wave, read.first())
                       }
                       it("changeOrderTest") {
                           table.insert(Bar(500, "500"))
                           table.insert(Bar(600, "500"))
                           table.insert(Bar(550, "500"))
                           val last = table.readAll()
                                   .last()
                           assertEquals(600, last.x)
                           val list = table.read(500, 550)
                                   .map { it.x }
                           assertEquals(listOf(500, 550), list)
                       }

                   }
                   on("complexReadOrdered") {
                       val table = persister.Table(WavePoint::class)
                       it("readOrdered") {
                           val w1 = WavePoint(Tick(5000, 5.0, OfferSide.BID), true, m1)
                           val w2 = WavePoint(Tick(6000, 5.0, OfferSide.BID), true, m1)
                           val w3 = WavePoint(Tick(7000, 5.0, OfferSide.BID), true, m1)
                           table.truncate()
                           table.insert(listOf(w1, w3, w2))
                           val read = table.readOrdered("isLong", true)
                           assertEquals(listOf(w1, w2, w3), read)
                       }
                       it("readMaxOrdered") {
                           table.clear()
                           val w1 = WavePoint(Tick(6000, 5.0, OfferSide.BID), true, m1)
                           val w2 = WavePoint(Tick(7000, 5.0, OfferSide.BID), true, m1)
                           val w3 = WavePoint(Tick(7500, 5.0, OfferSide.BID), true, m1)
                           val w4 = WavePoint(Tick(8000, 5.0, OfferSide.BID), true, m1)
                           val w5 = WavePoint(Tick(9000, 5.0, OfferSide.BID), true, m1)
                           val w6 = WavePoint(Tick(9500, 5.0, OfferSide.BID), true, m1)
                           table.truncate()
                           table.insert(listOf(w4, w1, w5, w6, w3, w2))
                           val read = table.read(7500, 10000, 3)
                           assertEquals(listOf(w3, w4, w5), read)
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
                   on("more than one Key test") {
                       it("test 2") {
                           val table = persister.Table(Descriptor::class)
                           table.persist()
                           val wp1 = WavePoint(Tick(500000L, 0.5, OfferSide.BID), true, m1)
                           val wp2 = WavePoint(Tick(650000L, 0.9, OfferSide.BID), false, m1)
                           val wp3 = WavePoint(Tick(620000L, 1.9, OfferSide.BID), true, m1)
                           val descriptor = Descriptor(wp1, wp2, true)
                           val descriptor2 = Descriptor(wp3, wp2, true)
                           table.insert(descriptor)
                           table.insert(descriptor2)
                           val read = table.readMultiple(listOf(wp1, wp2))
                           assertEquals(descriptor, read)
                       }
                       it("test autoId") {
                           val table = persister.Table(Wave::class)
                           table.persist()
                           val wp1 = WavePoint(Tick(500000L, 0.5, OfferSide.BID), true, m1)
                           val wp2 = WavePoint(Tick(650000L, 0.9, OfferSide.BID), false, m1)
                           val wp3 = WavePoint(Tick(620000L, 1.9, OfferSide.BID), true, m1)
                           val wave1 = Wave(listOf(wp1, wp2, wp3))
                           table.insert(wave1)
//                           val read = table.read(wave1)
//                           assertEquals(wave1, read)
                       }
                   }
                   afterGroup { database.delete() }
               }
           })