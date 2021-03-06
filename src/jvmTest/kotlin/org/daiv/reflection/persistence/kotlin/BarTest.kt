package org.daiv.reflection.persistence.kotlin

import kotlin.test.Test
import mu.KotlinLogging
import org.daiv.reflection.database.DatabaseHandler
import org.daiv.reflection.annotations.Including
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.common.FieldDataFactory
import org.daiv.reflection.common.asList
import org.daiv.reflection.common.createHashCode
import org.daiv.reflection.common.hashCodeX
import org.daiv.reflection.database.persister
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.PersisterPreference
import org.daiv.util.json.log
import org.junit.AfterClass
import kotlin.reflect.KClass
import kotlin.test.AfterTest
import kotlin.test.assertEquals

@MoreKeys(2)
data class Candle(val time: Long, val period: Period, val startTick: Tick, val tickList: List<Tick>)

data class Bar(val x: Int, val s: String)

@MoreKeys(1, true)
data class Wave(val wavePoints: List<WavePoint>)

@MoreKeys(2)
data class CWaveSet(val wave1: Wave, val wave2: Wave)

@MoreKeys(1, true)
data class TickList(val ticks: List<Tick>)

@MoreKeys(1, true)
data class TickList2(val ticks: List<Tick>)

@MoreKeys(2)
data class ComplexOfAuto(val id: Int, val tickList: TickList2)

data class HigherComplexOfAuto(val complexOfAuto: ComplexOfAuto, val s: String)

@MoreKeys(2, true)
data class ManyListsTickList(val ticks: List<Tick>, val ticks2: List<Tick>)

val m1 = Period(60000, "m1")

@MoreKeys(2)
data class Descriptor(val start: WavePoint, val end: WavePoint, val isValid: Boolean)

@MoreKeys(2)
data class ComplexObject(val x: Int, val y: Int, val s: String)

data class ListHolder(val x: Int, val list: List<ComplexObject>)

data class WaveHolder(val wave: Wave)

data class RawWave(val id: Int, val wavePoints: List<WavePoint>) {
    constructor(wavePoints: List<WavePoint>) : this(wavePoints.hashCode(), wavePoints)
}

data class DrawnWave(val id: Int, val rawWave: RawWave, val color: String) {
    constructor(rawWave: RawWave, color: String) : this(rawWave.hashCode() + 31 * color.hashCode(), rawWave, color)
}

data class WaveSet(val id: Int, val waves: List<DrawnWave>) {
    constructor(waves: List<DrawnWave>) : this(waves.hashCode(), waves)
}

@Including
data class SameHashCodeTest(val x1: Int, val x2: Int)

@MoreKeys(auto = true)
data class ComplexSameHashCode(val sameHashCodeTest: SameHashCodeTest)

data class ReadComplexSameHashCode(val x: Int, val complexSameHashCode: ComplexSameHashCode)
data class ReadComplexSameHashCode2(val complexSameHashCode: ComplexSameHashCode, val x: Int)

@MoreKeys(auto = true)
data class ReadListSameHashCode(val list: List<SameHashCodeTest>)

open class BarTest {
    val logger = KotlinLogging.logger { }
    val persister = persister("BarTest.db")
    val doubleRefPersister = Persister("BarTestDoubleRef.db", PersisterPreference(1000000))
    @AfterTest
    fun afterTest() {
        persister.delete(); doubleRefPersister.delete()
    }
    companion object {
//        @AfterClass
//        @JvmStatic
//        fun afterTest() {
//            persister.delete(); doubleRefPersister.delete()
//        }
    }
}
class FromTo : BarTest(){
    val list = (0 until 100).map { Bar(it, " - $it - ") }
        .toList()
    val table = persister.Table(Bar::class)
    init {
        table.persist()
        table.insert(list)
    }
    @Test fun fromTo(){
        val read = table.read(5, 10)
        assertEquals(list.subList(5, 11), read)
    }
    @Test fun first(){
        assertEquals(Bar(0, " - 0 - "), table.first())
    }
    @Test fun last(){
        assertEquals(Bar(99, " - 99 - "), table.last())
    }
    @Test fun size(){
        assertEquals(100, table.size())
    }
    @Test fun readLastBefore(){
        val taken = list.take(10)
        assertEquals(taken, table.readLastBefore(10, 10))
    }
    @Test fun readLastBefore2(){
        assertEquals(list.subList(5, 10), table.readLastBefore(5, 10))
    }
    @Test fun readNextAfter(){
        assertEquals(list.subList(6, 11), table.readNextAfter(5, 5))
    }
    @Test fun testHashcode(){
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
    @Test fun changeOrderTest(){
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

class ComplexReadOrdered : BarTest(){
    val table = persister.Table(WavePoint::class)
    init {
        table.persist()
    }
    @Test fun readOrdered(){
        val w1 = WavePoint(Tick(5000, 5.0, OfferSide.BID), true, m1)
        val w2 = WavePoint(Tick(6000, 5.0, OfferSide.BID), true, m1)
        val w3 = WavePoint(Tick(7000, 5.0, OfferSide.BID), true, m1)
//        table.truncate()
        table.insert(listOf(w1, w3, w2))
        val read = table.readOrdered("isLong", true)
        assertEquals(listOf(w1, w2, w3), read)
    }
    @Test fun readMaxOrdered(){
//        table.clear()
        val w1 = WavePoint(Tick(6000, 5.0, OfferSide.BID), true, m1)
        val w2 = WavePoint(Tick(7000, 5.0, OfferSide.BID), true, m1)
        val w3 = WavePoint(Tick(7500, 5.0, OfferSide.BID), true, m1)
        val w4 = WavePoint(Tick(8000, 5.0, OfferSide.BID), true, m1)
        val w5 = WavePoint(Tick(9000, 5.0, OfferSide.BID), true, m1)
        val w6 = WavePoint(Tick(9500, 5.0, OfferSide.BID), true, m1)
//        table.truncate()
        table.insert(listOf(w4, w1, w5, w6, w3, w2))
        val read = table.read(7500, 10000, 3)
        assertEquals(listOf(w3, w4, w5), read)
    }
}
class Timetest : BarTest(){
    val table = persister.Table(Candle::class)
    init {
        table.persist()
    }
    @Test fun insertCandles(){
        val candles = (0 until 50).map {
            Candle(it * 60L, m1,
                Tick(it * 60L + 1, 500.0, OfferSide.BID),
                (0..6).map { t -> Tick(it * 60L + 2 + t, 498.0, OfferSide.BID) })
        }
        logger.info { "starting inserting" }
        table.insert(candles)
        logger.info { "finished inserting" }
        val read = table.readAll()
        assertEquals(candles, read)
    }
}
class MoreThanOneKeyTest : BarTest(){
    @Test fun test2(){
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
        val first = table.first()
        assertEquals(descriptor, first!!)
    }
    @Test fun testAutoIdTicks(){
        val table = persister.Table(TickList::class)
        table.persist()
        val wp1 = Tick(500000L, 0.5, OfferSide.BID)
        val wp2 = Tick(650000L, 0.9, OfferSide.BID)
        val wp3 = Tick(620000L, 1.9, OfferSide.BID)
        val wave1 = TickList(listOf(wp1, wp2, wp3))
        val hashCode = TickList::class.createHashCode(listOf(wave1.ticks))
        assertEquals(501330605, hashCode)
        table.insert(wave1)
        val read = table.read(listOf(wp1, wp2, wp3))
        assertEquals(wave1, read)
    }
    @Test fun testAutoId(){
        val table = persister.Table(Wave::class)
        table.persist()
        val wp1 = WavePoint(Tick(500000L, 0.5, OfferSide.BID), true, m1)
        val wp2 = WavePoint(Tick(650000L, 0.9, OfferSide.BID), false, m1)
        val wp3 = WavePoint(Tick(620000L, 1.9, OfferSide.BID), true, m1)
        val wave1 = Wave(listOf(wp1, wp2, wp3))
        table.insert(wave1)
        table.clearCache()
        val read = table.read(listOf(wp1, wp2, wp3))
        assertEquals(wave1, read)
    }
    @Test fun testAutoIdManyListsTicks(){
        val table = persister.Table(ManyListsTickList::class)
        table.persist()
        val wp1 = Tick(500000L, 0.5, OfferSide.BID)
        val wp2 = Tick(650000L, 0.9, OfferSide.BID)
        val wp3 = Tick(620000L, 1.9, OfferSide.BID)
        val wave1 = ManyListsTickList(listOf(wp1, wp2, wp3), listOf(wp2, wp3))
        table.insert(wave1)
        val read = table.read(listOf(listOf(wp1, wp2, wp3), listOf(wp2, wp3)))
        assertEquals(wave1, read)
    }
    @Test fun testComplexKeyWithMorePrimaryKeysInList(){
        val table = persister.Table(ListHolder::class)
        table.persist()
        val c1 = ComplexObject(1, 1, "hallo1")
        val c2 = ComplexObject(1, 2, "hallo2")
        val c3 = ComplexObject(1, 3, "hallo3")
        val c4 = ComplexObject(1, 4, "hallo4")
        val lists = listOf(ListHolder(2, listOf(c1, c2)),
            ListHolder(3, listOf(c1, c2, c3)),
            ListHolder(4, listOf(c3, c4)))
        table.insert(lists)
        val read = table.readAll()
        assertEquals(lists[0], read[0], "0")
        assertEquals(lists[1], read[1], "1")
        assertEquals(lists[2], read[2], "2")
        assertEquals(lists, read, "all")
    }
    @Test fun testAutoIdInComplexObject(){
        val table = persister.Table(HigherComplexOfAuto::class)
        table.persist()
        val wp1 = Tick(500000L, 0.5, OfferSide.BID)
        val wp2 = Tick(650000L, 0.9, OfferSide.BID)
        val wp3 = Tick(620000L, 1.9, OfferSide.BID)
        val list1 = listOf(wp1, wp2)
        val list2 = listOf(wp1, wp3)
        val complexIfAutos = listOf(ComplexOfAuto(1, TickList2(list1)), ComplexOfAuto(1, TickList2(list2)))
        val higher = listOf(HigherComplexOfAuto(complexIfAutos.first(), "Hello"),
            HigherComplexOfAuto(complexIfAutos.last(), "World"))
        table.insert(higher)
        assertEquals(higher.toSet(), table.readAll().toSet())
    }
}

fun Persister.testDoubleReference(testName: String, initBlock: () -> Unit = {}) {
            val table = Table(WPDescriptor::class)
            val wpTable = Table(WavePoint::class)
            val tickTable = Table(Tick::class)

            table.persist()

            val wp1 = WavePoint(Tick(100L, 1.0, OfferSide.BID), true, m1)
            val wp2 = WavePoint(Tick(200L, 2.0, OfferSide.BID), false, m1)
            val wp3 = WavePoint(Tick(300L, 3.0, OfferSide.BID), true, m1)
            val wp4 = WavePoint(Tick(400L, 4.0, OfferSide.BID), false, m1)
            val des1 = WPDescriptor(wp1, wp2, emptyList())
            val des2 = WPDescriptor(wp2, wp3, emptyList())
            val des3 = WPDescriptor(wp3, wp4, emptyList())
            val wave1 = WPWave(listOf(des1, des2))

            val des4 = WPDescriptor(wp1, wp3, listOf(wave1))

            val wave2 = WPWave(listOf(des4, des3))
            val hashCodeDes1 = WPDescriptor::class.createHashCode(listOf(des1.wp1, des1.wp2))

            val hashCode1 = WPWave::class.createHashCode(listOf(wave1.wpDescriptors))
            val hashCode2 = WPWave::class.createHashCode(listOf(wave2.wpDescriptors))

            val des6 = WPDescriptor(wp1, wp4, listOf(wave2))

            table.insert(des6)

            val read = table.read(listOf(wp1, wp4))!!

            assertEquals(des6, read)

//                           table.insert(des4)
//                           val read = table.read(listOf(wp1, wp3))!!
//                           assertEquals(des4, read)
}

class TestDoubleReference :BarTest(){
    @Test
    fun testPersister() {
        persister.testDoubleReference("without cache")
    }
    @Test
    fun testDoubleRef(){
        doubleRefPersister.testDoubleReference("with cache")
    }
}

class TestSameTablesDifferentTableNames : BarTest(){

    fun getCandles(value: Double): List<Candle> {
        val candles = (0 until 50).map {
            Candle(it * 60L, m1,
                Tick(it * 60L + 1, value, OfferSide.BID),
                (0..5).map { t -> Tick(it * 60L + 2 + t, value, OfferSide.BID) })
        }
        return candles
    }

    fun createTable(tableNamePrefix: String, value: Double) {
        val test1 = persister.Table(Candle::class, tableNamePrefix = tableNamePrefix)
        test1.persist()
        val candles = getCandles(value)
        test1.insert(candles)
        val ticks = persister.Table(Tick::class, tableNamePrefix = tableNamePrefix)
        val read = ticks.readAll()
        assertEquals(350, read.size)
        read.forEach {
            assertEquals(value, it.value)
        }
    }
    @Test fun test(){
        createTable("test1", 200.0)
        createTable("test2", 300.0)
        createTable("test3", 400.0)
    }
}


val c1 = ComplexSameHashCode(SameHashCodeTest(1, 32))
val c2 = ComplexSameHashCode(SameHashCodeTest(2, 1))
class TestSameHashcode : BarTest(){
    @Test fun testRead(){
        val table = persister.Table(ComplexSameHashCode::class)
        table.persist()
        val list = listOf(c1, c2)
        table.insert(list)
        val read = table.readAll()
        val hash1 = listOf(1, 32).hashCodeX() + 31
        val hash2 = listOf(2, 1).hashCodeX() + 31
        logger.trace { "h1: $hash1, h2: $hash2" }
        val hashCodeRead = table.readHashCode(1055, persister.readCache())
//                           logger.trace { "hashCodeRead: $hashCodeRead" }
        hashCodeRead.log(logger, "hashCodeRead")
        assertEquals(list, read)
    }
    @Test fun testListSameHashCode(){
        val table = persister.Table(ReadListSameHashCode::class)
        table.persist()
        val r1 = ReadListSameHashCode(listOf(c1.sameHashCodeTest))
        val r2 = ReadListSameHashCode(listOf(c2.sameHashCodeTest))
        val rList = listOf(r1, r2)
        table.insert(rList)
        val read = table.readAll()
        assertEquals(rList, read)
    }
}
class ReadComplex : BarTest(){
    val table = persister.Table(ReadComplexSameHashCode::class)
    init {
        table.persist()
    }
    val c1 = ComplexSameHashCode(SameHashCodeTest(3, 94))
    val c2 = ComplexSameHashCode(SameHashCodeTest(4, 63))
    val r1 = ReadComplexSameHashCode(2, c1)
    val r2 = ReadComplexSameHashCode(3, c2)
    val rList = listOf(r1, r2)
    init {
        table.insert(rList)
    }
    @Test fun testReadComplexByReadAll(){
        val read = table.readAll()
        assertEquals(rList, read)
    }
    @Test fun testReadComplexByReadFromKey(){
        assertEquals(r1, table.read(2))
        assertEquals(r2, table.read(3))
    }
}
class ReadComplex2 : BarTest(){
    val table = persister.Table(ReadComplexSameHashCode2::class)
    init {
        table.persist()
    }
    val c1 = ComplexSameHashCode(SameHashCodeTest(3, 94))
    val c2 = ComplexSameHashCode(SameHashCodeTest(4, 63))
    val r1 = ReadComplexSameHashCode2(c1, 2)
    val r2 = ReadComplexSameHashCode2(c2, 3)
    val rList = listOf(r1, r2)
    init {
        table.insert(rList)
    }
    @Test fun testReadComplexByReadAll(){
        val read = table.readAll()
        assertEquals(rList, read)
    }
    @Test fun testReadComplexByReadFromKey(){
        assertEquals(r1, table.read(c1))
        assertEquals(r2, table.read(c2))
    }
}
class ReadAutoKeyObjectAsKey : BarTest(){
    val table = persister.Table(WaveHolder::class)
    init {
        table.persist()
    }
    val wp1 = WavePoint(Tick(1600000L, 0.5, OfferSide.BID), true, m1)
    val wp2 = WavePoint(Tick(1650000L, 0.9, OfferSide.BID), false, m1)
    val wp3 = WavePoint(Tick(1680000L, 1.9, OfferSide.BID), true, m1)
    val wp4 = WavePoint(Tick(1820000L, 1.9, OfferSide.BID), false, m1)
    val w1 = WaveHolder(Wave(listOf(wp1, wp2, wp3)))
    val w2 = WaveHolder(Wave(listOf(wp2, wp3, wp4)))
    val list = listOf(w1, w2)
    @Test fun testRead(){
        table.insert(list)
        table.clearCache()
        val read = table.readAll()
        assertEquals(list.toSet(), read.toSet())
    }
}

class InsertComplexKey : BarTest(){
    init {
        persister.clearCache()
    }
    val table = persister.Table(CWaveSet::class)
    init {
        table.persist()
    }
    val wp1 = WavePoint(Tick(1600000L, 0.5, OfferSide.BID), true, m1)
    val wp2 = WavePoint(Tick(1650000L, 0.9, OfferSide.BID), false, m1)
    val wp3 = WavePoint(Tick(1680000L, 1.9, OfferSide.BID), true, m1)
    val wp4 = WavePoint(Tick(1820000L, 1.9, OfferSide.BID), false, m1)
    val wave1 = Wave(listOf(wp1, wp2))
    val wave2 = Wave(listOf(wp3, wp4))
    val waveSet = CWaveSet(wave1, wave2)
    @Test fun testComplexHashCodeXKey(){
        table.insert(waveSet)
        persister.clearCache()
        val read = table.readAll()
        assertEquals(waveSet.asList(), read)
    }

}
