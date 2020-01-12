package org.daiv.reflection.common

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import mu.KotlinLogging
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.persister.InsertCachePreference
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.Table
import org.daiv.reflection.persister.PersisterPreference
import kotlin.reflect.KClass


const val numberOfWP = 1000
const val sequential: Boolean = true
const val completable: Boolean = true
const val numberOfCollectors = 50
//const val sequential: Boolean = true

private data class Period(val string: String)
private data class Tick(val time: Long, val string: String, val value: Double)

@MoreKeys(2)
private data class Candle(val time: Long, val period: Period, val string: String, val ticks: List<Tick>)

@MoreKeys(2)
private data class WavePoint(val tick: Tick, val period: Period, val string: String, val string2: String)

@MoreKeys(2)
private data class CandleList(val tick: Tick, val period: Period, val candles: List<Candle>)

@MoreKeys(2)
private data class PeriodMap(val tick: Tick,
                             val period: Period,
                             val candleList: CandleList,
                             val wavePoints: List<WavePoint>,
                             val descriptorBuilder: DescriptorBuilder)

@MoreKeys(1, auto = true)
private data class Wave(val descriptors: List<Descriptor>)

@MoreKeys(2)
private data class Descriptor(val wp1: WavePoint, val wp2: WavePoint, val waves: List<Wave>)

@MoreKeys(2)
private data class DescriptorBuilder(val tick: Tick, val period: Period, val waves: List<Wave>)

private data class Collector(val tick: Tick, val map: Map<Period, PeriodMap>)

private fun Period.periodMap(): PeriodMap {
    val w = numberOfWP
    val p = w * 2
    val n = p * 6 + 6
    val ticks = (0..n).map { Tick(it * 100L, "$it - value", (1000L + it).toDouble()) }
    val lastTick = Tick(ticks.last().time + 100L, "lastTick", 0.5)
    val candles = (0..p).map {
        val i = it * 6
        Candle(ticks[i].time, this, "c - $it", ticks.subList(i, i + 6))
    }
    val wavePoints = (0..w).map { WavePoint(ticks[12 * it], this, "w1 $it", "w2 $it") }
    return PeriodMap(lastTick,
                     this,
                     CandleList(lastTick, this, candles),
                     wavePoints,
                     DescriptorBuilder(ticks.last(), this, wavePoints.createWaves()))
}

private fun Tick.newTick(addon: Long = 10L): Tick {
    return Tick(time + addon, "candleTick", 0.6)
}

private fun CandleList.next(next: Tick): CandleList {
    val start = next.newTick()
    val p1 = start.newTick()
    val p2 = p1.newTick()
    val p3 = p2.newTick()
    val p4 = p3.newTick()
    val p5 = p4.newTick()
    val last = candles.last()
    return copy(next,
                last.period,
                candles + Candle(start.time, candles.last().period, "nextCandle", listOf(start, p1, p2, p3, p4, p5, next)))
}

private fun PeriodMap.next(next: Tick): PeriodMap {
    return copy(tick = next, candleList = candleList.next(next))
}

private fun Collector.append(): Collector {
    val next = tick.newTick(100000L)
    return copy(tick = next, map = map.values.map { it.period to it.next(next) }.toMap())
}

private fun List<Collector>.buildList(max: Int = numberOfCollectors): List<Collector> {
    if (size < max) {
        return (this + last().append()).buildList()
    }
    return this
}

private fun newCandle(t: Tick = Tick(0L, "t1", 0.5)): Candle {
    val t2 = t.newTick()
    val t3 = t2.newTick()
    return Candle(t.time, Period("1"), "candle", listOf(t, t2, t3))
}

private fun WorkData<Collector>.checkCollector(all: List<Collector>) {
    if (all == list) {
        logger.info { "is equal" }
    } else {
        logger.info { "read: ${all.size} - vs - ${list.size}" }
        if (all.size == list.size) {
            val firstA = all.first()
            val firstB = list.first()
            if (firstA != firstB) {
                if (firstA.tick == firstB.tick) {
                    logger.info { "tick equals" }
                } else {
                    logger.info { "tick not equals" }
                }
            }
        }
    }
}

fun main(args: Array<String>) {
    System.setProperty("logback.configurationFile", "logback.xml")
    val persister = Persister("PerformanceTest.db", PersisterPreference(1000000))
    try {
        val w = persister.createCollector()
//        val w = persister.createCandle()
//        val w = persister.createCandleList()
//        val w = persister.createPeriodMap()
        w.store()
    } finally {
        persister.delete()
    }
}


private data class WorkData<T : Any>(val table: Table<T>, val list: List<T>, val check: WorkData<T>.(List<T>) -> Unit) {
    val logger = KotlinLogging.logger {}
    fun store() {
        val cache = runBlocking(Dispatchers.Default) {
            logger.info { "start Caching ${if (!sequential) "parallel" else "sequential"}, numberOfWP: $numberOfWP, numberOfCollectors: $numberOfCollectors" }
            val r = if (sequential) {
                table.InsertCacheSeriell(InsertCachePreference(true, false))
            } else {
                table.InsertCacheParallel(CoroutineScope(newFixedThreadPoolContext(8, "actorContext")),
                                          this,
                                          completable,
                                          InsertCachePreference(true, false))
            }
            logger.info { "start inserting" }
            r.insert(list)
            r
        }
        logger.info { "caching done" }
        cache.commit()
        logger.info { "committed" }
//        table.insert(list)
        check(table.readAll())
    }
}

private fun List<Descriptor>.toWaves(): List<Wave> {
    val n = (0..(size / 4 - 4)).map { Wave(subList(it * 4, it * 4 + 4)) }
    val mod = size % 4
    return if (mod != 0) {
        n + Wave(takeLast(mod))
    } else {
        n
    }
}

private fun List<WavePoint>.createWaves(): List<Wave> {
    val lowest = dropLast(1).mapIndexed { i, e ->
        Descriptor(e, get(i + 1), emptyList())
    }
    return lowest.toWaves()
            .map { Descriptor(it.descriptors.first().wp1, it.descriptors.last().wp2, listOf(it)) }
            .toWaves()
}

private fun Persister.createCollector(): WorkData<Collector> {
    val map = (0..4).map { Period("$it") }
            .map { it to it.periodMap() }
            .toMap()
    val tick = map.values.find { true }!!.tick
    val c = Collector(tick, map)
    val list = listOf(c).buildList()
    val table = Table(Collector::class)
    table.persist()
    return WorkData(table, list) { checkCollector(it) }
}

private fun Persister.createCandle() = create(Candle::class) {
    it.list.first()
            .map.values.first()
            .candleList.candles.first()
}

private fun <T : Any> Persister.create(clazz: KClass<T>, block: (WorkData<Collector>) -> T): WorkData<T> {
    val c = createCollector()
    val table = Table(clazz)
    table.persist()
    return WorkData(table, listOf(block(c))) {
        if (list == it) {
            logger.info { "${clazz.simpleName} was equal" }
        } else {
            logger.error { "${clazz.simpleName} was different" }
        }
    }
}

private fun Persister.createCandleList() = create(CandleList::class) {
    it.list.first()
            .map.values.first()
            .candleList
}

private fun Persister.createPeriodMap() = create(PeriodMap::class) {
    it.list.first()
            .map.values.first()
}

fun doit(coroutineScope: CoroutineScope, delay: Long) {
    val logger = KotlinLogging.logger {}
    coroutineScope.launch {
        delay(delay)
        logger.info { "first" }
    }
}

private data class Test(val x: Long, val c: CompletableDeferred<Boolean>)

private fun checkActor(coroutineScope: CoroutineScope, second: CoroutineScope): SendChannel<Test> {
    val logger = KotlinLogging.logger {}
    return coroutineScope.actor<Test> {
        for (i in channel) {
            logger.info { "starting delay: ${i.x}" }
            delay(i.x)
            second.launch {
                i.c.complete(true)
                delay(200L)
                coroutineScope.launch {
                    delay(200L)
                    logger.info { "delay: $i" }
                }
            }
        }
    }
}

//fun main(args: Array<String>) {
//    val logger = KotlinLogging.logger {}
//    runBlocking(Dispatchers.Default) {
//        val actor = checkActor(CoroutineScope(Dispatchers.Default), this)
//
////        val actor = checkActor(this, this)
//        logger.info { "starting" }
//        launch {
//            val c = CompletableDeferred<Boolean>()
//            actor.send(Test(3000L, c))
//            val b:Boolean = c.await()
//        }
//        launch {
//            delay(2000L)
//            logger.info { "second" }
//        }
//        GlobalScope.launch {
//            delay(1000L)
//            logger.info { "third" }
//        }
//        logger.info { "all started" }
//    }
//    logger.info { "done" }
//}

