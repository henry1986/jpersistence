package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.annotations.MoreKeys


enum class X1 { B1, B2, B3 }
enum class X2(private val s: String) {
    B1("1"), B2("2"), B3("3");
}


enum class OfferSide {
    BID, ASK;
}

interface Point {
    val time: Long
    val value: Double
    val isLong: Boolean
}

data class Period(val interval: Long, val name: String)

interface Timeable {
    val time: Long
    val period: Period
}

data class Tick(val time: Long, val value: Double, val offerSide: OfferSide)

@MoreKeys(2)
data class WavePoint(val tick: Tick, override val period: Period, override val isLong: Boolean) : Point, Timeable {
    constructor(tick: Tick, isLong: Boolean, period: Period) : this(tick, period, isLong)

    override val time = tick.time
    override val value = tick.value
}

@MoreKeys(2)
data class WPDescriptor(val wp1: WavePoint, val wp2: WavePoint, val wpWaves: List<WPWave>)

@MoreKeys(1, true)
data class WPWave(val wpDescriptors: List<WPDescriptor>)

