package org.daiv.reflection.persistence.kotlin

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
