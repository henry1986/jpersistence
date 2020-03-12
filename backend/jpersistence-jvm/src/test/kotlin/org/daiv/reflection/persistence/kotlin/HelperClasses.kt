package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.Table
import org.daiv.reflection.plain.HashCodeKey
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.ObjectKeyToWrite
import org.daiv.reflection.plain.PersistenceKey
import org.jetbrains.spek.api.dsl.ActionBody
import org.jetbrains.spek.api.dsl.SpecBody
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.reflect.KClass
import kotlin.test.assertEquals


internal enum class X1 { B1, B2, B3 }

internal data class JustIt(val y: Int, val x: String)
internal enum class X2(val s: String, y: String) {
    B1("1", "one"), B2("2", "two"), B3("3", "three");

    val justIt = JustIt(this.ordinal, y)
}

internal fun <T : Any> ActionBody.testRead(table: Table<T>,
                                           list: List<T>,
                                           toAssert: T,
                                           key: List<Any>,
                                           block: Table<T>.() -> Unit) {
    it("test readAll") {
        table.persister.clearCache()
        val read = table.readAll()
        assertEquals(list.toSet(), read.toSet())
    }
    it("test single read") {
        table.persister.clearCache()
        val read = table.read(key)
        assertEquals(toAssert, read)
    }
    table.block()
}

fun <T : Any> Persister.testTable(sBody: ActionBody,
                                  clazz: KClass<T>,
                                  listToInsert: List<T>,
                                  key: List<Any>,
                                  toAssert: T,
                                  block: Table<T>.() -> Unit = {}) {
    val table: Table<T> = Table(clazz)
    table.persist()
    table.insert(listToInsert)
    sBody.testRead(table, listToInsert, toAssert, key, block)
}


internal interface TheInterface {
    val x: Int
}

internal interface RunX {
    fun testX(): Boolean
}

internal data class RunXImpl(val z: Int) : RunX {
    override fun testX(): Boolean {
        return true
    }
}

sealed class SealedInterface

object SealedObject : SealedInterface()

data class SealedData(val x: Long) : SealedInterface()

data class SealedHolder(val key: Long, val sealedInterface: SealedInterface)
@MoreKeys(2)
data class SealedKeyHolder(val key: SealedInterface, val y: SealedInterface, val x: String)

data class SealedKeyHolder2(val key: SealedInterface, val y: SealedInterface?, val x: String)

internal data class ObjectKeyToWriteForTest(val isAuto: Boolean,
                                            val theKey: List<Any>,
                                            val theObject: Any,
                                            val theHashCode: Int? = null) : ObjectKeyToWrite {
    override fun isAutoId(): Boolean {
        return isAuto
    }

    override fun theObject(): Any {
        return theObject
    }

    override fun itsKey(): List<Any> {
        return theKey
    }

    override fun itsHashCode(): Int {
        return theHashCode!!
    }

    override fun keyToWrite(counter: Int?): List<Any> {
        return if (isAuto) listOf(theHashCode!!, counter!!) else theKey
    }

    override fun toObjectKey(hashCounter: Int?): ObjectKey {
        return if (isAuto) HashCodeKey(theKey, theHashCode!!, hashCounter!!) else PersistenceKey(theKey)
    }
}

object TestSingleton : RunX {
    override fun testX(): Boolean {
        return false
    }

    override fun toString(): String {
        return "MyTestSingleton"
    }
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

