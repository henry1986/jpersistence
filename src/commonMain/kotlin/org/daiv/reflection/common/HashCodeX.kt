package org.daiv.reflection.common

import kotlin.jvm.JvmInline

fun Boolean.hashCodeX() = if (this) 1231 else 1237
fun Long.hashCodeX() = (this xor (this shr 32)).toInt()
fun Double.hashCodeX() = toRawBits().hashCodeX()
fun String.hashCodeX(): Int {
    var hash = 1
    for (i in (0 until length)) {
        hash += get(i).toInt() * 31
    }
    return hash
}

//@JvmInline
data class Blub private constructor(val x: Int) {
    constructor(x: Int, s: String) : this(x)
}

fun <T : Any> Iterator<T>.hashCodeX(hashCodeX: T.() -> Int): Int {
    var hashCode = 1
    var prime = 31
    while (hasNext()) {
        hashCode = hashCode * prime + next().hashCodeX()
    }
    return hashCode
}

fun <T : Any> List<T>.hashCodeX(hashCodeX: T.(Int) -> Int): Int {
    var hashCode = 1
    var prime = 31
    var i = 0
    while (i < size) {
        hashCode = hashCode * prime + get(i).hashCodeX(i)
        i++
    }
    return hashCode
}

fun List<Int>.hashCodeX(): Int {
    var hashCode = 1
    var prime = 31
    var i = 0
    while (i < size) {
        hashCode = hashCode * prime + get(i)
        i++
    }
    return hashCode
}
