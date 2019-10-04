/*
 * Copyright (c) 2018. Martin Heinrich - All Rights Reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package org.daiv.reflection.read

import org.daiv.reflection.common.*
import java.sql.SQLException
import kotlin.reflect.KClass

internal class ReadSimpleType<R : Any, T : Any>(override val propertyData: PropertyData<R, T, T>, override val prefix: String?) :
        SimpleTypes<R, T> {

    override fun toTableHead() = toTableHead(propertyData.clazz, prefixedName)

    override fun getValue(readValue: ReadValue, number: Int, key: List<Any>): NextSize<T> {
        try {
            val any = readValue.getObject(number, propertyData.clazz as KClass<Any>)
            val x = if (propertyData.clazz == Boolean::class)
                any == 1
            else
                any
            @Suppress("UNCHECKED_CAST")
            return NextSize(x as T, number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    override fun makeString(any: T): String {
        val s = any.toString()
        return when {
            any::class == String::class -> "\"" + s + "\""
            any::class == Boolean::class -> if (s.toBoolean()) "1" else "0"
//            any::class.isEnum() -> "\"${(any as Enum<*>).name}\""
            else -> s
        }
    }

    companion object {
        private val valueMappingJavaSQL = mapOf("long" to "bigInt", "String" to "Text")

        internal fun <T : Any> toTableHead(clazz: KClass<T>, name: String): String {
            val simpleName = clazz.simpleName
            val typeName = valueMappingJavaSQL[simpleName] ?: simpleName
            return "$name $typeName${" NOT NULL"}"
        }
    }
}


internal data class SimpleHashCodeable(val fieldReadable: FieldReadable<Any, Any>) : HashCodeable<Any>,
                                                                                     FieldReadable<Any, Any> by fieldReadable {
    override fun plainHashCodeX(o: Any): Int {
        return when (o::class) {
            Int::class -> {
                o as Int
            }
            Double::class -> {
                o as Double
                o.hashCodeX()
            }
            Long::class -> {
                o as Long
                o.hashCodeX()
            }
            Boolean::class -> {
                o as Boolean
                o.hashCodeX()
            }
            String::class -> {
                o as String
                o.hashCodeX()
            }
            else -> throw RuntimeException("cannot determine type: $o")
        }
    }

    override fun hashCodeX(r: Any): Int {
        val o = getObject(r)
        return plainHashCodeX(o)
    }
}

object EnumHashCodeable : HashCodeable<Any> {
    override fun plainHashCodeX(t: Any): Int {
        return t.toString()
                .hashCodeX()
    }

    override fun hashCodeX(t: Any): Int {
        return plainHashCodeX(t)
    }

    override fun getObject(o: Any) = o
}

internal class MapHashCodeable<M : Any, T : Any> constructor(val keyHashCodeable: KeyHashCodeable,
                                                             val valueHashCodeable: KeyHashCodeable,
                                                             val readable: MapReadable<Any, M, T>) : HashCodeable<Map<M, T>>,
                                                                                                     FieldReadable<Any, Map<M, T>> by readable {
    override fun plainHashCodeX(t: Any): Int {
        try {
            t as Map<M, T>
        } catch (t: Throwable) {
            throw t
        }
        return t.iterator()
                .hashCodeX { keyHashCodeable.hashCodeX(this.key) xor valueHashCodeable.hashCodeX(this.value) }
    }

    override fun hashCodeX(t: Any) = getObject(t).iterator()
            .hashCodeX { keyHashCodeable.hashCodeX(this.key) xor valueHashCodeable.hashCodeX(this.value) }
}

internal class ListHashCodeable<T : Any> constructor(val valueHashCodeable: HashCodeable<Any>, listReadable: ListReadable<T>) :
        HashCodeable<List<T>>, FieldReadable<Any, List<T>> by listReadable {
    override fun plainHashCodeX(t: Any): Int {
        try {
            t as List<Any>
        } catch (t: Throwable) {
            throw t
        }
        return t.iterator()
                .hashCodeX { valueHashCodeable.hashCodeX(this) }
    }

    override fun hashCodeX(r: Any): Int {
        val t = getObject(r)
        return t.iterator()
                .hashCodeX { valueHashCodeable.hashCodeX(this) }
    }
}

internal class SetHashCodeable(val valueHashCodeable: HashCodeable<Any>, setReadable: PropertyReader<Any, Set<Any>>) :
        HashCodeable<Set<Any>>, FieldReadable<Any, Set<Any>> by setReadable {
    override fun plainHashCodeX(t: Any): Int {
        try {
            t as Set<Any>
        } catch (t: Throwable) {
            throw t
        }
        return t.iterator()
                .hashCodeX { valueHashCodeable.hashCodeX(this) }
    }

    override fun hashCodeX(t: Any) = getObject(t).iterator().hashCodeX { valueHashCodeable.hashCodeX(this) }
}

internal class ComplexHashCodeable(val clazz: KClass<Any>,
                                   val provider: HashCodeableProvider,
                                   val complexReadable: FieldReadable<Any, Any>) :
        HashCodeable<Any>,
        FieldReadable<Any, Any> {

    init {
        provider.register(clazz)
    }

    val key: KeyHashCodeable
        get() = provider.hashCodeable(clazz)

    override fun getObject(o: Any): Any {
        return complexReadable.getObject(o)
    }

    override fun hashCodeX(t: Any): Int {
        return key.hashCodeX(getObject(t))
    }

    override fun plainHashCodeX(t: Any): Int {
        return key.hashCodeX(t)
    }
}

internal class KeyHashCodeable constructor(val list: List<HashCodeable<out Any>>) : HashCodeable<List<Any>> {
    override fun getObject(o: Any): List<Any> {
        return list.map { it.getObject(o) }
    }

    override fun plainHashCodeX(t: Any): Int {
        try {
            t as List<Any>
        } catch (t: Throwable) {
            throw t
        }
        return list.mapIndexed { i, e -> e.plainHashCodeX(t[i]) }
                .hashCodeX()
    }

    override fun hashCodeX(r: Any): Int {
        try {
            return list.mapIndexed { i, e -> e.hashCodeX(r) }
                    .hashCodeX()
        } catch (t: Throwable) {
            throw t
        }
    }
}


