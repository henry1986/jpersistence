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
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.PlainBooleanObject
import org.daiv.reflection.plain.PlainLongObject
import org.daiv.reflection.plain.PlainObject
import java.sql.SQLException
import kotlin.reflect.KClass

internal class ReadSimpleType constructor(override val propertyData: PropertyData, override val prefix: String?) : SimpleTypes {
    companion object {
        private val valueMappingJavaSQL = mapOf("long" to "bigInt", "String" to "Text")
    }

    override fun toString() = "$name - $typeName"

    override val typeName by lazy {
        val simpleName = propertyData.clazz.simpleName!!
        valueMappingJavaSQL[simpleName] ?: simpleName
    }

    override fun plainType(name: String) = when {
        name != prefixedName -> null
        propertyData.clazz == Long::class -> PlainLongObject(name)
        propertyData.clazz == Boolean::class -> PlainBooleanObject(name)
        else -> PlainObject(name)
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: ObjectKey): NextSize<ReadAnswer<Any>> {
        try {
            val any = if (propertyData.clazz == Long::class) {
                val x = readValue.resultSet.getLong(number)
                if(readValue.resultSet.wasNull()){
                    null
                } else {
                    x
                }
            } else {
                readValue.getObject(number)
            }
            val x = if (propertyData.clazz == Boolean::class)
                any == 1
            else
                any
            @Suppress("UNCHECKED_CAST")
            return NextSize(ReadAnswer(x), number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    override fun makeString(any: Any): String {
        val s = any.toString()
        return when {
            propertyData.clazz == String::class -> "\"" + s + "\""
            propertyData.clazz == Boolean::class -> if (s.toBoolean()) "1" else "0"
//            any::class.isEnum() -> "\"${(any as Enum<*>).name}\""
            else -> s
        }
    }
}


internal data class SimpleHashCodeable(val fieldReadable: FieldReadable) : HashCodeable,
                                                                           FieldReadable by fieldReadable {
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

object EnumHashCodeable : HashCodeable {
    override fun plainHashCodeX(t: Any): Int {
        return t.toString()
                .hashCodeX()
    }

    override fun hashCodeX(t: Any): Int {
        return plainHashCodeX(t)
    }

    override fun getObject(o: Any) = o
}

internal class MapHashCodeable constructor(val keyHashCodeable: KeyHashCodeable,
                                           val valueHashCodeable: KeyHashCodeable,
                                           val readable: MapReadable) : HashCodeable, FieldReadable by readable {
    override fun plainHashCodeX(t: Any): Int {
        try {
            t as Map<Any, Any>
        } catch (t: Throwable) {
            throw t
        }
        return t.iterator()
                .hashCodeX { keyHashCodeable.hashCodeX(this.key) xor valueHashCodeable.hashCodeX(this.value) }
    }

    override fun hashCodeX(t: Any) = (getObject(t) as Map<Any, Any>).iterator()
            .hashCodeX { keyHashCodeable.hashCodeX(this.key) xor valueHashCodeable.hashCodeX(this.value) }
}

internal class ListHashCodeable<T : Any> constructor(val valueHashCodeable: HashCodeable, listReadable: ListReadable<T>) :
        HashCodeable, FieldReadable by listReadable {
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
        t as List<Any>
        return t.iterator()
                .hashCodeX { valueHashCodeable.hashCodeX(this) }
    }
}

internal class SetHashCodeable(val valueHashCodeable: HashCodeable, setReadable: PropertyReader) :
        HashCodeable, FieldReadable by setReadable {
    override fun plainHashCodeX(t: Any): Int {
        try {
            t as Set<Any>
        } catch (t: Throwable) {
            throw t
        }
        return t.iterator()
                .hashCodeX { valueHashCodeable.hashCodeX(this) }
    }

    override fun hashCodeX(t: Any): Int {
        val x = getObject(t)
        x as Set<Any>
        return x.iterator()
                .hashCodeX { valueHashCodeable.hashCodeX(this) }
    }
}

internal class ObjectHashCodeable(val any: Any) : HashCodeable {

    private val hashCode = any.toString()
            .hashCodeX()

    override fun getObject(o: Any): Any {
        return any
    }

    override fun hashCodeX(t: Any): Int {
        return hashCode
    }

    override fun plainHashCodeX(t: Any): Int {
        return hashCode
    }
}

internal class InterfaceHashCodeable(val possibleHashImplementation: Map<String, PossibleHashImplementation>, readable: FieldReadable) :
        HashCodeable, FieldReadable by readable {

    private fun hashCodeCalc(tFromGetObject: Any, block: (HashCodeable) -> Int): Int {
        val name = InterfaceField.nameOfObj(tFromGetObject)
        val p = possibleHashImplementation[name] ?: throw RuntimeException("type unknown: $name - maybe no InterfaceField Annotation used?")
        return block(p.hashCodeable)
    }

    override fun hashCodeX(t: Any): Int {
        return hashCodeCalc(getObject(t)) { it.hashCodeX(t) }
    }

    override fun plainHashCodeX(t: Any): Int {
        return hashCodeCalc(t) { it.plainHashCodeX(t) }
    }

}

internal class ComplexHashCodeable(val clazz: KClass<Any>,
                                   val provider: HashCodeableProvider,
                                   val complexReadable: FieldReadable) : HashCodeable, FieldReadable by complexReadable {

    init {
        provider.register(clazz)
    }

    val key: KeyHashCodeable
        get() = provider.hashCodeable(clazz)

    override fun hashCodeX(t: Any): Int {
        return key.hashCodeX(getObject(t))
    }

    override fun plainHashCodeX(t: Any): Int {
        return key.hashCodeX(t)
    }
}

internal class KeyHashCodeable constructor(val list: List<HashCodeable>) : HashCodeable {
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


