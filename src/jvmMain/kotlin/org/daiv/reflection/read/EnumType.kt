/*
 * Copyright (c) 2019. Martin Heinrich - All Rights Reserved
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

import org.daiv.reflection.common.PropertyData
import org.daiv.reflection.common.ReadAnswer
import org.daiv.reflection.common.ReadValue
import org.daiv.reflection.common.SimpleTypes
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.ObjectOnlyReadObject
import org.daiv.reflection.plain.PlainEnumObject
import org.daiv.reflection.plain.SimpleReadObject
import java.sql.SQLException
import kotlin.reflect.KClass

/**
 * [T] is the enum Value
 */
internal class EnumType constructor(override val propertyData: PropertyData, override val prefix: String?) :
    SimpleTypes {

    override val typeName = "TEXT"

    override fun plainType(name: String): SimpleReadObject? {
        if (name == prefixedName) {
            return PlainEnumObject(name, propertyData.clazz as KClass<Enum<*>>)
        }
        return null
    }

    override fun getValue(
        readCache: ReadCache,
        readValue: ReadValue,
        number: Int,
        key: ObjectKey
    ): NextSize<ReadAnswer<Any>> {
        try {
            val obj = readValue.getObject(number)
            if (obj !is String?) {
                throw RuntimeException("expected a string, but got $obj")
            }
            return NextSize(ReadAnswer(getEnumValue(propertyData.clazz.java, obj)), number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    override fun makeString(any: Any): String {
        return "\"${(any as Enum<*>).name}\""
    }

    companion object {
        internal fun <T> getEnumValue(clazz: Class<T>, s: String?): T {
            return s?.let { clazz.enumConstants.filterIsInstance(Enum::class.java).first { it.name == s } } as T
        }
    }
}

internal class ObjectOnlyType constructor(override val propertyData: PropertyData, override val prefix: String?) :
    SimpleTypes {

    override val typeName = "TEXT"

    override fun plainType(name: String): SimpleReadObject? {
        if (name == prefixedName) {
            return ObjectOnlyReadObject(name)
        }
        return null
    }

    override fun getValue(
        readCache: ReadCache,
        readValue: ReadValue,
        number: Int,
        key: ObjectKey
    ): NextSize<ReadAnswer<Any>> {
        try {
            val obj = readValue.getObject(number)
            if (obj !is String?) {
                throw RuntimeException("expected a string, but got $obj")
            }
            return NextSize(ReadAnswer(obj?.let(::getInstance)), number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    override fun makeString(any: Any): String {
        return "\"${any::class.java.name}\""
    }

    companion object {
        fun getInstance(text: String): Any {
            return Class.forName(text).kotlin.objectInstance!!
        }
    }
}

internal class AutoKeyType(override val propertyData: PropertyData, override val prefix: String?) : SimpleTypes {
    override val typeName = "Int"

    override fun toString() = "$name - $typeName"

    override fun plainType(name: String): SimpleReadObject? {
        return null
    }

    override fun getValue(
        readCache: ReadCache,
        readValue: ReadValue,
        number: Int,
        key: ObjectKey
    ): NextSize<ReadAnswer<Any>> {
        val any = readValue.getObject(number)
        return NextSize(ReadAnswer(any), number + 1)
    }

    override fun makeString(t: Any): String {
        return t.toString()
    }

    fun autoIdFNEqualsValue(o: Any): String {
        return "$prefixedName = ${makeString(o)}"
    }
}
