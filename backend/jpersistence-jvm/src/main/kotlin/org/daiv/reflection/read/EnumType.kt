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
import org.daiv.reflection.common.ReadValue
import org.daiv.reflection.common.SimpleTypes
import org.daiv.reflection.persister.ReadCache
import java.sql.SQLException
import kotlin.reflect.KClass

/**
 * [T] is the enum Value
 */
internal class EnumType<R : Any, T : Any> constructor(override val propertyData: PropertyData<R, T, T>, override val prefix: String?) :
        SimpleTypes<R, T> {


    override fun toTableHead() = "${prefixedName} Text NOT NULL"


    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<T> {
        try {
            val any = readValue.getObject(number) as String
            return NextSize(getEnumValue(propertyData.clazz.qualifiedName!!, any), number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    override fun makeString(any: T): String {
        return "\"${(any as Enum<*>).name}\""
    }

    companion object {
        internal fun <T : Any> getEnumValue(clazzName: String, s: String): T {
            return Class.forName(clazzName).enumConstants.filterIsInstance(Enum::class.java).first { it.name == s } as T
        }
    }
}

internal class AutoKeyType(override val propertyData: PropertyData<Any,Any,Any>, override val prefix: String?) : SimpleTypes<Any, Any> {
    override fun toTableHead() = "$prefixedName Int NOT NULL"

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<Any> {
        val any = readValue.getObject(number)
        return NextSize(any as Int, number + 1)
    }

    override fun makeString(t: Any): String {
        return t.toString()
    }

    override fun makeStringOnEqualsValue(t: Any): String {
        return t.toString()
//        return propertyData.getObject(t).toString()

    }

    fun autoIdFNEqualsValue(o: Any): String {
        return "$prefixedName = ${makeString(o)}"
//        return "$prefixedName = ${makeString(o)}"
    }
}
