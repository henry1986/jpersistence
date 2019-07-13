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

import org.daiv.reflection.annotations.TableData
import org.daiv.reflection.common.*
import java.sql.SQLException
import kotlin.reflect.KClass

internal class ReadSimpleType<R : Any, T : Any>(override val propertyData: PropertyData<R, T, T>, override val prefix: String?) :
        NoList<R, T, T> {
    override fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects> = emptyList()
    override fun idFieldSimpleType() = this as FieldData<Any, Any, Any, Any>

    override fun subFields(): List<FieldData<Any, Any, Any, Any>> = emptyList()

    override fun storeManyToOneObject(t: List<T>) {}

    override fun persist() {}

    override fun getColumnValue(resultSet: ReadValue) = resultSet.getObject(1, propertyData.clazz as KClass<Any>)!!

    override fun keyClassSimpleType() = propertyData.clazz as KClass<Any>
    override fun keySimpleType(r: R) = propertyData.getObject(r)
    override fun keyLowSimpleType(t: T) = t

    override fun toTableHead() = toTableHead(propertyData.clazz, prefixedName)

    override fun key() = prefixedName
    override fun header() = listOf(prefixedName)

//    override fun joinNames(clazzSimpleName: String, keyName: String): List<JoinName> =
//            listOfNotNull(prefix).map { JoinName(it, clazzSimpleName, keyName) }

//    override fun foreignKey() = null

    override fun underscoreName() = prefix?.let { "${it}_$name" } ?: name

//    override fun joins(): List<String> = emptyList()

    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<T> {
        try {
            val any = readValue.getObject(number, propertyData.clazz as KClass<Any>)
            val x = if (propertyData.clazz == Boolean::class) {
                any == 1
            } else {
                any
            }
            @Suppress("UNCHECKED_CAST")
            return NextSize(x as T, number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    override fun insertObject(t: T): List<InsertObject> {
        return listOf(object : InsertObject {
            override fun insertValue(): String {
                return makeString(t)
            }

            override fun insertHead(): String {
                return prefixedName
            }
        })
    }

    override fun fNEqualsValue(o: T, sep: String): String {
        return "$prefixedName = ${makeString(o)}"
//        return static_fNEqualsValue(getObject(o), prefixedName, sep)
    }

    override fun keyTables(): List<TableData> = emptyList()
    override fun helperTables(): List<TableData> = emptyList()

    private fun makeString(any: T): String {
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
//        internal fun makeString(any: Any): String {
//            val s = any.toString()
//            return when {
//                any::class == String::class -> "\"" + s + "\""
//                any::class == Boolean::class -> if (s.toBoolean()) "1" else "0"
//                any::class.isEnum() -> "\"${(any as Enum<*>).name}\""
//                else -> s
//            }
//        }

        internal fun <T : Any> toTableHead(clazz: KClass<T>, name: String): String {
            val simpleName = clazz.simpleName
            val typeName = valueMappingJavaSQL[simpleName] ?: simpleName
            return "$name $typeName${" NOT NULL"}"
        }
//
//        internal fun <R : Any> static_fNEqualsValue(o: R, prefixed: String?, sep: String): String {
//            return "${(prefixed)} = ${makeString(o)}"
//        }
    }
}
