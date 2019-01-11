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

import org.daiv.reflection.annotations.TableData
import org.daiv.reflection.common.FieldData
import org.daiv.reflection.common.NoList
import org.daiv.reflection.common.PropertyData
import org.daiv.reflection.common.ReadValue
import java.sql.SQLException
import kotlin.reflect.KClass

/**
 * [T] is the enum Value
 */
internal class EnumType<R : Any, T : Any> constructor(override val propertyData: PropertyData<R, T, T>) : NoList<R, T, T> {

    override fun getColumnValue(resultSet: ReadValue) = resultSet.getObject(1, propertyData.clazz as KClass<Any>)!!

    override fun keyClassSimpleType() = propertyData.clazz as KClass<Any>
    override fun keySimpleType(r: R) = propertyData.getObject(r)
    override fun keyLowSimpleType(t: T) = t

    override fun toTableHead(prefix: String?) = "${name(prefix)} Text NOT NULL"

    override fun key(prefix: String?) = name(prefix)
    override fun header(prefix: String?) = listOf(name(prefix))

    override fun joinNames(prefix: String?, clazzSimpleName: String, keyName: String): List<FieldData.JoinName> =
        listOfNotNull(prefix).map { FieldData.JoinName(it, clazzSimpleName, keyName) }

    override fun foreignKey() = null

    override fun underscoreName(prefix: String?) = prefix?.let { "${it}_$name" } ?: name

//    override fun joins(prefix: String?): List<String> = emptyList()


    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<T> {
        try {
            val any = readValue.getObject(number, propertyData.clazz as KClass<Any>) as String
            return NextSize(getEnumValue(propertyData.clazz.qualifiedName!!, any), number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    override fun insertObject(o: R, prefix: String?): List<InsertObject<Any>> {
        return listOf(object : InsertObject<Any> {
            override fun insertValue(): String {
                val any = getObject(o)
                return makeString(any)
            }

            override fun insertHead(): String {
                return name(prefix)
            }
        })
    }

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        return ReadSimpleType.static_fNEqualsValue(getObject(o), name(prefix), sep)
    }

    override fun keyTables(): List<TableData> = emptyList()
    override fun helperTables(): List<TableData> = emptyList()

    companion object {
        internal fun<T:Any> getEnumValue(clazzName: String, s: String): T {
            return Class.forName(clazzName).enumConstants.filterIsInstance(Enum::class.java).first { it.name == s } as T
        }

        internal fun makeString(any: Any): String {
            return "\"${(any as Enum<*>).name}\""
        }
    }
}