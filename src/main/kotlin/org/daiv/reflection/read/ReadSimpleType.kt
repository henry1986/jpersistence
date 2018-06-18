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

import org.daiv.reflection.getKClass
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KProperty1

internal class ReadSimpleType<T : Any>(override val property: KProperty1<Any, T>) : ReadFieldData<T> {
    override fun key(prefix: String?): String {
        return name(prefix)
    }

    override fun toTableHead(prefix: String?): String {
        val simpleName = property.getKClass()
            .simpleName
        val typeName = valueMappingJavaSQL[simpleName] ?: simpleName
        return "${name(prefix)} $typeName${" NOT NULL"}"
    }

    override fun getValue(resultSet: ResultSet, number: Int): NextSize<T> {
        try {
            @Suppress("UNCHECKED_CAST")
            return NextSize(resultSet.getObject(number) as T, number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    companion object {
        private val valueMappingJavaSQL = mapOf("long" to "bigInt", "String" to "Text")
    }
}
