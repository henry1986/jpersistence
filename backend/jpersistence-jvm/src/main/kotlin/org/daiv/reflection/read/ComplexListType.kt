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

import org.daiv.reflection.common.FieldData
import org.daiv.reflection.common.NoList
import org.daiv.reflection.common.PropertyData
import java.sql.ResultSet

internal data class ComplexListType<R : Any, T : Any> constructor(override val propertyData: PropertyData<R, T, T>,
                                                                  val persisterData: ReadPersisterData<T, Any>) :
    NoList<R, T, T> {

    override fun getColumnValue(resultSet: ResultSet) = persisterData.readKey(resultSet)

    override fun keyClassSimpleType() = throw RuntimeException("no simple Type")

    override fun keySimpleType(r: R) = r
    override fun keyLowSimpleType(t: T) = t

    override fun insertObject(o: R, prefix: String?): List<InsertObject<Any>> {
        val t = propertyData.getObject(o)
        return persisterData.onFields { insertObject(t, this@ComplexListType.name(prefix)) }
            .flatten()
    }

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        val t = propertyData.getObject(o)
        return persisterData.onFields { fNEqualsValue(t, prefix, sep) }
            .joinToString(sep)
    }

    override fun toTableHead(prefix: String?): String? {
        val name = name(prefix)
        return persisterData.onFields { toTableHead(name) }
            .joinToString(", ")
    }

    override fun key(prefix: String?): String {
        return persisterData.onFields { key(this@ComplexListType.name(prefix)) }
            .joinToString(", ")//persisterData.createTableKeyData(name(prefix))
//        return persisterData.createTableKeyData(prefix)
    }

    override fun foreignKey(): FieldData.ForeignKey? {
        return null
    }

    override fun joinNames(prefix: String?, clazzSimpleName: String, keyName: String): List<FieldData.JoinName> {
        return listOf()
    }

    override fun underscoreName(prefix: String?): String {
        return persisterData.onFields { underscoreName(this@ComplexListType.name(prefix)) }
            .joinToString(", ")
    }

    override fun getValue(resultSet: ResultSet, number: Int, key: Any?): NextSize<T> {
        return persisterData.read(resultSet, number)
    }
}