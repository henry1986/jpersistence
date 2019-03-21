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
import org.daiv.reflection.common.ReadValue
import org.daiv.reflection.persister.Persister
import java.sql.ResultSet
import kotlin.reflect.KClass

internal data class ComplexSameTableType<R : Any, T : Any> constructor(override val propertyData: PropertyData<R, T, T>,
                                                                       override val prefix: String?,
                                                                       val persisterData: ReadPersisterData<T, Any>) :
        NoList<R, T, T> {
    override fun subFields() = persisterData.fields as List<FieldData<Any, Any, Any>>

    override fun idFieldSimpleType() = this as FieldData<Any, Any, Any>

    override fun storeManyToOneObject(t: T) {

    }

    override fun persist() {}

    override fun getColumnValue(resultSet: ReadValue) = persisterData.readKey(resultSet)

    override fun keyClassSimpleType() = propertyData.clazz as KClass<Any>

    override fun keySimpleType(r: R) = r
    override fun keyLowSimpleType(t: T) = t

    override fun insertObject(o: T): List<InsertObject<Any>> {
        return persisterData.onFields { this.insertObject(this.getObject(o)) }
                .flatten()
    }

    override fun fNEqualsValue(o: T,  sep: String): String {
        return persisterData.onFields { fNEqualsValue(getObject(o), sep) }
                .joinToString(sep)
    }

    override fun toTableHead(): String? {
        return persisterData.onFields { toTableHead() }
                .joinToString(", ")
    }

    override fun key(): String {
        return persisterData.onFields { key() }
                .joinToString(", ")//persisterData.createTableKeyData(name(prefix))
//        return persisterData.createTableKeyData(prefix)
    }

    //    override fun foreignKey(): FieldData.ForeignKey? {
//        return null
//    }
//
//    override fun joinNames(clazzSimpleName: String, keyName: String): List<FieldData.JoinName> {
//        return listOf()
//    }

    override fun underscoreName(): String {
        return persisterData.onFields { underscoreName() }
                .joinToString(", ")
    }

    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<T> {
        return persisterData.read(readValue, number)
    }

    override fun header() = persisterData.onFields { header() }.flatten()

    override fun size() = persisterData.onFields { size() }.sum()

    override fun keyTables() = persisterData.keyTables()

    override fun helperTables() = persisterData.helperTables()
}