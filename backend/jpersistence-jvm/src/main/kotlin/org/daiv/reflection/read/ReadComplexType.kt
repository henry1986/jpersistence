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

import org.daiv.reflection.annotations.ManyToOne
import org.daiv.reflection.common.*
import org.daiv.reflection.common.FieldData.JoinName
import org.daiv.reflection.persister.Persister

internal class ReadComplexType<R : Any, T : Any> constructor(override val propertyData: PropertyData<R, T, T>,
                                                             manyToOne: ManyToOne,
                                                             private val persister: Persister,
                                                             override val prefix: String?,
                                                             _persisterData: ReadPersisterData<T, Any>? = null) : NoList<R, T, T> {
    private val persisterData = _persisterData ?: ReadPersisterData(propertyData.clazz, prefixedName, persister, manyToOne.tableName)

    override fun idFieldSimpleType() = persisterData.onKey { idFieldSimpleType() }

    override fun subFields(): List<FieldData<Any, Any, Any, Any>> = persisterData.fields as List<FieldData<Any, Any, Any, Any>>

    override fun storeManyToOneObject(t: List<T>) {
        persisterData.storeManyToOneObject(t, table)
    }

    override fun persist() = table.persist()

    private val table = persister.Table(propertyData.clazz, manyToOne.tableName)
    override fun createTableForeign() = table.persist()

    val clazz = propertyData.clazz
    override fun getColumnValue(resultSet: ReadValue) = persisterData.readKey(resultSet)

//    override fun joinNames(clazzSimpleName: String, keyName: String): List<JoinName> {
//        return persisterData.onFields {
//            joinNames(this@ReadComplexType.prefixedName, clazzSimpleName, persisterData.keyName())
//        }
//                .flatMap { it }
//    }

    override fun keyClassSimpleType() = persisterData.onKey { keyClassSimpleType() }
    override fun keySimpleType(r: R) = persisterData.keySimpleType(propertyData.getObject(r))
    override fun keyLowSimpleType(t: T) = persisterData.keySimpleType(t)

//    override fun foreignKey(): ForeignKey {
//        return ForeignKey(name, "${clazz.simpleName}(${persisterData.keyName()})")
//    }

    override fun underscoreName(): String {
        return persisterData.onKey { underscoreName() }!!
//            .joinToString(", ")
    }

    override fun key(): String {
        return persisterData.onKey { key() }//persisterData.createTableKeyData(prefixedName)
    }

    override fun toTableHead(): String? {
        val name = prefixedName
        return persisterData.onKey { toTableHead() }
    }

    override fun header() = persisterData.onKey { header() }

    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<T> {
        val nextSize = persisterData.onKey { getValue(readValue, number, key) }
        val value = readValue.read(table, nextSize.t)
//        val value = table.read(nextSize.t)!!
        return NextSize(value, nextSize.i)
    }

    override fun fNEqualsValue(objectValue: T, sep: String): String {
//        val objectValue = getObject(o)
        return persisterData.onKey { fNEqualsValue(getObject(objectValue), sep) }
    }

    override fun insertObject(objectValue: T): List<InsertObject<Any>> {
//        val objectValue = getObject(o)
//        persisterData.storeManyToOneObject(objectValue, table)
        return persisterData.onKey { this.insertObject(this.getObject(objectValue)) }
    }

    override fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects> {
        return listOf(ToStoreManyToOneObjects(this, objectValue))
    }

    override fun keyTables() = table.keyTables() + table.tableData()

    override fun helperTables() = table.helperTables()
}