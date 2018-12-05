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

import org.daiv.reflection.annotations.ManyList
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.Table
import java.sql.ResultSet
import kotlin.reflect.KClass

internal class ReadListType<R : Any, T : Any>(override val propertyData: ListProperty<R, T>,
                                              private val many: ManyList,
                                              val persister: Persister,
                                              keyClass: KClass<Any>) : CollectionFieldData<R, List<T>, T> {

    private val identity = Identity(propertyData.clazz, persister, propertyData.name, 1, many.tableName)
    //    private val persisterData = ReadPersisterData<T, Any>(propertyData.clazz, persister)
    private val helperTable: Table<ComplexObject>

    private val helperTableName = "${propertyData.receiverType.simpleName}_$name"

    init {
        val readPersisterData: ReadPersisterData<InsertData, Any>
        val fields = listOf(ReadSimpleType(SimpleProperty(keyClass, propertyData.receiverType.simpleName!!, 0)),
                            ReadSimpleType(identity.simpleProperty))
        readPersisterData = ReadPersisterData(fields) { i: List<ReadFieldValue> ->
            InsertData(listOf(i[0].value, i[1].value))
        }
        val fields2: List<ComplexListType<ComplexObject, Any>> = listOf(ComplexListType(ComplexProperty(
            helperTableName), readPersisterData as ReadPersisterData<Any, Any>))
        val n: ReadPersisterData<ComplexObject, Any> = ReadPersisterData(fields2) { i: List<ReadFieldValue> ->
            ComplexObject(i.first().value as InsertData)
        }
        helperTable = persister.Table(n, helperTableName)

    }

    override fun insertLists(keySimpleType: Any, r: R) {
        val o = getObject(r)
        o.forEach {
            helperTable.insert(ComplexObject(InsertData(listOf(keySimpleType,
                                                               identity.persisterData.keySimpleType(it)))))
            identity.storeManyToOneObject(it)
        }
    }

    override fun createTable() = helperTable.persist()
    override fun createTableForeign() = identity.persist()

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        return getObject(o).map { identity.persisterData.fNEqualsValue(it, name(prefix), sep) }
            .joinToString(", ")
    }


    @Suppress("UNCHECKED_CAST")
    override fun getValue(resultSet: ResultSet, number: Int, key: Any?): NextSize<List<T>> {
        if (key == null) {
            throw NullPointerException("a List cannot be a key")
        }

        val complexList = helperTable.read(propertyData.receiverType.simpleName!!, key)
        val t = complexList.map { identity.getValue(it.insertData.list) }
        return NextSize(t, number)
    }
}