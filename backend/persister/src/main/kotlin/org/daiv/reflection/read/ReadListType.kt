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

import org.daiv.reflection.annotations.Many
import org.daiv.reflection.common.*
import org.daiv.reflection.common.FieldData.JoinName
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.Table
import java.sql.ResultSet
import kotlin.reflect.KClass

internal class ReadListType<R : Any, T : Any>(override val propertyData: ListProperty<R, T>,
                                              private val many: Many,
                                              val persister: Persister,
                                              keyClass: KClass<Any>) : FieldData<R, List<T>, T> {

    private val persisterData = ReadPersisterData<T, Any>(propertyData.clazz, persister)
    private val helperTable: Table<ComplexObject>

    private val helperTableName = "${propertyData.receiverType.simpleName}_$name"

    init {
        val readPersisterData: ReadPersisterData<InsertData, Any>
        val fields = listOf(ReadSimpleType(SimpleProperty(keyClass, propertyData.receiverType.simpleName!!, true)),
                            ReadSimpleType(SimpleProperty(persisterData.keyClassSimpleType(),
                                                          propertyData.name,
                                                          false)))
        readPersisterData = ReadPersisterData(fields) { i: List<ReadFieldValue> ->
            InsertData(i[0].value, i[1].value)
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
            helperTable.insert(ComplexObject(InsertData(keySimpleType, persisterData.keySimpleType(it))))
            storeManyToOneObject(persisterData, it, persister)
        }
    }

    override fun createTable() {
        helperTable.persist()
    }

    override fun keyClassSimpleType() = throw RuntimeException("a list cannot be a key")
    override fun keySimpleType(r: R) = throw RuntimeException("a list cannot be a key")
    override fun keyLowSimpleType(t: T) = throw RuntimeException("a list cannot be a key")

    override fun joinNames(prefix: String?, clazzSimpleName: String, keyName: String): List<JoinName> = emptyList()
    override fun foreignKey() = null

    override fun underscoreName(prefix: String?) = ""

//    override fun joins(prefix: String?):List<String> = emptyList()


    private fun map(f: (Int) -> String): String {
        return (0 until many.size).map(f)
            .joinToString(", ")
    }

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        val f: (Int) -> String = { persisterData.fNEqualsValue(getObject(o)[it], name(prefix), sep) }
        return map(f)
    }

    override fun insertObject(o: R, prefix: String?): List<InsertObject<Any>> {

        return listOf()
    }

    override fun toTableHead(prefix: String?) = null


    override fun key(prefix: String?): String {
        return map { persisterData.createTableKeyData(listElementName(prefix, it)) }
    }


    @Suppress("UNCHECKED_CAST")
    override fun getValue(resultSet: ResultSet, number: Int, key: Any?): NextSize<List<T>> {
        if (key == null) {
            throw NullPointerException("a List cannot be a key")
        }
        val insertData = helperTable.read(propertyData.receiverType.simpleName!!, key)
        val table = persister.Table(propertyData.clazz)
        val t = insertData.map { table.read(it.insertData.b)!! }
        return NextSize(t, number)
    }
}