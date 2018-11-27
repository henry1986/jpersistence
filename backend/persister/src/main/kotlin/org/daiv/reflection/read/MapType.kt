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

import org.daiv.reflection.annotations.ManyMap
import org.daiv.reflection.common.*
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.persister.Persister
import java.sql.ResultSet
import kotlin.reflect.KClass

internal data class Identity<T : Any>(val clazz: KClass<T>,
                                      val persister: Persister,
                                      val name: String,
                                      val index: Int,
                                      val tableName: String) {
    val persisterData: ReadPersisterData<T, Any> = ReadPersisterData(clazz, persister)
    val simpleProperty = SimpleProperty(persisterData.keyClassSimpleType(), name, index)

    fun storeManyToOneObject(t: T) = persisterData.storeManyToOneObject(t, persister, tableName)

    fun getValue(list: List<Any>): T {
        return if (clazz.java.isPrimitiveOrWrapperOrString()) {
            list[index] as T
        } else {
            val table = persister.Table(clazz, tableName)
            table.read(list[index])!!
        }
    }
}


internal class MapType<R : Any, T : Any, M : Any>(override val propertyData: MapProperty<R, T, M>,
                                                  val persister: Persister,
                                                  val manyMap: ManyMap,
                                                  keyClass: KClass<Any>) :
    CollectionFieldData<R, Map<M, T>, T> {
    private val keyIdentity = Identity(propertyData.keyClazz,
                                       persister,
                                       "key_${propertyData.name}",
                                       1,
                                       manyMap.tableNameKey)
    private val valueIdentity = Identity(propertyData.clazz,
                                         persister,
                                         "value_${propertyData.name}",
                                         2,
                                         manyMap.tableNameValue)
    private val helperTable: Persister.Table<ComplexObject>

    private val helperTableName = "${propertyData.receiverType.simpleName}_$name"

    init {
        val readPersisterData: ReadPersisterData<InsertData, Any>
        val fields = listOf(ReadSimpleType(SimpleProperty(keyClass, propertyData.receiverType.simpleName!!, 0)),
                            ReadSimpleType(keyIdentity.simpleProperty),
                            ReadSimpleType(valueIdentity.simpleProperty))
        readPersisterData = ReadPersisterData(fields) { i: List<ReadFieldValue> ->
            InsertData(listOf(i[0].value, i[1].value, i[2].value))
        }
        val fields2: List<ComplexListType<ComplexObject, Any>> = listOf(ComplexListType(ComplexProperty(
            helperTableName), readPersisterData as ReadPersisterData<Any, Any>))
        val n: ReadPersisterData<ComplexObject, Any> = ReadPersisterData(fields2) { i: List<ReadFieldValue> ->
            ComplexObject(i.first().value as InsertData)
        }
        helperTable = persister.Table(n, helperTableName)
    }

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        val m = getObject(o)
        return sequenceOf(m.values.map { valueIdentity.persisterData.fNEqualsValue(it, name(prefix), sep) },
                          m.keys.map { keyIdentity.persisterData.fNEqualsValue(it, name(prefix), sep) })
            .joinToString(", ")
    }

    override fun createTable() = helperTable.persist()

    override fun insertLists(keySimpleType: Any, r: R) {
        val o = getObject(r)
        o.forEach {
            helperTable.insert(ComplexObject(InsertData(listOf(keySimpleType,
                                                               keyIdentity.persisterData.keySimpleType(it.key),
                                                               valueIdentity.persisterData.keySimpleType(it.value)))))
            keyIdentity.storeManyToOneObject(it.key)
            valueIdentity.storeManyToOneObject(it.value)
        }
    }


    override fun getValue(resultSet: ResultSet, number: Int, key: Any?): NextSize<Map<M, T>> {
        if (key == null) {
            throw NullPointerException("a List cannot be a key")
        }
        val complexObjectList = helperTable.read(propertyData.receiverType.simpleName!!, key)
        val t = complexObjectList.map { keyIdentity.getValue(it.insertData.list) to valueIdentity.getValue(it.insertData.list) }
            .toMap()
        return NextSize(t, number)
    }
}