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
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertKey
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.InsertRequest
import org.daiv.reflection.persister.Persister

internal class ReadComplexType<R : Any, T : Any> constructor(override val propertyData: PropertyData<R, T, T>,
                                                             val moreKeys: MoreKeys,
                                                             val persisterProvider: PersisterProvider,
                                                             private val persister: Persister,
                                                             override val prefix: String?) : NoList<R, T, T> {
    val providerKey = ProviderKey(propertyData, prefixedName)

    //    private val persisterData: ReadPersisterData<T, Any> = ReadPersisterData(propertyData.clazz,
//                                                                             persister,
//                                                                             persisterProvider,
//                                                                             prefix = prefixedName,
//                                                                             parentTableName = manyToOne.tableName)
    init {
        persisterProvider.register(providerKey)
    }

    private val persisterData
        get() = persisterProvider.readPersisterData(providerKey) as ReadPersisterData<T, Any>

    private val table
        get() = persisterProvider.table(providerKey) as Persister.Table<T>

    override fun subFields(): List<FieldData<Any, Any, Any, Any>> = persisterData.fields as List<FieldData<Any, Any, Any, Any>>

    override fun numberOfKeyFields(): Int = moreKeys.amount

    override fun storeManyToOneObject(t: List<T>) {
        persisterData.storeManyToOneObject(t, table)
    }

    private fun <T : Any> T.checkDBValue(objectValue: T): T {
        if (this != objectValue) {
            val msg = "values are not the same -> " +
                    "databaseValue: $this vs manyToOne Value: $objectValue"
            throw RuntimeException(msg)
        }
        return objectValue
    }

    override fun toStoreData(insertMap: InsertMap, objectValue: List<R>) {
        objectValue.forEach {
            val obj = getObject(it)
            val key = persisterData.key.hashCodeXIfAutoKey(obj)
            val read = table.readMultipleUseHashCode(key)
            if (read != null) {
                read.checkDBValue(obj)
                val insertKey = InsertKey(table.tableName, key)
                if (!insertMap.exists(insertKey)) {
                    insertMap.put(insertKey, InsertRequest(emptyList()))
                }
            } else {
                table.readPersisterData.putInsertRequests(table.tableName, insertMap, listOf(obj))
            }
        }
    }

    override fun persist() {}

    override fun createTableForeign(tableNames: Set<String>): Set<String> {
        if (!tableNames.contains(this.table._tableName)) {
            return table.persistWithName(checkName = tableNames)
        }
        return tableNames
    }

    val clazz = propertyData.clazz
    override fun getColumnValue(resultSet: ReadValue) = persisterData.readKey(resultSet)

//    override fun joinNames(clazzSimpleName: String, keyName: String): List<JoinName> {
//        return persisterData.onFields {
//            joinNames(this@ReadComplexType.prefixedName, clazzSimpleName, persisterData.keyName())
//        }
//                .flatMap { it }
//    }

    override fun keySimpleType(r: R) = persisterData.keySimpleType(propertyData.getObject(r))

//    override fun foreignKey(): ForeignKey {
//        return ForeignKey(name, "${clazz.simpleName}(${persisterData.keyName()})")
//    }

    override fun underscoreName(): String {
        return persisterData.key.underscoreName()!!
    }

    override fun key(): String {
        return persisterData.key.keyString()//persisterData.createTableKeyData(prefixedName)
    }

    override fun toTableHead(): String? {
        return persisterData.key.toTableHead()
    }

    override fun copyTableName(): Map<String, String> {
        return persisterData.key.copyTableName()
    }

    override fun getValue(readValue: ReadValue, number: Int, key: List<Any>): NextSize<T> {
        val nextSize = persisterData.key.getValue(readValue, number, key)
        val read = table.readMultipleUseHashCode(nextSize.t)!!
//        val value = readValue.read(table, nextSize.t)
//        val value = table.read(nextSize.t)!!
        return NextSize(read, nextSize.i)
    }

    override fun fNEqualsValue(objectValue: T, sep: String): String {
        return persisterData.key.fNEqualsValue(persisterData.key.getObject(objectValue), sep)
    }

    override fun insertObject(objectValue: T): List<InsertObject> {
        return persisterData.key.insertObject(persisterData.key.hashCodeXIfAutoKey(objectValue))
    }

    override fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects> {
        return listOf(ToStoreManyToOneObjects(this, objectValue))
    }

//    override fun hashCodeXIfAutoKey(t: R): T {
//        val obj = getObject(t)
//        return persisterData.key.plainHashCodeXIfAutoKey(obj) as T
////        return super.hashCodeXIfAutoKey(t)
//    }
//
//    override fun plainHashCodeXIfAutoKey(t: T): T {
//        return persisterData.key.plainHashCodeXIfAutoKey(t) as T
//    }
}