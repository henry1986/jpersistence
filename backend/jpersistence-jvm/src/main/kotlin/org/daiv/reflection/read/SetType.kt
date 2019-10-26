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
import org.daiv.reflection.persister.*
import org.daiv.reflection.persister.Persister.HelperTable
import kotlin.reflect.full.isSubclassOf


internal class SetType<R : Any, T : Any> constructor(override val propertyData: SetProperty<R, T>,
                                                     val persisterProvider: PersisterProvider,
                                                     private val many: ManyList,
                                                     val persister: Persister,
                                                     override val prefix: String?) :
        CollectionFieldData<R, Set<T>, T, Set<T>> {
    private val helperTableName = "${propertyData.receiverType.simpleName}_$name"

    private lateinit var idField: KeyType

    private val valueField = propertyData.clazz.toFieldData(persisterProvider, KeyAnnotation(propertyData.property), "value", persister)

    override val helperTable
        get() = helper

    private lateinit var helper: HelperTable

    override fun onIdField(idField: KeyType) {
        this.idField = idField
        helper = persister.HelperTable(listOf(idField, valueField) as List<FieldData<Any, Any, Any, Any>>,
                                       { helperTableName },
                                       { helperTableName },
                                       2)
        persisterProvider.registerHelperTableName(helperTableName)
    }

    override fun isType(a: Any): Boolean {
        return a::class.isSubclassOf(Set::class)
    }

    override suspend fun toStoreData(insertMap: InsertMap, objectValue: List<R>) {
        val b = objectValue.flatMap { key ->
            val p = getObject(key).toList()
            p.map { key to it }
        }
        valueField.toStoreData(insertMap, b.map { it.second })
        b.forEach {
            val id = idField.getObject(it.first)
            val value = it.second
            val insertKey = InsertKey(helperTableName, listOf(id, value))
            insertMap.toBuild(insertKey, toBuild = {
                val x = idField.insertObject(id)
                        .first()
                val y = valueField.insertObject(value)
                        .first()
                listOf(InsertRequest(listOf(x, y)))
            }) { }
//            if (!insertMap.exists(insertKey)) {
//
//                val x = idField.insertObject(id)
//                        .first()
//                val y = valueField.insertObject(value)
//                        .first()
//                insertMap.put(insertKey, InsertRequest(listOf(x, y)))
//            }
        }
    }

    override fun deleteLists(keySimpleType: List<Any>) {
        helperTable.deleteBy(idField.name, keySimpleType)
    }

    override fun clearLists() {
        helperTable.clear()
    }

    override fun createTableForeign(tableNames: Set<String>): Set<String> {
        return valueField.createTableForeign(helperTable.persistWithName(checkName = tableNames))
    }

    override fun fNEqualsValue(o: Set<T>, sep: String): String {
        return o.map {
            valueField.fNEqualsValue(valueField.getObject(it), sep)
        }
                .joinToString(", ")
    }


    @Suppress("UNCHECKED_CAST")
    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<Set<T>> {
        if (key.isEmpty()) {
            throw NullPointerException("a List cannot be a key")
        }
        val fn = idField.autoIdFNEqualsValue(key, "AND")
        val read = helperTable.readIntern(fn, readCache)
        val ret = read
                .map {
                    it[1].value as T
                }
                .toSet()
        return NextSize(ret, number)
    }
}
