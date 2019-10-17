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

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.*
import org.daiv.reflection.persister.Persister.HelperTable
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf


internal class MapType<R : Any, T : Any, M : Any> constructor(override val propertyData: DefaultMapProperty<R, T, M>,
                                                              val persisterProvider: PersisterProvider,
                                                              override val prefix: String?,
                                                              val persister: Persister,
                                                              val parentClass: KClass<R>,
                                                              val mapEngine: MapEngine<R, M, T> = MapEngine(propertyData,
                                                                                                            persisterProvider,
                                                                                                            persister,
                                                                                                            parentClass) {
                                                                  propertyData.getObject(it)
                                                              }) :
        CollectionFieldData<R, Map<M, T>, T, Map<M, T>>, MapEngineInterface<R, M, T> by mapEngine {

    override fun isType(a: Any): Boolean {
        return a::class.isSubclassOf(Map::class)
    }

    override fun fNEqualsValue(o: Map<M, T>, sep: String): String {
        return mapEngine.fNEqualsValue(o, sep)
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<Map<M, T>> {
        return mapEngine.getValue(readCache, readValue, number, key)
    }
}


internal class ListType<R : Any, T : Any>(override val propertyData: ListMapProperty<R, T>,
                                          val persisterProvider: PersisterProvider,
                                          override val prefix: String?,
                                          val persister: Persister,
                                          val parentClass: KClass<R>,
                                          val converterToMap: (List<T>) -> Map<Int, T> = {
                                              it.mapIndexed { index, t -> index to t }
                                                      .toMap()
                                          },
                                          val mapEngine: MapEngine<R, Int, T> = MapEngine(propertyData,
                                                                                          persisterProvider,
                                                                                          persister,
                                                                                          parentClass) {
                                              converterToMap(propertyData.getObject(it))
                                          }) : CollectionFieldData<R, List<T>, T, List<T>>,
                                               MapEngineInterface<R, Int, T> by mapEngine {

    val converter: (Map<Int, T>) -> List<T> = {
        it.toList()
                .sortedBy { it.first }
                .map { it.second }
    }

    override fun isType(a: Any): Boolean {
        return a::class.isSubclassOf(List::class)
    }

    override fun fNEqualsValue(o: List<T>, sep: String): String {
        return mapEngine.fNEqualsValue(converterToMap(o), sep)
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<List<T>> {
        val ret = mapEngine.getValue(readCache, readValue, number, key)
        return NextSize(converter(ret.t), ret.i)
    }
}

internal interface MapEngineInterface<R : Any, M : Any, T : Any> {
    val helperTable: HelperTable

    fun onIdField(idField: KeyType)
    fun toStoreData(insertMap: InsertMap, objectValue: List<R>)

    fun createTableForeign(tableNames: Set<String>): Set<String>

    fun insertLists(r: List<R>)
    fun deleteLists(keySimpleType: Any)

    fun clearLists()
}

internal class MapEngine<R : Any, M : Any, T : Any>(val propertyData: MapProperty<*, *, T, *>,
                                                    val persisterProvider: PersisterProvider,
                                                    val persister: Persister,
                                                    val parrentClass: KClass<R>,
                                                    val getObjectMethod: (R) -> Map<M, T>) : MapEngineInterface<R, M, T> {
    override val helperTable: HelperTable
        get() = helper

    val name: String = propertyData.name
    private lateinit var idField: KeyType
    private lateinit var helper: HelperTable

    private val helperTableName = "${persisterProvider[parrentClass]}_${propertyData.receiverType.simpleName}_$name"

    private val keyField = propertyData.keyClazz.toFieldData(persisterProvider, KeyAnnotation(propertyData.property),
                                                             "key",
                                                             persister) as FieldData<Any, Any, Any, Any>
    private val valueField = propertyData.clazz.toFieldData(persisterProvider, KeyAnnotation(propertyData.property), "value", persister)


    override fun toStoreData(insertMap: InsertMap, r: List<R>) {
        val b = r.flatMap { key ->
            val p = getObjectMethod(key)
            p.map { key to it }
        }
        keyField.toStoreData(insertMap, b.map { it.second.key })
        b.forEach {
            val id1 = idField.hashCodeXIfAutoKey(it.first)
            val key = keyField.getObject(it.second.key)
            val insertKey = InsertKey(helperTableName, listOf(id1, key))
            if (!insertMap.exists(insertKey)) {
                val x = idField.insertObject(id1)
                val y = keyField.insertObject(key)
                val z = valueField.insertObject(it.second.value)
                val insertRequest = InsertRequest(x + y + z)
                insertMap.put(insertKey, insertRequest)
            }
        }
        valueField.toStoreData(insertMap, b.map { it.second.value })
    }


    override fun onIdField(idField: KeyType) {
        this.idField = idField
        val fields3 = listOf(idField, keyField, valueField) as List<FieldData<Any, Any, Any, Any>>
        helper = persister.HelperTable(fields3,
                                       helperTableName,
                                       idField.numberOfKeyFields() + keyField.numberOfKeyFields() + valueField.numberOfKeyFields())
    }

    fun fNEqualsValue(o: Map<M, T>, sep: String): String {
        return sequenceOf(o.values.map { valueField.fNEqualsValue(it, sep) },
                          o.keys.map { keyField.fNEqualsValue(it, sep) })
                .joinToString(", ")
    }

    override fun createTableForeign(tableNames: Set<String>): Set<String> {
        return valueField.createTableForeign(keyField.createTableForeign(helperTable.persistWithName(checkName = tableNames)))
    }

    override fun insertLists(r: List<R>) {
        val b = r.flatMap { key ->
            val p = getObjectMethod(key)
            p.map { key to it }
        }
        keyField.storeManyToOneObject(b.map { it.second.key })
        helperTable.insertListBy(b.map {
            val x = idField.insertObject(idField.hashCodeXIfAutoKey(it.first))
            val y = keyField.insertObject(keyField.getObject(it.second.key))
            val z = valueField.insertObject(it.second.value)
            x + y + z
        })
        valueField.storeManyToOneObject(b.map { it.second.value })
    }

    override fun deleteLists(keySimpleType: Any) {
        helperTable.deleteBy(idField.name, keySimpleType)
    }

    fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<Map<M, T>> {
        if (key.isEmpty()) {
            throw NullPointerException("a List cannot be a key")
        }
        val fn = idField.autoIdFNEqualsValue(key, " AND ")
        val read = helperTable.readIntern(fn, readCache)
        val map = read.map { it[1].value as M to it[2].value as T }
                .toMap()

        return NextSize(map, number)
    }

    override fun clearLists() {
        helperTable.clear()
    }
}