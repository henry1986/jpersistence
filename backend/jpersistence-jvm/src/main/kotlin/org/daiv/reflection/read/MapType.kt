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
import java.lang.RuntimeException
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf


internal class MapType constructor(override val propertyData: DefaultMapProperty,
                                   val persisterProvider: PersisterProvider,
                                   override val prefix: String?,
                                   val persister: Persister,
                                   val parentClass: KClass<Any>,
                                   val mapEngine: MapEngine = MapEngine(propertyData,
                                                                        persisterProvider,
                                                                        persister,
                                                                        parentClass) {
                                       propertyData.getObject(it) as Map<Any, Any>
                                   }) :
        CollectionFieldData, MapEngineInterface by mapEngine {

    override fun isType(a: Any): Boolean {
        return a::class.isSubclassOf(Map::class)
    }

    override fun fNEqualsValue(o: Any, sep: String): String {
        return mapEngine.fNEqualsValue(o, sep)
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<ReadAnswer<Any>> {
        return mapEngine.getValue(readCache, readValue, number, key)
    }
}

internal val listConverter: (Any) -> Map<Any, Any> = {
    it as List<Any>
    it.mapIndexed { index, t -> index to t }
            .toMap()
}

internal class ListType constructor(override val propertyData: ListMapProperty,
                                    val persisterProvider: PersisterProvider,
                                    override val prefix: String?,
                                    val persister: Persister,
                                    val parentClass: KClass<Any>,
                                    val mapEngine: MapEngine = MapEngine(propertyData,
                                                                         persisterProvider,
                                                                         persister,
                                                                         parentClass) {
                                        propertyData.getObject(it)
                                    }) : CollectionFieldData,
                                         MapEngineInterface by mapEngine {

    val converter: (ReadAnswer<Map<Int, Any>>) -> ReadAnswer<List<Any>> = {
        ReadAnswer(it.t!!.toList()
                           .sortedBy { it.first }
                           .map { it.second }, true)
    }

    override fun isType(a: Any): Boolean {
        return a::class.isSubclassOf(List::class)
    }

    override fun fNEqualsValue(o: Any, sep: String): String {
        return mapEngine.fNEqualsValue(listConverter(o as List<Any>), sep)
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<ReadAnswer<Any>> {
        val ret = mapEngine.getValue(readCache, readValue, number, key)
        return NextSize(converter(ret.t as ReadAnswer<Map<Int, Any>>), ret.i) as NextSize<ReadAnswer<Any>>
    }
}

internal interface MapEngineInterface {
    val helperTable: HelperTable

    fun onIdField(idField: KeyType)
    suspend fun toStoreData(insertMap: InsertMap, objectValue: List<Any>)

    fun createTableForeign(tableNames: Set<String>): Set<String>

    fun deleteLists(key: List<Any>)

    fun clearLists()
}

internal class MapEngine(val propertyData: MapProperty,
                         val persisterProvider: PersisterProvider,
                         val persister: Persister,
                         val parrentClass: KClass<Any>,
                         val getObjectMethod: (Any) -> Any) : MapEngineInterface {
    override val helperTable: HelperTable
        get() = helper

    val name: String = propertyData.name
    private lateinit var idField: KeyType
    private lateinit var helper: HelperTable

    val innerTableName: String = "${persisterProvider.innerTableName(parrentClass)}_${propertyData.receiverType.simpleName}_$name"

    private val helperTableName
        get() = "${persisterProvider[parrentClass]}_${propertyData.receiverType.simpleName}_$name"

    private val valueField = propertyData.type.toLowField(persisterProvider, 0, "value")


    override suspend fun toStoreData(insertMap: InsertMap, r: List<Any>) {
        val p = r.map { key -> key to getObjectMethod(key) }
        p.forEach { x ->
            val id1 = idField.hashCodeXIfAutoKey(x.first)
            insertMap.toBuild(RequestTask(InsertKey(helperTableName, id1), {
                val b = idField.insertObject(id1)
                valueField.insertObjects(x.second)
                        .map {
                            InsertRequest(b + it)
                        }
            }) {
                valueField.toStoreData(insertMap, listOf(x.second))
            })

        }
    }


    override fun onIdField(idField: KeyType) {
        this.idField = idField
        val fields3 = listOf(idField, valueField)
        val keyFields = (listOf(idField) + valueField.additionalKeyFields())
        helper = persister.HelperTable(keyFields,
                                       fields3,
                                       listOf(valueField),
                                       { innerTableName },
                                       { helperTableName })
        persisterProvider.registerHelperTableName(helperTableName)
    }

    fun fNEqualsValue(o: Any, sep: String): String {
        o as Map<Any, Any>
        return sequenceOf(o.values.map { valueField.fNEqualsValue(it, sep) })
                .joinToString(", ")
    }

    override fun createTableForeign(tableNames: Set<String>): Set<String> {
        return valueField.createTableForeign(helperTable.persistWithName(checkName = tableNames))
    }

    override fun deleteLists(key: List<Any>) {
        helperTable.deleteBy(idField.name, key)
    }

    fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<ReadAnswer<Any>> {
        if (key.isEmpty()) {
            throw NullPointerException("a List cannot be a key")
        }
        val fn = idField.autoIdFNEqualsValue(key, " AND ")
        val read = helperTable.readIntern(fn, readCache)
        return NextSize(ReadAnswer(valueField.readFromList(read.map { it.drop(1) })), number)
    }

    override fun clearLists() {
        helperTable.clear()
    }
}