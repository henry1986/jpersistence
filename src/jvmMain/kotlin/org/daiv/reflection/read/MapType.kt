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

import org.daiv.reflection.annotations.IFaceForList
import org.daiv.reflection.annotations.ObjectOnly
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.*
import org.daiv.reflection.persister.Persister.HelperTable
import org.daiv.reflection.plain.ObjectKey
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf


internal class MapType constructor(
    override val propertyData: CollectionProperty,
    val persisterProvider: PersisterProvider,
    val objectOnly: ObjectOnly?,
    val iFaceForList: IFaceForList?,
    override val prefix: String?,
    val persister: Persister,
    val parentClass: KClass<Any>
) :
    CollectionFieldData {
    override fun isType(a: Any): Boolean {
        return a::class.isSubclassOf(propertyData.type.classifier as KClass<*>)
    }

    override val helperTable: HelperTable
        get() = helper

    //    val name: String = propertyData.name
    private lateinit var idField: KeyType
    private lateinit var helper: HelperTable

    val innerTableName: String =
        "${persisterProvider.innerTableName(parentClass)}_${propertyData.receiverType.simpleName}_$name"

    private val helperTableName
        get() = "${persisterProvider[parentClass]}_${propertyData.receiverType.simpleName}_$name"

    private val valueField = propertyData.type.toLowField(persisterProvider, 0, "value", objectOnly, iFaceForList)


    override suspend fun toStoreData(insertMap: InsertMap, r: List<Any>) {
        val p = r.map { key -> key to propertyData.getObject(key) }
        p.forEach { x ->
            val id1 = idField.keyToWrite(x.first)
//            val objectKey = if (id1.isAutoId()) {
//                insertMap.toObjectKey(persisterProvider.table(propertyData.receiverType), id1)
//            } else {
//                id1.toObjectKey()
//            }
//            val id1 = idField.hashCodeXIfAutoKey(x.first)
            insertMap.toBuild(HelperRequestTask(InsertKey(helperTable, id1), {
                val objectKey =
                    insertMap.readCache.keyForObjectFromCache(persisterProvider.table(propertyData.receiverType), id1)!!
                val b = idField.insertObject(objectKey.keys(), insertMap.readCache)
                valueField.insertObjects(x.second, insertMap.readCache)
                    .map { InsertRequest(b + it) }
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
            if (valueField.isAlsoKeyField()) emptyList() else listOf(valueField),
            { innerTableName },
            { helperTableName })
        persisterProvider.registerHelperTableName(helper)
    }

    override fun fNEqualsValue(o: Any, sep: String, keyGetter: KeyGetter): String? {
        return valueField.fNEqualsValue(o, sep, keyGetter)
    }

    override fun createTableForeign(tableNames: Set<String>): Set<String> {
        return valueField.createTableForeign(helperTable.persistWithName(checkName = tableNames))
    }

    override fun deleteLists(key: List<Any>) {
        helperTable.deleteBy(idField.name, key)
    }

    override fun getValue(
        readCache: ReadCache,
        readValue: ReadValue,
        number: Int,
        objectKey: ObjectKey
    ): NextSize<ReadAnswer<Any>> {
        val key = objectKey.keys()
        if (key.isEmpty()) {
            throw NullPointerException("a Collection with no autoId cannot be a key")
        }
        val fn = idField.autoIdFNEqualsValue(key, " AND ", readCache)
        val read = helperTable.readIntern(fn, readCache)
        val x = valueField.readFromList(read.map { it.drop(1) })
        return NextSize(ReadAnswer(x), number)
    }

    override fun clearLists() {
        helperTable.clear()
    }
}

internal val listConverter: (Any) -> Map<Any, Any> = {
    it as List<Any>
    it.mapIndexed { index, t -> index to t }
        .toMap()
}
internal val listBackConverter: (Map<Any?, Any?>) -> Any = {
    it.toList()
        .sortedBy { it.first as Int }
        .map { it.second }
}

