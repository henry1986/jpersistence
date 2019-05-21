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

import mu.KotlinLogging
import org.daiv.reflection.annotations.ManyMap
import org.daiv.reflection.annotations.SameTable
import org.daiv.reflection.annotations.TableData
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.Table

data class MapHelper(val id: Any, val key: Any, val value: Any)
/**
 * EMH = EmbeddedMapHelper
 */
data class EMH(@SameTable val mapHelper: MapHelper)

internal class MapType<R : Any, T : Any, M : Any, X : Any>(override val propertyData: MapProperty<R, T, M>,
                                                           override val prefix: String?,
                                                           val persister: Persister,
                                                           val parentTableName: String,
                                                           val converter: (Map<M, T>) -> X,
                                                           val remoteIdField: FieldData<R, Any, Any, Any>) :
        CollectionFieldData<R, Map<M, T>, T, X> {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    private val helperTable: Table<EMH>

    private val helperTableName = "${parentTableName}_${propertyData.receiverType.simpleName}_$name"

    private val remoteKeyField = propertyData.keyClazz.toFieldData<M, Any>(KeyAnnotation(propertyData.property), "key", persister)
    private val remoteValueField = propertyData.clazz.toFieldData<T, Any>(KeyAnnotation(propertyData.property), "value", persister)

    val keyField = ForwardingField(KeyProperty<MapHelper>("") { key } as PropertyData<Any, Any, Any>,
                                   remoteKeyField as FieldData<Any, Any, Any, Any>)
    val valueField = ForwardingField(KeyProperty<MapHelper>("") { value } as PropertyData<Any, Any, Any>,
                                     remoteValueField as FieldData<Any, Any, Any, Any>)
    val idField = ForwardingField(KeyProperty<MapHelper>("") { id } as PropertyData<Any, Any, Any>,
                                  remoteIdField as FieldData<Any, Any, Any, Any>)

    init {
        val fields3 = listOf(idField, keyField, valueField)
        val listHelperPersisterData = ReadPersisterData(fields3 as List<FieldData<MapHelper, Any, Any, Any>>) { fieldValues ->
            MapHelper(fieldValues[0].value, fieldValues[1].value, fieldValues[2].value)
        }
        val c = ComplexSameTableType(HelperProperty<EMH, MapHelper>("", MapHelper::class) { mapHelper },
                                     null,
                                     null,
                                     persister,
                                     listHelperPersisterData)
        val r = ReadPersisterData(listOf(c as FieldData<EMH, Any, Any, Any>)) { fieldValues: List<ReadFieldValue> ->
            EMH(fieldValues[0].value as MapHelper)
        }
        helperTable = persister.Table(r, helperTableName)
    }

    override fun fNEqualsValue(o: Map<M, T>, sep: String): String {
        return sequenceOf(o.values.map { valueField.fNEqualsValue(it, sep) },
                          o.keys.map { keyField.fNEqualsValue(it, sep) })
                .joinToString(", ")
    }

    override fun createTable() = helperTable.persist()
    override fun createTableForeign() {
        keyField.persist()
        valueField.persist()
    }

    override fun insertLists(r: List<R>) {
        val b = r.flatMap { key ->
            val p = getObject(key)

            p.map { key to it }
        }
        keyField.storeManyToOneObject(b.map { it.second.key })
        valueField.storeManyToOneObject(b.map { it.second.value })
        helperTable.insert(b.map { EMH(MapHelper(it.first, it.second.key, it.second.value)) })
    }

    override fun deleteLists(keySimpleType: Any) {
        helperTable.delete(idField.name, keySimpleType)
    }

    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<X> {
        if (key == null) {
            throw NullPointerException("a List cannot be a key")
        }

        val map = readValue.helperTable(helperTable, idField.name, key)
                .map {
                    it.mapHelper.key as M to it.mapHelper.value as T
                }
                .toMap()
        return NextSize(converter(map), number)
    }

    override fun helperTables(): List<TableData> {
        return keyField.helperTables() + valueField.helperTables() + helperTable.tableData()
    }

    override fun keyTables(): List<TableData> {
        return keyField.keyTables() + valueField.keyTables()
    }
}
