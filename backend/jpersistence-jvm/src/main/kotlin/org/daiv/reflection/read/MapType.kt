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
import org.daiv.reflection.annotations.TableData
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.HelperTable


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

    private val helperTable: HelperTable

    private val helperTableName = "${parentTableName}_${propertyData.receiverType.simpleName}_$name"

    private val remoteKeyField = propertyData.keyClazz.toFieldData(KeyAnnotation(propertyData.property), "key", persister)
    private val remoteValueField = propertyData.clazz.toFieldData(KeyAnnotation(propertyData.property), "value", persister)

    val keyField = ForwardingField(KeyProperty("") as PropertyData<Any, Any, Any>,
                                   remoteKeyField as FieldData<Any, Any, Any, Any>)
    val valueField = ForwardingField(KeyProperty("") as PropertyData<Any, Any, Any>,
                                     remoteValueField as FieldData<Any, Any, Any, Any>)
    val idField = ForwardingField(KeyProperty("") as PropertyData<Any, Any, Any>,
                                  remoteIdField as FieldData<Any, Any, Any, Any>)

    init {
        val fields3 = listOf(idField, keyField, valueField)
        helperTable = persister.HelperTable(fields3, helperTableName, 2)
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
        helperTable.insertListBy(b.map {
            val x = idField.insertObject(it.first)
                    .first()
            val y = keyField.insertObject(it.second.key)
                    .first()
            val z = valueField.insertObject(it.second.value)
            listOf(x, y) + z
        })
    }

    override fun deleteLists(keySimpleType: Any) {
        helperTable.deleteBy(idField.name, keySimpleType)
    }

    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<X> {
        if (key == null) {
            throw NullPointerException("a List cannot be a key")
        }

        val read = readValue.helperTable(helperTable, idField.name, key)
        val map = read.map { it[1].value as M to it[2].value as T }
                .toMap()

        return NextSize(converter(map), number)
    }

    override fun clearLists() {
        helperTable.clear()
    }
}
