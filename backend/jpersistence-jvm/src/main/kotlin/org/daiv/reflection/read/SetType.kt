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
import org.daiv.reflection.annotations.SameTable
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.Table
import kotlin.reflect.KClass


data class ListHelper(val key: Any, val value: Any)
/**
 * ELH = EmbeddedListHelper
 */
data class ELH(@SameTable val listHelper: ListHelper)


data class HelperProperty<R : Any, T : Any>(override val name: String, override val clazz: KClass<T>, val func: R.() -> T) :
        PropertyData<R, T, T> {
    override val receiverType: KClass<R>? = null
    override fun getObject(r: R) = r.func()
}

data class KeyProperty<T : Any>(override val name: String, val func: T.() -> Any) :
        PropertyData<T, Any, Any> {
    override val clazz: KClass<Any> = Any::class
    override val receiverType: KClass<T>? = null
    override fun getObject(r: T) = r.func()
}

//data class ValueProperty(override val name: String) :
//        PropertyData<ListHelper, Any, Any> {
//    override val clazz: KClass<Any> = Any::class
//    override val receiverType: KClass<ListHelper>? = null
//    override fun getObject(r: ListHelper) = r.value
//}

internal class SetType<R : Any, T : Any> constructor(override val propertyData: SetProperty<R, T>,
                                                     private val many: ManyList,
                                                     val persister: Persister,
                                                     override val prefix: String?,
                                                     val remoteIdField: FieldData<R, Any, Any, Any>) :
        CollectionFieldData<R, Set<T>, T, Set<T>> {
    private val helperTable: Table<ELH>
    private val helperTableName = "${propertyData.receiverType.simpleName}_$name"

    private val remoteValueField = propertyData.clazz.toFieldData(KeyAnnotation(propertyData.property), "value", persister)

    val valueField = ForwardingField(KeyProperty<ListHelper>("") { value } as PropertyData<Any, Any, Any>,
                                     remoteValueField as FieldData<Any, Any, Any, Any>)
    val idField = ForwardingField(KeyProperty<ListHelper>("") { key } as PropertyData<Any, Any, Any>,
                                  remoteIdField as FieldData<Any, Any, Any, Any>)


    init {
        val fields3 = listOf(idField, valueField)
        val listHelperPersisterData = ReadPersisterData(fields3 as List<FieldData<ListHelper, Any, Any, Any>>) { fieldValues ->
            ListHelper(fieldValues[0].value, fieldValues[1].value)
        }
        val c = ComplexSameTableType(HelperProperty<ELH, ListHelper>("", ListHelper::class) { listHelper },
                                     null,
                                     null,
                                     persister,
                                     listHelperPersisterData)
        val r = ReadPersisterData(listOf(c as FieldData<ELH, Any, Any, Any>)) { fieldValues: List<ReadFieldValue> ->
            ELH(fieldValues[0].value as ListHelper)
        }
        helperTable = persister.Table(r, helperTableName)
    }

    /**
     * get receiver object as parameter [r] and insert its values to the helper table as well as they
     */
    override fun insertLists(r: List<R>) {
        val b = r.flatMap { key ->
            val p = getObject(key).toList()
            p.map { key to it }
        }
        remoteValueField.storeManyToOneObject(b.map { it.second })
        helperTable.insert(b.map { ELH(ListHelper(it.first, it.second)) })
    }

    override fun deleteLists(keySimpleType: Any) {
        helperTable.delete(idField.name, keySimpleType)
    }

    override fun clearLists(){
        helperTable.clear()
    }

    override fun createTable() = helperTable.persist()
    override fun createTableForeign() = valueField.persist()

    override fun fNEqualsValue(o: Set<T>, sep: String): String {
        return o.map {
            valueField.fNEqualsValue(remoteValueField.getObject(it), sep)
        }
                .joinToString(", ")
    }


    @Suppress("UNCHECKED_CAST")
    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<Set<T>> {
        if (key == null) {
            throw NullPointerException("a List cannot be a key")
        }
        val x = readValue.helperTable(helperTable, remoteIdField.name, key)
                .map { it.listHelper.value as T }
                .toSet()
        return NextSize(x, number)
    }

    override fun helperTables() = valueField.helperTables() + helperTable.tableData()

    override fun keyTables() = valueField.keyTables()
}