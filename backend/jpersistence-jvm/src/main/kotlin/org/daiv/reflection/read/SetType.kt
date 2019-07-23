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
import org.daiv.reflection.persister.Persister.HelperTable
import org.daiv.reflection.persister.Persister.Table
import kotlin.reflect.KClass

data class KeyProperty(override val name: String) :
        PropertyData<Any, Any, Any> {
    override val clazz: KClass<Any> = Any::class
    override val receiverType: KClass<Any>? = null
    override fun getObject(r: Any) = throw RuntimeException("should not have been called")
}

internal class SetType<R : Any, T : Any> constructor(override val propertyData: SetProperty<R, T>,
                                                     private val many: ManyList,
                                                     val persister: Persister,
                                                     override val prefix: String?,
                                                     val remoteIdField: FieldData<R, Any, Any, Any>) :
        CollectionFieldData<R, Set<T>, T, Set<T>> {
    private val helperTableName = "${propertyData.receiverType.simpleName}_$name"

    private val remoteValueField = propertyData.clazz.toFieldData(KeyAnnotation(propertyData.property), "value", persister)

    val valueField = ForwardingField(KeyProperty("") as PropertyData<Any, Any, Any>,
                                     remoteValueField as FieldData<Any, Any, Any, Any>)
    val idField = ForwardingField(KeyProperty("") as PropertyData<Any, Any, Any>,
                                  remoteIdField as FieldData<Any, Any, Any, Any>)
    private val helperTable = persister.HelperTable(listOf(idField, valueField), helperTableName, 2)



    /**
     * get receiver object as parameter [r] and insert its values to the helper table as well as they
     */
    override fun insertLists(r: List<R>) {
        val b = r.flatMap { key ->
            val p = getObject(key).toList()
            p.map { key to it }
        }
        remoteValueField.storeManyToOneObject(b.map { it.second })
        helperTable.insertListBy(b.map {
            val x = idField.insertObject(it.first)
                    .first()
            val y = remoteValueField.insertObject(it.second)
                    .first()
            listOf(x, y)
        })
//        helperTable.insert(b.map { ELH(ListHelper(it.first, it.second)) })
    }

    override fun deleteLists(keySimpleType: Any) {
        helperTable.deleteBy(idField.name, keySimpleType)
    }

    override fun clearLists() {
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
        val ret = readValue.helperTable(helperTable, idField.name, key)
                .map {
                    it[1].value as T
                }
                .toSet()
        return NextSize(ret, number)
    }
}
