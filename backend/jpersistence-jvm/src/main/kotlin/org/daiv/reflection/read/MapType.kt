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
import org.daiv.reflection.annotations.TableData
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.Table

//internal interface Identity<T : Any> {
//    val simpleProperty: SimpleProperty
//
//    fun storeManyToOneObject(t: T)
//
//    fun persist()
//
//    fun getValue(tableRow: List<Any>, readValue: ReadValue): T
//    fun keySimpleType(t: T): Any
//
//    fun keyTables(): List<TableData>
//
//    fun helperTables(): List<TableData>
//
//    fun fNEqualsValue(it: T, name: String, sep: String): String
//}
//
//internal data class SimpleIdentity<T : Any>(val clazz: KClass<T>,
//                                            val name: String,
//                                            val columnOfHelperTable: Int,
//                                            val tableName: String) : Identity<T> {
//    override val simpleProperty = SimpleProperty(clazz as KClass<Any>, name, columnOfHelperTable)
//
//    override fun storeManyToOneObject(t: T) {}
//
//    override fun persist() {}
//
//    override fun getValue(tableRow: List<Any>, readValue: ReadValue): T = tableRow[columnOfHelperTable] as T
//
//    override fun keySimpleType(t: T): Any = t
//
//    override fun keyTables(): List<TableData> = emptyList()
//
//    override fun helperTables(): List<TableData> = emptyList()
//
//    override fun fNEqualsValue(it: T, name: String, sep: String): String {
//        return ReadSimpleType.static_fNEqualsValue(it, this.name, sep)
//    }
//}
//
//internal data class EnumIdentity<T : Any>(val simpleIdentity: SimpleIdentity<T>) : Identity<T> by simpleIdentity {
//
//    override fun getValue(tableRow: List<Any>, readValue: ReadValue): T =
//            EnumType.getEnumValue(simpleIdentity.clazz.qualifiedName!!, tableRow[simpleIdentity.columnOfHelperTable] as String)
//
//}
//
//
//internal data class ComplexIdentity<T : Any> constructor(val clazz: KClass<T>,
//                                                         val persister: Persister,
//                                                         val name: String,
//                                                         val columnOfHelperTable: Int,
//                                                         val tableName: String) : Identity<T> {
//    private val persisterData: ReadPersisterData<T, Any> = ReadPersisterData(clazz, persister)
//    override val simpleProperty = SimpleProperty(persisterData.keyClassSimpleType(), name, columnOfHelperTable)
//
//    private val table = persister.Table(clazz, tableName)
//    override fun storeManyToOneObject(t: T) = persisterData.storeManyToOneObject(t, table)
//
//    override fun persist() = table.persist()
//
//    override fun getValue(tableRow: List<Any>, readValue: ReadValue): T {
//        return if (clazz.java.isPrimitiveOrWrapperOrString()) {
//            tableRow[columnOfHelperTable] as T
//        } else {
//            readValue.read(table, tableRow[columnOfHelperTable])
////            table.read(tableRow[columnOfHelperTable])!!
//        }
//    }
//
//    override fun keySimpleType(t: T) = persisterData.keySimpleType(t)
//
//    override fun keyTables() = persisterData.keyTables() + table.tableData()
//
//    override fun helperTables() = persisterData.helperTables()
//
//    override fun fNEqualsValue(it: T, name: String, sep: String) = persisterData.fNEqualsValue(it, name, sep)
//}
//
//internal class ComplexProperty<R : Any, T : Any>(override val clazz: KClass<T>,
//                                                 override val name: String,
//                                                 val field: FieldData<R, T, T>) : PropertyData<R, T, T> {
//    override fun getObject(r: R) = field.getObject(r)
//}
//
//internal fun <T : Any> getIdentity(clazz: KClass<T>,
//                                   persister: Persister,
//                                   name: String,
//                                   columnOfHelperTable: Int,
//                                   tableName: String): Identity<T> {
//    return when {
//        clazz.java.isPrimitiveOrWrapperOrString() -> {
//            SimpleIdentity(clazz, name, columnOfHelperTable, tableName)
//        }
//        clazz.isEnum() -> {
//            EnumIdentity(SimpleIdentity(clazz, name, columnOfHelperTable, tableName))
//        }
//        else -> {
//
//            ComplexIdentity(clazz, persister, name, columnOfHelperTable, tableName)
//        }
//    }
//}
data class MapHelper(val id: Any, val key: Any, val value: Any)

internal class MapType<R : Any, T : Any, M : Any>(override val propertyData: MapProperty<R, T, M>,
                                                  override val prefix: String?,
                                                  val persister: Persister,
                                                  val manyMap: ManyMap,
                                                  val idField: FieldData<R, Any, Any>) : CollectionFieldData<R, Map<M, T>, T> {

    //    private val keyIdentity = getIdentity(propertyData.keyClazz,
//                                          persister,
//                                          "key_${propertyData.name}",
//                                          1,
//                                          manyMap.tableNameKey)
//    private val valueIdentity = getIdentity(propertyData.clazz,
//                                            persister,
//                                            "value_${propertyData.name}",
//                                            2,
//                                            manyMap.tableNameValue)
    private val helperTable: Table<MapHelper>

    private val helperTableName = "${propertyData.receiverType.simpleName}_$name"

    //    private val firstColumn = "_${propertyData.receiverType.simpleName!!}"
    val keyField = FieldDataFactory.fieldsRead<M, Any>(propertyData.keyClazz, "key", persister)
            .first()
    val valueField = FieldDataFactory.fieldsRead<T, Any>(propertyData.clazz, "value", persister)
            .first()

    init {
        val fields3 = listOf(idField, keyField.idFieldSimpleType(), valueField.idFieldSimpleType())
        helperTable = persister.Table(ReadPersisterData(fields3 as List<FieldData<MapHelper, Any, Any>>) { fieldValues ->
            MapHelper(fieldValues[0], fieldValues[1], fieldValues[2])
        }, helperTableName)
//        val readPersisterData: ReadPersisterData<InsertData, Any>
//        val fields = listOf(ReadSimpleType(SimpleProperty(keyClass, propertyData.receiverType.simpleName!!, 0)),
//                            ReadSimpleType(keyIdentity.simpleProperty),
//                            ReadSimpleType(valueIdentity.simpleProperty))
//        readPersisterData = ReadPersisterData(fields) { i: List<ReadFieldValue> ->
//            InsertData(listOf(i[0].value, i[1].value, i[2].value))
//        }
//        val fields2: List<ComplexSameTableType<ComplexObject, Any>> = listOf(ComplexSameTableType(ComplexProperty(
//                ""), readPersisterData as ReadPersisterData<Any, Any>))
//        val n: ReadPersisterData<ComplexObject, Any> = ReadPersisterData(fields2) { i: List<ReadFieldValue> ->
//            ComplexObject(i.first().value as InsertData)
//        }
//        helperTable = persister.Table(n, helperTableName)
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

    override fun insertLists(keySimpleType: Any, r: R) {
        val o = getObject(r)
        o.forEach {
            helperTable.insert(MapHelper(keySimpleType, keyField.keyLowSimpleType(it.key), valueField.keyLowSimpleType(it.value)))
//            helperTable.insert(ComplexObject(InsertData(listOf(keySimpleType,
//                                                               keyIdentity.keySimpleType(it.key),
//                                                               valueIdentity.keySimpleType(it.value)))))
            keyField.storeManyToOneObject(it.key)
            valueField.storeManyToOneObject(it.value)
        }
    }

    override fun deleteLists(keySimpleType: Any) {
        helperTable.delete(idField.name, keySimpleType)
    }

    val keyTable = persister.Table(propertyData.keyClazz, manyMap.tableNameKey)
    val valueTable = persister.Table(propertyData.clazz, manyMap.tableNameValue)

    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<Map<M, T>> {
        if (key == null) {
            throw NullPointerException("a List cannot be a key")
        }

        val map = readValue.helperTable(helperTable, idField.name, key)
                .map {
                    readValue.read(keyTable, it.key) to readValue.read(valueTable, it.value)
                }
                .toMap()
        return NextSize(map, number)
//        val complexObjectList = readValue.helperTable(helperTable, firstColumn, key)
////        val complexObjectList = helperTable.read("_${propertyData.receiverType.simpleName!!}", key)
//        val t = complexObjectList.map {
//            keyIdentity.getValue(it.insertData.list, readValue) to valueIdentity.getValue(it.insertData.list, readValue)
//        }
//                .toMap()
//        return NextSize(t, number)
    }

    override fun helperTables(): List<TableData> {
        return keyField.helperTables() + valueField.helperTables() + helperTable.tableData()
    }

    override fun keyTables(): List<TableData> {
        return keyField.keyTables() + valueField.keyTables()
    }
}
