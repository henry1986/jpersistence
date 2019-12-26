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
import org.daiv.reflection.plain.ObjectKey
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible

internal data class ReadFieldValue(val value: Any?, val fieldData: FieldData)
private class Reader(val readCache: ReadCache,
                     val fields: List<FieldData>,
                     val key: ObjectKey,
                     val readValue: ReadValue) {
    fun read(counter: Int, i: Int = 0, list: List<ReadFieldValue> = emptyList()): NextSize<List<ReadFieldValue>> {
        if (i < fields.size) {
            val field = fields[i]
            val (value, nextCounter) = field.getValue(readCache, readValue, counter, key)
            val readFieldValue = ReadFieldValue(value.t, field)
            return read(nextCounter, i + 1, list + readFieldValue)
        }
        return NextSize(list, counter)
    }
}


internal interface InternalRPD {
    val key: KeyType
    val fields: List<FieldData>
    val noKeyFields: List<FieldData>
    fun field(fieldName: String): FieldCollection {
        return fields.find { it.name == fieldName }
                ?: fields.flatMap { it.subFields() }.find { it.name == fieldName }
                ?: key.fields.find { it.name == fieldName }
                ?: throw RuntimeException("couldn't find any fields with name: $fieldName")
    }

    fun dropHelper() {
        fields.forEach { it.dropHelper() }
    }

    fun fromWhere(fieldName: String, id: Any, sep: String, keyCreator: KeyCreator): String {
        try {
            return if (key.fieldName == fieldName) {
                key.whereClause(id as List<Any>, sep, keyCreator)
            } else {
                field(fieldName).whereClause(id, sep, keyCreator)
            }
        } catch (t: Throwable) {
            throw t
        }
    }

    fun recursiveCreateTableForeign(tableNames: Set<String>, i: Int = 0): Set<String> {
        if (i < fields.size) {
            return recursiveCreateTableForeign(fields[i].createTableForeign(tableNames), i + 1)
        }
        return tableNames
    }

    fun createTableForeign(tableName: Set<String>): Set<String> {
        return recursiveCreateTableForeign(tableName)
    }


    fun underscoreName(): String {
        return fields.mapNotNull { it.underscoreName() }
                .joinToString(", ")
    }

    private fun createTableInnerData(skip: Int): String {
        return fields
                .asSequence()
                .drop(skip)
                .map { it.toTableHead() }
                .filterNotNull()
                .joinToString(separator = ", ")

    }

    fun createTableKeyData(): String {
        return fields.joinToString(separator = ", ", transform = { it.key() })
    }

    fun getIdName() = key.fieldName

    fun createTable(): String {
        val createTableInnerData = createTableInnerData(0)
//${key.toTableHead()},
        val s = if (createTableInnerData == "") "" else "$createTableInnerData, "
        try {
            return ("(${s}PRIMARY KEY(${key.toPrimaryKey()}));")
        } catch (t: Throwable) {
            throw t
        }
    }

    fun readKey(readValue: ReadValue): Any {
        return key.getColumnValue(readValue)
    }

    fun innerRead(readValue: ReadValue, counter: Int = 1, readCache: ReadCache): NextSize<List<ReadFieldValue>> {
        val keyAnswer = key.getKeyValue(readCache, readValue, counter)
//        val x = key.getValue(readCache, readValue, counter, ObjectKey.empty)
        return Reader(readCache, noKeyFields, keyAnswer.t.t!!, readValue).read(keyAnswer.i,
                                                                               list = keyAnswer.t.t.keys().mapIndexed { i, e ->
                                                                                   ReadFieldValue(e, key.fields[i])
                                                                               })
    }

    /**
     * method reads the [ReadValue] only to a list of [ReadFieldValue], not to [R]
     */
    fun readWOObject(readValue: ReadValue, readCache: ReadCache): List<ReadFieldValue> {
        return innerRead(readValue, readCache = readCache).t
    }

    fun keyName(): String {
        return key.name()
    }

    fun keyColumnName() = key.columnName()

    fun <X> onFields(f: FieldData.() -> X): List<X> {
        return fields.map(f)
    }

    fun names() = fields.map { it.copyTableName() }.flatMap { it.entries }.map { it.key to it.value }.toMap()

    fun namesToList() = names().map { it.value }

    private fun mapOtherNames(otherNames: Map<String, String>, name: String, request: (String) -> String): String? {
        val ret = otherNames[name]
        return if (ret == "#addKeyColumn") {
            request(name)
        } else {
            ret
        }

    }

    fun copyTable(otherNames: Map<String, String>, request: (String) -> String): String {
        val names = namesToList()
        return "(${names.joinToString(", ")}) select ${names.map { mapOtherNames(otherNames, it, request) }.joinToString(", ")}"
    }

    fun copyHelperTable(map: Map<String, Map<String, String>>, request: (String, String) -> String) {
        fields.forEach { f -> map[f.prefixedName]?.let { f.copyData(it, request) } }
    }

    fun clearLists() {
        onFields { clearLists() }
    }

    fun classOfField(fieldName: String): KClass<Any> {
        return fields.find { it.name == fieldName }!!.propertyData.clazz
    }
}

internal data class ReadPersisterData private constructor(override val key: KeyType,
                                                          val persisterProvider: PersisterProvider,
                                                          override val fields: List<FieldData>,
                                                          private val className: String = "no name",
                                                          val method: (List<ReadFieldValue>) -> Any,
                                                          override val noKeyFields: List<FieldData> = fields.drop(
                                                                  if (key.isAuto()) 1 else key.fields.size)) : InternalRPD {

    private constructor(builder: FieldDataFactory.Builder,
                        persisterProvider: PersisterProvider,
                        className: String = "no name",
                        method: (List<ReadFieldValue>) -> Any) : this(builder.idField!!,
                                                                      persisterProvider,
                                                                      builder.fields,
                                                                      className,
                                                                      method)

    constructor(clazz: KClass<out Any>,
                persister: Persister,
                persisterProvider: PersisterProvider,
                prefix: String? = null) :
            this(FieldDataFactory(persisterProvider, clazz as KClass<Any>, prefix, persister).fieldsRead(),
                 persisterProvider,
                 clazz.simpleName ?: "no name",
                 readValue(clazz))


    init {
        if (fields.isEmpty()) {
            throw RuntimeException("this class has no fields: $className")
        }
    }

    fun evaluate(readValue: ReadValue, readCache: ReadCache): Any {
        return read(readValue, readCache).t
    }


    internal fun read(readValue: ReadValue, counter: Int, readCache: ReadCache): NextSize<Any> {
        return innerRead(readValue, counter, readCache).transform(method)
    }

    internal fun read(readValue: ReadValue, readCache: ReadCache): NextSize<Any> {
        return read(readValue, 1, readCache)
    }

    fun getKey(o: Any): Any {
        return key.keyValue(o)
    }


    fun keySimpleType(r: Any) = key.simpleType(r)

    suspend fun trueInsert(tableName: String, insertMap: InsertMap, it: Any, objectKey: ObjectKey) {
        val insertKey = InsertKey(tableName, key.toObjectKey(it, 0))
//        val insertKey = InsertKey(tableName, key.hashCodeXIfAutoKey(it))
        val request = RequestTask(insertKey, it, { listOf(InsertRequest(insertObject(it, objectKey, insertMap))) }) {
            fields.forEach { f -> f.toStoreData(insertMap, listOf(it)) }
        }
        insertMap.toBuild(request)
    }

    fun mapObjectToKey(o: Any, insertMap: KeyCreator, table: Persister.Table<*>): ObjectKey {
        val key = key.keyToWrite(o)
        return insertMap.toObjectKey(table, key)
    }

    suspend fun putInsertRequests(tableName: String, insertMap: InsertMap, o: List<Any>, table: Persister.Table<*>) {
        val keys = o.map { mapObjectToKey(it, insertMap, table) }
        putInsertRequests(tableName, insertMap, o, keys)
    }

    suspend fun putInsertRequests(tableName: String, insertMap: InsertMap, o: List<Any>, keys: List<ObjectKey>) {
        o.mapIndexed { i, it ->
            insertMap.actors.values.first()
                    .launch {
                        trueInsert(tableName, insertMap, it, keys[i])
                    }
        }
    }

    fun deleteLists(key: List<Any>) {
        onFields { deleteLists(key) }
    }

    private fun insertObject(o: Any, objectKey: ObjectKey, insertMap: InsertMap): List<InsertObject> {
        return fields.flatMap {
            val x = if (it is KeyType) {
                objectKey.keys()
            } else {
                it.hashCodeXIfAutoKey(o)
            }
            it.insertObject(x as Any?, insertMap)
        }
    }

    companion object {
        fun <T : Any> readValue(clazz: KClass<T>): (List<ReadFieldValue>) -> T {
            return { values ->
                try {
                    val primaryConstructor = clazz.primaryConstructor!!
                    primaryConstructor.isAccessible = true
                    primaryConstructor.callBy(
                            try {
                                primaryConstructor.parameters.map { it to values.first { v -> v.fieldData.name == it.name }.value }
                                        .toMap()
                            } catch (t: Throwable) {
                                throw RuntimeException("error for clazz $clazz at ${primaryConstructor.parameters} and $values", t)
                            })
                } catch (t: Throwable) {
                    throw t
                }
            }
        }
    }
}
