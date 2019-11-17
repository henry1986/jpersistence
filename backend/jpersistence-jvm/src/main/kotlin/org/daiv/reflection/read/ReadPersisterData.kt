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
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible

internal data class ReadFieldValue(val value: Any?, val fieldData: FieldData<Any, Any, Any, Any>)
private class Reader<R : Any, T : Any>(val readCache: ReadCache,
                                       val fields: List<FieldData<R, Any, T, Any>>,
                                       val key: List<Any>,
                                       val readValue: ReadValue) {
    fun read(counter: Int, i: Int = 0, list: List<ReadFieldValue> = emptyList()): NextSize<List<ReadFieldValue>> {
        if (i < fields.size) {
            val field = fields[i]
            val (value, nextCounter) = field.getValue(readCache, readValue, counter, key)
            val readFieldValue = ReadFieldValue(value.t, field as FieldData<Any, Any, Any, Any>)
            return read(nextCounter, i + 1, list + readFieldValue)
        }
        return NextSize(list, counter)
    }
}


internal interface InternalRPD<R : Any, T : Any> {
    val key: KeyType
    val fields: List<FieldData<R, Any, T, Any>>
    fun field(fieldName: String): FieldCollection<Any, Any, Any, Any> {
        return fields.find { it.name == fieldName } as FieldData<Any, Any, Any, Any>?
                ?: fields.flatMap { it.subFields() }.find { it.name == fieldName } ?: throw RuntimeException(
                        "couldn't find any fields with name: $fieldName")
    }

    fun dropHelper() {
        fields.forEach { it.dropHelper() }
    }

    fun fromWhere(fieldName: String, id: Any, sep: String): String {
        return if (key.fieldName == fieldName) {
//            if (id is List<*>)
            key.whereClause(id as List<Any>, sep)
//            else
//                key.whereClause(listOf(id), sep)
        } else {
            try {
                field(fieldName).whereClause(id, sep)
            } catch (t: Throwable) {
                throw t
            }
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

//    fun createTableString(prefix: String): String {
//        return createTableInnerData(prefix, 0)
//    }

    fun getIdName() = key.fieldName

//    internal fun foreignKey() = fields.map { it.foreignKey() }.filterNotNull().map { it.sqlMethod() }.joinToString(", ")

    fun createTable(): String {
        val createTableInnerData = createTableInnerData(key.fields.size)
//        val foreignKeys = fields.asSequence()
//            .mapNotNull { it.foreignKey() }
//            .map { it.sqlMethod() }
//            .joinToString(", ")

        val s = if (createTableInnerData == "") "" else "$createTableInnerData, "
        try {
            return ("(${key.toTableHead()}, $s"
                    + "PRIMARY KEY(${key.toPrimaryKey()})"
//                + "${if (foreignKeys == "") "" else ", $foreignKeys"}"
                    + ");")
        } catch (t: Throwable) {
            throw t
        }
    }

    fun readKey(readValue: ReadValue): Any {
        return key.getColumnValue(readValue)
    }

//    fun helperTables() = onFields { helperTables() }.flatten()
//    fun keyTables() = onFields { keyTables() }.flatten()

    fun innerRead(readValue: ReadValue, counter: Int = 1, readCache: ReadCache): NextSize<List<ReadFieldValue>> {
        val x = key.getValue(readCache, readValue, counter, emptyList())
        return Reader(readCache, fields.drop(key.fields.size), x.t.t!!, readValue).read(x.i,
                                                                                      list = x.t.t!!.mapIndexed { i, e ->
                                                                                          ReadFieldValue(e,
                                                                                                         key.fields[i])
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

    fun <X> onKey(f: FieldData<Any, Any, Any, Any>.() -> X): X {
        return key.onKey(f)
    }

    fun keyColumnName() = key.columnName()

    fun <X> onFields(f: FieldData<R, Any, T, Any>.() -> X): List<X> {
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

    fun insertBy(insertObjects: List<InsertObject>): String {
        val headString = insertObjects.insertHeadString()
        val valueString = insertObjects.insertValueString()
        return "($headString ) VALUES ($valueString);"
    }

    fun insertListBy(insertObjects: List<List<InsertObject>>): String {
        val headString = insertObjects.first()
                .insertHeadString()
        val valueString = insertObjects.joinToString(separator = "), (") { it.insertValueString() }
        return "($headString ) VALUES ($valueString);"
    }

    fun classOfField(fieldName: String): KClass<T> {
        return fields.find { it.name == fieldName }!!.propertyData.clazz
    }
}

internal data class ReadPersisterData<R : Any, T : Any>(override val key: KeyType,
                                                        val persisterProvider: PersisterProvider,
                                                        override val fields: List<FieldData<R, Any, T, Any>>,
                                                        private val className: String = "no name",
                                                        private val method: (List<ReadFieldValue>) -> R) : InternalRPD<R, T> {

    private constructor(builder: FieldDataFactory<R>.Builder,
                        persisterProvider: PersisterProvider,
                        className: String = "no name",
                        method: (List<ReadFieldValue>) -> R) : this(builder.idField!!,
                                                                    persisterProvider,
                                                                    builder.fields as List<FieldData<R, Any, T, Any>>,
                                                                    className,
                                                                    method)

    constructor(clazz: KClass<R>,
                persister: Persister,
                persisterProvider: PersisterProvider,
                prefix: String? = null) :
            this(FieldDataFactory(persisterProvider, clazz, prefix, persister).fieldsRead(),
                 persisterProvider,
                 clazz.simpleName ?: "no name",
                 readValue(clazz))

    init {
        if (fields.isEmpty()) {
            throw RuntimeException("this class has no fields: $className")
        }
    }

//    override fun evaluateToList(func:(Int) -> Any): List<R> {
//        return func.getList(::evaluate)
//    }

    fun evaluate(readValue: ReadValue, readCache: ReadCache): R {
        return read(readValue, readCache).t
    }


    internal fun read(readValue: ReadValue, counter: Int, readCache: ReadCache): NextSize<R> {
        return innerRead(readValue, counter, readCache).transform(method)
    }

    internal fun read(readValue: ReadValue, readCache: ReadCache): NextSize<R> {
        return read(readValue, 1, readCache)
    }

    fun getKey(o: R): Any {
        return key.keyValue(o)
    }


    fun keySimpleType(r: R) = key.simpleType(r)

    fun toMany(list: List<ToStoreManyToOneObjects>,
               i: Int = 0,
               ret: Map<FieldData<R, *, T, *>, List<Any>> = emptyMap()): Map<FieldData<R, *, T, *>, List<Any>> {
        if (i < list.size) {
            val t = list[i]
            val toInsert = ret[t.field]?.let { it + t.any } ?: listOf(t.any)
            val nextRet = ret + (t.field as FieldData<R, *, T, *> to toInsert)
            return toMany(list, i + 1, nextRet)
        }
        return ret
    }

//    private fun toManyStore(o: List<R>) {
//        /**
//         * only for fields, that have to store data in other tables. It ensures that this data
//         * is written in one time, so that it is faster. Other fields may have an empty implementation
//         * of this method
//         */
//        val z = o.flatMap { fields.flatMap { f -> f.toStoreObjects(f.hashCodeXIfAutoKey(it) as T) } }
//        val x = toMany(z)
//        x.forEach { t, u -> t.storeManyToOneObject(u as List<T>) }
//    }

    suspend fun trueInsert(tableName: String, insertMap: InsertMap, it: R) {
        val insertKey = InsertKey(tableName, key.hashCodeXIfAutoKey(it))
        val request = RequestTask(insertKey, it, { listOf(InsertRequest(insertObject(it))) }) {
            fields.forEach { f -> f.toStoreData(insertMap, listOf(it)) }
        }
        insertMap.toBuild(request)
    }

    suspend fun putInsertRequests(tableName: String, insertMap: InsertMap, o: List<R>) {
//        for (it in o){
//            withContext(Dispatchers.Default) {
//                launch(Dispatchers.Default) {
//                    val insertKey = InsertKey(tableName, key.hashCodeXIfAutoKey(it))
//                    insertMap.toBuild(insertKey, it, { InsertRequest(insertObject(it)) }) {
//                        fields.forEach { f -> f.toStoreData(insertMap, listOf(it)) }
//                    }
//                }
//            }
//        }
        o.map {
            //            if (insertMap.insertCachePreference.multiThreading) {
//                insertMap.actors.values.first()
//                        .requestScope.launch {
//                    trueInsert(tableName, insertMap, it)
//                }
//            } else {
            insertMap.actors.values.first()
                    .launch {
                        trueInsert(tableName, insertMap, it)
                    }
//            }

        }
    }

//    fun insert(o: R): String {
//        toManyStore(listOf(o))
//
//        val insertObjects = insertObject(o)
//        return insertBy(insertObjects)
//    }

    fun deleteLists(key: List<Any>) {
//        val key = keySimpleType(o)
        onFields { deleteLists(key) }
    }

    private fun insertObject(o: R): List<InsertObject> {
        return fields.flatMap {
            val x = it.hashCodeXIfAutoKey(o)
            it.insertObject(x as T?)
        }
    }

    companion object {

        fun <T : Any> readValue(clazz: KClass<T>): (List<ReadFieldValue>) -> T {
            return { values ->
                val primaryConstructor = clazz.primaryConstructor!!
                primaryConstructor.isAccessible = true
                primaryConstructor.callBy(
                        try {
                            primaryConstructor.parameters.map { it to values.first { v -> v.fieldData.name == it.name }.value }
                                    .toMap()
                        } catch (t: Throwable) {
                            throw RuntimeException("error for clazz $clazz at ${primaryConstructor.parameters} and $values", t)
                        })
            }
        }
    }
}
