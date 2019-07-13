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

import org.daiv.reflection.common.FieldData
import org.daiv.reflection.common.FieldData.JoinName
import org.daiv.reflection.common.FieldDataFactory
import org.daiv.reflection.common.ReadValue
import org.daiv.reflection.common.ToStoreManyToOneObjects
import org.daiv.reflection.persister.Persister
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.primaryConstructor

internal data class ReadFieldValue(val value: Any, val fieldData: FieldData<Any, Any, Any, Any>)

internal interface InternalRPD<R:Any, T : Any> {
    val fields: List<FieldData<R, Any, T, Any>>
    fun field(fieldName: String): FieldData<Any, Any, Any, Any> {
        return fields.find { it.name == fieldName } as FieldData<Any, Any, Any, Any>?
                ?: fields.flatMap { it.subFields() }.find { it.name == fieldName } ?: throw RuntimeException(
                        "couldn't find any fields with name: $fieldName")
    }

    fun createTableForeign() {
        fields.forEach { it.createTableForeign() }
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

    fun getIdName(): String {
        return fields.first()
                .name(null)
    }

//    internal fun foreignKey() = fields.map { it.foreignKey() }.filterNotNull().map { it.sqlMethod() }.joinToString(", ")

    fun createTable(): String {
        val createTableInnerData = createTableInnerData(1)
//        val foreignKeys = fields.asSequence()
//            .mapNotNull { it.foreignKey() }
//            .map { it.sqlMethod() }
//            .joinToString(", ")

        fields.forEach { it.createTable() }
        val s = if (createTableInnerData == "") "" else "$createTableInnerData, "
        return ("(${fields.first().toTableHead()}, $s"
                + "PRIMARY KEY(${fields.first().key()})"
//                + "${if (foreignKeys == "") "" else ", $foreignKeys"}"
                + ");")
    }

    private fun readColumn(field: FieldData<R, *, T, *>, readValue: ReadValue): Any {
        return field.getColumnValue(readValue)
    }

    fun readKey(readValue: ReadValue): Any {
        return readColumn(fields.first(), readValue)
    }

    tailrec fun read(readValue: ReadValue,
                     i: Int,
                     counter: Int = 1,
                     key: Any? = null,
                     list: List<ReadFieldValue> = emptyList()): NextSize<List<ReadFieldValue>> {
        if (i < fields.size) {
            val field = fields[i]
            val (value, nextCounter) = field.getValue(readValue, counter, key)
            val readFieldValue = ReadFieldValue(value, field as FieldData<Any, Any, Any, Any>)
            return read(readValue,
                        i + 1,
                        nextCounter,
                        if (i == 0) field.keyLowSimpleType(value) else key,
                        list + readFieldValue)
        }
        return NextSize(list, counter)
    }

    tailrec fun readToString(readValue: (Int) -> Any,
                             i: Int,
                             key: Any? = null,
                             list: List<String> = emptyList()): List<String> {
        if (i < fields.map { it.size() }.sum()) {
            val value = readValue(i + 1)
            return readToString(readValue,
                                i + 1,
                                if (i == 0) value else key,
                                list + value.toString())
        }
        return list
    }

    fun readToString(readValue: (Int) -> Any): List<String> {
        return readToString(readValue, 0)
    }

    fun helperTables() = onFields { helperTables() }.flatten()
    fun keyTables() = onFields { keyTables() }.flatten()

    /**
     * method reads the [ReadValue] only to a list of [ReadFieldValue], not to [R]
     */
    fun readWOObject(readValue: ReadValue): List<ReadFieldValue> {
        return read(readValue, 0, 1).t
    }

//    fun fNEqualsValue(o: R, sep: String): String {
//        val field = fields.first() as FieldData<Any, Any, Any>
//        return field.fNEqualsValue(field.getObject(o), sep)
//        //joinToString(separator = sep, transform = { it.fNEqualsValue(o, prefix, sep) })
//    }

    fun keyName(): String {
        return fields.first()
                .key()
    }

    //    fun keyClassSimpleType() = fields.first().keyClassSimpleType()

    fun <X> onKey(f: FieldData<R, Any, T, Any>.() -> X): X {
        return (fields.first())
                .f()
    }

    fun <X> onFields(f: FieldData<R, Any, T, Any>.() -> X): List<X> {
        return fields.map(f)
    }

    fun clearLists() {
        onFields { clearLists() }
    }

    fun insert(insertObjects: List<InsertObject>): String {
        val headString = insertObjects.insertHeadString()
        val valueString = insertObjects.insertValueString()
        return "($headString ) VALUES ($valueString);"
    }


    fun classOfField(fieldName: String): KClass<T> {
        return fields.find { it.name == fieldName }!!.propertyData.clazz
    }

    fun header() = fields.map { it.header() }.flatten()
}

internal data class ReadPersisterData<R : Any, T : Any>(override val fields: List<FieldData<R, Any, T, Any>>,
                                                        private val className: String = "no name",
                                                        private val method: (List<ReadFieldValue>) -> R) : InternalRPD<R, T> {

    constructor(clazz: KClass<R>,
                prefix: String?,
                persister: Persister,
                parentTableName: String? = null) : this(FieldDataFactory.fieldsRead(clazz,
                                                                                    prefix,
                                                                                    parentTableName,
                                                                                    persister),
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

    fun evaluate(readValue: ReadValue): R {
        return read(readValue).t
    }


    internal fun read(readValue: ReadValue, counter: Int): NextSize<R> {
        return read(readValue, 0, counter).transform(method)
    }

    internal fun read(readValue: ReadValue): NextSize<R> {
        return read(readValue, 1)
    }

    fun getKey(o: R): Any {
        return fields.first()
                .getObject(o)
    }


    fun keySimpleType(r: R) = fields.first().keySimpleType(r)

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


    fun insert(o: R): String {
        /**
         * only for fields, that have to store data in other tables. It ensures that this data
         * is written in one time, so that it is faster. Other fields may have an empty implementation
         * of this method
         */
        val x = toMany(fields.flatMap { it.toStoreObjects(it.getObject(o) as T) })
        x.forEach { t, u -> t.storeManyToOneObject(u as List<T>) }

        val insertObjects = insertObject(o)
        return insert(insertObjects)
    }

    fun insertList(o: List<R>): String {
        if (o.isEmpty()) {
            return "() VALUES ();"
        }

        val x = toMany(fields.flatMap { f -> o.flatMap { f.toStoreObjects(f.getObject(it) as T) } })
        x.forEach { t, u -> t.storeManyToOneObject(u as List<T>) }
        val headString = insertObject(o.first()).insertHeadString()
        val valueString = o.joinToString(separator = "), (") { insertObject(it).insertValueString() }
        return "($headString ) VALUES ($valueString);"
    }

    fun insertLists(o: R) {
//        val key = keySimpleType(o)
        onFields { insertLists(listOf(o)) }
    }

    fun deleteLists(o: R) {
        val key = keySimpleType(o)
        onFields { deleteLists(key) }
    }


    fun insertLists(l: List<R>) {
        fields.forEach { it.insertLists(l) }
//        l.forEach { o -> fields.forEach { it.insertLists(o, o) } }
    }

    private fun insertObject(o: R): List<InsertObject> {
        return fields.flatMap { it.insertObject(it.getObject(o) as T) }
    }


    companion object {

        private fun <T : Any> readValue(clazz: KClass<T>): (List<ReadFieldValue>) -> T {
            return { values ->
                val primaryConstructor = clazz.primaryConstructor!!
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
