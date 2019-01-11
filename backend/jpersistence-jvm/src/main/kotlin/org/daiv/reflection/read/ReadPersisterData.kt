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
import org.daiv.reflection.persister.Persister
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

internal data class ReadFieldValue(val value: Any, val fieldData: FieldData<Any, Any, Any>)


internal data class ReadPersisterData<R : Any, T : Any>(private val fields: List<FieldData<R, *, T>>,
                                                        private val className: String = "no name",
                                                        private val method: (List<ReadFieldValue>) -> R) {

    constructor(clazz: KClass<R>, persister: Persister) : this(FieldDataFactory.fieldsRead(clazz, persister),
                                                               clazz.simpleName ?: "no name",
                                                               readValue(clazz.constructors.first()))

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

    fun createTableForeign() {
        fields.forEach { it.createTableForeign() }
    }


    fun underscoreName(): String {
        return fields.mapNotNull { it.underscoreName(null) }
            .joinToString(", ")
    }


//    fun joins(): String {
//        return fields.map { it.joins(null) }
//            .filterNotNull()
//            .joinToString(" ")
//    }

    fun joinNames(tableName: String): List<JoinName> {
        return fields.map { it.joinNames(null, tableName, keyName()) }
            .flatMap { it }
            .distinct()
    }

    private fun createTableInnerData(prefix: String?, skip: Int): String {
        return fields
            .asSequence()
            .drop(skip)
            .map { it.toTableHead(prefix) }
            .filterNotNull()
            .joinToString(separator = ", ")

    }

    fun createTableKeyData(prefix: String?): String {
        return fields.joinToString(separator = ", ", transform = { it.key(prefix) })
    }

//    fun createTableString(prefix: String): String {
//        return createTableInnerData(prefix, 0)
//    }

    fun getIdName(): String {
        return fields.first()
            .name(null)
    }

    internal fun foreignKey() = fields.map { it.foreignKey() }.filterNotNull().map { it.sqlMethod() }.joinToString(", ")

    fun createTable(): String {
        val createTableInnerData = createTableInnerData(null, 1)
//        val foreignKeys = fields.asSequence()
//            .mapNotNull { it.foreignKey() }
//            .map { it.sqlMethod() }
//            .joinToString(", ")

        fields.forEach { it.createTable() }
        val s = if (createTableInnerData == "") "" else "$createTableInnerData, "
        return ("(${fields.first().toTableHead(null)}, $s"
                + "PRIMARY KEY(${fields.first().key(null)})"
//                + "${if (foreignKeys == "") "" else ", $foreignKeys"}"
                + ");")
    }

    private fun readColumn(field: FieldData<R, *, T>, readValue: ReadValue): Any {
        return field.getColumnValue(readValue)
    }

    fun readKey(readValue: ReadValue): Any {
        return readColumn(fields.first(), readValue)
    }

    private tailrec fun read(readValue: ReadValue,
                             i: Int,
                             counter: Int = 1,
                             key: Any? = null,
                             list: List<ReadFieldValue> = emptyList()): NextSize<List<ReadFieldValue>> {
        if (i < fields.size) {
            val field = fields[i]
            val (value, nextCounter) = field.getValue(readValue, counter, key)
            val readFieldValue = ReadFieldValue(value, field as FieldData<Any, Any, Any>)
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

    internal fun read(readValue: ReadValue, counter: Int): NextSize<R> {
        return read(readValue, 0, counter).transform(method)
    }

    internal fun read(readValue: ReadValue): NextSize<R> {
        return read(readValue, 1)
    }

    private fun insertObject(o: R, prefix: String?): List<InsertObject<Any>> {
        return fields.flatMap { it.insertObject(o, prefix) }
    }

    private fun insertHeadString(insertObjects: List<InsertObject<Any>>): String {
        return insertObjects.asSequence()
            .map { it.insertHead() }
            .joinToString(", ")
    }

    private fun insertValueString(insertObjects: List<InsertObject<Any>>): String {
        return insertObjects.asSequence()
            .map { it.insertValue() }
            .joinToString(", ")
    }

    fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        val field = fields.first()
        return field.fNEqualsValue(o, prefix, sep)
        //joinToString(separator = sep, transform = { it.fNEqualsValue(o, prefix, sep) })
    }

    fun getKey(o: R): Any {
        return fields.first()
            .getObject(o)
    }

    fun keyName(): String {
        return fields.first()
            .name
    }

    fun keyName(prefix: String?): String {
        return fields.first()
            .key(prefix)
    }

    fun keyClassSimpleType() = fields.first().keyClassSimpleType()
    fun keySimpleType(r: R) = fields.first().keySimpleType(r)

    fun <X> onKey(f: FieldData<R, *, T>.() -> X): X {
        return fields.first()
            .f()
    }

    fun <X> onFields(f: FieldData<R, *, T>.() -> X): List<X> {
        return fields.map(f)
    }

    fun insertLists(o: R) {
        val key = keySimpleType(o)
        onFields { insertLists(key, o) }
    }

    fun deleteLists(o: R) {
        val key = keySimpleType(o)
        onFields { deleteLists(key) }
    }

    fun insertLists(l: List<R>) {
        l.forEach { o -> fields.forEach { it.insertLists(keySimpleType(o), o) } }
    }

    fun insert(o: R): String {
        val insertObjects = insertObject(o, null)
        val headString = insertHeadString(insertObjects)
        val valueString = insertValueString(insertObjects)
        return "($headString ) VALUES ($valueString);"
    }

    fun classOfField(fieldName: String): KClass<T> {
        return fields.find { it.name == fieldName }!!.propertyData.clazz
    }

    fun header() = fields.map { it.header(null) }.flatten()

    fun insertList(o: List<R>): String {
        if (o.isEmpty()) {
            return "() VALUES ();"
        }
        val headString = insertHeadString(insertObject(o.first(), null))
        val valueString = o.joinToString(separator = "), (") { insertValueString(insertObject(it, null)) }
        return "($headString ) VALUES ($valueString);"
    }

    companion object {

        private fun <T : Any> readValue(primaryConstructor: KFunction<T>): (List<ReadFieldValue>) -> T {
            return { values ->
                primaryConstructor.callBy(
                    primaryConstructor.parameters.map { it to values.first { v -> v.fieldData.name == it.name }.value }
                        .toMap())
            }
        }


        fun <R : Any, T : Any> create(clazz: KClass<R>, persister: Persister): ReadPersisterData<R, T> {
            return ReadPersisterData(clazz, persister)
        }
    }
}
