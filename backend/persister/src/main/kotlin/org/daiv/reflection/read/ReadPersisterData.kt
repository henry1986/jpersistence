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
import org.daiv.reflection.common.getList
import org.daiv.reflection.persister.Persister
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

internal data class ReadFieldValue(val value: Any, val fieldData: FieldData<Any, Any>)

internal interface Evaluator<T> {
    fun evaluate(resultSet: ResultSet): T
    fun evaluateToList(resultSet: ResultSet): List<T>
}

internal data class ReadPersisterData<T : Any>(private val fields: List<FieldData<T, Any>>,
                                               private val method: (List<ReadFieldValue>) -> T) : Evaluator<T> {

    constructor(clazz: KClass<T>, persister: Persister) : this(FieldDataFactory.fieldsRead(clazz, persister),
                                                               readValue(clazz.constructors.first()))

    override fun evaluateToList(resultSet: ResultSet): List<T> {
        return resultSet.getList(::evaluate)
    }

    override fun evaluate(resultSet: ResultSet): T {
        return read(resultSet).t
    }

    fun underscoreName(): String {
        return fields.map { it.underscoreName(null) }
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

    fun createTableKeyData(prefix: String): String {
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

        fields.forEach { it.createTable(keyClassSimpleType()) }
        val s = if (createTableInnerData == "") "" else "$createTableInnerData, "
        return ("(${fields.first().toTableHead(null)}, $s"
                + "PRIMARY KEY(${fields.first().key(null)})"
//                + "${if (foreignKeys == "") "" else ", $foreignKeys"}"
                + ");")
    }

    private tailrec fun read(resultSet: ResultSet,
                             i: Int,
                             counter: Int,
                             list: List<ReadFieldValue>): NextSize<List<ReadFieldValue>> {
        if (i < fields.size) {
            val (value, nextCounter) = fields[i].getValue(resultSet, counter)
            val readFieldValue = ReadFieldValue(value, fields[i] as FieldData<Any, Any>)
            return read(resultSet, i + 1, nextCounter, list + readFieldValue)
        }
        return NextSize(list, counter)
    }

    internal fun read(resultSet: ResultSet, counter: Int): NextSize<T> {
        return read(resultSet, 0, counter, listOf()).transform(method)
    }

    internal fun read(resultSet: ResultSet): NextSize<T> {
        return read(resultSet, 1)
    }

    fun insertObject(o: T, prefix: String?): List<InsertObject<Any>> {
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

    fun fNEqualsValue(o: T, prefix: String?, sep: String): String {
        val field = fields.first()
        return field.fNEqualsValue(o, prefix, sep)
        //joinToString(separator = sep, transform = { it.fNEqualsValue(o, prefix, sep) })
    }

    fun getKey(o: T): Any {
        return fields.first()
            .getObject(o)
    }

    fun keyName(): String {
        return fields.first()
            .name
    }

    fun keyClassSimpleType() = fields.first().keyClassSimpleType()

    fun <R> onKey(f: FieldData<T, Any>.() -> R): R {
        return fields.first()
            .f()
    }

    fun <R> onFields(f: FieldData<T, Any>.() -> R): List<R> {
        return fields.map(f)
    }

    fun insert(o: T): String {
        val insertObjects = insertObject(o, null)
        val headString = insertHeadString(insertObjects)
        val valueString = insertValueString(insertObjects)
        return "($headString ) VALUES ($valueString);"
    }

    fun insertList(o: List<T>): String {
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


        fun <T : Any> create(clazz: KClass<T>, persister: Persister): ReadPersisterData<T> {
            return ReadPersisterData(clazz, persister)
        }

    }
}