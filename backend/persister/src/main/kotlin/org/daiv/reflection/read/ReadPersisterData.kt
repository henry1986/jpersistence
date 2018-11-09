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

internal data class ReadFieldValue(val value: Any, val fieldData: FieldData<Any, Any, Any>)

internal interface Evaluator<T> {
    fun evaluate(resultSet: ResultSet): T
    fun evaluateToList(resultSet: ResultSet): List<T>
}

internal data class ReadPersisterData<R : Any, T : Any>(private val fields: List<FieldData<R, *, T>>,
                                                        private val method: (List<ReadFieldValue>) -> R) :
    Evaluator<R> {

    constructor(clazz: KClass<R>, persister: Persister) : this(FieldDataFactory.fieldsRead(clazz, persister),
                                                               readValue(clazz.constructors.first()))

    override fun evaluateToList(resultSet: ResultSet): List<R> {
        return resultSet.getList(::evaluate)
    }

    override fun evaluate(resultSet: ResultSet): R {
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

    private tailrec fun read(resultSet: ResultSet,
                             i: Int,
                             counter: Int,
                             key: Any?,
                             list: List<ReadFieldValue>): NextSize<List<ReadFieldValue>> {
        if (i < fields.size) {
            val field = fields[i]
            val (value, nextCounter) = field.getValue(resultSet, counter, key)
            val readFieldValue = ReadFieldValue(value, field as FieldData<Any, Any, Any>)
            return read(resultSet,
                        i + 1,
                        nextCounter,
                        if (i == 0) field.keyLowSimpleType(value) else key,
                        list + readFieldValue)
        }
        return NextSize(list, counter)
    }

    internal fun read(resultSet: ResultSet, counter: Int): NextSize<R> {
        return read(resultSet, 0, counter, null, emptyList()).transform(method)
    }

    internal fun read(resultSet: ResultSet): NextSize<R> {
        return read(resultSet, 1)
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
        fields.forEach { it.insertLists(keySimpleType(o), o) }
    }

    fun insert(o: R): String {
        val insertObjects = insertObject(o, null)
        val headString = insertHeadString(insertObjects)
        val valueString = insertValueString(insertObjects)
        return "($headString ) VALUES ($valueString);"
    }

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
