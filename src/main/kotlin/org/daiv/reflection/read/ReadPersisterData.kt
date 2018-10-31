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
import org.daiv.reflection.common.FieldDataFactory
import org.daiv.reflection.common.getList
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

internal data class ReadFieldValue(val value: Any, val fieldData: FieldData<Any,Any>)

internal interface Evaluator<T> {
    fun evaluate(resultSet: ResultSet): T
    fun evaluateToList(resultSet: ResultSet): List<T>
}

internal data class ReadPersisterData<T : Any> private constructor(private val method: (List<ReadFieldValue>) -> T,
                                                                   private val fields: List<FieldData<Any, Any>>) :
    Evaluator<T> {
    override fun evaluateToList(resultSet: ResultSet): List<T> {
        return resultSet.getList(::evaluate)
    }

    override fun evaluate(resultSet: ResultSet): T {
        return read(resultSet).t
    }

    private fun createTableInnerData(prefix: String?, skip: Int): String {
        return fields
            .asSequence()
            .drop(skip)
            .joinToString(separator = ", ", transform = { it.toTableHead(prefix) })
    }

    fun createTableKeyData(prefix: String): String {
        return fields.joinToString(separator = ", ", transform = { it.key(prefix) })
    }

    fun size(): Int {
        return fields.size
    }

    fun createTableString(prefix: String): String {
        return createTableInnerData(prefix, 0)
    }

    fun getIdName(): String {
        return fields.first()
            .name(null)
    }

    fun createTable(): String {
        val createTableInnerData = createTableInnerData(null, 1)
        val s = if (createTableInnerData == "") "" else "$createTableInnerData, "
        return ("(${fields.first().toTableHead(null)}, $s"
                + "PRIMARY KEY(${fields.first().key(null)}));")
    }

    private tailrec fun read(resultSet: ResultSet,
                             i: Int,
                             counter: Int,
                             list: List<ReadFieldValue>): NextSize<List<ReadFieldValue>> {
        if (i < fields.size) {
            val (value, nextCounter) = fields[i].getValue(resultSet, counter)
            val readFieldValue = ReadFieldValue(value, fields[i])
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

    private fun map(t: (FieldData<Any, Any>) -> String): String {
        return fields.asSequence()
            .map(t)
            .filter { it != "" }
            .joinToString(separator = ", ")
    }

    fun insertHeadString(prefix: String?): String {
        return map { it.insertHead(prefix) }
    }

    fun insertValueString(o: T): String {
        return map { it.insertValue(o) }
    }

    fun fNEqualsValue(o: T, prefix: String?, sep: String): String {
        return fields.joinToString(separator = sep, transform = { it.fNEqualsValue(o, prefix, sep) })
    }

    fun getKey(o: T): Any {
        return fields.first()
            .getObject(o)
    }

    fun insert(o: T): String {
        val headString = insertHeadString(null)
        val valueString = insertValueString(o)
        return "($headString ) VALUES ($valueString);"
    }

    fun insertList(o: List<T>): String {
        val headString = insertHeadString(null)
        val valueString = o.joinToString(separator = "), (") { insertValueString(it) }
//        val valueString = insertValueString(o)
        return "($headString ) VALUES ($valueString);"
    }

    companion object {

        private fun <T : Any> readValue(primaryConstructor: KFunction<T>): (List<ReadFieldValue>) -> T {
            return { values ->
                primaryConstructor.callBy(
                    primaryConstructor.parameters.map { it to values.first { v -> v.fieldData.name() == it.name }.value }
                        .toMap())
            }
        }



        fun <T : Any> create(clazz: KClass<T>): ReadPersisterData<T> {
            return ReadPersisterData(readValue(clazz.constructors.first()), FieldDataFactory.fieldsRead(clazz))
        }

    }
}