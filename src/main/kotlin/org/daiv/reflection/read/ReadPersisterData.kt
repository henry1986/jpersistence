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

import org.daiv.immutable.utils.persistence.annotations.PersistenceRoot
import org.daiv.immutable.utils.persistence.annotations.ToPersistence
import org.daiv.reflection.common.FieldDataFactory
import org.daiv.reflection.common.getList
import java.lang.reflect.Constructor
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KFunction

internal data class ReadFieldValue(val value: Any, val fieldData: ReadFieldData<Any>)

internal interface Evaluator<T> {
    fun evaluate(resultSet: ResultSet): T
    fun evaluateToList(resultSet: ResultSet): List<T>
    val tableName: String
}

internal data class ReadPersisterData<T : Any> private constructor(override val tableName: String,
                                                                   private val method: (List<ReadFieldValue>) -> T,
                                                                   private val fields: List<ReadFieldData<Any>>) :
    Evaluator<T> {
    override fun evaluateToList(resultSet: ResultSet): List<T> {
        return resultSet.getList(::evaluate)
    }

    override fun evaluate(resultSet: ResultSet): T {
        return read(resultSet).t
    }

    private fun createTableInnerData(prefix: String?, skip: Int): String {
        return fields
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
        return ("CREATE TABLE IF NOT EXISTS "
                + "$tableName (${fields.first().toTableHead(null)}, $s"
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

//    fun read(resultSet: ResultSet, offset: Int): T {
//        val values = (0..(fields.size - 1))
//            .asSequence()
//            .map { ReadFieldValue(fields[it].getValue(resultSet, it + 1 + offset), fields[it]) }
//            .toList()
//        return method.invoke(values)
//    }

    internal fun read(resultSet: ResultSet): NextSize<T> {
        return read(resultSet, 1)
    }

    companion object {

        private fun <T : Any> readValue(primaryConstructor: KFunction<T>): (List<ReadFieldValue>) -> T {
            return { values ->
                primaryConstructor.callBy(
                    primaryConstructor.parameters.map { it to values.first { v -> v.fieldData.name() == it.name }.value }
                        .toMap())
            }
        }


        private fun <T : Any> javaReadValue(primaryConstructor: Constructor<T>): (List<ReadFieldValue>) -> T {
            return { values -> primaryConstructor.newInstance(*values.map { it.value }.toTypedArray()) }
        }

        private fun <T : Any> createJava(clazz: KClass<T>): (List<ReadFieldValue>) -> T {
            val declaredConstructors = clazz.java.declaredConstructors
            val first = declaredConstructors.first {
                it.isAnnotationPresent(ToPersistence::class.java)
            } as Constructor<T>
            return javaReadValue(first)
        }


        fun <T : Any> create(clazz: KClass<T>): ReadPersisterData<T> {
            val persistenceRoot = clazz.annotations.filter { it is PersistenceRoot }
                .map { it as PersistenceRoot }
                .firstOrNull(PersistenceRoot::isJava)
            return ReadPersisterData("`${clazz.java.simpleName}`",
                                     if (persistenceRoot?.isJava == true) createJava(clazz) else readValue(clazz.constructors.first()),
                                     FieldDataFactory.fieldsRead(clazz))
        }

        fun <T : Any> create(clazz: Class<T>): ReadPersisterData<T> {
            return create(clazz.kotlin)
        }
    }
}