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

package org.daiv.reflection.common

import org.daiv.reflection.annotations.AllTables
import org.daiv.reflection.persister.Persister.Table
import java.lang.RuntimeException
import java.sql.ResultSet
import kotlin.reflect.KClass

fun <R : Any> ResultSet.getList(method: ResultSet.() -> R): List<R> {
    val mutableList = mutableListOf<R>()
    while (next()) {
        mutableList.add(method())
    }
    return mutableList.toList()
}

fun <T : Any> KClass<T>.tableName() = "${this.java.simpleName}"


interface ReadValue {
    fun getObject(number: Int, clazz: KClass<Any>): Any
    fun <T : Any> read(table: Table<T>, t: Any): T
    fun helperTable(table: Table<ComplexObject>, fieldName: String, key: Any): List<ComplexObject>
}

class DBReadValue(private val resultSet: ResultSet) : ReadValue {
    override fun getObject(number: Int, clazz: KClass<Any>) = resultSet.getObject(number)!!

    override fun <T : Any> read(table: Table<T>, t: Any) = table.read(t)!!

    override fun helperTable(table: Table<ComplexObject>, fieldName: String, key: Any) = table.read(fieldName, key)
}

class TableDataReadValue(val tables: AllTables, val values: List<String>) : ReadValue {
    override fun getObject(number: Int, clazz: KClass<Any>): Any {
        val x = values[number-1]
        try {
            return when (clazz) {
                Boolean::class -> x.toBoolean()
                String::class -> x
                Int::class -> x.toInt()
                Double::class -> x.toDouble()
                else -> throw RuntimeException("unknow type: $clazz")
            }
        } catch (e: NumberFormatException) {
            throw e
        }
    }

    override fun <T : Any> read(table: Table<T>, t: Any): T {
        return table.readTableData(tables.copy(tableData = tables.keyTables.find { it.tableName == table._tableName }!!),
                                   t)
    }

    override fun helperTable(table: Table<ComplexObject>, fieldName: String, key: Any): List<ComplexObject> {
        val tableData = tables.helper.find { it.tableName == table._tableName }!!
        return table.readTableData(tables, tableData, fieldName, key)

    }

}