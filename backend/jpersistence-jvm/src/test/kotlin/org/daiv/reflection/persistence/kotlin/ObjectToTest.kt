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

package org.daiv.reflection.persistence.kotlin

import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.common.moreKeys
import org.daiv.reflection.database.DatabaseInterface
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.ReadPersisterData
import kotlin.reflect.KClass
import kotlin.test.assertEquals

enum class X1 { B1, B2, B3 }
enum class X2(private val s: String) {
    B1("1"), B2("2"), B3("3");
}


class ObjectToTest<T : Any>(private val o: T,
                            tableString: String,
                            insertString: String,
                            private val testName: String,
                            private val key: Any) {
    private val tableCreateString: String = createTableString(tableString)
    private val insertString: String = createInsertString(insertString)
    private val d: DatabaseInterface = DatabaseWrapper.create("PersisterTest$testName.db")
    private val p by lazy { Persister(d) }
    private val table by lazy { p.Table(o::class as KClass<T>) }
    private val r by lazy { ReadPersisterData<T, Any>(o::class as KClass<T>,null, p) }

    fun open() {
        d.open()
    }

    fun beforeReadFromDatabase() {
        try {
            table.persist()
            table.insert(o)
        } catch (ex: Exception) {
            throw RuntimeException("", ex)
        }
    }

    fun checkCreateTable() {
        val createTable = r.createTable()
        println(createTable)
        assertEquals(tableCreateString, createTable)
    }

    fun checkInsert() {
        val insert = r.insert(o)
        assertEquals(insertString, insert)
        println(insert)
    }

    fun checkReadData() {
//        val query = "SELECT * FROM ${r.tableName} WHERE ${r.getIdName()} = $key;"
        assertEquals(o, p.Table(o::class).read(key))
    }

    override fun toString(): String {
        return testName
    }

    fun delete() {
        d.delete()
    }
}

fun createTableString(values: String): String {
    return /*CREATE TABLE IF NOT EXISTS `$objectName` */"$values;"
}

fun createInsertString(values: String): String {
    return /*"INSERT INTO `$objectName` */"$values;"
}
