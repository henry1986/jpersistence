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

import org.daiv.reflection.annotations.Many
import org.daiv.reflection.common.FieldData
import org.daiv.reflection.persister.Persister
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

internal class ReadListType<R : Any, T : Any>(override val property: KProperty1<R, List<T>>,
                                              private val many: Many,
                                              val clazz: KClass<T>,
                                              val persister: Persister) : FieldData<R, List<T>> {

    private val persisterData = ReadPersisterData.create(clazz, persister)

    private fun map(f: (Int) -> String): String {
        return (0 until many.size).map(f)
            .joinToString(", ")
    }

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        val f: (Int) -> String = { persisterData.fNEqualsValue(getObject(o)[it], name(prefix), sep) }
        return map(f)
    }

    override fun insertObject(o: R, prefix: String?): List<InsertObject<Any>> {
        return onMany2One({
                              val list = getObject(o)
                              list.map { storeManyToOneObject(persisterData, it, persister) }
                              persisterData.onKey {
                                  (0 until list.size).flatMap {
                                      insertObject(list[it], this@ReadListType.listElementName(prefix, it))
                                  }
                              }
                          }) {
            (0 until many.size).flatMap {
                persisterData.insertObject(getObject(o)[it], listElementName(prefix, it))
            }
        }
    }

    override fun toTableHead(prefix: String?): String {
        return onMany2One({
                              (0 until many.size).map {
                                  persisterData.onKey { toTableHead(this@ReadListType.listElementName(prefix, it)) }
                              }
                                  .joinToString(", ")
                          }) { map { persisterData.createTableString(listElementName(prefix, it)) } }
    }

    override fun key(prefix: String?): String {
        return map { persisterData.createTableKeyData(listElementName(prefix, it)) }
    }

    private tailrec fun <T : Any> getValue(i: Int,
                                           counter: Int,
                                           list: List<T>,
                                           func: (Int) -> NextSize<T>): NextSize<List<T>> {
        if (i < many.size) {
            val (t, nextCounter) = func(counter)
            return getValue(i + 1, nextCounter, list + t, func)
        }
        return NextSize(list, counter)
    }

    @Suppress("UNCHECKED_CAST")
    override fun getValue(resultSet: ResultSet, number: Int): NextSize<List<T>> {
        return onMany2One({
                              val n = getValue(0, number, emptyList()) { persisterData.readKey(resultSet, it) }
                              val table = persister.Table(clazz)
                              NextSize(n.t.map { table.read(it)!! }, n.i)
                          }) { getValue(0, number, emptyList()) { persisterData.read(resultSet, it) } }
    }

}