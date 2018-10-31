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
import org.daiv.reflection.common.ListUtil
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

internal class ReadListType<R : Any, T : Any>(override val property: KProperty1<R, List<T>>,
                                              private val many: Many,
                                              val clazz: KClass<T>) : FieldData<R, List<T>>, ListUtil<R, List<T>> {
    private val persisterData = ReadPersisterData.create(clazz)

    private fun map(f: (Int) -> String): String {
        return (0 until many.size).map(f)
            .joinToString(", ")
    }

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        val f: (Int) -> String = { persisterData.fNEqualsValue(getObject(o)[it], name(prefix), sep) }
        return map(f)
    }

//    private fun <T> map(o: T, f: (Int, ReadPersisterData<out Any>) -> T): String {
//        val l = property.get(o as Any) as List<Any>
//        return (0..many.size)
//            .asSequence()
//            .map { i -> f(i, ReadPersisterData.create(l[i]::class)) }
//            .joinToString(", ")
//    }

    override fun insertValue(o: R): String {
        throw UnsupportedOperationException("currently not supported")
//        return map { _, p -> p.insertValueString() }
    }

    override fun insertHead(prefix: String?): String {
        throw UnsupportedOperationException("currently not supported")
//        return map { i, p -> p.insertHeadString(listElementName(i)) }
    }

    override fun toTableHead(prefix: String?): String {
        return map { persisterData.createTableString(listElementName(it)) }
    }

    override fun key(prefix: String?): String {
        return map { persisterData.createTableKeyData(listElementName(it)) }
    }

    private tailrec fun getValue(resultSet: ResultSet,
                                 p: ReadPersisterData<Any>,
                                 i: Int,
                                 counter: Int,
                                 list: List<T>): NextSize<List<T>> {
        if (i < p.size()) {
            val (t, nextCounter) = p.read(resultSet, counter)
            return getValue(resultSet, p, i + 1, nextCounter, list + t as T)
        }
        return NextSize(list, counter)
    }

    @Suppress("UNCHECKED_CAST")
    override fun getValue(resultSet: ResultSet, number: Int): NextSize<List<T>> {
        return getValue(resultSet, ReadPersisterData.create(getGenericListType()), 0, number, listOf())
//        return map({ i, p -> p.read(resultSet, number - 1 + i!! * p.size()) }, { toList() as T })
    }

}