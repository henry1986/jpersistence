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
import org.daiv.reflection.common.ListUtil
import java.sql.ResultSet
import kotlin.reflect.KProperty1

internal class ReadListType<T : Any>(override val property: KProperty1<Any, T>,
                                     private val many: Many) : ReadFieldData<T>, ListUtil<T> {
    override fun key(prefix: String?): String {
        return map({ i, p -> p.createTableKeyData(listElementName(i)) }, { joinToString(", ") })
    }

    private fun <T, R> map(f: (Int, ReadPersisterData<Any>) -> T,
                           collect: Sequence<T>.() -> R): R {
        val p = ReadPersisterData.create(getGenericListType())
        return (0..many.size)
            .asSequence()
            .map { i -> f(i, p) }
            .collect()
    }

    override fun toTableHead(prefix: String?): String {
        return map({ i, p -> p.createTableString(listElementName(i)) }, { joinToString(", ") })
    }

    private tailrec fun getValue(resultSet: ResultSet,
                                 p: ReadPersisterData<Any>,
                                 i: Int,
                                 counter: Int,
                                 list: List<out Any>): NextSize<T> {
        if (i < p.size()) {
            val (t, nextCounter) = p.read(resultSet, counter)
            return getValue(resultSet, p, i + 1, nextCounter, list + t)
        }
        return NextSize(list as T, counter)
    }

    @Suppress("UNCHECKED_CAST")
    override fun getValue(resultSet: ResultSet, number: Int): NextSize<T> {
        return getValue(resultSet, ReadPersisterData.create(getGenericListType()), 0, number, listOf())
//        return map({ i, p -> p.read(resultSet, number - 1 + i!! * p.size()) }, { toList() as T })
    }
}