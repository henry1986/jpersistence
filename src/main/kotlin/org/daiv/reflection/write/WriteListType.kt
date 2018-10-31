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

package org.daiv.reflection.write

import org.daiv.reflection.annotations.Many
import org.daiv.reflection.common.ListUtil
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

internal class WriteListType<T : Any>(override val property: KProperty1<Any, T>,
                                      private val many: Many,
                                      override val clazz: KClass<T>) : WriteFieldData<T>, ListUtil<T> {
    override fun fNEqualsValue(o: T, prefix: String?, sep: String): String {
        throw UnsupportedOperationException("currently not supported")
    }

    private fun <T> map(o:T, f: (Int, WritePersisterData<out Any>) -> T): String {
        val l = property.get(o as Any) as List<Any>
        return (0..many.size)
            .asSequence()
            .map { i -> f(i, WritePersisterData.create(l[i]::class)) }
            .joinToString(", ")
    }

    override fun insertValue(o: T): String {
        throw UnsupportedOperationException("currently not supported")
//        return map { _, p -> p.insertValueString() }
    }

    override fun insertHead(prefix: String?): String {
        throw UnsupportedOperationException("currently not supported")
//        return map { i, p -> p.insertHeadString(listElementName(i)) }
    }
}