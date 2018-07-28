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

import org.daiv.immutable.utils.persistence.annotations.FlatList
import org.daiv.reflection.getKClass
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.javaType

internal class WriteSimpleType<T : Any>(override val property: KProperty1<Any, T>,
                                        override val flatList: FlatList,
                                        override val clazz: KClass<T>) : WriteFieldData<T> {
    override fun insertValue(o: T): String {
        val s = getObject(o).toString()
        return if (property.getKClass() == String::class) {
            "\"" + s + "\""
        } else s
    }

    override fun insertHead(prefix: String?): String {
        return name(prefix)
    }

    override fun fNEqualsValue(o: T, prefix: String?, sep: String): String {
        return "${name(prefix)} = ${makeString(getObject(o))}"
    }

    companion object {
        internal fun makeString(o: Any): String {
            return if (o::class == String::class) "\"$o\"" else o.toString()
        }
    }
}