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

import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

internal class WriteSimpleType<T : Any>(override val property: KProperty1<Any, T>,
                                        override val clazz: KClass<T>) : WriteFieldData<T> {
    override fun insertValue(o: T): String {
        val any = getObject(o)
        return makeString(any)
    }

    override fun insertHead(prefix: String?): String {
        return name(prefix)
    }

    override fun fNEqualsValue(o: T, prefix: String?, sep: String): String {
        return "${name(prefix)} = ${makeString(getObject(o))}"
    }

    companion object {
        internal fun makeString(any: Any): String {
            val s = any.toString()
            return when (any::class){
                String::class -> "\"" + s + "\""
                Boolean::class -> if(s.toBoolean()) "1" else "0"
                else -> s
            }
        }
    }
}