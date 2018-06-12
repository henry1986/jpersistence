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

import org.daiv.reflection.common.FieldDataFactory
import kotlin.reflect.KClass

internal class WritePersisterData<T : Any> private constructor(private val clazz: KClass<out T>,
                                                               private val fields: List<WriteFieldData<Any>>) {
    val tableName
        get() = clazz.simpleName!!

    fun insertHeadString(prefix: String?): String {
        return fields.joinToString(separator = ", ", transform = { f -> f.insertHead(prefix) })
    }

    fun insertValueString(): String {
        return fields.joinToString(separator = ", ", transform = { f -> f.insertValue() })
    }

    fun insert(): String {
        val headString = insertHeadString(null)
        val valueString = insertValueString()
        return "INSERT INTO $tableName ($headString ) VALUES ($valueString);"
    }

    companion object {

        fun <T : Any> create(o: T): WritePersisterData<T> {
            return WritePersisterData(o::class, FieldDataFactory.fieldsWrite(o))
        }
    }
}