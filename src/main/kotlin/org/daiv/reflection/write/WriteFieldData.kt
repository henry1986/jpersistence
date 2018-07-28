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
import org.daiv.reflection.common.FieldData
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.isAccessible

internal interface WriteFieldData<T : Any> : FieldData<T> {
    val flatList: FlatList
    /**
     * object that has [property] as member and that is to read from
     */
    val clazz: KClass<T>


    /**
     * gets the [property] value of [o]
     */
    fun getObject(o: Any): T {
        property.isAccessible = true
        return property.get(o)
    }

    /**
     * this method creates the string for the sql command "INSERT INTO ", but
     * only the values until "VALUES". For the values afterwards, see
     * [insertValue]
     *
     * @param prefix
     * a possible prefix for the variables name. Null, if no prefix
     * is wanted.
     *
     * @return the string for the "INSERT INTO " command
     */
    fun insertHead(prefix: String?): String

    /**
     * this method creates the string for the sql command "INSERT INTO ", but
     * only the values after "VALUES". For the values before, see
     * [insertHead]
     *
     * @return the string for the "INSERT INTO " command
     */
    fun insertValue(o: T): String

    fun fNEqualsValue(o: T, prefix: String?, sep:String): String

}