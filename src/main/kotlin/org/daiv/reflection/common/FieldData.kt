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

import org.daiv.reflection.annotations.ManyToOne
import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.NextSize
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.jvm.isAccessible

internal interface FieldData<R : Any, T : Any> {
    val property: KProperty1<R, T>

    val name get() = property.name

    fun listElementName(prefix: String?, i: Int): String {
        val indexedName = "${name}_$i"
        return prefix?.let { "${it}_$indexedName" } ?: "$indexedName"
    }

    fun getGenericListType(): KClass<Any> {
        return property.returnType.arguments.first().type!!.classifier as KClass<Any>
    }

    /**
     * builds the name of the field in the database
     *
     * @param prefix
     * if the name of the field in the database shall have a prefix,
     * it is to be given here. null otherwise.
     * @return the name of the field in the database
     */
    fun name(prefix: String?) = prefix?.let { "${it}_$name" } ?: "$name"

    fun <R : Any> onMany2One(onManyToOne: (ManyToOne) -> R, noManyToOne: () -> R): R {
        return property.findAnnotation<ManyToOne>()?.let(onManyToOne) ?: run(noManyToOne)


    }

    /**
     * gets the [property] value of [o]
     */
    fun getObject(o: R): T {
        property.isAccessible = true
        return property.get(o)
    }

    fun insertObject(o: R, prefix: String?): List<InsertObject<Any>>

    fun fNEqualsValue(o: R, prefix: String?, sep: String): String

    /**
     * this method creates the string for the sql command "CREATE TABLE".
     *
     * @param prefix
     * a possible prefix for the variables name. Null, if no prefix
     * is wanted.
     * @return the string for the "CREATE TABLE" command
     */
    fun toTableHead(prefix: String?): String

    fun key(prefix: String?): String

    fun getValue(resultSet: ResultSet, number: Int): NextSize<T>

}