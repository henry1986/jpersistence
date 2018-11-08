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

import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.isAccessible

interface PropertyData<R : Any, T : Any> {
    val clazz: KClass<T>
    //    val receiverType: KClass<T>
    val name: String

    fun getObject(r: R): T
}

data class DefProperty<R : Any, T : Any>(val property: KProperty1<R, T>, val receiverType: KClass<R>) :
    PropertyData<R, T> {
    override val clazz: KClass<T> = property.returnType.classifier as KClass<T>
    override val name: String = property.name
    override fun getObject(r: R): T {
        property.isAccessible = true
        return property.get(r)
    }
}

data class InsertData(val a:Any, val b:Any)

data class SimpleProperty(override val clazz: KClass<Any>, override val name: String, val isFirst:Boolean) :
    PropertyData<InsertData, Any> {
    override fun getObject(r: InsertData) = if(isFirst) r.a else r.b
}