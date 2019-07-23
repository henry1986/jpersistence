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

/**
 * [R] is the type of the receiver of the property
 * [T] is the generic type of the PropertyData
 * [S] is the type of the value returned by the getObject method
 */
interface PropertyData<R : Any, S : Any, T : Any> {
    val clazz: KClass<T>
    val receiverType: KClass<R>?
    val name: String

    fun getObject(r: R): S
}

private fun <R : Any, T : Any> KProperty1<R, T>.getObject(r: R): T {
    isAccessible = true
    return get(r)
}

data class SimpleTypeProperty<R : Any>(override val clazz: KClass<R>, override val name: String) :
        PropertyData<R, R, R> {
    override val receiverType: KClass<R>? = null
    override fun getObject(r: R) = r
}

data class DefProperty<R : Any, T : Any>(val property: KProperty1<R, T>, override val receiverType: KClass<R>) :
        PropertyData<R, T, T> {
    override val clazz: KClass<T> = property.returnType.classifier as KClass<T>
    override val name: String = property.name
    override fun getObject(r: R) = property.getObject(r)
}

internal class KeyTypeProperty(val fields: List<FieldData<Any, Any, Any, Any>>) : PropertyData<Any, List<Any>, Any> {
    override val clazz = Any::class
    override val receiverType: KClass<Any>? = Any::class
    override val name: String = fields.joinToString("_") { it.name }
    override fun getObject(r: Any): List<Any> {
        return fields.map { it.getObject(r) }
    }

}

data class SetProperty<R : Any, T : Any> constructor(val property: KProperty1<R, Set<T>>,
                                                     override val receiverType: KClass<R>) :
        PropertyData<R, Set<T>, T> {
    override val clazz: KClass<T> = property.returnType.arguments.first().type!!.classifier as KClass<T>
    override val name = property.name
    override fun getObject(r: R) = property.getObject(r)
}

interface MapProperty<R : Any, T : Any, M : Any> : PropertyData<R, Map<M, T>, T> {
    val keyClazz: KClass<M>
    val property: KProperty1<*, *>
    override val receiverType: KClass<R>
}

data class DefaultMapProperty<R : Any, T : Any, M : Any>(override val property: KProperty1<R, Map<M, T>>,
                                                         override val receiverType: KClass<R>) :
        MapProperty<R, T, M> {
    override val clazz: KClass<T> = property.returnType.arguments[1].type!!.classifier as KClass<T>
    override val keyClazz: KClass<M> = property.returnType.arguments.first().type!!.classifier as KClass<M>
    override val name = property.name
    override fun getObject(r: R) = property.getObject(r)
}

data class ListMapProperty<R : Any, T : Any>(override val property: KProperty1<R, List<T>>,
                                             override val receiverType: KClass<R>) : MapProperty<R, T, Int> {
    override val clazz: KClass<T> = property.returnType.arguments.first().type!!.classifier as KClass<T>
    override val keyClazz: KClass<Int> = Int::class
    override val name = property.name
    override fun getObject(r: R) = property.getObject(r).mapIndexed { index, t -> index to t }.toMap()
}

//data class InsertData(val list: List<Any>)
//
//data class ComplexObject(val insertData: InsertData)
//
//internal class ComplexProperty(override val name: String) : PropertyData<ComplexObject, Any, Any> {
//    override val clazz: KClass<Any> = InsertData::class as KClass<Any>
//    override fun getObject(r: ComplexObject) = r.insertData
//}
//
//data class SimpleProperty(override val clazz: KClass<Any>, override val name: String, val index: Int) : PropertyData<InsertData, Any, Any> {
//    override fun getObject(r: InsertData) = r.list[index]
//}
