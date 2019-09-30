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

import org.daiv.reflection.read.KeyHashCodeable
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.isAccessible

/**
 * [R] is the type of the receiver of the property
 * [T] is the generic type of the PropertyData
 * [S] is the type of the value returned by the getObject method
 */
internal interface PropertyData<R : Any, S : Any, T : Any> : FieldReadable<R, S> {
    val clazz: KClass<T>
    val receiverType: KClass<R>?
    val name: String

}

private fun <R : Any, T : Any> KProperty1<R, T>.getObject(r: R): T {
    isAccessible = true
    try {
        return get(r)
    } catch (t: Throwable) {
        throw RuntimeException("$r does not fit to ${this.returnType}")
    }
}

object SimpleTypeReadable : FieldReadable<Any, Any> {
    override fun getObject(o: Any) = o
}

data class SimpleTypeProperty<R : Any> constructor(override val clazz: KClass<R>, override val name: String) :
        PropertyData<R, R, R> {
    override val receiverType: KClass<R>? = null
    override fun getObject(r: R) = r
}

internal interface PropertyReader<R : Any, S : Any> : FieldReadable<R, S> {
    val property: KProperty1<R, S>
    override fun getObject(r: R) = property.getObject(r)
}

class DefaultProperyReader<R : Any, S : Any>(override val property: KProperty1<R, S>) : PropertyReader<R, S>

data class DefProperty<R : Any, T : Any>(override val property: KProperty1<R, T>,
                                         override val receiverType: KClass<R>,
                                         override val name: String = property.name) :
        PropertyReader<R, T>, PropertyData<R, T, T> {
    override val clazz: KClass<T> = property.returnType.classifier as KClass<T>

}

internal class AutoKeyProperty(val key: KeyHashCodeable) : PropertyData<Any, Any, Any> {
    override val clazz: KClass<Any> = Any::class
    override val receiverType: KClass<Any>? = Any::class
    override val name: String = "autoID"
    override fun getObject(r: Any): Int {
        try {
            return key.hashCodeX(r)
        } catch (t: Throwable) {
            throw t
        }
    }

    fun hashCodeX(r: Any): Int {
        return key.hashCodeX(r)
    }
}
//
//internal class HashCodeProperty(val hashCodeable: HashCodeable<Any>, val property: KProperty1<Any, Any>): PropertyData<Any,Any,Any>{
//    override val clazz: KClass<Any> = Any::class
//    override val receiverType: KClass<Any>? = Any::class
//    override val name: String = "autoID"
//    override fun getObject(r: Any) = hashCodeable.hashCodeX(r)
//
//}

internal class KeyTypeProperty(val fields: List<FieldData<Any, Any, Any, Any>>) : PropertyData<Any, List<Any>, Any> {
    override val clazz = Any::class
    override val receiverType: KClass<Any>? = Any::class
    override val name: String = fields.joinToString("_") { it.name }
    override fun getObject(r: Any): List<Any> {
        return fields.map { it.getObject(r) }
    }

    override fun toString(): String {
        return "KeyTypeProperty: $fields"
    }
}

data class SetProperty<R : Any, T : Any> constructor(override val property: KProperty1<R, Set<T>>,
                                                     override val receiverType: KClass<R>) :
        PropertyData<R, Set<T>, T>, PropertyReader<R, Set<T>> {
    override val clazz: KClass<T> = property.returnType.arguments.first().type!!.classifier as KClass<T>
    override val name = property.name
}

internal class MapReadable<R : Any, M : Any, T : Any>(override val property: KProperty1<R, Map<M, T>>) : PropertyReader<R, Map<M, T>> {
    val clazz: KClass<T> = property.returnType.arguments[1].type!!.classifier as KClass<T>
    val keyClazz: KClass<M> = property.returnType.arguments.first().type!!.classifier as KClass<M>
}

internal interface MapProperty<R : Any, S : Any, T : Any, M : Any> : PropertyData<R, S, T> {
    val keyClazz: KClass<M>
    val property: KProperty1<R, S>
    override val receiverType: KClass<R>
}

data class DefaultMapProperty<R : Any, T : Any, M : Any>(override val property: KProperty1<R, Map<M, T>>,
                                                         override val receiverType: KClass<R>) :
        MapProperty<R, Map<M, T>, T, M>, PropertyReader<R, Map<M, T>> {
    override val clazz: KClass<T> = property.returnType.arguments[1].type!!.classifier as KClass<T>
    override val keyClazz: KClass<M> = property.returnType.arguments.first().type!!.classifier as KClass<M>
    override val name = property.name
}

class ListReadable<T : Any>(val property: KProperty1<Any, List<T>>) : FieldReadable<Any, List<T>> {
    override fun getObject(o: Any): List<T> {
        return property.getObject(o)
    }
}

data class ListMapProperty<R : Any, T : Any>(override val property: KProperty1<R, List<T>>,
                                             override val receiverType: KClass<R>) : MapProperty<R, List<T>, T, Int> {
    override val clazz: KClass<T> = property.returnType.arguments.first().type!!.classifier as KClass<T>
    override val keyClazz: KClass<Int> = Int::class
    override val name = property.name
    //    override fun getObject(r: R) = property.getObject(r).mapIndexed { index, t -> index to t }.toMap()
    override fun getObject(o: R) = property.getObject(o)
}
