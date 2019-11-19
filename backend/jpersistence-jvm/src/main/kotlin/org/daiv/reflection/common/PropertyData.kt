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
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.jvm.isAccessible

/**
 * [R] is the type of the receiver of the property
 * [T] is the generic type of the PropertyData
 * [S] is the type of the value returned by the getObject method
 */
internal interface PropertyData : FieldReadable {
    val clazz: KClass<Any>
    val type: KType
    val receiverType: KClass<Any>?
    val name: String
    val isNullable: Boolean
        get() = false
}

private fun <R : Any, T : Any> KProperty1<R, T>.getObject(r: R): T {
    isAccessible = true
    try {
        return get(r)
    } catch (t: Throwable) {
        throw RuntimeException("$r does not fit to ${this.returnType}")
    }
}

object SimpleTypeReadable : FieldReadable {
    override fun getObject(o: Any) = o
}

data class SimpleTypeProperty constructor(override val clazz: KClass<Any>, override val name: String) :
        PropertyData {
    override val type: KType = clazz.createType()
    override val receiverType: KClass<Any>? = null
    override fun getObject(r: Any) = r
}


internal interface PropertyReader : FieldReadable {
    val property: KProperty1<Any, Any>
    override fun getObject(r: Any) = property.getObject(r)
}

class DefaultProperyReader(override val property: KProperty1<Any, Any>) : PropertyReader

data class DefProperty(override val property: KProperty1<Any, Any>,
                       override val receiverType: KClass<Any>,
                       override val name: String = property.name) :
        PropertyReader, PropertyData {
    override val type: KType = property.returnType
    override val clazz: KClass<Any> = type.classifier as KClass<Any>
    override val isNullable = property.returnType.isMarkedNullable
}

internal class AutoKeyProperty(val key: KeyHashCodeable) : PropertyData {
    override val type: KType = Any::class.createType()
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

internal class KeyTypeProperty(val fields: List<FieldData>) : PropertyData {
    override val type: KType = Any::class.createType()
    override val clazz = Any::class
    override val receiverType: KClass<Any>? = Any::class
    override val name: String = fields.joinToString("_") { it.name }
    override fun getObject(r: Any): Any {
        return fields.map { it.getObject(r) }
    }

    override fun toString(): String {
        return "KeyTypeProperty: $fields"
    }
}

data class SetProperty constructor(override val property: KProperty1<Any, Any>,
                                   override val receiverType: KClass<Any>) :
        PropertyData, PropertyReader {
    override val type: KType = property.returnType.arguments.first().type!!
    override val clazz: KClass<Any> = property.returnType.arguments.first().type!!.classifier as KClass<Any>
    override val name = property.name
}

internal class MapReadable(override val property: KProperty1<Any, Any>) : PropertyReader {
    val clazz: KClass<Any> = property.returnType.arguments[1].type!!.classifier as KClass<Any>
    val keyClazz: KClass<Any> = property.returnType.arguments.first().type!!.classifier as KClass<Any>
}

internal interface TwoTypeProperty {
    val keyClazz: KType
    //    val property: KProperty1<R, S>
    val subType: KType
}

internal interface MapProperty : PropertyData, TwoTypeProperty {
    //    val property: KProperty1<R, S>
    override val receiverType: KClass<Any>
}

internal interface RealMapProperty : MapProperty

data class DefaultMapProperty(override val property: KProperty1<Any, Any>,
                              override val receiverType: KClass<Any>) :
        RealMapProperty, PropertyReader {
    override val type: KType = property.returnType
    override val subType: KType = property.returnType.arguments[1].type!!
    override val clazz: KClass<Any> = property.returnType.arguments[1].type!!.classifier as KClass<Any>
    override val keyClazz: KType = property.returnType.arguments.first().type!!
    override val name = property.name
}

class SimpleMapTypeProperty(override val type: KType) : RealMapProperty {
    override val subType: KType = type.arguments[1].type!!
    override val clazz: KClass<Any> = subType.classifier as KClass<Any>
    override val name: String = ""
    override val keyClazz = type.arguments[0].type!!
    override val receiverType: KClass<Any> = Any::class

    override fun getObject(o: Any): Any {
        return o
    }
}

class SimpleListTypeProperty(override val type: KType) : MapProperty {
    override val subType: KType = type.arguments[0].type!!
    override val clazz: KClass<Any> = type.arguments[0].type!!.classifier as KClass<Any>
    override val name: String = ""
    override val keyClazz = Int::class.createType()
    override val receiverType: KClass<Any> = Any::class as KClass<Any>

    override fun getObject(o: Any): List<Any> {
        return o as List<Any>
    }
}

class ListReadable<T : Any>(val property: KProperty1<Any, List<T>>) : FieldReadable {
    override fun getObject(o: Any): List<T> {
        return property.getObject(o)
    }
}

data class ListMapProperty(val property: KProperty1<Any, Any>,
                           override val receiverType: KClass<Any>) : MapProperty {
    override val type: KType = property.returnType
    override val subType: KType = property.returnType.arguments.first().type!!
    override val clazz: KClass<Any> = property.returnType.arguments.first().type!!.classifier as KClass<Any>
    override val keyClazz = Int::class.createType()
    override val name = property.name
    //    override fun getObject(r: R) = property.getObject(r).mapIndexed { index, t -> index to t }.toMap()
    override fun getObject(o: Any) = property.getObject(o)
}
