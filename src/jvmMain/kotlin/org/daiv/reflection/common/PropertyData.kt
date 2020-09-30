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

internal interface PropertyData : FieldReadable {
    //    val clazz: KClass<Any>
    val type: KType
    val clazz: KClass<Any>
        get() = type.classifier as KClass<Any>
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

internal interface OtherClassPropertyData : PropertyData {
    fun otherClazz(clazz: KClass<Any>): PropertyData

}

internal data class SimpleTypeProperty constructor(override val type: KType, override val name: String) : SimpleProperty,
                                                                                                          OtherClassPropertyData {
    override val receiverType: KClass<Any>? = null
    override fun otherClazz(clazz: KClass<Any>): PropertyData {
        return copy(clazz.createType())
    }
}


internal interface PropertyReader : FieldReadable {
    val property: KProperty1<Any, Any>
    override fun getObject(r: Any) = property.getObject(r)
}

class DefaultProperyReader(override val property: KProperty1<Any, Any>) : PropertyReader

internal class InterfaceProperty(override val property: KProperty1<Any, Any>,
                                 override val receiverType: KClass<Any>,
                                 override val name: String = property.name) : PropertyData, PropertyReader {
    override val type: KType = property.returnType

    override val isNullable = property.returnType.isMarkedNullable
}

internal data class DefProperty(override val property: KProperty1<Any, Any>,
                                override val receiverType: KClass<Any>,
                                override val type: KType = property.returnType,
                                override val isNullable: Boolean = property.returnType.isMarkedNullable,
                                override val name: String = property.name) :
        PropertyReader, OtherClassPropertyData {
    override fun otherClazz(clazz: KClass<Any>): PropertyData {
        return copy(type = clazz.createType())
    }
}

internal class AutoKeyProperty(val key: KeyHashCodeable) : PropertyData {
    override val type: KType = Any::class.createType()
    override val receiverType: KClass<Any>? = Any::class
    override val name: String = autoIdName
    override fun getObject(r: Any): Int {
        try {
            return key.hashCodeX(r)
        } catch (t: Throwable) {
            throw t
        }
    }

//    fun hashCodeX(r: Any): Int {
//        return key.hashCodeX(r)
//    }

    companion object {
        val autoIdName = "hashCodeXAutoID"
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
    override val receiverType: KClass<Any>? = Any::class
    override val name: String = fields.joinToString("_") { it.name }
    override fun getObject(r: Any): Any {
        return fields.map { it.getObject(r) }
    }

    override fun toString(): String {
        return "KeyTypeProperty: $fields"
    }
}

internal interface CollectionProperty : PropertyData {
    val subType: KType
    override val receiverType: KClass<Any>
}

internal interface SetProperty : CollectionProperty {
    override val subType: KType
        get() = type.arguments[0].type!!
    override val clazz: KClass<Any>
        get() = subType.classifier as KClass<Any>
}

internal interface SimpleProperty : PropertyData {
    override fun getObject(o: Any) = o
}

internal data class DefSetProperty constructor(override val property: KProperty1<Any, Any>,
                                               override val receiverType: KClass<Any>) : SetProperty, PropertyData, PropertyReader {
    override val type: KType = property.returnType
    override val name = property.name
}


internal data class SimpleSetProperty constructor(override val type: KType) : SetProperty, SimpleProperty {
    override val name: String = ""
    override val receiverType: KClass<Any> = Any::class
}

internal class MapReadable(override val property: KProperty1<Any, Any>) : PropertyReader {
    val clazz: KClass<Any> = property.returnType.arguments[1].type!!.classifier as KClass<Any>
    val keyClazz: KClass<Any> = property.returnType.arguments.first().type!!.classifier as KClass<Any>
}


internal interface MapProperty : CollectionProperty {
    val keyClazz: KType
}

internal data class DefaultMapProperty(override val property: KProperty1<Any, Any>,
                                       override val receiverType: KClass<Any>) :
        MapProperty, PropertyReader {
    override val type: KType = property.returnType
    override val subType: KType = property.returnType.arguments[1].type!!
    override val clazz: KClass<Any> = property.returnType.arguments[1].type!!.classifier as KClass<Any>
    override val keyClazz: KType = property.returnType.arguments.first().type!!
    override val name = property.name
}

internal class SimpleMapTypeProperty(override val type: KType) : MapProperty, SimpleProperty {
    override val subType: KType = type.arguments[1].type!!
    override val clazz: KClass<Any> = subType.classifier as KClass<Any>
    override val name: String = ""
    override val keyClazz = type.arguments[0].type!!
    override val receiverType: KClass<Any> = Any::class
}

internal class SimpleListTypeProperty(override val type: KType) : MapProperty, SetProperty, SimpleProperty {
    override val name: String = ""
    override val keyClazz = Int::class.createType()
    override val receiverType: KClass<Any> = Any::class
}

internal class ListReadable<T : Any>(val property: KProperty1<Any, List<T>>) : FieldReadable {
    override fun getObject(o: Any): List<T> {
        return property.getObject(o)
    }
}

internal data class ListMapProperty(val property: KProperty1<Any, Any>,
                                    override val receiverType: KClass<Any>) : MapProperty {
    override val type: KType = property.returnType
    override val subType: KType = property.returnType.arguments.first().type!!
    override val clazz: KClass<Any> = property.returnType.arguments.first().type!!.classifier as KClass<Any>
    override val keyClazz = Int::class.createType()
    override val name = property.name
    //    override fun getObject(r: R) = property.getObject(r).mapIndexed { index, t -> index to t }.toMap()
    override fun getObject(o: Any) = property.getObject(o)
}
