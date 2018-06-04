package org.daiv.reflection.common

import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

internal interface ListUtil <T : Any>{
    val property : KProperty1<Any, T>

    fun listElementName(i: Int): String {
        return property.name + "_" + i
    }

    fun getGenericListType(): KClass<Any> {
        return property.returnType.arguments.first().type!!.classifier as KClass<Any>
//        return (field.getGenericType() as ParameterizedType).actualTypeArguments[0] as Class<Any>
    }

}