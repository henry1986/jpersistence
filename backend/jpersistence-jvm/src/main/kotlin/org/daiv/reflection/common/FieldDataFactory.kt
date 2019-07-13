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

import org.daiv.reflection.annotations.ManyList
import org.daiv.reflection.annotations.ManyMap
import org.daiv.reflection.annotations.ManyToOne
import org.daiv.reflection.annotations.SameTable
import org.daiv.reflection.getKClass
import org.daiv.reflection.isEnum
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.*
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor

interface CheckAnnotation {
    fun isSameTabe(): Boolean
    fun manyToOne(): ManyToOne
}

fun getManyToOne() = ManyToOne::class.constructors.first().call("")

class KeyAnnotation(private val property: KProperty1<*, *>) : CheckAnnotation {
    override fun isSameTabe(): Boolean {
        return property.findAnnotation<SameTable>() != null
    }

    override fun manyToOne(): ManyToOne {
        return property.findAnnotation() ?: getManyToOne()
    }

}

internal fun <T : Any> KClass<T>.isNoMapAndNoListAndNoSet() = this != List::class && this != Map::class && this != Set::class

internal fun <T : Any> KClass<T>.toFieldData(checkAnnotation: CheckAnnotation,
                                                      prefix: String?,
                                                      persister: Persister): FieldData<Any, Any, T, Any> {
    return when {
        this.java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(SimpleTypeProperty(this,
                                                                                      this.simpleName!!),
                                                                   prefix) as FieldData<Any, Any, T, Any>
        this.isEnum() -> EnumType(SimpleTypeProperty(this, this.simpleName!!), prefix) as FieldData<Any, Any, T, Any>
        checkAnnotation.isSameTabe() -> {
            val propertyData = SimpleTypeProperty(this, this.simpleName!!)
            ComplexSameTableType(propertyData, prefix, null, persister) as FieldData<Any, Any, T, Any>
        }
        this.isNoMapAndNoListAndNoSet() -> ReadComplexType(SimpleTypeProperty(this, this.simpleName!!),
                                                           checkAnnotation.manyToOne(),
                                                           persister, prefix) as FieldData<Any, Any, T, Any>
        else -> {
            throw RuntimeException("this: $this not possible")
        }
    }
}

internal interface FieldDataFactory {

    companion object {

        fun <T : Any, R : Any> create(property: KProperty1<R, T>,
                                      receiverClass: KClass<R>,
                                      persister: Persister,
                                      prefix: String?,
                                      parentTableName: String?,
                                      idField: FieldData<R, Any, Any, Any>?): FieldData<R, *, T, *> {
            return when {
                property.getKClass().java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(DefProperty(property,
                                                                                                       receiverClass), prefix)
                property.getKClass().isEnum() -> EnumType(DefProperty(property as KProperty1<R, Enum<*>>,
                                                                      receiverClass), prefix) as FieldData<R, *, T, *>
                property.findAnnotation<SameTable>() != null -> {
                    val propertyData = DefProperty(property, receiverClass)
                    ComplexSameTableType(propertyData, prefix, parentTableName!!, persister)
                }
                (property.returnType.classifier as KClass<T>).isNoMapAndNoListAndNoSet() -> ReadComplexType(DefProperty(property,
                                                                                                                        receiverClass),
                                                                                                            property.findAnnotation()
                                                                                                                    ?: ManyToOne::class.constructors.first().call(
                                                                                                                            ""),
                                                                                                            persister, prefix)
                idField == null -> {
                    throw RuntimeException("class: $idField -> the primary Key must not be a collection!")
                }
                property.returnType.classifier as KClass<T> == Map::class -> {
                    MapType(DefaultMapProperty(property as KProperty1<R, Map<Any, T>>, receiverClass),
                            prefix,
                            persister,
                            parentTableName!!,
                            { it },
                            idField)
                }
                property.returnType.classifier as KClass<T> == List::class -> {
                    MapType(ListMapProperty(property as KProperty1<R, List<T>>, receiverClass),
                            prefix,
                            persister,
                            parentTableName!!,
                            {
                                it.toList()
                                        .sortedBy { it.first }
                                        .map { it.second }
                            },
                            idField)
                }
                property.returnType.classifier as KClass<T> == Set::class -> {
                    SetType(SetProperty(property as KProperty1<R, Set<T>>, receiverClass),
                            property.findAnnotation() ?: ManyList::class.constructors.first().call(""),
                            persister,
                            prefix,
                            idField)
                }
                else -> {
                    throw RuntimeException("unknown type : ${property.returnType}")
                }
            }
        }


        private fun <R : Any, T : Any> next(i: Int,
                                            prefix: String?,
                                            clazz: KClass<R>,
                                            persister: Persister,
                                            constructor: KFunction<R>,
                                            idFieldData: FieldData<R, Any, Any, Any>?,
                                            parentTableName: String?,
                                            ret: List<FieldData<R, Any, T, Any>>): List<FieldData<R, Any, T, Any>> {
            if (i < constructor.parameters.size) {
                val parameter = constructor.parameters[i]
                val c = create(clazz.declaredMemberProperties.find { it.name == parameter.name } as KProperty1<R, T>,
                               clazz,
                               persister,
                               prefix,
                               parentTableName,
                               idFieldData) as FieldData<R, Any, T, Any>
                val idField = idFieldData ?: c.idFieldSimpleType() as FieldData<R, Any, Any, Any>
                return next(i + 1, prefix, clazz, persister, constructor, idField, parentTableName, ret + c)
            }
            return ret
        }

        internal fun <R : Any, T : Any> fieldsRead(clazz: KClass<R>,
                                                   prefix: String?,
                                                   parentTableName: String?,
                                                   persister: Persister): List<FieldData<R, Any, T, Any>> {
            if (clazz.java.isPrimitiveOrWrapperOrString()) {
                return listOf(ReadSimpleType(SimpleTypeProperty(clazz, clazz.tableName()), prefix) as FieldData<R, Any, T, Any>)
            }
            return next(0,
                        prefix,
                        clazz,
                        persister,
                        clazz.primaryConstructor ?: run {
                            throw RuntimeException("clazz $clazz has no primary constructor")
                        },
                        null,
                        parentTableName,
                        emptyList())
//            val primaryConstructor: KFunction<R> = clazz.constructors.first()
//            val member: Collection<KProperty1<R, *>> = clazz.declaredMemberProperties
//            return primaryConstructor.parameters.map { parameter ->
//                create(member.find { it.name == parameter.name } as KProperty1<R, T>,
//                       clazz,
//                       persister)
//            }
        }
    }
}
