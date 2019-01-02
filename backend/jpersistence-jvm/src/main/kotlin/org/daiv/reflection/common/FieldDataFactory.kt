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
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.*
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf

internal fun <R : Any, T : Any> KProperty1<R, T>.isNoMapAndNoList() =
    (returnType.classifier as KClass<T>) != List::class && (returnType.classifier as KClass<T>) != Map::class

internal interface FieldDataFactory {

    companion object {

        private fun <T : Any, R : Any> create(property: KProperty1<R, T>,
                                              receiverClass: KClass<R>,
                                              persister: Persister,
                                              keyClass: KClass<Any>?): FieldData<R, *, T> {
            return when {
                property.getKClass().java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(DefProperty(property,
                                                                                                       receiverClass))
                property.getKClass().isSubclassOf(Enum::class) -> EnumType(DefProperty(property as KProperty1<R, Enum<*>>,
                                                                                       receiverClass)) as FieldData<R, *, T>
                property.findAnnotation<SameTable>() != null -> {
                    val propertyData = DefProperty(property, receiverClass)
                    ComplexSameTableType(propertyData, ReadPersisterData(propertyData.clazz, persister))
                }
                property.isNoMapAndNoList() -> ReadComplexType(DefProperty(property, receiverClass),
                                                               property.findAnnotation()
                                                                   ?: ManyToOne::class.constructors.first().call(""),
                                                               persister)
                keyClass == null -> {
                    throw RuntimeException("class: $keyClass -> the primary Key must not be a collection!")
                }
                property.returnType.classifier as KClass<T> == Map::class -> {
                    MapType(MapProperty(property as KProperty1<R, Map<Any, T>>, receiverClass),
                            persister,
                            property.findAnnotation() ?: ManyMap::class.constructors.first().call("", ""),
                            keyClass)
                }
                else -> {
                    ReadListType(ListProperty(property as KProperty1<R, List<T>>, receiverClass),
                                 property.findAnnotation() ?: ManyList::class.constructors.first().call(""),
                                 persister,
                                 keyClass)
                }
            }
        }

        private fun <R : Any, T : Any> next(member: Collection<KProperty1<R, *>>,
                                            i: Int,
                                            clazz: KClass<R>,
                                            persister: Persister,
                                            constructor: KFunction<R>,
                                            keyClass: KClass<Any>?,
                                            ret: List<FieldData<R, *, T>>): List<FieldData<R, *, T>> {
            if (i < member.size) {
                val parameter = constructor.parameters[i]
                val c = create(member.find { it.name == parameter.name } as KProperty1<R, T>,
                               clazz,
                               persister,
                               keyClass)
                return next(member, i + 1, clazz, persister, constructor, keyClass ?: c.keyClassSimpleType(), ret + c)
            }
            return ret
        }

        internal fun <R : Any, T : Any> fieldsRead(clazz: KClass<R>,
                                                   persister: Persister): List<FieldData<R, *, T>> {
            if (clazz.java.isPrimitiveOrWrapperOrString()) {
                return listOf(ReadSimpleType(SimpleTypeProperty(clazz, clazz.tableName())) as FieldData<R, *, T>)
            }
            return next(clazz.declaredMemberProperties,
                        0,
                        clazz,
                        persister,
                        clazz.constructors.first(),
                        null,
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
