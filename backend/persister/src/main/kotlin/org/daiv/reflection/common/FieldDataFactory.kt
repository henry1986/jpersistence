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

import org.daiv.reflection.annotations.Many
import org.daiv.reflection.getKClass
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.ReadComplexType
import org.daiv.reflection.read.ReadListType
import org.daiv.reflection.read.ReadSimpleType
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation

internal interface FieldDataFactory<T : Any, R : FieldData<Any, T>> {

    fun getSimpleType(property: KProperty1<Any, T>): R

    fun getListType(property: KProperty1<Any, List<T>>,
                    persister: Persister,
                    many: Many,
                    listType: KClass<T>): FieldData<Any, List<T>>

    fun getComplexType(property: KProperty1<Any, T>, persister: Persister): R

//    fun foreignKey(property: KProperty1<Any, T>, flatList: FlatList): R
//
//    fun foreignKeyList(property: KProperty1<Any, T>, flatList: FlatList): R


    companion object {
        private inline fun <reified T : Any, R : FieldData<Any, T>> create(property: KProperty1<Any, T>,
                                                                           f: FieldDataFactory<T, R>,
                                                                           persister: Persister): R {
            return when {
                property.getKClass().java.isPrimitiveOrWrapperOrString() -> f.getSimpleType(property)
//                property.annotations.any { it == ManyToOne::class } -> if (property.getKClass().java != List::class.java) {
//                    f.foreignKey(property, flatList)
//                } else {
//                    f.foreignKeyList(property, flatList)
//                }
                (property.returnType.classifier as KClass<T>) != List::class -> ReadComplexType(property,
                                                                                                persister) as R
                else -> {
                    if (property.getKClass().java != List::class.java) {
                        throw RuntimeException("Field's size is not 1, so type of field must be List")
                    }
                    f.getListType(property as KProperty1<Any, List<T>>,
                                  persister,
                                  property.findAnnotation()!!,
                                  property.returnType.arguments.first().type!!.classifier as KClass<T>) as R
                }
            }
        }

        private inline fun <reified T : Any, R : FieldData<Any, T>> getFieldsKotlin(clazz: KClass<out T>,
                                                                                    factory: FieldDataFactory<T, R>,
                                                                                    persister: Persister): List<R> {
            val primaryConstructor = clazz.constructors.first()
//            val es = (primaryConstructor.annotations.firstOrNull { it == ToPersistence::class.java } as? ToPersistence)?.elements.orEmpty()
            val member = clazz.declaredMemberProperties
            return primaryConstructor.parameters.map { parameter ->
                create(member.find { it.name == parameter.name } as KProperty1<Any, T>,
//                       es.filter { it.name == parameter.name }.getOrElse(0),
                       factory,
                       persister)
            }
        }

//        private fun <T : Any, R : FieldData<T>> getFieldsJava(clazz: KClass<out T>,
//                                                              factory: FieldDataFactory<T, R>): List<R> {
//            val declaredConstructors = clazz.java.declaredConstructors
//            val first = declaredConstructors.first {
//                it.isAnnotationPresent(ToPersistence::class.java)
//            } as Constructor<T>
//            val ms = clazz.declaredMemberProperties
//            val map: List<R> = first.getDeclaredAnnotation(ToPersistence::class.java)!!.elements.map {
//                create(ms.first { m -> m.name == it.name } as KProperty1<Any, T>, it, factory)
//            }
//            return map
//        }

//        private fun <T : Any, R : FieldData<T>> getFields(clazz: KClass<out T>,
//                                                          factory: FieldDataFactory<T, R>): List<R> {
//            val persistenceRoot = clazz.annotations.asSequence()
//                .filter { it is PersistenceRoot }
//                .map { it as PersistenceRoot }
//                .firstOrNull(PersistenceRoot::isJava)
//            return if (persistenceRoot?.isJava == true) getFieldsJava(clazz, factory) else getFieldsKotlin(clazz,
//                                                                                                           factory)
//        }

//        internal fun fieldsWrite(o: KClass<Any>): List<WriteFieldData<Any>> {
//            return getFieldsKotlin(o, WriteFactory<Any>(o))
//        }

        internal fun <T : Any> fieldsRead(clazz: KClass<T>, persister: Persister): List<FieldData<Any, Any>> {
            return getFieldsKotlin(clazz, ReadFactory<Any>(), persister)
        }

    }
}

//private class WriteFactory<T : Any>(val o: KClass<T>) : FieldDataFactory<T, WriteFieldData<T>> {
////    override fun foreignKey(property: KProperty1<Any, T>, flatList: FlatList): WriteFieldData<T> {
////    }
//
//    override fun getSimpleType(property: KProperty1<Any, T>) =
//        WriteSimpleType(property, o)
//
//    override fun getListType(property: KProperty1<Any, T>, many: Many) = WriteListType(property, many, o)
//
//    override fun getComplexType(property: KProperty1<Any, T>) =
//        WriteComplexType(property, o)
//}
//inline fun <reified T : Any> classOfList(list: List<T>) = T::class

private class ReadFactory<T : Any> : FieldDataFactory<T, FieldData<Any, T>> {
//    override fun foreignKey(property: KProperty1<Any, T>, flatList: FlatList): ReadFieldData<T> {
//    }

    override fun getSimpleType(property: KProperty1<Any, T>) = ReadSimpleType(property)

    override fun getListType(property: KProperty1<Any, List<T>>,
                             persister: Persister,
                             many: Many,
                             clazz: KClass<T>): FieldData<Any, List<T>> {
        val r = ReadListType(property, many, clazz, persister)
        return r
    }

    override fun getComplexType(property: KProperty1<Any, T>, persister: Persister) =
        ReadComplexType(property, persister)
}