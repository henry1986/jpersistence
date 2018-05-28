package org.daiv.reflection.common

import org.daiv.immutable.utils.persistence.annotations.FLCreator
import org.daiv.immutable.utils.persistence.annotations.FlatList
import org.daiv.immutable.utils.persistence.annotations.PersistenceRoot
import org.daiv.immutable.utils.persistence.annotations.ToPersistence
import org.daiv.reflection.getKClass
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.read.ReadComplexType
import org.daiv.reflection.read.ReadFieldData
import org.daiv.reflection.read.ReadListType
import org.daiv.reflection.read.ReadSimpleType
import org.daiv.reflection.write.WriteComplexType
import org.daiv.reflection.write.WriteFieldData
import org.daiv.reflection.write.WriteListType
import org.daiv.reflection.write.WriteSimpleType
import java.lang.reflect.Constructor
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

internal interface FieldDataFactory<T : Any, R : FieldData<T>> {

    fun getSimpleType(property: KProperty1<Any, T>,
                      flatList: FlatList): R

    fun getListType(property: KProperty1<Any, T>,
                    flatList: FlatList): R

    fun getComplexType(property: KProperty1<Any, T>,
                       flatList: FlatList): R


    companion object {
        private fun <T : Any, R : FieldData<T>> create(property: KProperty1<Any, T>,
                                                       flatList: FlatList,
                                                       f: FieldDataFactory<T, R>): R {
            return when {
                property.getKClass().java.isPrimitiveOrWrapperOrString() -> f.getSimpleType(property, flatList)
                flatList.size == 1 -> f.getComplexType(property, flatList)
                else -> {
                    if (property.getKClass().java != List::class.java) {
                        throw RuntimeException("Field's size is not 1, so type of field must be List")
                    }
                    f.getListType(property, flatList)
                }
            }
        }

//        internal fun <T : Any> createWrite(property: KProperty1<Any, T>,
//                                           flatList: FlatList,
//                                           o: Any): WriteFieldData<T> {
//            return create(property, flatList, WriteFactory<T>(o))
//        }
//
//        internal fun <T : Any> createRead(property: KProperty1<Any, T>, flatList: FlatList): ReadFieldData<T> {
//            return create(property, flatList, ReadFactory<T>())
//        }

        private fun <T : Any, R : FieldData<T>> getFieldsKotlin(clazz: KClass<out T>,
                                                                factory: FieldDataFactory<T, R>): List<R> {
            val primaryConstructor = clazz.constructors.first()
            val es = (primaryConstructor.annotations.firstOrNull { it == ToPersistence::class.java } as? ToPersistence)?.elements.orEmpty()
            val member = clazz.declaredMemberProperties
            return primaryConstructor.parameters.map { parameter ->
                create(member.find { it.name == parameter.name } as KProperty1<Any, T>,
                       es.filter { it.name == parameter.name }.getOrElse(0, { FLCreator.get(parameter.name) }),
                       factory)
            }
        }

        private fun <T : Any, R : FieldData<T>> getFieldsJava(clazz: KClass<out T>,
                                                              factory: FieldDataFactory<T, R>): List<R> {
            val declaredConstructors = clazz.java.declaredConstructors
            val first = declaredConstructors.first {
                it.isAnnotationPresent(ToPersistence::class.java)
            } as Constructor<T>
            val ms = clazz.declaredMemberProperties
            val map: List<R> = first.getDeclaredAnnotation(ToPersistence::class.java)!!.elements.map {
                create(ms.first { m -> m.name == it.name } as KProperty1<Any, T>, it, factory)
            }
            return map
        }

        private fun <T : Any, R : FieldData<T>> getFields(clazz: KClass<out T>,
                                                  factory: FieldDataFactory<T, R>): List<R> {
            val persistenceRoot = clazz.annotations.filter { it is PersistenceRoot }
                .map { it as PersistenceRoot }
                .firstOrNull(PersistenceRoot::isJava)
            return if (persistenceRoot?.isJava == true) getFieldsJava(clazz, factory) else getFieldsKotlin(clazz,
                                                                                                           factory)
        }

        internal fun fieldsWrite(o: Any): List<WriteFieldData<Any>> {
            return getFields(o::class, WriteFactory<Any>(o))
        }

        internal fun <T : Any> fieldsRead(clazz: KClass<T>): List<ReadFieldData<Any>> {
            return getFields(clazz, ReadFactory<Any>())
        }

    }
}

private class WriteFactory<T : Any>(val o: Any) : FieldDataFactory<T, WriteFieldData<T>> {
    override fun getSimpleType(property: KProperty1<Any, T>, flatList: FlatList) =
        WriteSimpleType(property, flatList, o)

    override fun getListType(property: KProperty1<Any, T>, flatList: FlatList) = WriteListType(property, flatList, o)

    override fun getComplexType(property: KProperty1<Any, T>, flatList: FlatList) =
        WriteComplexType(property, flatList, o)

}

private class ReadFactory<T : Any>() : FieldDataFactory<T, ReadFieldData<T>> {
    override fun getSimpleType(property: KProperty1<Any, T>, flatList: FlatList) = ReadSimpleType(property)

    override fun getListType(property: KProperty1<Any, T>, flatList: FlatList) = ReadListType(property, flatList)

    override fun getComplexType(property: KProperty1<Any, T>, flatList: FlatList) = ReadComplexType(property)
}