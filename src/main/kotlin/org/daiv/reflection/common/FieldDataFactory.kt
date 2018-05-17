package org.daiv.reflection.common

import org.daiv.immutable.utils.persistence.annotations.FlatList
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
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

interface FieldDataFactory<T : Any, R : FieldData<T>> {

    fun getSimpleType(): R
    fun getListType(): R
    fun getComplexType(): R


    companion object {
        private fun <T : Any, R : FieldData<T>> create(property: KProperty1<Any, T>, flatList: FlatList, f: FieldDataFactory<T, R>): R {
            return when {
                property.getKClass().java.isPrimitiveOrWrapperOrString() -> f.getSimpleType()
                flatList.size == 1 -> f.getComplexType()
                else -> {
                    if (property.getKClass().java != List::class.java) {
                        throw RuntimeException("Field's size is not 1, so type of field must be List")
                    }
                    f.getListType()
                }
            }
        }

        internal fun <T : Any> createWrite(property: KProperty1<Any, T>, flatList: FlatList, o: Any): WriteFieldData<T> {
            return create(property, flatList, WriteFactory(property, flatList, o))
        }

        internal fun <T : Any> createRead(property: KProperty1<Any, T>, flatList: FlatList): ReadFieldData<T> {
            return create(property, flatList, ReadFactory(property, flatList))
        }
    }
}

private class WriteFactory<T : Any>(val property: KProperty1<Any, T>, val flatList: FlatList, val o: Any) : FieldDataFactory<T, WriteFieldData<T>> {
    override fun getSimpleType() = WriteSimpleType(property, flatList, o)

    override fun getListType()= WriteListType(property, flatList, o)

    override fun getComplexType() = WriteComplexType(property, flatList, o)

}

private class ReadFactory<T : Any>(val property: KProperty1<Any, T>, val flatList: FlatList) : FieldDataFactory<T, ReadFieldData<T>> {
    override fun getSimpleType() = ReadSimpleType(property)

    override fun getListType() = ReadListType(property, flatList)

    override fun getComplexType() = ReadComplexType(property)
}