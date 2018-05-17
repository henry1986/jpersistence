package org.daiv.reflection.write

import org.daiv.immutable.utils.persistence.annotations.FLCreator
import org.daiv.immutable.utils.persistence.annotations.ToPersistence
import org.daiv.reflection.common.FieldDataFactory
import org.daiv.reflection.read.ReadFieldData
import org.daiv.reflection.read.ReadPersisterData
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

data class WritePersisterData<T : Any>(val tableName: String, val fields: List<WriteFieldData<Any>>) {
    fun insertHeadString(prefix: String?): String {
        return fields.joinToString(separator = ", ", transform = { f -> f.insertHead(prefix) })
    }

    fun insertValueString(): String {
        return fields.joinToString(separator = ", ", transform = { f -> f.insertValue() })
    }

    fun insert(): String {
        val headString = insertHeadString(null)
        val valueString = insertValueString()
        return "INSERT INTO $tableName ($headString ) VALUES ($valueString);"
    }

    companion object {
        fun <T : Any> create(o: T): WritePersisterData<T> {
            val clazz = o::class
//            if (!clazz.isData) {
//                throw RuntimeException("class $clazz is not a data class")
//            }
            val primaryConstructor: KFunction<T> = clazz.constructors.first()
            val es = (primaryConstructor.annotations.firstOrNull { it == ToPersistence::class.java } as? ToPersistence)?.elements.orEmpty()
            val map: List<WriteFieldData<Any>> = clazz.declaredMemberProperties.map { kProperty ->
                FieldDataFactory.createWrite(kProperty as KProperty1<Any, Any>,
                                             es.filter { it.name == kProperty.name }.getOrElse(
                                                 0, { FLCreator.get(kProperty.name) }), o)
            }

            return WritePersisterData(clazz.java.simpleName, map)


        }
    }
}