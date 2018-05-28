package org.daiv.reflection.write

import org.daiv.immutable.utils.persistence.annotations.FLCreator
import org.daiv.immutable.utils.persistence.annotations.PersistenceRoot
import org.daiv.immutable.utils.persistence.annotations.ToPersistence
import org.daiv.reflection.common.FieldDataFactory
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

class WritePersisterData<T : Any> private constructor(val clazz: KClass<out T>,
                                                      private val fields: List<WriteFieldData<Any>>) {
    fun insertHeadString(prefix: String?): String {
        return fields.joinToString(separator = ", ", transform = { f -> f.insertHead(prefix) })
    }

    fun insertValueString(): String {
        return fields.joinToString(separator = ", ", transform = { f -> f.insertValue() })
    }

    fun insert(): String {
        val tableName = clazz.simpleName!!
        val headString = insertHeadString(null)
        val valueString = insertValueString()
        return "INSERT INTO $tableName ($headString ) VALUES ($valueString);"
    }

    companion object {

        fun <T : Any> create(o: T): WritePersisterData<T> {
            return WritePersisterData(o::class, FieldDataFactory.fieldsWrite(o))
        }
    }
}