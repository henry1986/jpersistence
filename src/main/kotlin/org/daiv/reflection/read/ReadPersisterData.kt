package org.daiv.reflection.read

import org.daiv.immutable.utils.persistence.annotations.FLCreator
import org.daiv.immutable.utils.persistence.annotations.PersistenceRoot
import org.daiv.immutable.utils.persistence.annotations.ToPersistence
import org.daiv.reflection.common.FieldDataFactory
import java.lang.reflect.Constructor
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

data class ReadFieldValue(val value: Any, val fieldData: ReadFieldData<Any>)

data class ReadPersisterData<T> private constructor(val tableName: String,
                                                    private val method: (List<ReadFieldValue>) -> T,
                                                    private val fields: List<ReadFieldData<Any>>) {
    private fun createTableInnerData(prefix: String?, skip: Int): String {
        return fields
            .drop(skip)
            .joinToString(separator = ", ", transform = { it.toTableHead(prefix) })
    }

    fun createTableKeyData(prefix: String): String {
        return fields.joinToString(separator = ", ", transform = { it.key(prefix) })
    }

    fun size(): Int {
        return fields.size
    }

    fun createTableString(prefix: String): String {
        return createTableInnerData(prefix, 0)
    }

    fun getIdName(): String {
        return fields.first()
            .name(null)
    }

    fun createTable(): String {
        return ("CREATE TABLE IF NOT EXISTS "
                + "$tableName (${fields.first().toTableHead(null)},"
                + " ${createTableInnerData(null, 1)}, "
                + "PRIMARY KEY(${fields.first().key(null)}));")
    }

    fun read(resultSet: ResultSet, offset: Int): T {
        val values = (0..(fields.size - 1))
            .asSequence()
            .map { ReadFieldValue(fields[it].getValue(resultSet, it + 1 + offset), fields[it]) }
            .toList()
        return method.invoke(values)
    }

    fun read(resultSet: ResultSet): T {
        return read(resultSet, 0)
    }

    companion object {

        private fun <T : Any> readValue(primaryConstructor: KFunction<T>): (List<ReadFieldValue>) -> T {
            return { values ->
                primaryConstructor.callBy(
                    primaryConstructor.parameters.map { it to values.first { v -> v.fieldData.name() == it.name }.value }
                        .toMap())
            }
        }

        private fun <T : Any> createKotlin(clazz: KClass<T>): ReadPersisterData<T> {
            val primaryConstructor: KFunction<T> = clazz.constructors.first()
            val es = (primaryConstructor.annotations.firstOrNull { it == ToPersistence::class.java } as? ToPersistence)?.elements.orEmpty()
            val map: List<ReadFieldData<Any>> = clazz.declaredMemberProperties.map { kProperty ->
                FieldDataFactory.createRead(kProperty as KProperty1<Any, Any>,
                                            es.filter { it.name == kProperty.name }.getOrElse(
                                                0, { FLCreator.get(kProperty.name) }))
            }
            return ReadPersisterData(clazz.java.simpleName, readValue(primaryConstructor), map)
        }

        private fun <T : Any> javaReadValue(primaryConstructor: Constructor<T>): (List<ReadFieldValue>) -> T {
            return { values -> primaryConstructor.newInstance(*values.map { it.value }.toTypedArray()) }
        }

        private fun <T : Any> createJava(clazz: KClass<T>): ReadPersisterData<T> {
            val declaredConstructors = clazz.java.declaredConstructors
            val first = declaredConstructors.first {
                it.isAnnotationPresent(ToPersistence::class.java)
            } as Constructor<T>
            val ms = clazz.declaredMemberProperties
            val map: List<ReadFieldData<Any>> = first.getDeclaredAnnotation(ToPersistence::class.java)!!.elements.map {
                FieldDataFactory.createRead(ms.first { m -> m.name == it.name } as KProperty1<Any, Any>, it)
            }
            return ReadPersisterData(clazz.java.simpleName, javaReadValue(first), map)
        }


        fun <T : Any> create(clazz: KClass<T>): ReadPersisterData<T> {
            val persistenceRoot = clazz.annotations.filter { it is PersistenceRoot }
                .map { it as PersistenceRoot }
                .firstOrNull(PersistenceRoot::isJava)
            return if (persistenceRoot?.isJava == true) createJava(clazz) else createKotlin(clazz)
        }

        fun <T : Any> create(clazz: Class<T>): ReadPersisterData<T> {
            return create(clazz.kotlin)
        }
    }
}