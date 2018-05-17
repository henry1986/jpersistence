package org.daiv.reflection.read

import org.daiv.immutable.utils.persistence.annotations.FlatList
import org.daiv.reflection.getKClass
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KProperty1

class ReadSimpleType<T : Any>(override val property: KProperty1<Any, T>) : ReadFieldData<T> {


    override fun toTableHead(prefix: String?, key: Boolean): String {
        val simpleName = property.getKClass().simpleName
        val typeName = valueMappingJavaSQL[simpleName] ?: simpleName
        return "${name(prefix)} $typeName${if (key) " PRIMARY KEY NOT NULL" else " NOT NULL"}"
    }

    override fun getValue(resultSet: ResultSet, number: Int): T {
        try {
            @Suppress("UNCHECKED_CAST")
            return resultSet.getObject(number) as T
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    companion object {
        private val valueMappingJavaSQL = mapOf("long" to "bigInt", "String" to "Text")
    }

}