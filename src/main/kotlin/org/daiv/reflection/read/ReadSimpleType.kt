package org.daiv.reflection.read

import org.daiv.reflection.getKClass
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KProperty1

internal class ReadSimpleType<T : Any>(override val property: KProperty1<Any, T>) : ReadFieldData<T> {
    override fun key(prefix: String?): String {
        return name(prefix)
    }

    override fun toTableHead(prefix: String?): String {
        val simpleName = property.getKClass()
            .simpleName
        val typeName = valueMappingJavaSQL[simpleName] ?: simpleName
        return "${name(prefix)} $typeName${" NOT NULL"}"
    }

    override fun getValue(resultSet: ResultSet, number: Int): NextSize<T> {
        try {
            @Suppress("UNCHECKED_CAST")
            return NextSize(resultSet.getObject(number) as T, number + 1)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    companion object {
        private val valueMappingJavaSQL = mapOf("long" to "bigInt", "String" to "Text")
    }

}