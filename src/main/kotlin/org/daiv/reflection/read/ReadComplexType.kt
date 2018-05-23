package org.daiv.reflection.read

import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

class ReadComplexType<T : Any>(override val property: KProperty1<Any, T>) : ReadFieldData<T> {
    override fun key(prefix: String?): String {
        return persisterData.createTableKeyData(property.name)
    }

    private val persisterData = ReadPersisterData.create(property.returnType.classifier as KClass<T>)

    override fun toTableHead(prefix: String?): String {
        return persisterData.createTableString(property.name)
    }

    override fun getValue(resultSet: ResultSet, number: Int): T {
        return persisterData.read(resultSet, number - 1)
    }
}