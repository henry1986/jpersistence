package org.daiv.reflection.read

import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

class ReadComplexType<T : Any>(override val property: KProperty1<Any, T>) : ReadFieldData<T> {

    private val persisterData = ReadPersisterData.create(
        property.returnType.classifier as KClass<T>)//PersisterData.create(field.type)

    override fun toTableHead(prefix: String?, key: Boolean): String {
        if (key) {
            throw RuntimeException("a complex type must not be PRIMARY KEY")
        }
        return persisterData.createTableString(property.name)
    }

    override fun getValue(resultSet: ResultSet, number: Int): T {
        return persisterData.read(resultSet, number - 1)
    }
}