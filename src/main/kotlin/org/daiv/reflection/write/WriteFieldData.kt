package org.daiv.reflection.write

import org.daiv.immutable.utils.persistence.annotations.FlatList
import org.daiv.reflection.common.FieldData
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.isAccessible

internal interface WriteFieldData<T : Any> : FieldData<T> {
    val flatList: FlatList
    /**
     * object that has [property] as member and that is to read from
     */
    val o: Any


    /**
     * gets the [property] value of [o]
     */
    fun getObject(o: Any): T {
        property.isAccessible = true
        return property.get(o)
    }

    /**
     * this method creates the string for the sql command "INSERT INTO ", but
     * only the values until "VALUES". For the values afterwards, see
     * [insertValue]
     *
     * @param prefix
     * a possible prefix for the variables name. Null, if no prefix
     * is wanted.
     *
     * @return the string for the "INSERT INTO " command
     */
    fun insertHead(prefix: String?): String

    /**
     * this method creates the string for the sql command "INSERT INTO ", but
     * only the values after "VALUES". For the values before, see
     * [insertHead]
     *
     * @return the string for the "INSERT INTO " command
     */
    fun insertValue(): String
}