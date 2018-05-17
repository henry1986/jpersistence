package org.daiv.reflection.common

import kotlin.reflect.KProperty1

interface FieldData<T : Any> {
    val property: KProperty1<Any, T>


    /**
     *
     * @return the name of the property
     */
    fun name(): String {
        return property.name
    }

    /**
     * builds the name of the field in the database
     *
     * @param prefix
     * if the name of the field in the database shall have a prefix,
     * it is to be given here. null otherwise.
     * @return the name of the field in the database
     */
    fun name(prefix: String?): String {
        return if (prefix == null) property.name else prefix + "_" + property.name
    }

}