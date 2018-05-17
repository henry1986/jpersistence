package org.daiv.reflection.write

import org.daiv.immutable.utils.persistence.annotations.FlatList
import org.daiv.reflection.getKClass
import kotlin.reflect.KProperty1
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.javaType

class WriteSimpleType <T:Any>(override val property: KProperty1<Any, T>, override val flatList: FlatList, override val o: Any) : WriteFieldData<T>{
    override fun insertValue(): String {
        val s = getObject(o).toString()
        return if (property.getKClass() == String::class) {
            "\"" + s + "\""
        } else s
    }

    override fun insertHead(prefix: String?): String {
        return name(prefix)
    }
}