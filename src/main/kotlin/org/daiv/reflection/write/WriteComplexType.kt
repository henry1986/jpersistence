package org.daiv.reflection.write

import org.daiv.immutable.utils.persistence.annotations.FlatList
import kotlin.reflect.KProperty1

class WriteComplexType<T : Any>(override val property: KProperty1<Any, T>, override val flatList: FlatList, override val o: Any) : WriteFieldData<T> {
    private val persisterData = WritePersisterData.create(getObject(o))

    override fun insertValue(): String {
        return persisterData.insertValueString()
    }

    override fun insertHead(prefix: String?): String {
        return persisterData.insertHeadString(property.name)
    }
}