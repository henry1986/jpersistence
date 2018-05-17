package org.daiv.reflection.write

import org.daiv.immutable.utils.persistence.annotations.FlatList
import org.daiv.reflection.common.ListUtil
import kotlin.reflect.KProperty1

class WriteListType<T : Any>(override val property: KProperty1<Any, T>, override val flatList: FlatList, override val o: Any) : WriteFieldData<T>, ListUtil<T> {
    private fun <T> map(f: (Int, WritePersisterData<Any>) -> T): String {
        val l = property.get(o) as List<Any>
        return (0..flatList.size)
            .asSequence()
            .map { i -> f.invoke(i, WritePersisterData.create(l[i])) }
            .joinToString(", ")
    }

    override fun insertValue(): String {
        return map({ _, p -> p.insertValueString() })
    }

    override fun insertHead(prefix: String?): String {
        return map({ i, p -> p.insertHeadString(listElementName(i)) })
    }
}