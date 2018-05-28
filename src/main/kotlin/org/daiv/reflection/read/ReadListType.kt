package org.daiv.reflection.read

import org.daiv.immutable.utils.persistence.annotations.FlatList
import org.daiv.reflection.common.ListUtil
import java.sql.ResultSet
import kotlin.reflect.KProperty1

internal class ReadListType<T : Any>(override val property: KProperty1<Any, T>,
                                     private val flatList: FlatList) : ReadFieldData<T>, ListUtil<T> {
    override fun key(prefix: String?): String {
        return map({ i, p -> p.createTableKeyData(listElementName(i)) }, { joinToString(", ") })
    }

    private fun <T, R> map(f: (Int, ReadPersisterData<Any>) -> T,
                           collect: Sequence<T>.() -> R): R {
        val p = ReadPersisterData.create(getGenericListType())
        return (0..flatList.size)
            .asSequence()
            .map { i -> f(i, p) }
            .collect()
    }

    override fun toTableHead(prefix: String?): String {
        return map({ i, p -> p.createTableString(listElementName(i)) }, { joinToString(", ") })
    }

    private tailrec fun getValue(resultSet: ResultSet,
                                 p: ReadPersisterData<Any>,
                                 i: Int,
                                 counter: Int,
                                 list: List<out Any>): NextSize<T> {
        if (i < p.size()) {
            val (t, nextCounter) = p.read(resultSet, counter)
            return getValue(resultSet, p, i + 1, nextCounter, list + t)
        }
        return NextSize(list as T, counter)
    }

    @Suppress("UNCHECKED_CAST")
    override fun getValue(resultSet: ResultSet, number: Int): NextSize<T> {
        return getValue(resultSet, ReadPersisterData.create(getGenericListType()), 0, number, listOf())
//        return map({ i, p -> p.read(resultSet, number - 1 + i!! * p.size()) }, { toList() as T })
    }
}