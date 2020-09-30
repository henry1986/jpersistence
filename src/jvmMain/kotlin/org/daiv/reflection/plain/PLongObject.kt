package org.daiv.reflection.plain

import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.EnumType
import java.sql.ResultSet
import kotlin.reflect.KClass

internal interface SimpleReadObject {
    val name: String
    fun resultMatch(resultSet: ResultSet, counter: Int): Any
}

internal data class PlainLongObject(override val name: String) : SimpleReadObject {
    override fun resultMatch(resultSet: ResultSet, counter: Int): Long {
        return resultSet.getLong(counter)
    }
}

internal data class PlainObject(override val name: String) : SimpleReadObject {
    override fun resultMatch(resultSet: ResultSet, counter: Int): Any {
        return resultSet.getObject(counter)
    }
}

internal data class PlainEnumObject<T : Enum<*>>(override val name: String, val clazz: KClass<T>) : SimpleReadObject {
    override fun resultMatch(resultSet: ResultSet, counter: Int): T {
        val text = resultSet.getString(counter)
        return EnumType.getEnumValue(clazz.java, text)
    }
}

internal data class PlainBooleanObject(override val name: String) : SimpleReadObject {
    override fun resultMatch(resultSet: ResultSet, counter: Int): Any {
        val i = resultSet.getInt(counter)
        return i == 1
    }
}

class RequestBuilder<X : Any> internal constructor(private val list: MutableList<SimpleReadObject> = mutableListOf(),
                                                   private val table: Persister.Table<out Any>,
                                                   private val listener: (List<Any>) -> X) {

//    fun l(name: String): RequestBuilder<X> {
//        list.add(PlainLongObject(name))
//        return this
//    }
//
//    fun <T : Enum<T>> e(name: String, clazz: KClass<T>): RequestBuilder<X> {
//        list.add(PlainEnumObject(name, clazz))
//        return this
//    }
//
//    fun b(name: String): RequestBuilder<X> {
//        list.add(PlainBooleanObject(name))
//        return this
//    }
//
//    fun p(name: String): RequestBuilder<X> {
//        list.add(PlainObject(name))
//        return this
//    }

    fun read(table: Persister.Table<out Any> = this.table) = table.readPlain(list, listener)
}

internal fun <X : Any> readPlainMapper(list: List<SimpleReadObject>,
                                       listener: (List<Any>) -> X): (ResultSet) -> List<X> = { resultSet ->
    val ret = mutableListOf<X>()
    while (resultSet.next()) {
        var counter = 1
        val tmp = mutableListOf<Any>()
        list.forEach {
            tmp.add(it.resultMatch(resultSet, counter++))
        }
        ret.add(listener(tmp))
    }
    ret
}