package org.daiv.reflection.persister

import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

class AccessObject(private val p0: List<KProperty1<*, *>>) {
    constructor(vararg p0: KProperty1<*, *>) : this(p0.asList())

    fun List<KProperty1<*, *>>.getName(i: Int = 0, ret: String = ""): String {
        if (i < size) {
            val p = get(i)
            return getName(i + 1, if (i == 0) p.name else "${ret}_${p.name}")
        }
        return ret
    }

    fun getPair(p: PropertyValue<*>): String {
        val name = (this.p0 + p.p0).getName()
        val firstValue = when (p.p0.returnType.classifier) {
            String::class -> "\"${p.value}\""
            Boolean::class -> if (p.value == true) "1" else "0"
            else -> p.value
        }
        return "$name=$firstValue"
    }
}

data class PropertyValue<T>(val value: T, val p0: KProperty1<*, T>)

private fun <T:Any> T.getPropertyValuesIntern(list: Collection<KProperty1<out T, *>>): List<PropertyValue<*>> {
    return list.map {
        it as KProperty1<Any, Any>
        val got = it.get(this)
        PropertyValue(got, it)
    }
}

fun <T:Any> T.getPropertyValues(vararg list: KProperty1<out T, *>): List<PropertyValue<*>>{
    return getPropertyValuesIntern(list.asList())
}

fun <T:Any> T.getAllPropertyValues(): List<PropertyValue<*>> {
    return getPropertyValuesIntern(this::class.declaredMemberProperties)
}

fun List<PropertyValue<*>>.toRequest(accessObject: AccessObject): String {
    return map { accessObject.getPair(it) }.joinToString(" AND ")
}
