package org.daiv.reflection

import kotlin.reflect.KClass
import kotlin.reflect.KClassifier
import kotlin.reflect.KProperty1
import kotlin.reflect.KType

class PrimitiveChecker {
}

private val primitiveWrapperTypeMap = listOf(
    Boolean::class.java,
    Byte::class.java,
    Char::class.java,
    Double::class.java,
    Float::class.java,
    Int::class.java,
    Long::class.java,
    Short::class.java,
    String::class.java)


/**
 * Check if the parameter represents a primitive wrapper, i.e. Boolean,
 * Byte, Character, Short, Integer, Long, Float, or Double.
 *
 * @param clazz
 * the class to check
 * @return true, if the parameter is a primitive wrapper class, false
 * otherwise
 */
fun Class<*>.isPrimitiveWrapperOrString(): Boolean {
    return primitiveWrapperTypeMap.contains(this)
}

/**
 * Check if the parameter represents a primitive (boolean, byte, char,
 * short, int, long, float, or double) or a primitive wrapper (Boolean,
 * Byte, Character, Short, Integer, Long, Float, or Double) or a String.
 *
 * @param clazz
 * the class to check
 * @return true, if the parameter is a primitive or primitive wrapper class
 * or String, false otherwise
 */
fun Class<*>.isPrimitiveOrWrapperOrString(): Boolean {
    return isPrimitive() || isPrimitiveWrapperOrString()
}

fun KType.getKClass(): KClass<Any> {
    return classifier as KClass<Any>
}

fun KProperty1<Any, Any>.getKClass(): KClass<Any>{
    return returnType.getKClass()
}