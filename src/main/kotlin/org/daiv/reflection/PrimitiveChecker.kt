/*
 * Copyright (c) 2018. Martin Heinrich - All Rights Reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package org.daiv.reflection

import kotlin.reflect.KClass
import kotlin.reflect.KClassifier
import kotlin.reflect.KProperty1
import kotlin.reflect.KType


private val primitiveWrapperTypeMap = setOf(
    Boolean::class.javaObjectType,
    Byte::class.javaObjectType,
    Char::class.javaObjectType,
    Double::class.javaObjectType,
    Float::class.javaObjectType,
    Int::class.javaObjectType,
    Long::class.javaObjectType,
    Short::class.javaObjectType,
    String::class.javaObjectType)


/**
 * Check if the parameter represents a primitive wrapper, i.e. Boolean,
 * Byte, Character, Short, Integer, Long, Float, or Double.
 *
 * @param clazz
 * the class to check
 * @return true, if the parameter is a primitive wrapper class, false
 * otherwise
 */
internal fun Class<*>.isPrimitiveWrapperOrString(): Boolean {
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
internal fun Class<*>.isPrimitiveOrWrapperOrString(): Boolean {
    return isPrimitive || isPrimitiveWrapperOrString()
}

internal fun<R:Any> KType.getKClass(): KClass<R> {
    return classifier as KClass<R>
}

internal fun<R:Any, T:Any> KProperty1<R, T>.getKClass(): KClass<T>{
    return returnType.getKClass()
}