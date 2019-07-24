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

package org.daiv.reflection.common

import org.daiv.reflection.annotations.ManyList
import org.daiv.reflection.annotations.ManyToOne
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.annotations.SameTable
import org.daiv.reflection.isEnum
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.*
import org.daiv.reflection.toKClass
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor

interface CheckAnnotation {
    fun isSameTabe(): Boolean
    fun manyToOne(): ManyToOne
}

fun getManyToOne() = ManyToOne::class.constructors.first().call("")

internal fun <T : Any> KClass<T>.createObject(vararg args: Any) = this.constructors.first().call(*args)

internal fun <T : Any> T?.default(clazz: KClass<T>, vararg args: Any): T {
    return this ?: clazz.createObject(*args)
}

fun MoreKeys?.default(i: Int = 1) = default(MoreKeys::class, i, false)
fun moreKeys(i: Int) = MoreKeys::class.createObject(i, false)

class KeyAnnotation(private val property: KProperty1<*, *>) : CheckAnnotation {
    override fun isSameTabe(): Boolean {
        return property.findAnnotation<SameTable>() != null
    }

    override fun manyToOne(): ManyToOne {
        return property.findAnnotation() ?: getManyToOne()
    }
}

internal fun <T : Any> KClass<T>.isNoMapAndNoListAndNoSet() = this != List::class && this != Map::class && this != Set::class

internal fun <T : Any> KClass<T>.toFieldData(checkAnnotation: CheckAnnotation,
                                             prefix: String?,
                                             persister: Persister): FieldData<Any, Any, T, Any> {
    return when {
        this.java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(SimpleTypeProperty(this,
                                                                                      this.simpleName!!),
                                                                   prefix) as FieldData<Any, Any, T, Any>
        this.isEnum() -> EnumType(SimpleTypeProperty(this, this.simpleName!!), prefix) as FieldData<Any, Any, T, Any>
        checkAnnotation.isSameTabe() -> {
            val propertyData = SimpleTypeProperty(this, this.simpleName!!)
            ComplexSameTableType(propertyData, prefix, null, persister) as FieldData<Any, Any, T, Any>
        }
        this.isNoMapAndNoListAndNoSet() -> ReadComplexType(SimpleTypeProperty(this, this.simpleName!!),
                                                           checkAnnotation.manyToOne(),
                                                           persister, prefix) as FieldData<Any, Any, T, Any>
        else -> {
            throw RuntimeException("this: $this not possible")
        }
    }
}

internal class FieldDataFactory<R : Any>(val clazz: KClass<R>,
                                         val prefix: String?,
                                         val parentTableName: String?,
                                         val persister: Persister) {
    val moreKeys = clazz.findAnnotation<MoreKeys>()
            .default(1)

    inner class Builder(val idField: FieldData<R, *, *, *>?, val fields: List<FieldData<R, *, *, *>> = emptyList()) {
        fun next(i: Int, constructor: KFunction<R>): Builder {
            if (i < constructor.parameters.size) {
                val parameter = constructor.parameters[i]
                val y = clazz.declaredMemberProperties.find { it.name == parameter.name } as KProperty1<R, out Any>
                val c = if (i < moreKeys.amount && !moreKeys.auto) {
                    create(y) ?: throw RuntimeException("type unkown ${y.returnType} -> or a type, that needs" +
                                                                " autogenerated key, so use MoreValues(auto = true)")
                } else {
                    create(y, idField ?: throw NullPointerException("null on $i - ${fields.size}, moreKeys: $moreKeys"))
                }
                val fields = fields + c
                val idField: FieldData<R, *, *, *>? = when {
                    i == moreKeys.amount - 1 -> KeyType(fields as List<FieldData<Any, Any, Any, Any>>) as FieldData<R, *, *, *>
                    i >= moreKeys.amount -> idField
                    else -> null
                }
                return Builder(idField, fields).next(i + 1, constructor)
            }
            return this
//            throw RuntimeException("unknown type : ${property.returnType} or a type, " +
//                                           "that needs a autogenerated key, so use MoreValues(auto = true)")
        }

        fun create(property: KProperty1<R, out Any>): FieldData<R, *, *, *>? {
            return when {
                property.toKClass().java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(DefProperty(property,
                                                                                                      clazz), prefix)
                property.toKClass().isEnum() -> EnumType(DefProperty(property as KProperty1<R, Enum<*>>,
                                                                     clazz), prefix)
                property.findAnnotation<SameTable>() != null -> {
                    val propertyData = DefProperty(property, clazz)
                    ComplexSameTableType(propertyData, prefix, parentTableName!!, persister)
                }
                (property.returnType.classifier as KClass<out Any>).isNoMapAndNoListAndNoSet() ->
                    ReadComplexType(DefProperty(property, clazz),
                                    property.findAnnotation<ManyToOne>().default(ManyToOne::class, ""),
                                    persister, prefix)
                else -> {
                    null
                }
            }
        }

        fun create(property: KProperty1<R, out Any>, idField: FieldData<R, *, *, *>): FieldData<R, *, *, *> {
            val simple = create(property)
            if (simple != null) {
                return simple
            }
            return when {
                idField == null -> {
                    throw RuntimeException("class: $idField -> the primary Key must not be a collection!")
                }
                property.returnType.classifier as KClass<out Any> == Map::class -> {
                    MapType(DefaultMapProperty(property as KProperty1<R, Map<Any, out Any>>, clazz),
                            prefix,
                            persister,
                            parentTableName!!,
                            { it },
                            idField)
                }
                property.returnType.classifier as KClass<out Any> == List::class -> {
                    MapType(ListMapProperty(property as KProperty1<R, List<out Any>>, clazz),
                            prefix,
                            persister,
                            parentTableName!!,
                            {
                                it.toList()
                                        .sortedBy { it.first }
                                        .map { it.second }
                            },
                            idField)
                }
                property.returnType.classifier as KClass<out Any> == Set::class -> {
                    SetType(SetProperty(property as KProperty1<R, Set<out Any>>, clazz),
                            property.findAnnotation() ?: ManyList::class.constructors.first().call(""),
                            persister,
                            prefix,
                            idField)
                }
                else -> {
                    throw RuntimeException("unknown type : ${property.returnType}")
                }
            }
        }
    }

    private fun MoreKeys.createAutoKey(constructor: KFunction<R>): List<FieldData<R, *, *, *>> {
        val field = if (auto) {
            ReadSimpleType(AutoKeyProperty(clazz.createHashCodeableKey()), prefix)
        } else {
            null
        }
        return Builder(field as FieldData<R, *, *, *>?).next(0, constructor)
                .fields
    }

    fun fieldsRead(): List<FieldData<R, *, *, *>> {
        if (clazz.java.isPrimitiveOrWrapperOrString()) {
            return listOf(ReadSimpleType(SimpleTypeProperty(clazz, clazz.tableName()), prefix))
        }
        return clazz.findAnnotation<MoreKeys>()
                .default(1)
                .createAutoKey(clazz.primaryConstructor ?: run {
                    throw RuntimeException("clazz $clazz has no primary constructor")
                })
    }
}

internal fun KClass<out Any>.createHashCodeableKey(moreKeys: MoreKeys = this.findAnnotation<MoreKeys>().default(1)) =
        createHashCodeables(moreKeys, moreKeys.amount).key()

internal class HashCodeableHandler(val moreKeys: MoreKeys, val list: List<HashCodeable<out Any>>) {
    fun key() = KeyHashCodeable(list.take(moreKeys.amount))
}

internal fun KClass<out Any>.createHashCodeables(moreKeys: MoreKeys = this.findAnnotation<MoreKeys>().default(1),
                                                 maxSize: Int = -1,
                                                 i: Int = 0,
                                                 constructor: KFunction<Any> = this.primaryConstructor ?: run {
                                                     throw RuntimeException("clazz $this has no primary constructor")
                                                 },
                                                 ret: List<HashCodeable<out Any>> = emptyList()): HashCodeableHandler {
    if (i < constructor.parameters.size && (maxSize != -1 || i < maxSize)) {
        val parameter = constructor.parameters[i]
        val property = this.declaredMemberProperties.find { it.name == parameter.name }
        val hashCodeable = property!!.createHashCodeable()
        return createHashCodeables(moreKeys, maxSize, i + 1, constructor, ret + hashCodeable)
    }
    return HashCodeableHandler(moreKeys, ret)

}


internal fun KProperty1<*, *>.createHashCodeable(): HashCodeable<out Any> {
    return when {
        this.toKClass().java.isPrimitiveOrWrapperOrString() -> SimpleHashCodeable
        this.toKClass().isEnum() -> EnumHashCodeable
        this.findAnnotation<SameTable>() != null ->
            ComplexSameTableHashCodeable((this.returnType.classifier as KClass<Any>).createHashCodeables().list,
                                         DefaultProperyReader(this as KProperty1<Any, Any>))

        (this.returnType.classifier as KClass<Any>).isNoMapAndNoListAndNoSet() ->
            ComplexHashCodeable((this.returnType.classifier as KClass<Any>).createHashCodeables().key(),
                                DefaultProperyReader(this as KProperty1<Any, Any>))
        this.returnType.classifier as KClass<Any> == Map::class -> {
            val mapReadable = MapReadable(this as KProperty1<Any, Map<Any, Any>>)
            MapHashCodeable<Any, Any>(mapReadable.keyClazz.createHashCodeables().key(),
                                      mapReadable.clazz.createHashCodeables().key(),
                                      mapReadable)
        }
        this.returnType.classifier as KClass<Any> == List::class -> {
            MapHashCodeable<Int, Any>(KeyHashCodeable(listOf(SimpleHashCodeable)),
                                      (this as KProperty1<Any, Map<Int, Any>>).keyHashCodeable(),
                                      ListReadable(this as KProperty1<Any, List<Any>>))
        }
        this.returnType.classifier as KClass<Any> == Set::class -> {
            this as KProperty1<Any, Set<Any>>
            SetHashCodeable(this.keyHashCodeable(), DefaultProperyReader(this))
        }
        else -> {
            throw RuntimeException("unknown type : ${this.returnType}")
        }
    }
}

internal fun <R : Any, S : Any> KProperty1<R, S>.keyHashCodeable() =
        (returnType.arguments.first().type!!.classifier as KClass<Any>).createHashCodeables().key()