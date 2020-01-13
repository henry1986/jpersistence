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

import org.daiv.reflection.annotations.*
import org.daiv.reflection.isEnum
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.*
import org.daiv.reflection.toKClass
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty1
import kotlin.reflect.full.createType
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible

interface CheckAnnotation {
    fun manyToOne(): ManyToOne
}

internal fun getManyToOne() = ManyToOne::class.constructors.first().call("")

internal fun getIFaceForList(iFaceForObject: Array<IFaceForObject>) = IFaceForList::class.constructors.first().call(iFaceForObject)

internal fun <T : Any> KClass<T>.createObject(vararg args: Any) = this.constructors.first().call(*args)

internal fun <T : Any> T?.default(clazz: KClass<T>, vararg args: Any): T {
    return this ?: clazz.createObject(*args)
}

internal fun MoreKeys?.default(i: Int = 1) = default(MoreKeys::class, i, false)
//fun Including?.default(include: Boolean = false) = default(Including::class, include)
//fun moreKeys(i: Int) = MoreKeys::class.createObject(i, false)

internal fun <T : Any> KClass<T>.moreKeys() = this.findAnnotation<MoreKeys>()
        .default(1)

internal fun <T : Any> KClass<T>.including() = this.findAnnotation<Including>()
        .default(Including::class, false)

internal fun <T : Any> KProperty1<*, *>.toMap(clazz: KClass<*>,
                                              createFct: (String, KClass<*>) -> T): Map<String, T> {
    val interf = findAnnotation<IFaceForObject>()
            ?: throw RuntimeException("missing @IFaceForObject annotation in $clazz")
    val map = interf.classesNames.map {
        val name = InterfaceField.nameOfClass(it)
        name to createFct(name, it)
    }
            .toMap()
    return map

}

internal class KeyAnnotation(private val property: KProperty1<*, *>) : CheckAnnotation {
    override fun manyToOne(): ManyToOne {
        return property.findAnnotation() ?: getManyToOne()
    }
}

internal fun <T : Any> KClass<T>.isNoMapAndNoListAndNoSet() = this != List::class && this != Map::class && this != Set::class
internal fun <T : Any> KClass<T>.isMapListOrSet() = this == List::class || this == Map::class || this == Set::class
internal fun KClass<Any>.toProperty(property: KProperty1<Any, Any>, receiverClass: KClass<Any>) = when {
    this == Map::class -> DefaultMapProperty(property, receiverClass)
    this == Set::class -> DefSetProperty(property, receiverClass)
    this == List::class -> ListMapProperty(property, receiverClass)
    else -> throw RuntimeException("Only map, set and list are tested in this function")
}
//internal fun KClass<Any>.toFieldData(persisterProvider: PersisterProvider,
//                                     prefix: String?): FieldData {
//    return when {
//        this.java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(SimpleTypeProperty(this,
//                                                                                      this.simpleName!!),
//                                                                   prefix)
//        this.isEnum() -> EnumType(SimpleTypeProperty(this, this.simpleName!!), prefix)
//        this.isNoMapAndNoListAndNoSet() -> ReadComplexType(SimpleTypeProperty(this, this.simpleName!!),
//                                                           moreKeys(),
//                                                           including(),
//                                                           persisterProvider,
//                                                           prefix)
////        this == List::class -> ListType(SimpleTypeProperty(this, this.simpleName!!), persisterProvider,)
//        else -> {
//            throw RuntimeException("this: $this not possible")
//        }
//    }
//}


internal class FieldDataFactory constructor(val persisterProvider: PersisterProvider,
                                            val clazz: KClass<Any>,
                                            val prefix: String?,
                                            val persister: Persister) {
    val including = clazz.including()
    val moreKeys = clazz.moreKeys()

    inner class Builder(val idField: KeyType?,
                        val keyFields: List<FieldData> = emptyList(),
                        val fields: List<FieldData> = emptyList()) {

        fun Int.include() = this < moreKeys.amount || including.include
        fun next(i: Int, constructor: KFunction<Any>): Builder {
            if (i < constructor.parameters.size) {
                val parameter = constructor.parameters[i]
                val y = clazz.declaredMemberProperties.find { it.name == parameter.name } as KProperty1<Any, Any>
                y.isAccessible = true
                val c: FieldData
                val keyField: FieldData?
                when {
                    i.include() && moreKeys.auto -> {
                        c = createKeyDependent(y)
                        keyField = c
                    }
                    i.include() -> {
                        c = create(y) ?: throw RuntimeException("in class: $clazz -> type unkown ${y.returnType} -> or a type," +
                                                                        " that needs autogenerated key, so use MoreValues(auto = true)")
                        keyField = c
                    }
                    else -> {
                        idField ?: throw NullPointerException("in class: $clazz -> null on $i - ${fields.size}, moreKeys: $moreKeys")
                        keyField = null
                        c = createKeyDependent(y)
                    }
                }
                val nextKeyFields = keyField?.let { keyFields + it } ?: keyFields
                val fields = fields + c
                val idField: KeyType? = when {
                    !including.include && (i == moreKeys.amount - 1 && idField == null) || (including.include && i + 1 == constructor.parameters.size) -> {
                        KeyType(nextKeyFields)
                    }
                    i >= moreKeys.amount || idField != null -> idField
                    else -> null
                }
                return Builder(idField, nextKeyFields, fields).next(i + 1, constructor)
            }

            return if (moreKeys.auto) {
                val keyHashCodeable = clazz.createHashCodeableKey(HashCodeableProvider())
                val autoIdField = KeyType(listOf(AutoKeyType(AutoKeyProperty(keyHashCodeable), prefix),
                                                 ReadSimpleType(SimpleTypeProperty(Int::class.createType(), "hashCodeXCounter"), prefix)))
                val keyType = KeyType(autoIdField!!.fields, keyHashCodeable, idField)
                val fields = (listOf(keyType) + fields)
                fields.forEach { it.onIdField(keyType) }
                Builder(keyType, fields, fields)
            } else {
                fields.forEach { it.onIdField(idField!!) }
                this
            }
        }

        private fun createWithoutInterface(thisClass: KClass<Any>, property: KProperty1<Any, Any>): FieldData? {
            return when {
                thisClass.java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(DefProperty(property, clazz), prefix)
                thisClass.objectInstance != null ->
                    ObjectField(DefProperty(property, clazz, thisClass.createType(), true, thisClass.simpleName!!), prefix)
                thisClass.isEnum() -> EnumType(DefProperty(property, clazz), prefix)
                thisClass.isNoMapAndNoListAndNoSet() -> {
                    val propertyData = DefProperty(property, clazz, thisClass.createType(), true, thisClass.simpleName!!)
                    ReadComplexType(propertyData, moreKeys, propertyData.clazz.including(), persisterProvider, prefix)
                }
                else -> {
                    null
                }
            }
        }

        fun create(property: KProperty1<Any, Any>): FieldData? {
            val kClass = property.toKClass()
            return when {
                kClass.java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(DefProperty(property, clazz), prefix)
                kClass.objectInstance != null -> ObjectField(DefProperty(property, clazz), prefix)
                kClass.isEnum() -> EnumType(DefProperty(property, clazz), prefix)
                kClass.java.isInterface && kClass.isNoMapAndNoListAndNoSet() -> {
                    val map = property.toMap(kClass) { s, c ->
                        PossibleImplementation(s, createWithoutInterface(c as KClass<Any>, property)!!)
                    }
                    InterfaceField(InterfaceProperty(property, clazz), prefix, map)
                }
                kClass.isNoMapAndNoListAndNoSet() -> {
                    val propertyData = DefProperty(property, clazz)
                    ReadComplexType(propertyData, moreKeys, propertyData.clazz.including(), persisterProvider, prefix)
                }
                else -> {
                    null
                }
            }
        }


        fun createKeyDependent(property: KProperty1<Any, Any>): FieldData {
            val simple = create(property)
            if (simple != null) {
                return simple
            }
            val proClass = property.returnType.classifier as KClass<Any>
            return when {
                proClass.isMapListOrSet() ->
                    MapType(proClass.toProperty(property, clazz), persisterProvider, property.findAnnotation() ?: kotlin.run {
                        property.findAnnotation<IFaceForObject>()
                                ?.let { getIFaceForList(arrayOf(it)) }
                    }, prefix, persister, clazz)
                else -> {
                    throw RuntimeException("unknown type : ${property.returnType}")
                }
            }
        }
    }

    private fun MoreKeys.createAutoKey(constructor: KFunction<Any>): Builder {
        return Builder(null, emptyList()).next(0, constructor)
    }

    fun fieldsRead(): Builder {
        if (clazz.java.isPrimitiveOrWrapperOrString()) {
            val key = KeyType(listOf(ReadSimpleType(SimpleTypeProperty(clazz.createType(), clazz.tableName()), prefix)) as List<FieldData>)
            val fields = listOf(key) as List<FieldData>
            return Builder(key, fields, fields)
        }
        return clazz.findAnnotation<MoreKeys>()
                .default(1)
                .createAutoKey(clazz.primaryConstructor ?: run {
                    throw RuntimeException("clazz $clazz has no primary constructor")
                })
    }
}

fun KClass<out Any>.createHashCode(obj: List<Any>) = createHashCodeableKey(HashCodeableProvider()).plainHashCodeX(obj)

internal fun KClass<out Any>.createHashCodeableKey(provider: HashCodeableProvider,
                                                   moreKeys: MoreKeys = this.findAnnotation<MoreKeys>().default(1),
                                                   including: Including? = this.findAnnotation()) =
        createHashCodeables(provider, moreKeys, including, moreKeys.amount).key()

internal class HashCodeableHandler(val moreKeys: MoreKeys, val including: Including?, val list: List<HashCodeable>) {
    fun key() = KeyHashCodeable(including?.let { list } ?: list.take(moreKeys.amount))
}

internal fun KClass<out Any>.createHashCodeables(provider: HashCodeableProvider,
                                                 moreKeys: MoreKeys = this.findAnnotation<MoreKeys>().default(1),
                                                 including: Including? = this.findAnnotation(),
                                                 maxSize: Int = moreKeys.amount,
                                                 i: Int = 0,
                                                 constructor: KFunction<Any> = this.primaryConstructor ?: run {
                                                     throw RuntimeException("clazz $this has no primary constructor")
                                                 },
                                                 ret: List<HashCodeable> = emptyList()): HashCodeableHandler {
    if (i < constructor.parameters.size && (maxSize != -1 || i < maxSize)) {
        val parameter = constructor.parameters[i]
        val property = this.declaredMemberProperties.find { it.name == parameter.name }
        val hashCodeable = property!!.createHashCodeable(provider)
        return createHashCodeables(provider, moreKeys, including, maxSize, i + 1, constructor, ret + hashCodeable)
    }
    return HashCodeableHandler(moreKeys, including, ret)

}

internal data class PossibleHashImplementation(val key: String, val hashCodeable: HashCodeable)

internal fun KProperty1<*, *>.createForInterface(provider: HashCodeableProvider, kClass: KClass<*>): HashCodeable {
    return when {
        kClass.java.isPrimitiveOrWrapperOrString() -> SimpleHashCodeable(DefaultProperyReader(this as KProperty1<Any, Any>))
        kClass.isEnum() -> EnumHashCodeable
        kClass.objectInstance != null -> ObjectHashCodeable(kClass.objectInstance!!)
        kClass.isNoMapAndNoListAndNoSet() ->
            ComplexHashCodeable(kClass as KClass<Any>, provider, DefaultProperyReader(this as KProperty1<Any, Any>))
        else -> throw RuntimeException("type not supported: $kClass")
    }

}

internal fun KProperty1<*, *>.createHashCodeable(provider: HashCodeableProvider): HashCodeable {
    val kClass = this.toKClass()
    return when {
        kClass.java.isPrimitiveOrWrapperOrString() -> SimpleHashCodeable(DefaultProperyReader(this as KProperty1<Any, Any>))
        kClass.isEnum() -> EnumHashCodeable
        kClass.objectInstance != null -> ObjectHashCodeable(kClass.objectInstance!!)
        kClass.java.isInterface && kClass.isNoMapAndNoListAndNoSet() -> {
            val pReader = DefaultProperyReader(this as KProperty1<Any, Any>)
            val map = toMap(kClass) { name, clazz ->
                PossibleHashImplementation(name, createForInterface(provider, clazz))
            }
            InterfaceHashCodeable(map, pReader)
        }
        kClass.isNoMapAndNoListAndNoSet() ->
            ComplexHashCodeable(kClass, provider, DefaultProperyReader(this as KProperty1<Any, Any>))
        kClass == Map::class -> {
            val mapReadable = MapReadable(this as KProperty1<Any, Map<Any, Any>>)
            MapHashCodeable(mapReadable.keyClazz.createHashCodeables(provider).key(),
                            mapReadable.clazz.createHashCodeables(provider).key(),
                            mapReadable)
        }
        kClass == List::class -> {
            ListHashCodeable((this as KProperty1<Any, List<Any>>).keyHashCodeable(provider), ListReadable(this))
        }
        kClass == Set::class -> {
            this as KProperty1<Any, Set<Any>>
            SetHashCodeable(this.keyHashCodeable(provider), DefaultProperyReader(this))
        }
        else -> {
            throw RuntimeException("unknown type : ${this.returnType}")
        }
    }
}

internal fun KClass<*>.createHashCodeablesWithoutInterface(provider: HashCodeableProvider): HashCodeable {
    return when {
        this.java.isPrimitiveOrWrapperOrString() -> SimpleHashCodeable(SimpleTypeReadable)
        this.isEnum() -> EnumHashCodeable
        this.isNoMapAndNoListAndNoSet() -> ComplexHashCodeable(this as KClass<Any>, provider, SimpleTypeReadable)
        else -> throw RuntimeException("type not known: $this")
    }
}

internal fun KClass<Any>.createHashCodeable(provider: HashCodeableProvider, interf: IFaceForList?): HashCodeable {
    return when {
        this.java.isPrimitiveOrWrapperOrString() -> SimpleHashCodeable(SimpleTypeReadable)
        this.isEnum() -> EnumHashCodeable
        this.java.isInterface && this.isNoMapAndNoListAndNoSet() -> {
            val map = this.createType()
                    .toMap(interf, 1) { name, clazz ->
                        PossibleHashImplementation(name, clazz.createHashCodeablesWithoutInterface(provider))
                    }
            InterfaceHashCodeable(map, SimpleTypeReadable)
        }
        this.isNoMapAndNoListAndNoSet() -> ComplexHashCodeable(this, provider, SimpleTypeReadable)
        this == Map::class -> {
            val mapReadable = MapReadable(this as KProperty1<Any, Map<Any, Any>>)
            MapHashCodeable(mapReadable.keyClazz.createHashCodeables(provider).key(),
                            mapReadable.clazz.createHashCodeables(provider).key(),
                            mapReadable)
        }
        this == List::class -> {
            ListHashCodeable((this as KProperty1<Any, List<Any>>).keyHashCodeable(provider), ListReadable(this))
        }
        this == Set::class -> {
            this as KProperty1<Any, Set<Any>>
            SetHashCodeable(this.keyHashCodeable(provider), DefaultProperyReader(this))
        }
        else -> {
            throw RuntimeException("unknown type : ${this}")
        }
    }
}

internal fun <R : Any, S : Any> KProperty1<R, S>.keyHashCodeable(provider: HashCodeableProvider): HashCodeable {
    return (returnType.arguments.first().type!!.classifier as KClass<Any>).createHashCodeable(provider, findAnnotation<IFaceForObject>()
            ?.let { getIFaceForList(arrayOf(it)) })
}

