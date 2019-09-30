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
//fun moreKeys(i: Int) = MoreKeys::class.createObject(i, false)

internal fun <T : Any> KClass<T>.moreKeys() = this.findAnnotation<MoreKeys>()
        .default(1)

class KeyAnnotation(private val property: KProperty1<*, *>) : CheckAnnotation {
    override fun isSameTabe(): Boolean {
        return property.findAnnotation<SameTable>() != null
    }

    override fun manyToOne(): ManyToOne {
        return property.findAnnotation() ?: getManyToOne()
    }
}

internal fun <T : Any> KClass<T>.isNoMapAndNoListAndNoSet() = this != List::class && this != Map::class && this != Set::class

internal fun <T : Any> KClass<T>.toFieldData(persisterProvider: PersisterProvider,
                                             checkAnnotation: CheckAnnotation,
                                             prefix: String?,
                                             persister: Persister): FieldData<Any, Any, T, Any> {
    return when {
        this.java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(SimpleTypeProperty(this,
                                                                                      this.simpleName!!),
                                                                   prefix) as FieldData<Any, Any, T, Any>
        this.isEnum() -> EnumType(SimpleTypeProperty(this, this.simpleName!!), prefix) as FieldData<Any, Any, T, Any>
        checkAnnotation.isSameTabe() -> {
            val propertyData = SimpleTypeProperty(this, this.simpleName!!)
            ComplexSameTableType(propertyData, persisterProvider, prefix, null, persister) as FieldData<Any, Any, T, Any>
        }
        this.isNoMapAndNoListAndNoSet() -> ReadComplexType(SimpleTypeProperty(this, this.simpleName!!),
                                                           moreKeys(),
                                                           persisterProvider,
                                                           checkAnnotation.manyToOne(),
                                                           persister, prefix) as FieldData<Any, Any, T, Any>
        else -> {
            throw RuntimeException("this: $this not possible")
        }
    }
}

internal class FieldDataFactory<R : Any> constructor(val persisterProvider: PersisterProvider,
                                                     val clazz: KClass<R>,
                                                     val prefix: String?,
                                                     val parentTableName: String?,
                                                     val persister: Persister) {
    val moreKeys = clazz.findAnnotation<MoreKeys>()
            .default(1)

    inner class Builder(val idField: KeyType?,
                        val keyFields: List<FieldData<R, *, *, *>> = emptyList(),
                        val fields: List<FieldData<R, *, *, *>> = emptyList()) {
        fun next(i: Int, constructor: KFunction<R>): Builder {
            if (i < constructor.parameters.size) {
                val parameter = constructor.parameters[i]
                val y = clazz.declaredMemberProperties.find { it.name == parameter.name } as KProperty1<R, out Any>
                val c: FieldData<R, *, *, *>
                val keyField: FieldData<R, *, *, *>?
                when {
                    i < moreKeys.amount && moreKeys.auto -> {
                        c = createKeyDependent(y)
                        keyField = c
                    }
                    i < moreKeys.amount -> {
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
                    i == moreKeys.amount - 1 && idField == null -> {
                        KeyType(nextKeyFields as List<FieldData<Any, Any, Any, Any>>)
                    }
                    i >= moreKeys.amount || idField != null -> idField
                    else -> null
                }
                return Builder(idField, nextKeyFields, fields).next(i + 1, constructor)
            }

            return if (moreKeys.auto) {
                val keyHashCodeable = clazz.createHashCodeableKey(HashCodeableProvider())
                val autoIdField = KeyType(listOf(AutoKeyType(AutoKeyProperty(keyHashCodeable),
                                                             prefix)) as List<FieldData<Any, Any, Any, Any>>)
                val keyType = KeyType(autoIdField!!.fields, keyHashCodeable, idField)
                val fields = (listOf(keyType) + fields) as List<FieldData<R, *, *, *>>
                fields.forEach { it.onIdField(keyType) }
                Builder(keyType, fields, fields)
            } else {
                fields.forEach { it.onIdField(idField!!) }
                this
            }
        }

        fun create(property: KProperty1<R, out Any>): FieldData<R, *, *, *>? {
            return when {
                property.toKClass().java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(DefProperty(property,
                                                                                                      clazz), prefix)
                property.toKClass().isEnum() -> EnumType(DefProperty(property as KProperty1<R, Enum<*>>,
                                                                     clazz), prefix)
                property.findAnnotation<SameTable>() != null -> {
                    val propertyData = DefProperty(property, clazz)
                    ComplexSameTableType(propertyData, persisterProvider, prefix, parentTableName!!, persister)
                }
                (property.returnType.classifier as KClass<out Any>).isNoMapAndNoListAndNoSet() ->
                    ReadComplexType(DefProperty(property, clazz),
                                    moreKeys,
                                    persisterProvider,
                                    property.findAnnotation<ManyToOne>().default(ManyToOne::class, ""),
                                    persister, prefix)
                else -> {
                    null
                }
            }
        }

        fun createKeyDependent(property: KProperty1<R, out Any>): FieldData<R, *, *, *> {
            val simple = create(property)
            if (simple != null) {
                return simple
            }
            return when {
                property.returnType.classifier as KClass<out Any> == Map::class -> {
                    MapType(DefaultMapProperty(property as KProperty1<R, Map<Any, out Any>>, clazz),
                            persisterProvider,
                            prefix,
                            persister,
                            parentTableName!!)
                }
                property.returnType.classifier as KClass<out Any> == List::class -> {
                    ListType(ListMapProperty(property as KProperty1<R, List<out Any>>, clazz),
                             persisterProvider,
                             prefix,
                             persister,
                             parentTableName!!)
                }

                property.returnType.classifier as KClass<out Any> == Set::class -> {
                    SetType(SetProperty(property as KProperty1<R, Set<out Any>>, clazz),
                            persisterProvider,
                            property.findAnnotation() ?: ManyList::class.constructors.first().call(""),
                            persister,
                            prefix)
                }
                else -> {
                    throw RuntimeException("unknown type : ${property.returnType}")
                }
            }
        }
    }

    private fun MoreKeys.createAutoKey(constructor: KFunction<R>): Builder {
        return Builder(null, emptyList()).next(0, constructor)
    }

    fun fieldsRead(): Builder {
        if (clazz.java.isPrimitiveOrWrapperOrString()) {
            val key = KeyType(listOf(ReadSimpleType(SimpleTypeProperty(clazz, clazz.tableName()),
                                                    prefix)) as List<FieldData<Any, Any, Any, Any>>)
            val fields = listOf(key) as List<FieldData<R, *, *, *>>
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
                                                   moreKeys: MoreKeys = this.findAnnotation<MoreKeys>().default(1)) =
        createHashCodeables(provider, moreKeys, moreKeys.amount).key()

internal class HashCodeableHandler(val moreKeys: MoreKeys, val list: List<HashCodeable<out Any>>) {
    fun key() = KeyHashCodeable(list.take(moreKeys.amount))
}

internal fun KClass<out Any>.createHashCodeables(provider: HashCodeableProvider,
                                                 moreKeys: MoreKeys = this.findAnnotation<MoreKeys>().default(1),
                                                 maxSize: Int = moreKeys.amount,
                                                 i: Int = 0,
                                                 constructor: KFunction<Any> = this.primaryConstructor ?: run {
                                                     throw RuntimeException("clazz $this has no primary constructor")
                                                 },
                                                 ret: List<HashCodeable<out Any>> = emptyList()): HashCodeableHandler {
    if (i < constructor.parameters.size && (maxSize != -1 || i < maxSize)) {
        val parameter = constructor.parameters[i]
        val property = this.declaredMemberProperties.find { it.name == parameter.name }
        val hashCodeable = property!!.createHashCodeable(provider)
        return createHashCodeables(provider, moreKeys, maxSize, i + 1, constructor, ret + hashCodeable)
    }
    return HashCodeableHandler(moreKeys, ret)

}

internal fun KProperty1<*, *>.createHashCodeable(provider: HashCodeableProvider): HashCodeable<out Any> {
    return when {
        this.toKClass().java.isPrimitiveOrWrapperOrString() -> SimpleHashCodeable(DefaultProperyReader(this as KProperty1<Any, Any>))
        this.toKClass().isEnum() -> EnumHashCodeable
        this.findAnnotation<SameTable>() != null ->
            ComplexSameTableHashCodeable((this.returnType.classifier as KClass<Any>).createHashCodeables(provider).list,
                                         DefaultProperyReader(this as KProperty1<Any, Any>))

        (this.returnType.classifier as KClass<Any>).isNoMapAndNoListAndNoSet() ->
            ComplexHashCodeable((this.returnType.classifier as KClass<Any>), provider,
                                DefaultProperyReader(this as KProperty1<Any, Any>))
        this.returnType.classifier as KClass<Any> == Map::class -> {
            val mapReadable = MapReadable(this as KProperty1<Any, Map<Any, Any>>)
            MapHashCodeable(mapReadable.keyClazz.createHashCodeables(provider).key(),
                            mapReadable.clazz.createHashCodeables(provider).key(),
                            mapReadable)
        }
        this.returnType.classifier as KClass<Any> == List::class -> {
            ListHashCodeable((this as KProperty1<Any, List<Any>>).keyHashCodeable(provider), ListReadable(this))
        }
        this.returnType.classifier as KClass<Any> == Set::class -> {
            this as KProperty1<Any, Set<Any>>
            SetHashCodeable(this.keyHashCodeable(provider), DefaultProperyReader(this))
        }
        else -> {
            throw RuntimeException("unknown type : ${this.returnType}")
        }
    }
}

internal fun KClass<Any>.createHashCodeable(provider: HashCodeableProvider): HashCodeable<out Any> {
    return when {
        this.java.isPrimitiveOrWrapperOrString() -> SimpleHashCodeable(SimpleTypeReadable)
        this.isEnum() -> EnumHashCodeable
        this.isNoMapAndNoListAndNoSet() ->
            ComplexHashCodeable(this, provider, SimpleTypeReadable)
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

internal fun <R : Any, S : Any> KProperty1<R, S>.keyHashCodeable(provider: HashCodeableProvider): HashCodeable<Any> {
    return (returnType.arguments.first().type!!.classifier as KClass<Any>).createHashCodeable(provider) as HashCodeable<Any>
}


internal data class ProviderKey(val propertyData: PropertyData<*, *, *>, val prefixedName: String)
internal data class ProviderValue(val readPersisterData: ReadPersisterData<*, *>, val table: Persister.Table<*>)

internal interface PersisterProvider {
    fun readPersisterData(providerKey: ProviderKey): ReadPersisterData<*, *>
    fun register(providerKey: ProviderKey, tableName: String)
    fun table(providerKey: ProviderKey): Persister.Table<*>
}

internal class PersisterProviderImpl(val persister: Persister) : PersisterProvider {
    private val map: MutableMap<ProviderKey, ProviderValue> = mutableMapOf()
    private val registeredSet: MutableSet<ProviderKey> = mutableSetOf()

    override fun readPersisterData(providerKey: ProviderKey): ReadPersisterData<*, *> {
        return map[providerKey]!!.readPersisterData
    }

    override fun register(providerKey: ProviderKey, tableName: String) {
        if (!registeredSet.contains(providerKey)) {
            registeredSet.add(providerKey)
            val r = ReadPersisterData<Any, Any>(providerKey.propertyData.clazz as KClass<Any>,
                                                persister,
                                                this,
                                                prefix = providerKey.prefixedName,
                                                parentTableName = tableName)
            val table = persister.Table(providerKey.propertyData.clazz, tableName, null, this)
            map[providerKey] = ProviderValue(r, table)
        }
    }

    override fun table(providerKey: ProviderKey): Persister.Table<*> {
        return map[providerKey]!!.table
    }
}

internal class HashCodeableProvider {
    private val map: MutableMap<KClass<Any>, KeyHashCodeable> = mutableMapOf()
    private val registeredSet: MutableSet<KClass<Any>> = mutableSetOf()

    fun hashCodeable(clazz: KClass<Any>): KeyHashCodeable {
        return map[clazz]!!
    }

    fun register(clazz: KClass<Any>) {
        if (!registeredSet.contains(clazz)) {
            registeredSet.add(clazz)
            map[clazz] = clazz.createHashCodeables(this)
                    .key()
        }
    }
}