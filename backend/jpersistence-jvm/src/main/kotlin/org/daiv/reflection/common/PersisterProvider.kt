package org.daiv.reflection.common

import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.KeyHashCodeable
import org.daiv.reflection.read.ReadPersisterData
import kotlin.reflect.KClass

internal data class ProviderKey(val propertyData: PropertyData<*, *, *>, val prefixedName: String)
internal data class ProviderValue(val readPersisterData: ReadPersisterData<*, *>, val table: Persister.Table<*>)


internal interface PersisterProvider {
    fun readPersisterData(providerKey: ProviderKey): ReadPersisterData<*, *>
    fun register(providerKey: ProviderKey)
    fun tableName(clazz: KClass<out Any>): String
    operator fun get(clazz: KClass<out Any>): String {
        return tableName(clazz)
    }

    fun table(providerKey: ProviderKey): Persister.Table<*>

    fun rename(clazz: KClass<out Any>, newTableName: String)

    operator fun set(clazz: KClass<out Any>, newTableName: String) {
        rename(clazz, newTableName)
    }
}

internal class PersisterProviderImpl(val persister: Persister,
                                     _tableNames: Map<KClass<out Any>, String>,
                                     val tableNamePrefix: String?) : PersisterProvider {
    private val map: MutableMap<ProviderKey, ProviderValue> = mutableMapOf()
    private val registeredSet: MutableSet<ProviderKey> = mutableSetOf()
    private val tableNames: MutableMap<String, String> = _tableNames.map { it.key.java.name to it.value }
            .toMap()
            .toMutableMap()

    override fun readPersisterData(providerKey: ProviderKey): ReadPersisterData<*, *> {
        return map[providerKey]!!.readPersisterData
    }

    override fun tableName(clazz: KClass<out Any>): String {
        val mapName = tableNames[clazz.java.name] ?: clazz.simpleName!!
        return tableNamePrefix?.let { "${it}_$mapName" } ?: mapName
    }

    override fun rename(clazz: KClass<out Any>, newTableName: String) {
        tableNames[clazz.java.name] = newTableName
    }

    override fun register(providerKey: ProviderKey) {
        if (!registeredSet.contains(providerKey)) {
            registeredSet.add(providerKey)
            val r = ReadPersisterData<Any, Any>(providerKey.propertyData.clazz as KClass<Any>,
                                                persister,
                                                this,
                                                prefix = providerKey.prefixedName)
            val table = persister.Table(providerKey.propertyData.clazz, this)
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