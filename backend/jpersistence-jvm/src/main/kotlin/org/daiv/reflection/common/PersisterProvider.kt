package org.daiv.reflection.common

import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.KeyHashCodeable
import org.daiv.reflection.read.ReadPersisterData
import kotlin.reflect.KClass

internal data class ProviderKey(val propertyData: PropertyData, val prefixedName: String)
internal data class ProviderValue(val readPersisterData: ReadPersisterData, val table: Persister.Table<*>)


internal interface PersisterProvider {
    fun readPersisterData(providerKey: ProviderKey): ReadPersisterData
    fun register(providerKey: ProviderKey)
    fun innerTableName(clazz: KClass<out Any>): String
    fun tableName(clazz: KClass<out Any>): String
    operator fun get(clazz: KClass<out Any>): String {
        return tableName(clazz)
    }

    fun table(providerKey: ProviderKey): Persister.Table<*>

    fun tableNamesIncludingPrefix(): List<String>

    fun rename(clazz: KClass<out Any>, newTableName: String)
    fun registerHelperTableName(helperTableName: String)
    fun exists(clazz: KClass<out Any>): Boolean

    operator fun set(clazz: KClass<out Any>, newTableName: String) {
        rename(clazz, newTableName)
    }
    fun remove(clazz: KClass<out Any>)

    fun setIfNotExists(clazz: KClass<out Any>, newTableName: String) {
        if (!exists(clazz)) {
            set(clazz, newTableName)
        }
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
    private val helperTableNames = mutableSetOf<String>()

    override fun registerHelperTableName(helperTableName: String) {
        helperTableNames.add(helperTableName)
    }

    override fun readPersisterData(providerKey: ProviderKey): ReadPersisterData {
        return map[providerKey]!!.readPersisterData
    }

    override fun innerTableName(clazz: KClass<out Any>): String {
        return tableNames[clazz.java.name] ?: clazz.simpleName!!
    }

    private fun toPrefixedName(tableName: String) = tableNamePrefix?.let { "${it}_$tableName" } ?: tableName

    override fun tableNamesIncludingPrefix(): List<String> {
        return tableNames.map { toPrefixedName(it.value) } + helperTableNames
    }

    override fun tableName(clazz: KClass<out Any>): String {
        val mapName = innerTableName(clazz)
        return toPrefixedName(mapName)
    }

    override fun rename(clazz: KClass<out Any>, newTableName: String) {
        tableNames[clazz.java.name] = newTableName
    }

    override fun remove(clazz: KClass<out Any>){
        tableNames.remove(clazz.java.name)
    }

    override fun exists(clazz: KClass<out Any>): Boolean {
        return tableNames.containsKey(clazz.java.name)
    }

    override fun register(providerKey: ProviderKey) {
        if (!registeredSet.contains(providerKey)) {
            registeredSet.add(providerKey)
            val r = ReadPersisterData(providerKey.propertyData.clazz,
                                      persister,
                                      this,
                                      prefix = providerKey.prefixedName)
            val table = persister.Table(providerKey.propertyData.clazz, this)
            map[providerKey] = ProviderValue(r, table)
            tableNames[providerKey.propertyData.clazz.java.name] = providerKey.propertyData.clazz.simpleName!!
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