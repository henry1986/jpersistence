package org.daiv.reflection.common

import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.Table
import org.daiv.reflection.read.DefaultMapper
import org.daiv.reflection.read.KeyHashCodeable
import org.daiv.reflection.read.Mapper
import org.daiv.reflection.read.ReadPersisterData
import kotlin.reflect.KClass

internal data class ProviderKey constructor(val clazz: KClass<out Any>, val prefixedName: String)

internal interface TableHandlerCreator {
    fun allTables(): Map<KClass<out Any>, Persister.InternalTable>
    fun isAutoIdTable(clazz: KClass<out Any>): Boolean
    fun getHelperTableNames(): Collection<Persister.InternalTable>
}

internal interface PersisterProvider : TableHandlerCreator {
    fun readPersisterData(providerKey: ProviderKey): ReadPersisterData
    fun register(providerKey: ProviderKey)
    fun innerTableName(clazz: KClass<out Any>): String
    fun tableName(clazz: KClass<out Any>): String
    operator fun get(clazz: KClass<out Any>): String {
        return tableName(clazz)
    }

    fun mapProviderClazz(propertyData: OtherClassPropertyData): PropertyData

    fun map(any: Any?): Any?
    fun backmap(any: Any?): Any?

    fun table(clazz: KClass<out Any>): Table<*>

    fun rename(clazz: KClass<out Any>, newTableName: String)
    fun registerHelperTableName(helperTableName: Persister.InternalTable)
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

    fun setAutoTableId(clazz: KClass<out Any>, autoId: Boolean, table: Table<*>)
}

internal class PersisterProviderImpl constructor(val persister: Persister, val tableNamePrefix: String?) : PersisterProvider {
    private val map: MutableMap<ProviderKey, ReadPersisterData> = mutableMapOf()
    private val tableMap: MutableMap<KClass<out Any>, Table<*>> = mutableMapOf()
    private val isAutoKey: MutableMap<KClass<out Any>, Boolean> = mutableMapOf()
    private val registeredSet: MutableSet<ProviderKey> = mutableSetOf()
    private val tableNames: MutableMap<KClass<out Any>, String> = mutableMapOf()
    //            = _tableNames.map { it.key.java.name to it.value }
//            .toMap()
//            .toMutableMap()
    private val helperTableNames = mutableSetOf<Persister.InternalTable>()

    override fun getHelperTableNames() = helperTableNames

    override fun mapProviderClazz(propertyData: OtherClassPropertyData): PropertyData {
        val otherClazz = persister.clazzMapper[propertyData.clazz]
        if (otherClazz == null) {
            return propertyData
        } else {
            return propertyData.otherClazz(otherClazz as KClass<Any>)
        }
    }

    override fun registerHelperTableName(helperTableName: Persister.InternalTable) {
        helperTableNames.add(helperTableName)
    }

    override fun setAutoTableId(clazz: KClass<out Any>, autoId: Boolean, table: Table<*>) {
        isAutoKey[clazz] = autoId
        tableMap[clazz] = table
    }

    private fun mapper(clazz: KClass<*>): Mapper<Any, Any> {
        return (persister.mapper[clazz] ?: DefaultMapper(clazz)) as Mapper<Any, Any>
    }

    private fun backmapper(clazz: KClass<*>): Mapper<Any, Any> {
        return (persister.backmapper[clazz] ?: DefaultMapper(clazz)) as Mapper<Any, Any>
    }

    override fun map(any: Any?): Any? {
        any ?: return null
        return mapper(any::class).map(any)
    }

    override fun backmap(any: Any?): Any? {
        any ?: return null
        return backmapper(any::class).backwards(any)
    }

    override fun isAutoIdTable(clazz: KClass<out Any>): Boolean {
        return isAutoKey[clazz]
                ?: throw RuntimeException("don't know about $clazz")
    }

    override fun readPersisterData(providerKey: ProviderKey): ReadPersisterData {
        return map[providerKey]!!
    }

    override fun innerTableName(clazz: KClass<out Any>): String {
        return tableNames[clazz] ?: clazz.simpleName!!
    }

    private fun toPrefixedName(tableName: String) = tableNamePrefix?.let { "${it}_$tableName" } ?: tableName

    override fun allTables(): Map<KClass<out Any>, Persister.InternalTable> {
        return tableMap
    }

    override fun tableName(clazz: KClass<out Any>): String {
        val mapName = innerTableName(clazz)
        return toPrefixedName(mapName)
    }

    override fun rename(clazz: KClass<out Any>, newTableName: String) {
        tableNames[clazz] = newTableName
    }

    override fun remove(clazz: KClass<out Any>) {
        tableNames.remove(clazz)
    }

    override fun exists(clazz: KClass<out Any>): Boolean {
        return tableNames.containsKey(clazz)
    }

    override fun register(providerKey: ProviderKey) {
        if (!registeredSet.contains(providerKey)) {
            registeredSet.add(providerKey)
            val r = ReadPersisterData(providerKey.clazz, persister, this, prefix = providerKey.prefixedName)
            val table = persister.Table(providerKey.clazz, this)
            map[providerKey] = r
            tableMap[providerKey.clazz] = table
            tableNames[providerKey.clazz] = providerKey.clazz.simpleName!!
            isAutoKey[providerKey.clazz] = r.key.isAuto()
        }
    }

    override fun table(clazz: KClass<out Any>): Table<*> {
        return tableMap[clazz]
                ?: throw RuntimeException("table for $clazz not found")
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