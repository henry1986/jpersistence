package org.daiv.reflection.persister

import mu.KotlinLogging
import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.insertHeadString
import org.daiv.reflection.read.insertValueString

internal data class InsertKey constructor(val tableName: String, val key: List<Any>)

internal data class InsertRequest(val insertObjects: List<InsertObject>)

internal data class InsertMap constructor(val persister: Persister, val readCache: ReadCache) {
    private val map: MutableMap<InsertKey, InsertRequest> = mutableMapOf()

    private fun insertListBy(insertObjects: List<List<InsertObject>>): String {
        val headString = insertObjects.first()
                .insertHeadString()
        val valueString = insertObjects.joinToString(separator = "), (") { it.insertValueString() }
        return "($headString ) VALUES ($valueString);"
    }

    fun exists(insertKey: InsertKey) = map.containsKey(insertKey)

    fun put(insertKey: InsertKey, insertRequest: InsertRequest) {
        map[insertKey] = insertRequest
    }

    fun put(insertKey: InsertKey, insertRequest: InsertRequest, obj: Any) {
        map[insertKey] = insertRequest
        readCache[insertKey] = obj
    }

    fun insertAll() {
        val group = map.map { it }
                .groupBy { it.key.tableName }
        val res = group.map { it.key to it.value.map { it.value.insertObjects }.filter { it.isNotEmpty() } }
                .filter { !it.second.isEmpty() }
                .toMap()
        res.map { persister.write("INSERT INTO ${it.key} ${insertListBy(it.value)} ") }
    }
}

internal interface PersisterListener {
    fun onDelete(tableName: String, key: List<Any>)

    fun onDelete(tableName: String)

    fun onClear(tableName: String)

    fun onUpdate(tableName: String)

    fun onUpdate(tableName: String, key: List<Any>)
}

internal class ReadCache(val persisterPreference: PersisterPreference) : PersisterListener {
    private val logger = KotlinLogging.logger {}
    private val map: MutableMap<InsertKey, Any> = mutableMapOf()

    fun <T : Any> read(table: Persister.Table<T>, key: List<Any>): T? {
        val readCacheKey = InsertKey(table._tableName, key)
        val got = map[readCacheKey]
        return if (got == null) {
            val read = table.innerReadMultipleUseHashCode(key, this)
            if (read != null) {
                if (persisterPreference.useCache && map.size > persisterPreference.clearCacheAfterNumberOfStoredObjects) {
                    logger.info { "clearing cache, because more than ${persisterPreference.clearCacheAfterNumberOfStoredObjects} are stored (${map.size}" }
                    map.clear()
                }
                map[readCacheKey] = read
            }
            read
        } else {
            got as T
        }
    }

    operator fun set(insertKey: InsertKey, obj: Any) {
        if (map.containsKey(insertKey)) {
            throw RuntimeException("key: ${insertKey.key} already exists in ${insertKey.tableName}")
        }
        map[insertKey] = obj
    }

    fun <T : Any> readNoNull(table: Persister.Table<T>, key: List<Any>): T {
        return read(table, key) ?: throw RuntimeException("did not find value for key $key")
    }

    override fun onDelete(tableName: String, key: List<Any>) {
        map.clear()
    }

    override fun onClear(tableName: String) {
        map.clear()
    }

    override fun onDelete(tableName: String) {
        map.clear()
    }

    override fun onUpdate(tableName: String) {
        map.clear()
    }

    override fun onUpdate(tableName: String, key: List<Any>) {
        map.clear()
    }
}

//internal data class ReadMap(){
//    private val map: MutableMap<InsertKey, Any> = mutableMapOf()
//
//    fun read(key:Any)
//}
