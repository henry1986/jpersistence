package org.daiv.reflection.persister

import mu.KotlinLogging
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.ObjectKeyToWrite
import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.insertHeadString
import org.daiv.reflection.read.insertValueString

internal data class InsertKey constructor(val tableName: String, val key: ObjectKey)

internal data class InsertRequest(val insertObjects: List<InsertObject>)

/**
 * if [checkCacheOnly] is activated, before inserting it is only checked, if the value is already in the readCache.
 * if not, the database is read
 */
data class InsertCachePreference(val checkCacheOnly: Boolean)

internal data class InsertMap constructor(val persister: Persister,
                                          val insertCachePreference: InsertCachePreference,
                                          val actors: Map<String, TableHandler>,
                                          val readCache: ReadCache) {
    private val logger = KotlinLogging.logger {}

    private fun insertListBy(insertObjects: List<List<List<InsertObject>>>): String {
        val headString = insertObjects.first()
                .first()
                .insertHeadString()
        val valueString = insertObjects.flatten()
                .joinToString(separator = "), (") { it.insertValueString() }
        return "($headString ) VALUES ($valueString);"
    }


    private fun <T : Any> T.checkDBValue(objectValue: T, key: ObjectKey): T {
        if (this != objectValue) {
            val firstTryMsg = "values are not the same -> " +
                    "databaseValue:       $this \n vs manyToOne Value: $objectValue"
            val msg = if (firstTryMsg.length > 1000) {
                "values of class ${this::class} are not fitting. Object is too big to print - key: $key"
            } else {
                firstTryMsg
            }
            throw RuntimeException(msg)
        }
        return objectValue
    }

    private fun<T:Any> readHashCode(table: Persister.Table<T>, key: ObjectKeyToWrite, obj: T){

    }

    suspend fun <T : Any> nextTask(table: Persister.Table<T>, key: ObjectKeyToWrite, obj: T, nextTask: suspend () -> Unit) {
        val objectKey = key.toObjectKey(0)
        if (insertCachePreference.checkCacheOnly) {
            if (readCache.isInCache(table, objectKey)) {
                return
            }
        } else {
            val read = readCache.read(table, objectKey)
            if (read != null) {
                read.checkDBValue(obj, objectKey)
                return
            }
        }
        nextTask()
    }

    suspend fun toBuild(requestTask: RequestTask) {
        val actor = actors[requestTask.insertKey.tableName]!!
        actor.send(requestTask)

    }

    fun insertAll() {
//        val map = mutableMapOf<InsertKey, InsertRequest>()
        val x = actors.map { it.key to it.value.map.map { it } }
        logger.trace { "size of map: ${actors.map { it.value.map.size }.sum()}" }
        logger.trace { "size of maps: ${actors.map { it.key to it.value.map.size }}" }
//        val group = map.map { it }
//                .groupBy { it.key.tableName }
        val res = x.map {
            it.first to it.second.map {
                it.value()
                        .map { it.insertObjects }
            }.filter { it.isNotEmpty() }
        }
                .filter { !it.second.isEmpty() }
                .map { "INSERT INTO `${it.first}` ${insertListBy(it.second)} " }

//                .toMap()
        logger.trace { "done converting" }
        res.map { persister.write(it) }
    }
}

internal interface PersisterListener {
    fun onDelete(tableName: String, key: List<Any>)

    fun onDelete(tableName: String)

    fun onClear(tableName: String)

    fun onUpdate(tableName: String)

    fun onUpdate(tableName: String, key: List<Any>)
}

internal class ReadTableCache(val tableName: String) {
    private val map: MutableMap<ObjectKey, Any> = mutableMapOf()
    fun containsKey(insertKey: InsertKey): Boolean {
        return map.containsKey(insertKey.key)
    }

    operator fun get(insertKey: InsertKey): Any? {
        return map[insertKey.key]
    }

    operator fun set(insertKey: InsertKey, any: Any) {
        map[insertKey.key] = any
    }
}


internal class ReadCache(val persisterPreference: PersisterPreference) : PersisterListener {
    private val logger = KotlinLogging.logger {}
    private val map: MutableMap<String, ReadTableCache> = mutableMapOf()

    fun <T : Any> read(table: Persister.Table<T>, key: ObjectKey): T? {
        val readCacheKey = InsertKey(table.tableName, key)
        val tableCache = if (!map.containsKey(readCacheKey.tableName)) {
            val tableCache = ReadTableCache(readCacheKey.tableName)
            map[readCacheKey.tableName] = tableCache
            tableCache
        } else {
            map[readCacheKey.tableName]!!
        }
        if (!tableCache.containsKey(readCacheKey)) {
            val read = table.innerReadMultipleUseHashCode(key, this)
            if (read != null) {
                if (persisterPreference.useCache && map.size > persisterPreference.clearCacheAfterNumberOfStoredObjects) {
                    logger.info { "clearing cache, because more than ${persisterPreference.clearCacheAfterNumberOfStoredObjects} are stored (${map.size}" }
                    map.clear()
                }
                tableCache[readCacheKey] = read
                return read
            } else {
                return null
            }
        } else {
            return tableCache[readCacheKey]!! as T
        }
    }

    fun <T : Any> isInCache(table: Persister.Table<T>, key: ObjectKey): Boolean {
        val readCacheKey = InsertKey(table._tableName, key)
        return map[readCacheKey.tableName]?.get(readCacheKey) != null
    }

    fun readTableCache(tableName: String): ReadTableCache {
        if (!map.containsKey(tableName)) {
            val r = ReadTableCache(tableName)
            map[tableName] = r
            return r
        }
        return map[tableName]!!
    }

//    operator fun set(insertKey: InsertKey, obj: Any) {
//        if (map.containsKey(insertKey)) {
//            throw RuntimeException("key: ${insertKey.key} already exists in ${insertKey.tableName}")
//        }
//        map[insertKey] = obj
//    }

    fun <T : Any> readNoNull(table: Persister.Table<T>, key: ObjectKey): T {
        return read(table, key)
                ?: throw RuntimeException("did not find value for key $key in ${table._tableName}")
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
