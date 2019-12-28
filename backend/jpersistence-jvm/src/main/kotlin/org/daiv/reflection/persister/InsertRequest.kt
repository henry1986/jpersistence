package org.daiv.reflection.persister

import mu.KotlinLogging
import org.daiv.reflection.plain.HashCodeKey
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
                                          val readCache: ReadCache) : KeyCreator {
    private val logger = KotlinLogging.logger {}

    override val checkCacheOnly: Boolean
        get() = insertCachePreference.checkCacheOnly

    override fun toObjectKey(table: Persister.Table<*>, keyToWrite: ObjectKeyToWrite): ObjectKey {
        return readCache.toObjectKey(table, keyToWrite, this)
    }

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


    suspend fun <T : Any> nextTask(table: Persister.Table<T>, key: ObjectKeyToWrite, nextTask: suspend (ObjectKey) -> Unit) {
        val objectKey = readCache.toObjectKey(table, key, this)
        if (insertCachePreference.checkCacheOnly) {
            if (readCache.isInCache(table, objectKey)) {
                return
            }
        } else {
            val read = readCache.read(table, objectKey, this)
            if (read != null) {
                read.checkDBValue(key.theObject(), objectKey)
                return
            }
        }
        nextTask(objectKey)
    }

    suspend fun toBuild(requestTask: RequestTask) {
        val actor = actors[requestTask.insertKey.tableName]
                ?: throw RuntimeException("actor for ${requestTask.insertKey} not found")
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

internal class ReadHashCodeTableCache(val tableName: String) : ReadTableCache {
    private val map: MutableMap<Int, List<Any>> = mutableMapOf()

    private fun InsertKey.hashCodeKey(): HashCodeKey {
        if (this.key is HashCodeKey) {
            return this.key
        } else {
            throw RuntimeException("this is not a HashCodeKey: $this")
        }
    }

    override fun containsKey(insertKey: InsertKey): Boolean {
        val key = insertKey.hashCodeKey()
        return map[key.hashCodeX]?.size?.let { it > key.hashCodeCounter } ?: false
    }

    fun containsHashCode(hashCodeX: Int): Boolean {
        return map.containsKey(hashCodeX)
    }

    fun getHashCodeObjects(hashCodeX: Int) = map[hashCodeX]

    override operator fun get(insertKey: InsertKey): Any? {
        val key = insertKey.hashCodeKey()
        return map[key.hashCodeX]?.getOrNull(key.hashCodeCounter)
    }

    override operator fun set(insertKey: InsertKey, any: Any) {
        val key = insertKey.hashCodeKey()
        val list = if (map.containsKey(key.hashCodeX)) {
            map[key.hashCodeX]!! + any
        } else {
            listOf(any)
        }
        map[key.hashCodeX] = list
    }
}

internal interface ReadTableCache {
    fun containsKey(insertKey: InsertKey): Boolean

    operator fun get(insertKey: InsertKey): Any?

    operator fun set(insertKey: InsertKey, any: Any)
}

internal class ReadTableCacheImpl(val tableName: String) : ReadTableCache {
    private val map: MutableMap<ObjectKey, Any> = mutableMapOf()
    override fun containsKey(insertKey: InsertKey): Boolean {
        return map.containsKey(insertKey.key)
    }

    override operator fun get(insertKey: InsertKey): Any? {
        return map[insertKey.key]
    }

    override operator fun set(insertKey: InsertKey, any: Any) {
        map[insertKey.key] = any
    }
}

internal interface KeyCreator {
    val checkCacheOnly: Boolean
    fun toObjectKey(table: Persister.Table<*>, key: ObjectKeyToWrite): ObjectKey
}


internal class ReadCache(val persisterPreference: PersisterPreference) : PersisterListener {
    private val logger = KotlinLogging.logger {}
    private val map: MutableMap<String, ReadTableCache> = mutableMapOf()

    private fun readDatabase(table: Persister.Table<*>,
                             key: ObjectKeyToWrite,
                             readTableCache: ReadHashCodeTableCache,
                             keyCreator: KeyCreator,
                             hashCodeX: Int = key.itsHashCode()): ObjectKey {
        val counter = if (keyCreator.checkCacheOnly) {
            0
        } else {
            val readHashCode = table.readHashCode(hashCodeX, this, keyCreator)
            readHashCode.forEach {
                readTableCache[InsertKey(table._tableName, key.toObjectKey(it.first))] = it.second
            }
            readHashCode.find { it.second == key.theObject() }?.first ?: readHashCode.size
        }
        val objectKey = key.toObjectKey(counter)
        readTableCache[InsertKey(readTableCache.tableName, objectKey)] = key.theObject()
        return objectKey
    }

    fun <T : Any> toObjectKey(table: Persister.Table<T>, key: ObjectKeyToWrite, keyCreator: KeyCreator): ObjectKey {
        return if (key.isAutoId()) {
            val hashCodeX = key.itsHashCode()
            val readTableCache = readTableCache(table, true) as ReadHashCodeTableCache
            if (readTableCache.containsHashCode(hashCodeX)) {
                val objects = readTableCache.getHashCodeObjects(hashCodeX)!!
                key.toObjectKey(objects.withIndex().find { it.value == key.theObject() }?.index ?: objects.size)
            } else {
                readDatabase(table, key, readTableCache, keyCreator, hashCodeX)
            }
        } else {
            key.toObjectKey()
        }
    }

    fun <T : Any> read(table: Persister.Table<T>, key: ObjectKey, keyCreator: KeyCreator): T? {
        val readCacheKey = InsertKey(table.tableName, key)
        val tableCache = if (!map.containsKey(readCacheKey.tableName)) {
            val tableCache = if (readCacheKey.key.isAutoId())
                ReadHashCodeTableCache(readCacheKey.tableName)
            else
                ReadTableCacheImpl(readCacheKey.tableName)
            map[readCacheKey.tableName] = tableCache
            tableCache
        } else {
            map[readCacheKey.tableName]!!
        }
        if (!tableCache.containsKey(readCacheKey)) {
            val read = table.innerReadMultipleUseHashCode(key, this, keyCreator)
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

    fun readTableCache(tableName: String, isHashCodeTable: Boolean): ReadTableCache {
        if (!map.containsKey(tableName)) {
            val r = if (isHashCodeTable) ReadHashCodeTableCache(tableName) else ReadTableCacheImpl(tableName)
            map[tableName] = r
            return r
        }
        return map[tableName]!!
    }

    fun <T : Any> readTableCache(table: Persister.Table<T>, isHashCodeTable: Boolean): ReadTableCache {
        return readTableCache(table._tableName, isHashCodeTable)
    }

//    operator fun set(insertKey: InsertKey, obj: Any) {
//        if (map.containsKey(insertKey)) {
//            throw RuntimeException("key: ${insertKey.key} already exists in ${insertKey.tableName}")
//        }
//        map[insertKey] = obj
//    }

    fun <T : Any> readNoNull(table: Persister.Table<T>, key: ObjectKey): T {
        return read(table, key, allCheck)
                ?: throw RuntimeException("did not find value for key $key in ${table._tableName}")
    }

    fun clear() = map.clear()

    override fun onDelete(tableName: String, key: List<Any>) {
        clear()
    }

    override fun onClear(tableName: String) {
        clear()
    }

    override fun onDelete(tableName: String) {
        clear()
    }

    override fun onUpdate(tableName: String) {
        clear()
    }

    override fun onUpdate(tableName: String, key: List<Any>) {
        clear()
    }

    val allCheck = object : KeyCreator {
        override val checkCacheOnly = false

        override fun toObjectKey(table: Persister.Table<*>, key: ObjectKeyToWrite): ObjectKey {
            return toObjectKey(table, key, this)
        }
    }
}

//internal data class ReadMap(){
//    private val map: MutableMap<InsertKey, Any> = mutableMapOf()
//
//    fun read(key:Any)
//}
