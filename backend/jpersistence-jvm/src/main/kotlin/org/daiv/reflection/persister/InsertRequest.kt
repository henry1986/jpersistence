package org.daiv.reflection.persister

import mu.KotlinLogging
import org.daiv.reflection.plain.HashCodeKey
import org.daiv.reflection.plain.IsAutoId
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.ObjectKeyToWrite
import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.insertHeadString
import org.daiv.reflection.read.insertValueString

internal data class InsertKey constructor(val table: Persister.InternalTable, val key: ObjectKeyToWrite)
internal data class ReadKey constructor(val table: Persister.InternalTable, val key: ObjectKey)


internal data class InsertRequest(val insertObjects: List<InsertObject>)

/**
 * if [checkCacheOnly] is activated, before inserting it is only checked, if the value is already in the readCache.
 * if not, the database is read
 */
data class InsertCachePreference(val checkCacheOnly: Boolean)

internal data class InsertMap constructor(val persister: Persister,
                                          val insertCachePreference: InsertCachePreference,
                                          val actors: Map<Persister.InternalTable, TableHandler>,
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


    private fun <T : Any> T.checkDBValue(objectValue: T, key: ObjectKeyToWrite): T {
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


    suspend fun <T : Any> nextTask(table: Persister.Table<T>, key: ObjectKeyToWrite, nextTask: suspend (ObjectKeyToWrite) -> Unit) {
//        val objectKey = readCache.toObjectKey(table, key, this)
//        objectKey?.let {
        if (insertCachePreference.checkCacheOnly) {
            if (readCache.isInCache(table, key)) {
                return
            }
        } else {
            val read = readCache.read(table, key)
            if (read != null) {
                read.checkDBValue(key.theObject(), key)
                return
            }
        }
//        }
        nextTask(key)
    }

    suspend fun toBuild(requestTask: RequestTask) {
        val actor = actors[requestTask.insertKey.table]
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
                .map { "INSERT INTO `${it.first._tableName}` ${insertListBy(it.second)} " }

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

internal class ReadHashCodeTableCache(val table: Persister.InternalTable) : ReadTableCache {
    private val map: MutableMap<Int, MutableMap<Int, Any>> = mutableMapOf()

    private fun ReadKey.hashCodeKey(): HashCodeKey {
        if (this.key.isAutoId()) {
            return this.key as HashCodeKey
        } else {
            throw RuntimeException("this is not a HashCodeKey: $this")
        }
    }

    override fun containsObject(key: ObjectKeyToWrite): Boolean {
        if (containsHashCode(key.itsHashCode())) {
            return getHashCodeObjects(key.itsHashCode())!!.any { it == key.theObject() }
        }
        return false
    }


    override fun keyForObject(key: ObjectKeyToWrite): ObjectKey? {
        if (containsHashCode(key.itsHashCode())) {
            val x = getHashCodeObjects(key.itsHashCode())!!.toList()
                    .find { it.second == key.theObject() }
            return x?.let { key.toObjectKey(it.first) }
        } else {
            return null
        }
    }

    override fun containsKey(insertKey: ReadKey): Boolean {
        val key = insertKey.hashCodeKey()
        return map[key.hashCodeX]?.size?.let { it > key.hashCodeCounter } ?: false
    }

    private fun createCounter(hashCodeX: Int) = map[hashCodeX]?.size ?: 0

    override fun createReadKey(keyToWrite: ObjectKeyToWrite): ReadKey {
        val counter = createCounter(keyToWrite.itsHashCode())
        return ReadKey(table, keyToWrite.toObjectKey(counter))
    }

    private fun containsHashCode(hashCodeX: Int): Boolean {
        return map.containsKey(hashCodeX)
    }

    private fun getHashCodeObjects(hashCodeX: Int) = map[hashCodeX]

    override operator fun get(insertKey: ReadKey): Any? {
        val key = insertKey.hashCodeKey()
        return map[key.hashCodeX]?.get(key.hashCodeCounter)
    }

    override operator fun set(insertKey: ReadKey, any: Any) {
        if (insertKey.table != table) {
            throw RuntimeException("this key $insertKey is not fitting to $table")
        }
        val key = insertKey.hashCodeKey()
        if (map.containsKey(key.hashCodeX)) {
            map[key.hashCodeX]!![key.hashCodeCounter] = any
        } else {
            map[key.hashCodeX] = mutableMapOf(key.hashCodeCounter to any)
        }
    }
}

internal interface ReadTableCache {
    fun containsKey(insertKey: ReadKey): Boolean

    fun createReadKey(keyToWrite: ObjectKeyToWrite): ReadKey

    fun containsObject(key: ObjectKeyToWrite): Boolean
    fun keyForObject(key: ObjectKeyToWrite): ObjectKey?

    operator fun get(insertKey: ReadKey): Any?

    operator fun set(insertKey: ReadKey, any: Any)
}

internal class ReadTableCacheImpl(val table: Persister.InternalTable) : ReadTableCache {
    private val map: MutableMap<ObjectKey, Any> = mutableMapOf()

    override fun createReadKey(keyToWrite: ObjectKeyToWrite): ReadKey {
        return ReadKey(table, keyToWrite.toObjectKey())
    }

    override fun keyForObject(key: ObjectKeyToWrite): ObjectKey? {
        return key.toObjectKey()
    }

    override fun containsKey(insertKey: ReadKey): Boolean {
        return map.containsKey(insertKey.key)
    }

    override fun containsObject(key: ObjectKeyToWrite): Boolean {
        return map.containsKey(key.toObjectKey())
    }

    override operator fun get(insertKey: ReadKey): Any? {
        return map[insertKey.key]
    }

    override operator fun set(insertKey: ReadKey, any: Any) {
        map[insertKey.key] = any
    }
}

//internal interface KeyCreator {
//    val checkCacheOnly: Boolean
//    fun toObjectKey(table: Persister.Table<*>, key: ObjectKeyToWrite): ObjectKey
//}

internal fun List<Pair<Int, Any>>.find(key: ObjectKeyToWrite) = find { it.second == key.theObject() }

internal interface KeyGetter {
    fun keyForObject(table: Persister.Table<*>, key: ObjectKeyToWrite): ObjectKey? {
        return keyForObjectFromCache(table, key) ?: kotlin.run { keyForObjectFromDB(table, key) }
    }

    fun keyForObjectFromCache(table: Persister.Table<*>, key: ObjectKeyToWrite): ObjectKey?

    fun keyForObjectFromDB(table: Persister.Table<*>, key: ObjectKeyToWrite): ObjectKey?

}

internal class ReadCache constructor(val persisterPreference: PersisterPreference) : PersisterListener, KeyGetter {
    private val logger = KotlinLogging.logger {}
    private val map: MutableMap<Persister.InternalTable, ReadTableCache> = mutableMapOf()

    private fun readDatabase(table: Persister.Table<*>,
                             key: ObjectKeyToWrite,
                             readTableCache: ReadHashCodeTableCache,
                             hashCodeX: Int = key.itsHashCode()): Pair<Int, Any?> {
        val readHashCode = table.readHashCode(hashCodeX, this)
        readHashCode.forEach {
            readTableCache[ReadKey(table, key.toObjectKey(it.first))] = it.second
        }
        return readHashCode.find { it.second == key.theObject() } ?: (readHashCode.size to null)
    }

    override fun keyForObjectFromCache(table: Persister.Table<*>, key: ObjectKeyToWrite): ObjectKey? {
        return map[table]?.keyForObject(key)
    }

    override fun keyForObjectFromDB(table: Persister.Table<*>, key: ObjectKeyToWrite): ObjectKey? {
        return if (key.isAutoId()) {
            table.readHashCode(key.itsHashCode(), this)
                    .find(key)
                    ?.let { key.toObjectKey(it.first) }
        } else {
            key.toObjectKey()
        }
    }

    private fun read(table: Persister.Table<*>,
                     key: ObjectKeyToWrite,
                     readTableCache: ReadHashCodeTableCache,
                     hashCodeX: Int = key.itsHashCode()): Any? {
        return readDatabase(table, key, readTableCache, hashCodeX).second
    }

    private fun getCounter(table: Persister.Table<*>,
                           key: ObjectKeyToWrite,
                           readTableCache: ReadHashCodeTableCache,
                           hashCodeX: Int = key.itsHashCode()): Int {
        return readDatabase(table, key, readTableCache, hashCodeX).first
    }

//    fun <T : Any> toObjectKey(table: Persister.Table<T>, key: ObjectKeyToWrite, keyCreator: KeyCreator): ObjectKey? {
//        return if (key.isAutoId()) {
//            val hashCodeX = key.itsHashCode()
//            val readTableCache = readTableCache(table, true) as ReadHashCodeTableCache
//            if (readTableCache.containsHashCode(hashCodeX)) {
//                val objects = readTableCache.getHashCodeObjects(hashCodeX)!!
//                objects.withIndex()
//                        .find { it.value == key.theObject() }
//                        ?.index?.let { key.toObjectKey(it) }
//            } else {
//                if (!keyCreator.checkCacheOnly) {
//                    readDatabase(table, key, readTableCache, keyCreator, hashCodeX)
//                } else {
//                    null
//                }
//            }
//        } else {
//            key.toObjectKey()
//        }
//    }

    private fun getTableCache(table: Persister.Table<*>, key: IsAutoId): ReadTableCache {
        val tableCache = if (!map.containsKey(table)) {
            val tableCache = if (key.isAutoId())
                ReadHashCodeTableCache(table)
            else
                ReadTableCacheImpl(table)
            map[table] = tableCache
            tableCache
        } else {
            map[table]!!
        }
        return tableCache
    }

    fun read(table: Persister.Table<*>, key: ObjectKeyToWrite): Any? {
        val tableCache = getTableCache(table, key)
        if (!tableCache.containsObject(key)) {
            if (key.isAutoId()) {
                return read(table, key, tableCache as ReadHashCodeTableCache)
            } else {
                return table.innerReadMultipleUseHashCode(key.toObjectKey(), this)
            }
        } else {
            return key.theObject()
        }
    }

    fun <T : Any> read(table: Persister.Table<T>, key: ObjectKey): T? {
        val readCacheKey = ReadKey(table, key)
        val tableCache = getTableCache(table, key)
        if (!tableCache.containsKey(readCacheKey)) {
            val read = table.innerReadMultipleUseHashCode(key, this)
            if (read != null) {
//                if (map.size > persisterPreference.clearCacheAfterNumberOfStoredObjects) {
//                    logger.info { "clearing cache, because more than ${persisterPreference.clearCacheAfterNumberOfStoredObjects} are stored (${map.size}" }
//                    map.clear()
//                }
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
        val readCacheKey = ReadKey(table, key)
        return map[table]?.get(readCacheKey) != null
    }

    fun <T : Any> isInCache(table: Persister.Table<T>, key: ObjectKeyToWrite): Boolean {
        return map[table]?.containsObject(key) ?: false
    }

    fun readTableCache(table: Persister.InternalTable, isHashCodeTable: Boolean): ReadTableCache {
        if (!map.containsKey(table)) {
            val r = if (isHashCodeTable) ReadHashCodeTableCache(table) else ReadTableCacheImpl(table)
            map[table] = r
            return r
        }
        return map[table]!!
    }

//    fun <T : Any> readTableCache(table: Persister.Table<T>, isHashCodeTable: Boolean): ReadTableCache {
//        return readTableCache(table, isHashCodeTable)
//    }

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
}

//internal data class ReadMap(){
//    private val map: MutableMap<InsertKey, Any> = mutableMapOf()
//
//    fun read(key:Any)
//}
