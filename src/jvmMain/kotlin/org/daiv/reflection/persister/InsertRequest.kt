package org.daiv.reflection.persister

import mu.KotlinLogging
import org.daiv.reflection.common.TableHandlerCreator
import org.daiv.reflection.plain.*
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
data class InsertCachePreference(val checkCacheOnly: Boolean, val check4Same: Boolean)

data class InsertPartialAnswer(
    val objectsThatShouldHaveBeenWritten: List<Any>,
    val writtenSuccess: Boolean,
    val t: Throwable? = null
)

class DifferentObjectSameKeyException(message: String?, val objectFromCache: Any, val objectToInsert: Any) :
    RuntimeException(message)

internal data class InsertMap constructor(
    val persister: Persister,
    val insertCachePreference: InsertCachePreference,
    val actors: Map<Persister.InternalTable, TableHandler>,
    val readCache: ReadCache
) {
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
                    "databaseValue:       \n$this \n vs manyToOne Value: \n$objectValue"
            val msg = if (firstTryMsg.length > 1000) {
                "values of class ${this::class} are not fitting. Object is too big to print - key: $key"
            } else {
                firstTryMsg
            }
            throw RuntimeException(msg)
        }
        return objectValue
    }


    suspend fun <T : Any> nextTask(
        table: Persister.Table<T>,
        key: ObjectKeyToWrite,
        nextTask: suspend (ObjectKeyToWrite) -> Unit
    ) {
//        val objectKey = readCache.toObjectKey(table, key, this)
//        objectKey?.let {
        if (insertCachePreference.checkCacheOnly) {
            if (insertCachePreference.check4Same) {
                val read = readCache.readCacheOnly(table, key)
                val theObject = key.theObject()
                if (read != null && read != theObject) {
                    val errorMessage =
                        "object that is already in Cache \n$read \n is not same as to be insert: \n$theObject"
                    throw DifferentObjectSameKeyException(errorMessage, read, theObject)
                }
                if (read != null) {
                    return
                }
            } else if (readCache.isInCache(table, key)) {
                return
            }
//            if (readCache.isInCache(table, key)) {
//                return
//            }

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

    fun insertPartial(table: Persister.Table<*>, block: (Int, Any) -> Boolean): InsertPartialAnswer {
        val actor = actors[table]!!
        val map = actor.map.map {
            it.key.key.theObject() to it.value().map { it.insertObjects }
        }
            .filter { it.second.isNotEmpty() }
        val taken = map.filterIndexed { i, e -> block(i, e.first) }
        val s = "INSERT INTO `${table._tableName}` ${insertListBy(taken.map { it.second })} "
        val objects = taken.map { it.first }
        try {
            persister.write(s)
        } catch (t: Throwable) {
            return InsertPartialAnswer(objects, false, t)
        }
        return InsertPartialAnswer(objects, true)
    }

    fun insertAll() {
//        val map = mutableMapOf<InsertKey, InsertRequest>()
        val x = actors.map { it.key to it.value.map }
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
        val z = res.filter { it.second.isNotEmpty() }
            .map { "INSERT INTO ${it.first.tableName} ${insertListBy(it.second)} " }

//                .toMap()
        logger.trace { "done converting" }
        z.map {
            persister.write(it)
        }
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
    private val keyMap: MutableMap<PersistenceKey, Any> = mutableMapOf()

    override fun readWithOriginalKey(itsKey: List<Any>): Any? {
        return keyMap[PersistenceKey(itsKey)]
//        return map.values.find { it.values.find { it.hashCodeKey.keys == itsKey } }
    }

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
        keyMap[PersistenceKey(insertKey.key.keys())] = any
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
    fun readWithOriginalKey(itsKey: List<Any>): Any?
}

internal class ReadTableCacheImpl(val table: Persister.InternalTable) : ReadTableCache {
    private val map: MutableMap<ObjectKey, Any> = mutableMapOf()

    override fun readWithOriginalKey(itsKey: List<Any>): Any? {
        return map[PersistenceKey(itsKey)]
    }

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

internal class ReadCache() : PersisterListener, KeyGetter {
    private val logger = KotlinLogging.logger {}
    private val map: MutableMap<Persister.InternalTable, ReadTableCache> = mutableMapOf()

    fun tableHandlers(persisterProvider: TableHandlerCreator) = (persisterProvider.allTables().map {
        it.value to SequentialTableHandler(this.readTableCache(it.value, persisterProvider.isAutoIdTable(it.key)))
    } + persisterProvider.getHelperTableNames().map {
        it to SequentialTableHandler(this.readTableCache(it, false))
    })
        .toMap()

    private fun readDatabase(
        table: Persister.Table<*>,
        key: ObjectKeyToWrite,
        readTableCache: ReadHashCodeTableCache,
        hashCodeX: Int = key.itsHashCode()
    ): Pair<Int, Any?> {
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

    private fun read(
        table: Persister.Table<*>,
        key: ObjectKeyToWrite,
        readTableCache: ReadHashCodeTableCache,
        hashCodeX: Int = key.itsHashCode()
    ): Any? {
        return readDatabase(table, key, readTableCache, hashCodeX).second
    }

//    private fun getCounter(
//        table: Persister.Table<*>,
//        key: ObjectKeyToWrite,
//        readTableCache: ReadHashCodeTableCache,
//        hashCodeX: Int = key.itsHashCode()
//    ): Int {
//        return readDatabase(table, key, readTableCache, hashCodeX).first
//    }

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

    fun readCacheOnly(table: Persister.Table<*>, key: ObjectKeyToWrite): Any? {
        val tableCache = map[table] ?: return null
        return tableCache.readWithOriginalKey(key.itsKey())
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

    private fun <T : Any> read(table: Persister.Table<T>, key: ObjectKey): T? {
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
