package org.daiv.reflection.persister

import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.insertHeadString
import org.daiv.reflection.read.insertValueString

internal data class InsertKey(val tableName: String, val key: List<Any>)

internal data class InsertRequest(val insertObjects: List<InsertObject>)

internal data class InsertMap constructor(val persister: Persister) {
    private val map: MutableMap<InsertKey, InsertRequest> = mutableMapOf()
    val readCache = ReadCache()

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

    fun insertAll() {
        val group = map.map { it }
                .groupBy { it.key.tableName }
        val res = group.map { it.key to it.value.map { it.value.insertObjects }.filter { it.isNotEmpty() } }
                .filter { !it.second.isEmpty() }
                .toMap()
        res.map { persister.write("INSERT INTO ${it.key} ${insertListBy(it.value)} ") }
    }
}

private data class ReadCacheKey(val tableName: String, val key: List<Any>)

class ReadCache {
    private val map: MutableMap<ReadCacheKey, Any> = mutableMapOf()

    fun <T : Any> read(table: Persister.Table<T>, key: List<Any>): T? {
        val readCacheKey = ReadCacheKey(table.tableName, key)
        val got = map[readCacheKey]
        return if (got == null) {
            val read = table.readMultipleUseHashCode(key, this)
            if (read != null) {
                map[readCacheKey] = read
            }
            read
        } else {
            got as T
        }
    }

    fun <T : Any> readNoNull(table: Persister.Table<T>, key: List<Any>): T {
        return read(table, key) ?: throw RuntimeException("did not find value for key $key")
    }
}

//internal data class ReadMap(){
//    private val map: MutableMap<InsertKey, Any> = mutableMapOf()
//
//    fun read(key:Any)
//}
