package org.daiv.reflection.persister

import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.insertHeadString
import org.daiv.reflection.read.insertValueString

internal data class InsertKey(val tableName: String, val key: List<Any>)

internal data class InsertRequest(val insertObjects: List<InsertObject>)

internal data class InsertMap(val persister: Persister) {
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

    fun insertAll() {
        val group = map.map { it }
                .groupBy { it.key.tableName }
        val res = group.map { it.key to it.value.map { it.value.insertObjects }.filter { it.isNotEmpty() } }
                .filter { !it.second.isEmpty() }
                .toMap()
        res.map { persister.write("INSERT INTO ${it.key} ${insertListBy(it.value)} ") }
    }
}

//internal data class ReadMap(){
//    private val map: MutableMap<InsertKey, Any> = mutableMapOf()
//
//    fun read(key:Any)
//}
