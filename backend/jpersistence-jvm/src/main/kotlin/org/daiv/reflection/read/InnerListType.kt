package org.daiv.reflection.read

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.ReadCache

//internal class InnerListType<T : Any>(override val propertyData: PropertyData<Any, List<T>, T>,
//                                      override val prefix: String?,
//                                      val persisterProvider: PersisterProvider) :
//        SimpleCollectionFieldData<Any, List<T>, T, List<T>> {
//    override fun fNEqualsValue(o: List<T>, sep: String): String {
//        throw UnsupportedOperationException("no support yet")
//    }
//
//    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<ReadAnswer<List<T>>> {
//    }
//
//    override fun deleteLists(key: List<Any>) {
//    }
//
//    override fun clearLists() {
//    }
//
//    override fun createTableForeign(tableName: Set<String>): Set<String> {
//    }
//
//    override suspend fun toStoreData(insertMap: InsertMap, objectValue: List<Any>) {
//    }
//
//    override fun onIdField(idField: KeyType) {
//    }
//
//    override fun copyData(map: Map<String, String>, request: (String, String) -> String) {
//        // currently nothing
//    }
//
//    override fun dropHelper() {
//        // currently nothing
//    }
//
//}


internal class InnerMapType<R : Any, T : Any, M : Any> constructor(override val propertyData: RealMapProperty<R, T, M>,
                                                                   val depth: Int,
                                                                   val persisterProvider: PersisterProvider,
                                                                   override val prefix: String?) :
        SimpleCollectionFieldData<R, Map<M, T>, T, Map<M, T>> {

    private fun fieldName(name: String) = if (depth == 0) name else "$name$depth"
    private val keyField = propertyData.keyClazz.toLowField(persisterProvider, depth + 1, fieldName("key"))
    private val valueField = propertyData.subType.toLowField(persisterProvider, depth + 1, fieldName("value"))

    override fun additionalKeyFields(): List<FieldData<Any, Any, Any, Any>> {
        return listOf(keyField) + valueField.additionalKeyFields()
    }

    override fun toTableHead(nullable: Boolean): String? {
        return "${keyField.toTableHead(nullable)} , ${valueField.toTableHead(nullable)}"
    }

    override fun insertObject(o: T?): List<InsertObject> {
        throw RuntimeException("this must not be called")
    }

    override fun insertObjects(o: T): List<List<InsertObject>> {
        val x = o as Map<M, T>
        return x.flatMap { entry ->
            valueField.insertObjects(entry.value)
                    .map {
                        keyField.insertObject(entry.key) + it
                    }
        }
    }

    override fun fNEqualsValue(o: Map<M, T>, sep: String): String {
        return sequenceOf(o.values.map { valueField.fNEqualsValue(it, sep) },
                          o.keys.map { keyField.fNEqualsValue(it, sep) })
                .joinToString(", ")
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<ReadAnswer<Map<M, T>>> {
        val x = valueField.getValue(readCache, readValue, number, key)
        return NextSize(ReadAnswer(x.t.t), number) as NextSize<ReadAnswer<Map<M, T>>>
    }

    override fun deleteLists(key: List<Any>) {
    }

    override fun clearLists() {
    }

    override fun createTableForeign(tableName: Set<String>): Set<String> {
        return valueField.createTableForeign(keyField.createTableForeign(tableName))
    }

    override fun copyData(map: Map<String, String>, request: (String, String) -> String) {
    }

    override suspend fun toStoreData(insertMap: InsertMap, r: List<R>) {
        val o = r as List<Map<Any, Any>>
        r.forEach { x ->
            keyField.toStoreData(insertMap, x.map { it.key })
            valueField.toStoreData(insertMap, x.map { it.value })
        }
    }

    override fun onIdField(idField: KeyType) {
    }

    override fun dropHelper() {
    }

    override fun readFromList(list: List<ReadFieldValue>): Any {
        return list.first().value to valueField.readFromList(list.drop(1))
    }

    override fun buildPair(any: List<Any>): Any {
        val pairs = any as List<Pair<ReadFieldValue, Any>>
        return pairs.map { it.first to it.second }
                .toMap()
    }
}
