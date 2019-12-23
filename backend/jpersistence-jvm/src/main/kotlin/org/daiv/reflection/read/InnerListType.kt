package org.daiv.reflection.read

import org.daiv.reflection.annotations.IFaceForList
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.ObjectKey
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

internal class InnerSetType constructor(override val propertyData: SetProperty,
                                        val depth: Int,
                                        val persisterProvider: PersisterProvider,
                                        override val prefix: String?) : SimpleCollectionFieldData {

    private fun fieldName(name: String) = if (depth == 0) name else "$name$depth"
    private val valueField = propertyData.subType.toLowField(persisterProvider, depth + 1, fieldName("value"))


    override fun isAlsoKeyField() = true

    override fun copyTableName(): Map<String, String> {
        return valueField.copyTableName()
    }

    override fun additionalKeyFields(): List<FieldData> {
        return listOf(valueField) + valueField.additionalKeyFields()
    }

    override fun toTableHead(nullable: Boolean): String? {
        return valueField.toTableHead(nullable)
    }

    override fun insertObject(o: Any?): List<InsertObject> {
        throw RuntimeException("this must not be called")
    }

    override fun insertObjects(o: Any): List<List<InsertObject>> {
        o as Set<Any>
        return o.flatMap { entry -> valueField.insertObjects(entry) }
    }

    override fun fNEqualsValue(o: Any, sep: String): String {
        o as Set<Any>
        return sequenceOf(o.map { valueField.fNEqualsValue(it, sep) })
                .joinToString(", ")
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: ObjectKey): NextSize<ReadAnswer<Any>> {
        val x = valueField.getValue(readCache, readValue, number, key)
        return NextSize(ReadAnswer(x.t.t), number)
    }

    override fun deleteLists(key: List<Any>) {
    }

    override fun clearLists() {
    }

    override fun createTableForeign(tableName: Set<String>): Set<String> {
        return valueField.createTableForeign(tableName)
    }

    override fun copyData(map: Map<String, String>, request: (String, String) -> String) {
    }

    override suspend fun toStoreData(insertMap: InsertMap, r: List<Any>) {
        r.forEach { z ->
            z as Set<Any>
            valueField.toStoreData(insertMap, z.map { it })
        }
    }

    override fun onIdField(idField: KeyType) {
    }

    override fun dropHelper() {
    }

    override fun readFromList(list: List<List<ReadFieldValue>>): Any {
        val x = list.flatMap { it.map { it.value } }
        return x.toSet()
//        return valueField.readFromList(list)!!
//        val group = list.groupBy {
//            it.first()
//                    .value
//        }
//                .map { it.key to valueField.readFromList(it.value.map { it.drop(1) }) }
//                .toMap()
//        return backConverter(group)
    }
}


internal class InnerMapType constructor(override val propertyData: MapProperty,
                                        val depth: Int,
                                        val iFaceForList: IFaceForList?,
                                        val persisterProvider: PersisterProvider,
                                        override val prefix: String?,
                                        val converter: (Any) -> Map<Any, Any> = { it as Map<Any, Any> },
                                        val backConverter: (Map<Any?, Any?>) -> Any = { it }) :
        SimpleCollectionFieldData {

    private fun fieldName(name: String) = if (depth == 0) name else "$name$depth"
    private val keyField = propertyData.keyClazz.toLowField(persisterProvider, depth + 1, fieldName("key"))
    private val valueField = propertyData.subType.toLowField(persisterProvider, depth + 1, fieldName("value"), iFaceForList)


    override fun copyTableName(): Map<String, String> {
        return keyField.copyTableName() + valueField.copyTableName()
    }

    override fun additionalKeyFields(): List<FieldData> {
        return listOf(keyField) + valueField.additionalKeyFields()
    }

    override fun toTableHead(nullable: Boolean): String? {
        return "${keyField.toTableHead(nullable)} , ${valueField.toTableHead(nullable)}"
    }

    override fun insertObject(o: Any?): List<InsertObject> {
        throw RuntimeException("this must not be called")
    }

    override fun insertObjects(o: Any): List<List<InsertObject>> {
        val x = converter(o)
//        val x = o as Map<Any, Any>
        return x.flatMap { entry ->
            valueField.insertObjects(entry.value)
                    .map {
                        keyField.insertObject(entry.key) + it
                    }
        }
    }

    override fun fNEqualsValue(o: Any, sep: String): String {
        o as Map<Any, Any>
        return sequenceOf(o.values.map { valueField.fNEqualsValue(it, sep) },
                          o.keys.map { keyField.fNEqualsValue(it, sep) })
                .joinToString(sep)
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: ObjectKey): NextSize<ReadAnswer<Any>> {
        val x = valueField.getValue(readCache, readValue, number, key)
        return NextSize(ReadAnswer(x.t.t), number)
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

    override suspend fun toStoreData(insertMap: InsertMap, r: List<Any>) {
        r.forEach { z ->
            val x = converter(z)
            keyField.toStoreData(insertMap, x.map { it.key })
            valueField.toStoreData(insertMap, x.map { it.value })
        }
    }

    override fun onIdField(idField: KeyType) {
    }

    override fun dropHelper() {
    }

    override fun readFromList(list: List<List<ReadFieldValue>>): Any {
        val group = list.groupBy {
            it.first()
                    .value
        }
                .map { it.key to valueField.readFromList(it.value.map { it.drop(1) }) }
                .toMap()
        return backConverter(group)
    }
}
